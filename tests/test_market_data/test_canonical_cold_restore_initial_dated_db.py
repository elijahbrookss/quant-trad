from dataclasses import replace
from datetime import timedelta
import shutil
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES, COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_identity_schema import _assert_header_partition, fact_header_partition_name
from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from scripts.db import canonical_cold_activate as activation
from scripts.db.canonical_cold_audit import audit_candidate
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_identity_rebuild import prepare_identity_suffix
from scripts.db.canonical_cold_load import TargetDatabase, load_checkpoint
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_restore_initial_dated import capture_initial_dated_donor, restore_initial_dated_day
from tests.test_market_data.test_canonical_cold_activate_db import (
    _configure_fixture_catalog, _fence_connections,
)
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage


pytestmark = pytest.mark.db


@pytest.fixture
def initially_dated_rollback_clone(storage, tmp_path, monkeypatch, request):
    state = getattr(request, "param", "verified")
    engine = storage.database._engine
    day = storage.today - timedelta(days=2)
    end = day + timedelta(days=1)
    migrating_days = (day, end) if state == "empty" else (day,)
    cutoff = migrating_days[-1] + timedelta(days=1)
    _placement(monkeypatch, day)
    start, finish = BASE - timedelta(seconds=1), BASE + timedelta(seconds=1)
    selection = {"series_id": storage.series_id, "start": start, "end": finish}
    if state != "empty":
        _ingest(storage)
        frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(**selection)])
        correction = replace(storage.fact, payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"})
        _ingest(storage, correction)
    else:
        from portal.backend.db.fact_storage_schema import ensure_fact_payload_partition

        with engine.begin() as conn:
            ensure_fact_payload_partition(conn, day)
        _placement(monkeypatch, end)
        _ingest(storage)  # The actual cutover requires a nonempty total retired prefix.
        frozen = None
    expected_frozen = (storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
                       if frozen is not None else [])
    expected_latest = storage.repo.read_facts(**selection)
    old_store = FilesystemRawArchiveObjectStore(tmp_path / "source-archive")
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=old_store, temporary_directory=tmp_path / "source-stage",
    )
    for old_day in migrating_days:
        archive.seal_partition(old_day)
    if state not in {"sealed", "empty"}:
        assert archive.stage_next_page(day)["status"] == "page_acknowledged"
        assert archive.verify_next_page(day)["status"] == "page_verified"
        assert archive.verify_partition(day)["status"] == "partition_verified"
    if state == "reclaimed":
        reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
        assert reclaimer.reclaim_partition(day, eligible_before=end, execute=True)["status"] == "partition_reclaimed"

    with engine.begin() as conn:
        cluster = conn.scalar(text("SELECT system_identifier::text FROM pg_control_system()"))
        source = PostgresMigrationSource(engine, expected_system_identifier=cluster)
        target_identity = source.identity(conn)
        source_commit = storage.repo._current_commit_seq_with_session(conn)
    donor_name = f"qt_bench_dated_{uuid4().hex[:12]}"
    donor_engine = create_engine(engine.url.set(database=donor_name))
    admin = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    donor_root = tmp_path / "donor-archive"
    shutil.copytree(old_store.root, donor_root)
    donor_store = FilesystemRawArchiveObjectStore(donor_root, writable=False)
    candidate_store = FilesystemRawArchiveObjectStore(tmp_path / "candidate-archive" / "objects")
    token = f"{cluster}/{target_identity['database_oid']}"
    try:
        engine.dispose()
        with admin.connect() as conn:
            template = conn.dialect.identifier_preparer.quote_identifier(target_identity["database"])
            conn.exec_driver_sql(f"CREATE DATABASE {donor_name} TEMPLATE {template}")
        with engine.begin() as conn:
            _configure_fixture_catalog(conn, token, candidate_store.root)
            custody_tablespace = conn.scalar(text("SELECT dattablespace::bigint FROM pg_database WHERE datname=current_database()"))
            custody_oids = {table: conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                {"name": f"market.{table}"}) for table in COLD_CUSTODY_TABLE_NAMES}
            payload_children = {} if state == "reclaimed" else {
                name: conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": f"market.{name}"})
                for name in map(fact_partition_name, migrating_days)
            }
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            prepare_identity_suffix(conn, expected_database=token, cutoff=cutoff)
        target = TargetDatabase(cluster, target_identity["database"], target_identity["database_oid"])
        checkpoint = MigrationCheckpoint(tmp_path / "dated-checkpoint.sqlite")
        audit_scope = {"target": target, "object_store": candidate_store, "cutoff": cutoff}
        for old_day in migrating_days:
            assert stage(source, checkpoint, day=old_day, source_store=old_store, destination_store=candidate_store,
                         temporary_directory=tmp_path / "staging")["cohort_complete"]
            assert load_checkpoint(engine, checkpoint, target=target, object_store=candidate_store,
                                   day=old_day)["checkpoint_end_reached"]
        manifests = [audit_candidate(engine, checkpoint, day=old_day, **audit_scope) for old_day in migrating_days]
        forward = manifests[0]
        receipts = [audit_candidate(engine, checkpoint, **audit_scope), *manifests,
                    audit_candidate(engine, checkpoint, inventory=True, **audit_scope)]
        audit = validate_audit_receipts(receipts)
        donor = PostgresMigrationSource(donor_engine, expected_system_identifier=cluster)
        with donor.snapshot() as conn:
            donor_identity = donor.identity(conn)
            donor_scope = capture_initial_dated_donor(conn, donor_identity=donor_identity, day=day, audit=audit, source_commit=source_commit)
        execution = {"schema": activation.ACTIVATION_VERSION,
                     "evidence": {gate: "e" * 64 for gate in activation._PHYSICAL_GATES},
                     "approved_package_hash": "a" * 64, "target_database": target_identity,
                     "source_database": target_identity, "audit_set_hash": audit["audit_set_hash"],
                     "source_commit_seq": source_commit, "loaded_inventory_hash": audit["loaded_inventory_hash"],
                     "payload_children": payload_children, "custody_target_id": "activation-fixture",
                     "custody_tablespace_oid": custody_tablespace, "custody_relation_oids": custody_oids}
        outcome = {"execution": execution, "expected_execution_hash": archive_evidence_hash(execution)}
        with _fence_connections(storage) as (conn, control):
            with conn.begin():
                verified = activation.verify_mounted_candidate(conn, audit=audit, store=candidate_store,
                                                               limits=ArchiveVerificationLimits())
            fence = close_database_fence(conn, control, expected_database=target_identity, timeout_seconds=60,
                                         record=lambda *_: None)
            with conn.begin():
                certificate = activation.activate_cold_storage(conn, **outcome, receipts=receipts,
                                                              verified_objects=verified, database_fence=fence)
            reopen_database_fence(fence, control, expected_outcome="committed", record=lambda *_: None,
                                  read_outcome=lambda db: activation.read_activation_outcome(db, **outcome))
        yield SimpleNamespace(
            storage=storage, source=source, donor=donor, target_identity=target_identity, donor_identity=donor_identity,
            donor_scope=donor_scope, audit=audit, forward_receipts=[forward], candidate_store=candidate_store,
            donor_store=donor_store, source_commit=source_commit, dated_days=(day,), end_day=end,
            frozen=frozen, expected_frozen=expected_frozen, expected_latest=expected_latest, selection=selection,
            checkpoint=checkpoint, state="sealed" if state == "empty" else state,
            expected_rows=0 if state == "empty" else 2, certificate=certificate, activation_outcome=outcome,
        )
    finally:
        donor_engine.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {donor_name} WITH (FORCE)")
        admin.dispose()


@pytest.mark.parametrize("initially_dated_rollback_clone", ["sealed", "verified", "reclaimed", "empty"], indirect=True)
def test_restore_initial_dated_day_retains_routing_and_original_payload_placement(initially_dated_rollback_clone):
    scope = initially_dated_rollback_clone
    day = scope.dated_days[0]
    header, payload = fact_header_partition_name(day), fact_partition_name(day)
    original = scope.donor_scope
    with _fence_connections(scope.storage) as (conn, control), scope.donor.snapshot() as donor:
        fence = close_database_fence(conn, control, expected_database=scope.target_identity, timeout_seconds=60,
                                     record=lambda *_: None)
        args = dict(fence=fence, original=original, expected_original_hash=original["receipt_sha256"],
                    audit=scope.audit, forward_receipts=scope.forward_receipts, store=scope.candidate_store,
                    donor_store=scope.donor_store, max_copy_bytes=2 * 1024**2, max_seconds=30)
        if scope.expected_rows:
            with pytest.raises(RuntimeError, match="copy_budget_exceeded"):
                with conn.begin():
                    restore_initial_dated_day(conn, donor, **(args | {"max_copy_bytes": 1}))
        with conn.begin():
            assert conn.scalar(text("SELECT to_regclass(:h) IS NULL AND to_regclass(:p) IS NULL"),
                               {"h": f"market.{header}", "p": f"market.{payload}"})
            report = restore_initial_dated_day(conn, donor, **args)
            assert report["header_oid"] != report["original_header_oid"]
            assert report["row_count"] == scope.expected_rows
            assert (report["copy_bytes"] > 0) == bool(scope.expected_rows)
            assert report["registered_at"] == original["registered_at"]
            assert report["retention_state"] == scope.state
            assert report["retention_sha256"] == archive_evidence_hash(original["retention"])
            assert (report["payload_oid"] is None) is (scope.state == "reclaimed")
            if report["payload_oid"] is not None:
                assert report["payload_oid"] != report["original_payload_oid"]
            _assert_header_partition(conn, day)
            assert conn.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == COLD_HISTORY_LAYOUT_VERSION
            assert conn.scalar(text("SELECT count(*) FROM market.fact_identities WHERE storage_day=:day"),
                               {"day": day}) == scope.expected_rows
            assert conn.scalar(text("SELECT NOT datallowconn FROM pg_database WHERE datname=current_database()"))
        with conn.begin():
            assert restore_initial_dated_day(conn, donor, **args) == report
            if scope.expected_rows:
                with pytest.raises(DBAPIError, match="immutable market-data relation"):
                    with conn.begin_nested():
                        conn.exec_driver_sql(f"UPDATE market.{header} SET row_hash=row_hash")
