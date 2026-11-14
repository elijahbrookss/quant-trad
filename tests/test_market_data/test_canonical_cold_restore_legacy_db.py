from dataclasses import replace
from datetime import timedelta
import os
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
from portal.backend.db.fact_header_legacy_schema import assert_fact_header_legacy_contract
from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from scripts.db import canonical_cold_activate as activation
from scripts.db.canonical_cold_audit import audit_candidate
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_identity_rebuild import prepare_identity_suffix
from scripts.db.canonical_cold_load import TargetDatabase, load_checkpoint
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from scripts.db.canonical_cold_raw_index import inspect_raw_mapping_index_member
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_restore_legacy import measure_legacy_donor, restore_legacy_headers
from tests.test_market_data.test_canonical_cold_activate_db import (
    _attach_legacy_child, _configure_fixture_catalog, _create_retained_parent, _fence_connections, _stub_raw_index_case,
)
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage


pytestmark = pytest.mark.db


@pytest.fixture
def legacy_rollback_clone(storage, tmp_path, monkeypatch, request):
    case = getattr(request, "param", "verified")
    native_raw_index = case == "verified-native-raw-index"
    state = "verified" if native_raw_index else case
    tablespace_path = os.environ.get("QT_ROLLBACK_RAW_INDEX_TABLESPACE_DIRECTORY") if native_raw_index else None
    if native_raw_index and not tablespace_path:
        pytest.skip("set QT_ROLLBACK_RAW_INDEX_TABLESPACE_DIRECTORY to an empty fixture PostgreSQL server directory")
    engine = storage.database._engine
    if native_raw_index and (engine.url.host not in (None, "127.0.0.1", "::1", "timescaledb")
                             or ("host" in engine.url.query and not str(engine.url.query["host"]).startswith("/"))):
        raise ValueError("native rollback raw index test requires a local PostgreSQL fixture")
    day = storage.today - timedelta(days=2)
    end = day + timedelta(days=1)
    _placement(monkeypatch, day)
    _ingest(storage)
    start, finish = BASE - timedelta(seconds=1), BASE + timedelta(seconds=1)
    selection = {"series_id": storage.series_id, "start": start, "end": finish}
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(**selection)])
    correction = replace(storage.fact, payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"})
    _ingest(storage, correction)
    expected_frozen = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    expected_latest = storage.repo.read_facts(**selection)
    old_store = FilesystemRawArchiveObjectStore(tmp_path / "source-archive")
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=old_store, temporary_directory=tmp_path / "source-stage",
    )
    archive.seal_partition(day)
    if state != "sealed":
        assert archive.stage_next_page(day)["status"] == "page_acknowledged"
        assert archive.verify_next_page(day)["status"] == "page_verified"
        assert archive.verify_partition(day)["status"] == "partition_verified"
    if state == "reclaimed":
        reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
        assert reclaimer.reclaim_partition(day, eligible_before=end, execute=True)["status"] == "partition_reclaimed"

    with engine.begin() as conn:
        _attach_legacy_child(conn, day, end)
        _create_retained_parent(conn)
        cluster = conn.scalar(text("SELECT system_identifier::text FROM pg_control_system()"))
        source = PostgresMigrationSource(engine, expected_system_identifier=cluster)
        target_identity = source.identity(conn)
        source_commit = storage.repo._current_commit_seq_with_session(conn)
    donor_name = f"qt_bench_legacy_{uuid4().hex[:12]}"
    donor_engine = create_engine(engine.url.set(database=donor_name))
    admin = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    donor_root = tmp_path / "donor-archive"
    shutil.copytree(old_store.root, donor_root)
    donor_store = FilesystemRawArchiveObjectStore(donor_root, writable=False)
    candidate_store = FilesystemRawArchiveObjectStore(tmp_path / "candidate-archive" / "objects")
    token = f"{cluster}/{target_identity['database_oid']}"
    raw_tablespace = f"qt_rollback_raw_{uuid4().hex[:12]}" if native_raw_index else None
    try:
        raw_scope = None
        if native_raw_index:
            with admin.connect() as conn:
                conn.execute(text(f"CREATE TABLESPACE {raw_tablespace} LOCATION :path"), {"path": tablespace_path})
            with engine.begin() as conn:
                conn.exec_driver_sql(f"ALTER TABLE market.raw_archive_record_mappings SET TABLESPACE {raw_tablespace}")
                member = inspect_raw_mapping_index_member(conn)
                raw_scope = {"target_id": "rollback-fixture-history", "tablespace_oid": member["table_tablespace_oid"],
                             "table_oid": member["table_oid"], "index_oid": member["index_oid"],
                             "max_copy_bytes": 2 * 1024**2, "timeout_seconds": 15}
        engine.dispose()
        with admin.connect() as conn:
            template = conn.dialect.identifier_preparer.quote_identifier(target_identity["database"])
            conn.exec_driver_sql(f"CREATE DATABASE {donor_name} TEMPLATE {template}")
        with engine.begin() as conn:
            _configure_fixture_catalog(conn, token, candidate_store.root)
            if raw_scope is not None:
                conn.execute(text("""
                    INSERT INTO public.portal_storage_targets(id,label,filesystem_uuid,root,medium,roles)
                    VALUES ('rollback-fixture-history','Rollback fixture history','rollback-fixture-device',
                            :root,'hdd','["history"]')
                """), {"root": tablespace_path})
                conn.execute(text("""
                    INSERT INTO public.portal_storage_header_tablespaces(database_identity,target_id,tablespace_oid,binding)
                    VALUES (:identity,'rollback-fixture-history',:oid,'{}')
                """), {"identity": token, "oid": raw_scope["tablespace_oid"]})
            custody_tablespace = conn.scalar(text("SELECT dattablespace::bigint FROM pg_database WHERE datname=current_database()"))
            custody_oids = {table: conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                {"name": f"market.{table}"}) for table in COLD_CUSTODY_TABLE_NAMES}
            payload = fact_partition_name(day)
            payload_children = {} if state == "reclaimed" else {payload: conn.scalar(text("""
                SELECT to_regclass(:name)::oid::bigint
            """), {"name": f"market.{payload}"})}
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            prepare_identity_suffix(conn, expected_database=token, cutoff=end)
        target = TargetDatabase(cluster, target_identity["database"], target_identity["database_oid"])
        checkpoint = MigrationCheckpoint(tmp_path / "legacy-checkpoint.sqlite")
        assert stage(source, checkpoint, day=day, source_store=old_store, destination_store=candidate_store,
                     temporary_directory=tmp_path / "staging")["cohort_complete"]
        audit_scope = {"target": target, "object_store": candidate_store, "cutoff": end}
        assert load_checkpoint(engine, checkpoint, target=target, object_store=candidate_store,
                               day=day)["checkpoint_end_reached"]
        forward = audit_candidate(engine, checkpoint, day=day, **audit_scope)
        receipts = [audit_candidate(engine, checkpoint, **audit_scope), forward,
                    audit_candidate(engine, checkpoint, inventory=True, **audit_scope)]
        audit = validate_audit_receipts(receipts)
        donor = PostgresMigrationSource(donor_engine, expected_system_identifier=cluster)
        with donor.snapshot() as conn:
            donor_identity = donor.identity(conn)
            donor_scope = measure_legacy_donor(conn, donor_identity=donor_identity, audit=audit, source_commit=source_commit)
        execution = {"schema": activation.ACTIVATION_VERSION,
                     "evidence": {gate: "e" * 64 for gate in activation._PHYSICAL_GATES},
                     "approved_package_hash": "a" * 64, "target_database": target_identity,
                     "source_database": target_identity, "audit_set_hash": audit["audit_set_hash"],
                     "source_commit_seq": source_commit, "loaded_inventory_hash": audit["loaded_inventory_hash"],
                     "payload_children": payload_children, "custody_target_id": "activation-fixture",
                     "custody_tablespace_oid": custody_tablespace, "custody_relation_oids": custody_oids}
        execution["raw_mapping_index"] = raw_scope if native_raw_index else _stub_raw_index_case(monkeypatch)
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
            donor_store=donor_store, source_commit=source_commit, legacy_days=(day,), end_day=end,
            frozen=frozen, expected_frozen=expected_frozen, expected_latest=expected_latest, selection=selection,
            checkpoint=checkpoint, state=state, certificate=certificate, activation_outcome=outcome,
        )
    finally:
        donor_engine.dispose()
        if raw_tablespace is not None:
            with engine.begin() as conn:
                conn.exec_driver_sql("ALTER TABLE market.raw_archive_record_mappings SET TABLESPACE pg_default")
                conn.exec_driver_sql("ALTER INDEX market.pk_market_raw_archive_record_mapping SET TABLESPACE pg_default")
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {donor_name} WITH (FORCE)")
            if raw_tablespace is not None:
                conn.exec_driver_sql(f"DROP TABLESPACE IF EXISTS {raw_tablespace}")
        admin.dispose()


@pytest.mark.parametrize("legacy_rollback_clone", ["sealed", "verified", "reclaimed"], indirect=True)
def test_restore_exact_legacy_headers_with_new_oid_keeps_cold_clone_fenced(legacy_rollback_clone):
    scope = legacy_rollback_clone
    with _fence_connections(scope.storage) as (conn, control), scope.donor.snapshot() as donor:
        fence = close_database_fence(conn, control, expected_database=scope.target_identity, timeout_seconds=60,
                                     record=lambda *_: None)
        args = dict(fence=fence, donor_scope=scope.donor_scope, expected_donor_hash=scope.donor_scope["scope_sha256"],
                    audit=scope.audit, forward_receipts=scope.forward_receipts, store=scope.candidate_store,
                    donor_store=scope.donor_store, max_copy_bytes=2 * 1024**2, max_seconds=30)
        with pytest.raises(RuntimeError, match="copy_budget_exceeded"):
            with conn.begin():
                restore_legacy_headers(conn, donor, **(args | {"max_copy_bytes": 1}))
        with pytest.raises(RuntimeError, match="extra_identity"):
            with conn.begin():
                conn.execute(text("ALTER TABLE market.fact_identities DISABLE TRIGGER trg_require_fact_identity_header"))
                conn.execute(text("""
                    INSERT INTO market.fact_identities(id,storage_day,series_id,observation_key,revision)
                    VALUES ('orphan',:day,:series,'unrelated-pinned-fixture',1)
                """), {"day": scope.legacy_days[0], "series": scope.storage.series_id})
                conn.execute(text("ALTER TABLE market.fact_identities ENABLE ALWAYS TRIGGER trg_require_fact_identity_header"))
                restore_legacy_headers(conn, donor, **args)
        with conn.begin():
            assert conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NULL"))
            assert conn.scalar(text("SELECT to_regnamespace('qt_cold_legacy_restore') IS NULL"))
            result = restore_legacy_headers(conn, donor, **args)
            assert result["legacy_oid"] != result["original_oid"]
            assert result["rows"] == 2 and result["copy_bytes"] > 0
            assert assert_fact_header_legacy_contract(conn) == scope.end_day
            assert conn.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == COLD_HISTORY_LAYOUT_VERSION
            assert conn.scalar(text("SELECT state FROM market.fact_retention_partitions WHERE storage_day=:day"),
                               {"day": scope.legacy_days[0]}) == scope.state
            assert conn.scalar(text("SELECT count(*) FROM market.fact_identities")) == 2
            assert conn.scalar(text("SELECT NOT datallowconn FROM pg_database WHERE datname=current_database()"))
        with conn.begin():
            assert restore_legacy_headers(conn, donor, **args) == result
            for mutation in ("UPDATE market.fact_versions_legacy SET row_hash=row_hash", "TRUNCATE market.fact_versions_legacy"):
                with pytest.raises(DBAPIError, match="fact_header_legacy_sealed|immutable market-data relation"):
                    with conn.begin_nested():
                        conn.exec_driver_sql(mutation)
