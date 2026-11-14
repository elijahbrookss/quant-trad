from datetime import timedelta
import shutil
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import ArchiveChecksumMismatch, archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES
from portal.backend.db.fact_identity_schema import fact_header_partition_name
from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from scripts.db import canonical_cold_activate as activation
from scripts.db.canonical_cold_audit import audit_candidate
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_identity_rebuild import prepare_identity_suffix
from scripts.db.canonical_cold_load import TargetDatabase, load_checkpoint
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from scripts.db.canonical_cold_rollback_cohort import inspect_old_cohort, verify_restored_cohort
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog, _fence_connections
from tests.test_market_data.test_fact_reclamation_db import _prepare
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage  # noqa: F401


pytestmark = pytest.mark.db


@pytest.mark.parametrize("state", ["sealed", "verified", "reclaimed"])
def test_restored_old_cohort_preserves_exact_original_placement_and_cold_page_inverse(storage, tmp_path, monkeypatch, state):
    if state == "sealed":
        day = storage.today - timedelta(days=2)
        _placement(monkeypatch, day)
        _ingest(storage)
        archive = PostgresCanonicalFactArchiveRepository(
            database=storage.database, object_store=FilesystemRawArchiveObjectStore(tmp_path / "source"),
            temporary_directory=tmp_path / "source-stage",
        )
        archive.seal_partition(day)
    else:
        day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch)
    if state == "reclaimed":
        assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"
    window = {"series_id": storage.series_id, "start": BASE - timedelta(seconds=5), "end": BASE + timedelta(seconds=5)}
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(**window)])
    expected_latest = storage.repo.read_facts(**window)
    expected_frozen = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    engine = storage.database._engine
    with engine.begin() as conn:
        cluster, name, oid = conn.execute(text("""
            SELECT control.system_identifier::text,database.datname,database.oid::bigint
            FROM pg_control_system() AS control CROSS JOIN pg_database AS database WHERE database.datname=current_database()
        """)).one()
        source = PostgresMigrationSource(engine, expected_system_identifier=cluster)
        original_identity = source.identity(conn)
        scope = source.cohort(conn, day)
        header = fact_header_partition_name(day)
        header_oid = conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": "market." + header})
        placement = inspect_old_cohort(conn, day=day, header=header, header_oid=header_oid, state=scope.state)

    # An isolated PostgreSQL template copy supplies a separately addressed
    # old layout with the original table and index identifiers. The separately
    # copied archive exercises native v2 reclaimed reads independently.
    clone_name = f"qt_bench_rollback_{uuid4().hex[:12]}"
    clone_engine = create_engine(engine.url.set(database=clone_name))
    admin = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    restore_root = tmp_path / "restored"
    shutil.copytree(archive.object_store.root, restore_root)
    restored_store = FilesystemRawArchiveObjectStore(restore_root, writable=False)
    candidate_store = FilesystemRawArchiveObjectStore(tmp_path / "candidate")
    candidate = TargetDatabase(cluster, name, oid)
    try:
        engine.dispose()
        with admin.connect() as conn:
            source_name = conn.dialect.identifier_preparer.quote_identifier(name)
            conn.exec_driver_sql(f"CREATE DATABASE {clone_name} TEMPLATE {source_name}")
        clone = PostgresMigrationSource(clone_engine, expected_system_identifier=cluster)
        with clone.snapshot() as conn:
            restore_identity = clone.identity(conn)
        with engine.begin() as conn:
            _configure_fixture_catalog(conn, f"{cluster}/{oid}", candidate_store.root)
            commit = storage.repo._current_commit_seq_with_session(conn)
            custody_tablespace = conn.scalar(text("SELECT dattablespace::bigint FROM pg_database WHERE datname=current_database()"))
            custody_oids = {table: conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                {"name": f"market.{table}"}) for table in COLD_CUSTODY_TABLE_NAMES}
            payload = fact_partition_name(day)
            payload_children = {} if state == "reclaimed" else {payload: conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                                                         {"name": f"market.{payload}"})}
        checkpoint = MigrationCheckpoint(tmp_path / "rollback-checkpoint.sqlite")
        assert stage(source, checkpoint, day=day, source_store=archive.object_store, destination_store=candidate_store,
                     temporary_directory=tmp_path / "candidate-stage")["cohort_complete"]
        assert load_checkpoint(engine, checkpoint, target=candidate, object_store=candidate_store,
                               day=day)["checkpoint_end_reached"]
        audit_scope = {"target": candidate, "object_store": candidate_store, "cutoff": day + timedelta(days=1)}
        forward = audit_candidate(engine, checkpoint, day=day, **audit_scope)
        receipts = [audit_candidate(engine, checkpoint, **audit_scope), forward,
                    audit_candidate(engine, checkpoint, inventory=True, **audit_scope)]
        audit = validate_audit_receipts(receipts)
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            prepare_identity_suffix(conn, expected_database=f"{cluster}/{oid}", cutoff=audit_scope["cutoff"])
        execution = {
            "schema": activation.ACTIVATION_VERSION, "evidence": {gate: "e" * 64 for gate in activation._PHYSICAL_GATES},
            "approved_package_hash": "a" * 64, "target_database": original_identity,
            "source_database": original_identity, "audit_set_hash": audit["audit_set_hash"],
            "source_commit_seq": commit, "loaded_inventory_hash": audit["loaded_inventory_hash"],
            "payload_children": payload_children, "custody_target_id": "activation-fixture",
            "custody_tablespace_oid": custody_tablespace, "custody_relation_oids": custody_oids,
        }
        outcome = {"execution": execution, "expected_execution_hash": archive_evidence_hash(execution)}
        events = []

        def record(phase, _evidence):
            events.append(phase)

        with _fence_connections(storage) as (conn, control):
            with conn.begin():
                witnesses = activation.verify_mounted_candidate(conn, audit=audit, store=candidate_store,
                                                                limits=ArchiveVerificationLimits())
            fence = close_database_fence(conn, control, expected_database=original_identity,
                                         timeout_seconds=60, record=record)
            with conn.begin():
                activation.activate_cold_storage(conn, **outcome, receipts=receipts, verified_objects=witnesses,
                                                database_fence=fence)
            reopen_database_fence(fence, control, expected_outcome="committed", record=record,
                                  read_outcome=lambda db: activation.read_activation_outcome(db, **outcome))
        assert events[-1] == "database_reopened"

        def compare(**overrides):
            args = dict(day=day, original_identity=restore_identity, candidate_identity=original_identity,
                        audit=audit, forward_receipts=[forward], source_commit=commit,
                        old_store=restored_store, candidate_store=candidate_store,
                        expected_placement_hash=placement["placement_hash"])
            with clone.snapshot() as old, source.snapshot() as new:
                return verify_restored_cohort(old, new, **(args | overrides))

        result = compare()
        assert result["old_placement"]["state"] == state
        assert result["verified_rows"] == scope.expected_rows and result["patched_rows"] == 0
        assert result["verified_pages"] == 1 and result == compare()
        assert result["verified_archive_bytes"] > 0
        with pytest.raises(RuntimeError, match="archive_byte_budget_exceeded"):
            compare(max_archive_bytes=1)
        with pytest.raises(RuntimeError, match="old_placement_changed"):
            compare(expected_placement_hash="0" * 64)
        pages = checkpoint.iter_completed_pages(day=day)
        try:
            page = next(pages).manifest
        finally:
            pages.close()
        path = (candidate_store if state == "sealed" else restored_store).local_path(page.object_key)
        original_bytes = path.read_bytes()
        path.write_bytes(original_bytes + b"invalid")
        with pytest.raises(ArchiveChecksumMismatch, match="canonical_archive_checksum_mismatch"):
            compare()
        path.write_bytes(original_bytes)
        if state != "sealed":
            path.unlink()
            path.hardlink_to(candidate_store.local_path(page.object_key))
            with pytest.raises(RuntimeError, match="archive_copies_share_file_identity"):
                compare()
            path.unlink()
            path.write_bytes(original_bytes)
        restored_database = Database(clone_engine.url.render_as_string(hide_password=False))
        try:
            assert restored_database.ensure_schema(), str(restored_database.last_error)
            with monkeypatch.context() as patch:
                patch.setattr(market_data, "db", restored_database)
                patch.setattr(market_data, "canonical_fact_storage_repository", PostgresCanonicalFactStorageRepository(
                    object_store_factory=lambda: restored_store, read_cache_factory=lambda: None,
                ))
                old_runtime = market_data.PostgresMarketDataRepository()
                assert old_runtime.read_facts(**window) == expected_latest
                assert old_runtime.read_dataset_fact_revisions(
                    dataset_id=frozen.dataset_id, series_id=storage.series_id,
                ) == expected_frozen
        finally:
            restored_database._reset_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO market.cold_fact_pages
                    (id,object_key,object_sha256,manifest_hash,row_count,byte_count,first_commit_seq,last_commit_seq,descriptor)
                SELECT 'extra_initial_page','fixture/extra',object_sha256,manifest_hash,row_count,byte_count,
                       first_commit_seq,last_commit_seq,descriptor FROM market.cold_fact_pages WHERE id=:id
            """), {"id": page.manifest_id})
        with pytest.raises(RuntimeError, match="candidate_inventory_changed"):
            compare()
    finally:
        clone_engine.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {clone_name} WITH (FORCE)")
        admin.dispose()
