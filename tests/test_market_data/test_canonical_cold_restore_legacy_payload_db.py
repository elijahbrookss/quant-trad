"""Exact native payload restoration with a disposable read-only donor."""

from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import create_engine, text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, fact_partition_name
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_migration import PostgresMigrationSource
from scripts.db.canonical_cold_restore_legacy import VERSION as LEGACY_HEADER_VERSION
from scripts.db.canonical_cold_restore_legacy_payload import capture_original_payload_day, restore_legacy_payload_day
from tests.test_market_data.exact_deployed_reader import check_deployed_source_reader
from tests.test_market_data.migration_test_support import fresh_migration_database
from tests.test_market_data.test_canonical_cold_activate_db import (
    _attach_legacy_child, _configure_fixture_catalog, _fence_connections,
)
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage  # noqa: F401


pytestmark = pytest.mark.db
BUDGET = {"max_rows": 100, "max_bytes": 16*1024**2}


def _identities(conn):
    system = conn.scalar(text("SELECT system_identifier::text FROM pg_control_system()"))
    database = PostgresMigrationSource(conn.engine, expected_system_identifier=system).identity(conn)
    relation = conn.scalar(text("SELECT 'market.fact_versions_legacy'::regclass::oid::bigint"))
    return database, relation


@pytest.mark.parametrize("state", ["sealed", "verified", "reclaimed"])
def test_recover_exact_original_legacy_payload_day(storage, tmp_path, monkeypatch, state):
    day, end_day = storage.today - timedelta(days=2), storage.today - timedelta(days=1)
    _placement(monkeypatch, day)
    _ingest(storage)
    correction = replace(storage.fact, accepted_at=BASE + timedelta(seconds=1), known_at=BASE + timedelta(seconds=1),
                         payload={**storage.fact.payload, "rate": "0.25", "raw_rate": "0.25"})
    _ingest(storage, correction)
    selection = {"series_id": storage.series_id, "start": BASE - timedelta(seconds=1), "end": BASE + timedelta(seconds=1)}
    expected = storage.repo.read_fact_revisions(**selection)
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(**selection)])
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", PostgresCanonicalFactStorageRepository(
        object_store_factory=lambda: store, read_cache_factory=lambda: None,
    ))
    archive = PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=store,
                                                      temporary_directory=tmp_path / "staging")
    if state == "sealed":
        archive.seal_partition(day)
    else:
        _archive_day(archive, day)
        if state == "reclaimed":
            reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
            assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"
    engine = storage.database._engine
    scope_commit = storage.repo.current_commit_seq()
    relation = f'market."{fact_partition_name(day)}"'
    with engine.begin() as conn:
        _attach_legacy_child(conn, day, end_day)
        original_identity, original_oid = _identities(conn)
        certificate = conn.scalar(text("SELECT evidence FROM market.fact_storage_state"))
    with fresh_migration_database("payload_donor", install_extensions=False) as donor_dsn:
        # Clone the entire original fixture SQL before stripping the target
        # payload. PostgreSQL template cloning preserves actual heap OIDs,
        # immutable relations and every column of the old retention marker.
        donor_engine = create_engine(donor_dsn)
        admin = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
        engine.dispose()
        try:
            with admin.connect() as conn:
                quote = conn.dialect.identifier_preparer.quote_identifier
                conn.exec_driver_sql(f"DROP DATABASE {quote(donor_engine.url.database)}")
                conn.exec_driver_sql(f"CREATE DATABASE {quote(donor_engine.url.database)} "
                                     f"TEMPLATE {quote(engine.url.database)}")
            with donor_engine.connect().execution_options(isolation_level="REPEATABLE READ") as donor, donor.begin():
                donor.exec_driver_sql("SET TRANSACTION READ ONLY")
                donor_identity, donor_oid = _identities(donor)
                assert donor_oid == original_oid
                original = capture_original_payload_day(
                    donor, donor_identity=donor_identity, day=day, expected_header_oid=donor_oid,
                    expected_source_commit_seq=scope_commit, **BUDGET,
                )
                assert original["retention"]["state"] == state and original["row_count"] == 2
                audit = "e" * 64
                with engine.begin() as conn:
                    if state != "reclaimed":
                        conn.exec_driver_sql(f"DROP TABLE {relation}")
                    _configure_fixture_catalog(conn, f"{original_identity['system_identifier']}/{original_identity['database_oid']}", store.root)
                    conn.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                                 {"layout": COLD_HISTORY_LAYOUT_VERSION})
                    conn.execute(text("""
                        INSERT INTO market.cold_fact_retired_days
                          (storage_day,manifest_set_hash,row_count,reclaimed_header_bytes,protected_dataset_ranges,handoff_kind)
                        VALUES (:day,:hash,2,0,0,'initial')
                    """), {"day": day, "hash": audit})
                header = {
                    "schema": LEGACY_HEADER_VERSION,
                    "target_database": original_identity, "donor_identity": donor_identity,
                    "legacy_oid": original_oid, "original_oid": donor_oid, "end_day": end_day.isoformat(),
                    "audit_set_hash": audit,
                }
                header["receipt_sha256"] = archive_evidence_hash(header)
                with _fence_connections(storage) as (conn, control):
                    fence = close_database_fence(conn, control, expected_database=original_identity,
                                                  timeout_seconds=60, record=lambda *_: None)
                    args = dict(fence=fence, original=original, expected_original_hash=original["receipt_sha256"],
                                legacy_header_receipt=header, expected_legacy_header_receipt_hash=header["receipt_sha256"],
                                store=store, object_limits=ArchiveVerificationLimits())
                    with conn.begin(), pytest.raises(ValueError, match="receipt_changed"):
                        restore_legacy_payload_day(conn, donor, **(args | {"expected_original_hash": "f" * 64}), **BUDGET)
                    if state == "reclaimed":
                        path = next(store.root.rglob("*.parquet"))
                        content = path.read_bytes()
                        path.write_bytes(content + b"damaged")
                        try:
                            with conn.begin(), pytest.raises(RuntimeError, match="archive_verification_size_mismatch"):
                                restore_legacy_payload_day(conn, donor, **args, **BUDGET)
                        finally:
                            path.write_bytes(content)
                    with conn.begin():
                        result = restore_legacy_payload_day(conn, donor, **args, **BUDGET)
                        assert (result["payload_oid"] is None) == (state == "reclaimed")
                        assert result["retention_state"] == state and result["row_count"] == 2
                    with conn.begin():
                        assert restore_legacy_payload_day(conn, donor, **args, **BUDGET) == result
                        conn.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                                     {"layout": FACT_STORAGE_LAYOUT_VERSION})
                        assert conn.scalar(text("SELECT evidence FROM market.fact_storage_state")) == certificate
                    reopen_database_fence(fence, control, expected_outcome="original",
                        read_outcome=lambda _: "original", record=lambda *_: None)
        finally:
            admin.dispose()
            donor_engine.dispose()
    restored = Database(storage.dsn)
    try:
        assert restored.ensure_schema(), str(restored.last_error)
        monkeypatch.setattr(market_data, "db", restored)
        assert storage.repo.read_fact_revisions(**selection) == expected
        assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == expected
        check_deployed_source_reader(restored._engine.url, store, frozen.dataset_id, storage.series_id, expected)
    finally:
        restored._reset_engine()
