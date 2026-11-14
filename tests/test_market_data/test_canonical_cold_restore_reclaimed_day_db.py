from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.db.fact_identity_schema import fact_header_partition_name
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract, fact_partition_name
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data
from portal.backend.service.storage.repos.cold_retirement import prune_oldest_retired_identities
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_restore_reclaimed_day import restore_reclaimed_header_day
from scripts.db.canonical_cold_restore_references import capture_original_reference_bindings, restore_original_reference_bindings
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog, _fence_connections
from tests.test_market_data.exact_deployed_reader import check_deployed_source_reader
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage  # noqa: F401


pytestmark = pytest.mark.db


def test_restore_one_recurring_reclaimed_day_reuses_verified_payload_for_original_reader(storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    _ingest(storage)
    selection = {"series_id": storage.series_id, "start": BASE - timedelta(seconds=1), "end": BASE + timedelta(seconds=1)}
    original = storage.repo.read_fact_revisions(**selection)
    dataset = storage.repo.freeze_dataset([DatasetSeriesRequest(**selection)])
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", PostgresCanonicalFactStorageRepository(
        object_store_factory=lambda: store, read_cache_factory=lambda: None,
    ))
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
    )
    _archive_day(archive, day)
    engine = storage.database._engine
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
        original_fks = capture_original_reference_bindings(conn)
        original_certificate = conn.scalar(text("SELECT evidence FROM market.fact_storage_state"))
    with engine.begin() as conn:
        identity = original_fks["database"]
        _configure_fixture_catalog(conn, f"{identity['system_identifier']}/{identity['database_oid']}", store.root)
        prepare_cold_dependency_bindings(conn)
        conn.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                     {"layout": COLD_HISTORY_LAYOUT_VERSION})
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "cold_page_admitted"
    retired = reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert retired["status"] == "partition_reclaimed"
    assert prune_oldest_retired_identities(storage.database, deadline=lambda: None)["day_complete"]
    assert storage.repo.read_fact_revisions(**selection) == original

    with _fence_connections(storage) as (conn, control):
        fence = close_database_fence(conn, control, expected_database=identity, timeout_seconds=60, record=lambda *_: None)
        args = {"fence": fence, "day": day, "store": store, "limits": archive.limits,
                "object_limits": ArchiveVerificationLimits(), "expected_manifest_set_hash": retired["manifest_set_hash"]}
        file = next(store.root.rglob("*.parquet"))
        data = file.read_bytes()
        file.write_bytes(data + b"damaged")
        try:
            with conn.begin(), pytest.raises(RuntimeError, match="archive_verification_size_mismatch"):
                restore_reclaimed_header_day(conn, **args)
        finally:
            file.write_bytes(data)
        with conn.begin():
            assert conn.scalar(text("SELECT to_regclass(:name) IS NULL"),
                               {"name": f"market.{fact_header_partition_name(day)}"})
            result = restore_reclaimed_header_day(conn, **args)
            assert result["row_count"] == result["page_count"] == 1 and not result["already_restored"]
        with conn.begin():
            repeated = restore_reclaimed_header_day(conn, **args)
            assert repeated["ordered_content_hash"] == result["ordered_content_hash"] and repeated["already_restored"]
            assert conn.scalar(text("SELECT to_regclass(:name) IS NULL"),
                               {"name": f"market.{fact_partition_name(day)}"})
            restore_original_reference_bindings(conn, fence=fence, original=original_fks,
                                                expected_hash=archive_evidence_hash(original_fks))
            conn.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout WHERE layout_version=:prior"),
                         {"layout": FACT_STORAGE_LAYOUT_VERSION, "prior": COLD_HISTORY_LAYOUT_VERSION})
            assert conn.scalar(text("SELECT evidence FROM market.fact_storage_state")) == original_certificate
            assert_fact_storage_contract(conn)

        def original_outcome(check):
            assert_fact_storage_contract(check)
            assert check.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == FACT_STORAGE_LAYOUT_VERSION
            return "original"

        reopen_database_fence(fence, control, expected_outcome="original", read_outcome=original_outcome, record=lambda *_: None)

    restored = Database(storage.dsn)
    try:
        assert restored.ensure_schema(), str(restored.last_error)
        monkeypatch.setattr(market_data, "db", restored)
        assert storage.repo.read_fact_revisions(**selection) == original
        assert storage.repo.read_dataset_fact_revisions(dataset_id=dataset.dataset_id, series_id=storage.series_id) == original
        check_deployed_source_reader(restored._engine.url, store, dataset.dataset_id, storage.series_id, original)
    finally:
        restored._reset_engine()
