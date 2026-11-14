import pytest
from sqlalchemy import text

from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data, market_structure
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_raw_lineage_db import _publish_book_result, _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


def test_level_two_startup_preserves_counters_after_last_hot_book_fact_retires(storage, tmp_path, monkeypatch):
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    book = _raw_book_fixture(storage, tmp_path, monkeypatch)
    for index in range(len(book.results)):
        _publish_book_result(book, index)
    store = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: book.store, read_cache_factory=lambda: None)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", store)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", store)
    before = book.structures.archive_status(definition_id=book.claim.definition_id)["book_reconstruction"]
    assert (before["snapshot_count"], before["batch_count"]) == (1, 1)
    archive = PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=book.store,
                                                       temporary_directory=tmp_path / "staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    _archive_day(archive, book.day)
    with storage.database.session() as session:
        identity = session.scalar(text("""
            SELECT control.system_identifier::text || '/' || db.oid::text
            FROM pg_control_system() AS control CROSS JOIN pg_database AS db
            WHERE db.datname=current_database()
        """))
        _configure_fixture_catalog(session.connection(), identity, book.store.root)
        prepare_cold_dependency_bindings(session.connection())
        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                        {"layout": COLD_HISTORY_LAYOUT_VERSION})

    def retire():
        return reclaimer.reclaim_partition(book.day, eligible_before=storage.today, execute=True)["status"]

    assert retire() == "cold_page_admitted"
    assert retire() == "partition_reclaimed"
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:id"),
                              {"id": book.series_id}) == 0

    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
        monkeypatch.setattr(market_structure, "db", restarted)
        assert book.structures.archive_status(definition_id=book.claim.definition_id)["book_reconstruction"] == before
    finally:
        restarted._reset_engine()
