import pytest
from sqlalchemy import text

from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.service.storage.repos import market_data, market_structure
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_raw_lineage_db import _canonical_trade, _raw_trade_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


def test_trade_reconciliation_uses_exact_canonical_scope_after_retirement(storage, tmp_path, monkeypatch):
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    trade = _raw_trade_fixture(storage, tmp_path, monkeypatch)
    for raw in trade.raws:
        storage.repo.ingest_facts(series_id=trade.series_id, source_id=trade.source_id,
                                  facts=[_canonical_trade(trade, raw)])
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: trade.store, read_cache_factory=lambda: None)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", reader)

    def verify():
        overlap = trade.structures.recent_trade_id_overlap(
            definition_id=trade.claim.definition_id, provider_trade_ids=["missing", "same-trade", "same-trade"],
        )
        assert overlap["canonical_overlap_trade_ids"] == ["same-trade"]
        assert overlap["rest_only_trade_ids"] == ["missing"]
        replay = trade.structures.reconcile_manifest_trade_ids(
            manifest_id=trade.manifests[0], provider_product_id="BTC-USD", provider_trade_ids=["same-trade"],
        )
        assert replay["canonical_reconciliation_complete"] and replay["raw_mapping_complete"]
        assert replay["canonical_trade_count"] == 1

    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.market_trade_identities")) == 0
    verify()

    archive = PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=trade.store,
                                                       temporary_directory=tmp_path / "staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    _archive_day(archive, trade.day)
    with storage.database.session() as session:
        database_identity = session.scalar(text("""
            SELECT control.system_identifier::text || '/' || db.oid::text
            FROM pg_control_system() AS control CROSS JOIN pg_database AS db
            WHERE db.datname=current_database()
        """))
        _configure_fixture_catalog(session.connection(), database_identity, trade.store.root)
        prepare_cold_dependency_bindings(session.connection())
        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                        {"layout": COLD_HISTORY_LAYOUT_VERSION})
    assert reclaimer.reclaim_partition(trade.day, eligible_before=storage.today,
                                       execute=True)["status"] == "cold_page_admitted"
    verify()
    assert reclaimer.reclaim_partition(trade.day, eligible_before=storage.today,
                                       execute=True)["status"] == "partition_reclaimed"
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:id"),
                              {"id": trade.series_id}) == 0
    verify()
