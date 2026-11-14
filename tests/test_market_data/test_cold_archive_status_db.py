import pytest
from sqlalchemy import event, text

from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.service.storage.repos import fact_dependencies, market_data, market_structure
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_flow_retention_db import _flow_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


def test_archive_status_discloses_unavailable_history_without_opening_cold_files(storage, tmp_path, monkeypatch):
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    flow = _flow_fixture(storage, tmp_path, monkeypatch)
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: flow.store, read_cache_factory=lambda: None)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(fact_dependencies, "canonical_fact_storage_repository", reader)

    def status():
        return flow.structures.archive_status(definition_id="flow-retention")

    before = status()
    assert before["canonical_trade_count"] is None
    assert before["trade_flow_aggregate_status"] == {"state": "complete"}
    assert len(before["trade_flow_aggregates"]) == 1
    assert before["trade_flow_aggregates"][0]["bucket_count"] == 2
    archive = PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=flow.store,
                                                       temporary_directory=tmp_path / "staging")
    for day in (flow.source_day, flow.flow_day):
        _archive_day(archive, day)
    with storage.database.session() as session:
        identity = session.scalar(text("""
            SELECT control.system_identifier::text || '/' || db.oid::text
            FROM pg_control_system() AS control CROSS JOIN pg_database AS db
            WHERE db.datname=current_database()
        """))
        _configure_fixture_catalog(session.connection(), identity, flow.store.root)
        prepare_cold_dependency_bindings(session.connection())
        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                        {"layout": COLD_HISTORY_LAYOUT_VERSION})
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)

    def reclaim(day):
        return reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"]

    assert reclaim(flow.source_day) == "cold_page_admitted"
    assert reclaim(flow.source_day) == "partition_reclaimed"
    assert reclaim(flow.flow_day) == "cold_page_admitted"
    assert status()["trade_flow_aggregates"] == before["trade_flow_aggregates"]
    assert reclaim(flow.flow_day) == "partition_reclaimed"

    def reject_cold_reads(*args, **kwargs):
        pytest.fail("archive status must not hydrate lifetime cold history")

    monkeypatch.setattr(reader, "cold_history", reject_cold_reads)

    def reject_legacy_reads(_conn, _cursor, statement, *_):
        assert "market.market_trade_identities" not in statement

    event.listen(storage.database._engine, "before_cursor_execute", reject_legacy_reads)
    try:
        after = status()
    finally:
        event.remove(storage.database._engine, "before_cursor_execute", reject_legacy_reads)
    assert after["canonical_trade_count_status"] == before["canonical_trade_count_status"]
    assert after["trade_flow_aggregates"] == []
    assert after["trade_flow_aggregate_status"] == {
        "state": "unavailable", "reason": "exact_lifetime_aggregate_requires_cold_history_scan",
    }
