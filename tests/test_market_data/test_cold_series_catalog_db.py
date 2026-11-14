from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.canonical_storage import record_from_storage_row, record_to_storage_row
from market_data.cold_history import publish_cold_page
from portal.backend.db import InstrumentRecord
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.service.storage.repos import market_data
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog
from tests.test_market_data.test_canonical_fact_archive import _row
from tests.test_market_data.test_fact_reclamation_db import _prepare
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage

pytestmark = pytest.mark.db


def test_series_catalog_preserves_exact_counts_and_bounds_after_daily_retirement(storage, tmp_path, monkeypatch):
    with storage.database.session() as session:
        session.add(InstrumentRecord(
            id="cold-only-series", datasource="TEST", exchange="ISOLATED", symbol="ETH-TEST",
            instrument_type="spot", can_short=False, short_requires_borrow=False,
            has_funding=False, extra_metadata={},
        ))
    cold_series = storage.repo.register_series(instrument_id="cold-only-series", fact_type=storage.fact.fact_type,
        timeframe_seconds=None, contract_version=storage.fact.payload_schema_id)
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    storage.repo.ingest_facts(series_id=cold_series, source_id=storage.source_id, facts=[storage.fact])
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch)
    _placement(monkeypatch, storage.today)
    correction = replace(storage.fact, observation_key="reclaim-0", known_at=BASE + timedelta(minutes=1),
                         accepted_at=BASE + timedelta(minutes=1), payload={**storage.fact.payload,
                             "rate": "0.3", "raw_rate": "0.3"})
    assert _ingest(storage, correction).corrected_count == 1
    later = BASE + timedelta(minutes=2)
    _ingest(storage, replace(storage.fact, observation_key="entirely-hot", known_at=later,
                             accepted_at=later, observation_time=later))
    before = storage.repo.list_series()
    expected = {row["id"]: row for row in before}
    assert expected[cold_series]["fact_count"] == expected[cold_series]["version_count"] == 1
    mixed = expected[storage.series_id]
    assert (mixed["fact_count"], mixed["version_count"]) == (3, 5)
    assert (mixed["first_fact_time"], mixed["last_fact_time"]) == (BASE, later)

    with storage.database.session() as session:
        database_identity = session.scalar(text("""
            SELECT control.system_identifier::text || '/' || db.oid::text
            FROM pg_control_system() AS control CROSS JOIN pg_database AS db
            WHERE db.datname=current_database()
        """))
        _configure_fixture_catalog(session.connection(), database_identity, archive.object_store.root)
        prepare_cold_dependency_bindings(session.connection())
        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                        {"layout": COLD_HISTORY_LAYOUT_VERSION})
    assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "cold_page_admitted"
    assert storage.repo.list_series() == before
    assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"
    assert storage.repo.list_series() == before

    with storage.database.session() as session:
        cold = market_data.canonical_fact_storage_repository.cold_history(session)
        hidden_record = record_from_storage_row(_row(series_id=cold_series, commit=10_000))
        hidden_row = record_to_storage_row(replace(hidden_record, row_hash=None, fact_version_id=None,
            fact=replace(hidden_record.fact, observation_key="never-visible", observation_time=BASE - timedelta(days=365))),
            series_dimensions={})
        hidden_page = publish_cold_page([hidden_row], object_store=archive.object_store, temporary_directory=tmp_path)
        cold.admit(session, hidden_page, staged_storage_day=later.date())
    assert storage.repo.list_series(instrument_id="cold-only-series") == [expected[cold_series]]
    assert storage.repo.list_series() == before
