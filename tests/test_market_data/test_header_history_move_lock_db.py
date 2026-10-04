"""Minimum responsiveness gate before enabling historical table movement.

An exclusive lock represents the period when a historical table is moved.
Passing this probe does not replace a real concurrent-copy workload benchmark.
"""
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.test_market_data.test_fact_series_day_db import BASE, DAY, directory_engine
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


@pytest.mark.parametrize("operation", ["range_read", "recent_insert"])
def test_recent_work_does_not_wait_for_unrelated_historical_header_lock(directory_engine, operation):
    with directory_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO market.fact_versions(id,series_id,storage_day,observation_time)
            VALUES ('old',1,:old_day,:old_time),('recent',1,:recent_day,:recent_time)
        """), {"old_day": DAY, "recent_day": DAY + timedelta(days=1),
               "old_time": BASE, "recent_time": BASE + timedelta(days=1)})
    with directory_engine.connect() as blocker:
        transaction = blocker.begin()
        try:
            blocker.execute(text("LOCK TABLE market.fact_versions_day IN ACCESS EXCLUSIVE MODE"))
            # Witness that the historical lock is active on an independent connection.
            with pytest.raises(DBAPIError, match="statement timeout"):
                with directory_engine.begin() as witness:
                    witness.execute(text("SET LOCAL statement_timeout='250ms'"))
                    witness.execute(text("SELECT id FROM market.fact_versions_day"))
            with directory_engine.begin() as current:
                current.execute(text("SET LOCAL statement_timeout='1s'"))
                if operation == "range_read":
                    rows = current.execute(text("""
                        SELECT id FROM market.read_fact_headers_in_range(1,:start,:end)
                    """), {"start": BASE + timedelta(days=1), "end": BASE + timedelta(days=2)}).scalars().all()
                    assert rows == ["recent"]
                else:
                    current.execute(text("""
                        INSERT INTO market.fact_versions(id,series_id,storage_day,observation_time)
                        VALUES ('during-move',1,:day,:observed)
                    """), {"day": DAY + timedelta(days=1), "observed": BASE + timedelta(days=1, seconds=1)})
        finally:
            transaction.rollback()


@pytest.mark.parametrize("plan_mode", ["auto", "force_generic_plan"])
def test_canonical_ingestion_does_not_wait_for_unrelated_history(
        storage, monkeypatch, plan_mode):
    from dataclasses import replace
    from sqlalchemy import event
    from tests.test_market_data.test_fact_storage_tiers_db import _placement, BASE

    engine = storage.database._engine
    old_day = storage.today - timedelta(days=45)
    _placement(monkeypatch, old_day)
    assert storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
                                    facts=[storage.fact]).inserted_count == 1
    _placement(monkeypatch, storage.today)

    def limits(connection, record, proxy):
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout='1500ms'")
            cursor.execute("SET plan_cache_mode="+plan_mode)

    event.listen(engine, "checkout", limits)
    try:
        with engine.connect() as blocker:
            transaction = blocker.begin()
            blocker.exec_driver_sql("LOCK TABLE market.fact_versions_"+old_day.strftime("%Y%m%d")+
                                   " IN ACCESS EXCLUSIVE MODE")
            try:
                current = replace(storage.fact, observation_key="current-during-history-move",
                                  observation_time=BASE+timedelta(days=2))
                assert storage.repo.ingest_facts(series_id=storage.series_id,
                    source_id=storage.source_id, facts=[current]).inserted_count == 1
            finally:
                transaction.rollback()
    finally:
        event.remove(engine, "checkout", limits)
