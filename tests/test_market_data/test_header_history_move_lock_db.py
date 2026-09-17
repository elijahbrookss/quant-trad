"""Minimum responsiveness gate before enabling historical table movement.

An exclusive lock represents the period when a historical table is moved.
Passing this probe does not replace a real concurrent-copy workload benchmark.
"""
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.test_market_data.test_fact_series_day_db import BASE, DAY, directory_engine

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
