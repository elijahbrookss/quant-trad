from dataclasses import replace
import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

from scripts.db.canonical_cold_load_pressure import CatalogLoadLimits, LoadPressure, _measure_wal


@pytest.fixture
def limits():
    return CatalogLoadLimits("fixture-ssd", "fixture-hdd", 1024, 1024, 1024, 1024, 1024, 1 << 40, 10_000, 64)


@pytest.mark.parametrize("changes", [
    {"recent_target_id": "fixture-hdd"}, {"min_recent_free_bytes": False},
    {"max_page_wal_bytes": 0}, {"temp_file_limit_kib": 2**31}, {"max_ready_wal_segments": -1},
])
def test_load_pressure_requires_explicit_bounded_allowances(limits, changes):
    with pytest.raises(ValueError, match="canonical_cold_load_pressure_invalid"):
        replace(limits, **changes)


@pytest.mark.db
def test_native_wal_observation_keeps_retained_bytes_and_new_writes_separate(limits):
    if os.getenv("QT_DB_TEST_ISOLATED") != "1" or not os.getenv("PG_DSN"):
        pytest.skip("requires native PostgreSQL test endpoint")
    url = make_url(os.environ["PG_DSN"])
    if url.host not in ("127.0.0.1", "::1"):
        pytest.skip("requires a disposable loopback PostgreSQL fixture")
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            current = _measure_wal(conn, limits=limits)
            assert "/" in current
            with pytest.raises(LoadPressure, match="resident_wal_pressure"):
                _measure_wal(conn, limits=replace(limits, max_resident_wal_bytes=1))
            with pytest.raises(LoadPressure, match="page_wal_budget_exceeded"):
                _measure_wal(conn, limits=limits, previous_lsn="0/0")
    finally:
        engine.dispose()
