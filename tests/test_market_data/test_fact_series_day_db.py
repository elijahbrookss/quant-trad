"""Transactional range-directory guards, isolated from full application bootstrap."""
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from queue import Queue
from time import monotonic, sleep

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from portal.backend.db import MarketFactHeaderSeriesDayRecord
from portal.backend.db.fact_series_day_schema import (
    assert_fact_series_day_contract, install_fact_series_day_functions,
)
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db
DAY = date(2026, 9, 1)
BASE = datetime(2020, 1, 1, tzinfo=UTC)


@pytest.fixture
def directory_engine():
    with fresh_migration_database("series_day", install_extensions=False) as dsn:
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE SCHEMA market;
                    CREATE TABLE market.fact_versions (
                        id text,series_id bigint NOT NULL,storage_day date NOT NULL,
                        observation_time timestamptz NOT NULL
                    ) PARTITION BY RANGE(storage_day);
                    CREATE TABLE market.fact_versions_day PARTITION OF market.fact_versions
                        FOR VALUES FROM ('2026-09-01') TO ('2026-09-02');
                    CREATE TABLE market.fact_versions_next_day PARTITION OF market.fact_versions
                        FOR VALUES FROM ('2026-09-02') TO ('2026-09-03');
                """))
                MarketFactHeaderSeriesDayRecord.__table__.create(conn)
                install_fact_series_day_functions(conn)
                assert_fact_series_day_contract(conn)
            yield engine
        finally:
            engine.dispose()


def _insert(conn, number, *, series=1, direct=False):
    relation = "market.fact_versions_day" if direct else "market.fact_versions"
    conn.execute(text(
        f"INSERT INTO {relation}(id,series_id,storage_day,observation_time) "
        "VALUES(:id,:series,:day,:observation)"
    ), {"id": str(number), "series": series, "day": DAY,
        "observation": BASE + timedelta(seconds=number)})


def test_bounds_expand_for_late_rows_and_rollback_with_insertion(directory_engine):
    with directory_engine.begin() as conn:
        _insert(conn, 10)
        _insert(conn, -20, direct=True)
        _insert(conn, 50)
        _insert(conn, 1, series=2)
        rows = conn.execute(text(
            "SELECT series_id,min_observation_time,max_observation_time "
            "FROM market.fact_header_series_days ORDER BY series_id"
        )).all()
        assert rows == [
            (1,BASE - timedelta(seconds=20),BASE + timedelta(seconds=50)),
            (2,BASE + timedelta(seconds=1),BASE + timedelta(seconds=1)),
        ]
    with pytest.raises(RuntimeError, match="simulated_abort"):
        with directory_engine.begin() as conn:
            _insert(conn, -100)
            _insert(conn, 100, series=3)
            raise RuntimeError("simulated_abort")
    with directory_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one() == 4
        assert conn.execute(text(
            "SELECT min_observation_time FROM market.fact_header_series_days WHERE series_id=1"
        )).scalar_one() == BASE - timedelta(seconds=20)
        assert conn.execute(text(
            "SELECT count(*) FROM market.fact_header_series_days WHERE series_id=3"
        )).scalar_one() == 0


def test_concurrent_writers_preserve_both_range_extremes(directory_engine):
    def write(number):
        with directory_engine.begin() as conn:
            _insert(conn, number)
    numbers = [60,-70,80,-90,100,-110,120,-130]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(write, numbers))
    with directory_engine.connect() as conn:
        assert conn.execute(text(
            "SELECT min_observation_time,max_observation_time FROM market.fact_header_series_days"
        )).one() == (BASE - timedelta(seconds=130),BASE + timedelta(seconds=120))
        assert conn.execute(text("SELECT count(*) FROM market.fact_versions")).scalar_one() == len(numbers)


def test_directory_cannot_be_narrowed_rekeyed_removed_or_truncated(directory_engine):
    with directory_engine.begin() as conn:
        _insert(conn, 0)
        _insert(conn, 10)
    for sql in (
        "UPDATE market.fact_header_series_days SET min_observation_time=min_observation_time+interval '1 second'",
        "UPDATE market.fact_header_series_days SET max_observation_time=max_observation_time-interval '1 second'",
        "UPDATE market.fact_header_series_days SET storage_day=storage_day+1",
        "UPDATE market.fact_header_series_days SET series_id=99",
        "DELETE FROM market.fact_header_series_days",
        "TRUNCATE market.fact_header_series_days",
    ):
        with pytest.raises(DBAPIError, match="canonical_series_day_(narrowing|removal)_forbidden"):
            with directory_engine.begin() as conn:
                conn.execute(text(sql))
    with directory_engine.begin() as conn:
        # Extra coverage is conservative and cannot hide canonical rows.
        conn.execute(text(
            "UPDATE market.fact_header_series_days "
            "SET min_observation_time=min_observation_time-interval '1 day'"
        ))
        assert_fact_series_day_contract(conn)


def test_disabled_capture_and_missing_directory_fail_admission(directory_engine):
    with directory_engine.begin() as conn:
        conn.execute(text("ALTER TABLE market.fact_versions DISABLE TRIGGER trg_extend_fact_header_series_day"))
        with pytest.raises(RuntimeError, match="canonical_series_day_trigger_incompatible"):
            assert_fact_series_day_contract(conn)
        conn.execute(text("ALTER TABLE market.fact_versions ENABLE ALWAYS TRIGGER trg_extend_fact_header_series_day"))
        conn.execute(text("DROP TABLE market.fact_header_series_days"))
        with pytest.raises(RuntimeError, match="canonical_series_day_directory_missing"):
            assert_fact_series_day_contract(conn)


def test_stable_reader_keeps_one_snapshot_across_concurrent_new_day(directory_engine):
    with directory_engine.begin() as conn:
        _insert(conn, 0)
    parameters = {"series":1,"start":BASE - timedelta(days=1),"end":BASE + timedelta(days=1)}
    query = text("SELECT id FROM market.read_fact_headers_in_range(:series,:start,:end) ORDER BY id")
    reader_pid = Queue()

    def read():
        with directory_engine.begin() as conn:
            conn.execute(text("SET LOCAL statement_timeout='15s'"))
            reader_pid.put(conn.execute(text("SELECT pg_backend_pid()")).scalar_one())
            return conn.execute(query, parameters).scalars().all()

    with ThreadPoolExecutor(max_workers=1) as pool:
        with directory_engine.connect() as blocker:
            transaction = blocker.begin()
            blocker_pid = blocker.execute(text("SELECT pg_backend_pid()")).scalar_one()
            blocker.execute(text("LOCK TABLE market.fact_versions_day IN ACCESS EXCLUSIVE MODE"))
            future = pool.submit(read)
            try:
                pid = reader_pid.get(timeout=5)
                with directory_engine.connect() as observer:
                    deadline = monotonic() + 10
                    while blocker_pid not in observer.execute(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid":pid}
                    ).scalar_one():
                        if future.done():
                            raise AssertionError(f"reader did not reach the expected header lock: {future.result()}")
                        if monotonic() >= deadline:
                            raise AssertionError("reader did not reach the expected header lock")
                        sleep(0.02)
                # Bypass the blocked child, but exercise its sibling's inherited
                # directory trigger. Commit AFTER the reader's directory lookup.
                with directory_engine.begin() as writer:
                    writer.execute(text("""
                        INSERT INTO market.fact_versions_next_day
                            (id,series_id,storage_day,observation_time)
                        VALUES ('new-day',1,'2026-09-02',:observed)
                    """), {"observed":BASE})
            finally:
                transaction.rollback()
            assert future.result(timeout=10) == ["0"]
    with directory_engine.connect() as conn:
        assert conn.execute(query, parameters).scalars().all() == ["0","new-day"]


def test_reader_volatility_cannot_be_changed_silently(directory_engine):
    with directory_engine.begin() as conn:
        conn.execute(text(
            "ALTER FUNCTION market.read_fact_headers_in_range(bigint,timestamptz,timestamptz) VOLATILE"
        ))
        with pytest.raises(RuntimeError, match="canonical_series_day_reader_incompatible"):
            assert_fact_series_day_contract(conn)


def test_disabled_child_capture_fails_admission(directory_engine):
    with directory_engine.begin() as conn:
        conn.execute(text(
            "ALTER TABLE market.fact_versions_day DISABLE TRIGGER trg_extend_fact_header_series_day"
        ))
        with pytest.raises(RuntimeError, match="canonical_series_day_child_capture_incompatible"):
            assert_fact_series_day_contract(conn)


@pytest.mark.parametrize("drift", ["capture_filter", "function_volatility"])
def test_directory_admission_rejects_filtered_capture_or_changed_function_semantics(directory_engine, drift):
    with directory_engine.begin() as conn:
        if drift == "capture_filter":
            conn.execute(text("DROP TRIGGER trg_extend_fact_header_series_day ON market.fact_versions"))
            conn.execute(text("""
                CREATE TRIGGER trg_extend_fact_header_series_day
                AFTER INSERT ON market.fact_versions FOR EACH ROW
                WHEN (NEW.series_id > 1)
                EXECUTE FUNCTION market.extend_fact_header_series_day()
            """))
            conn.execute(text(
                "ALTER TABLE market.fact_versions ENABLE ALWAYS TRIGGER trg_extend_fact_header_series_day"
            ))
        else:
            conn.execute(text("ALTER FUNCTION market.extend_fact_header_series_day() STABLE"))
        with pytest.raises(RuntimeError, match="canonical_series_day_(trigger|function)_incompatible"):
            assert_fact_series_day_contract(conn)
