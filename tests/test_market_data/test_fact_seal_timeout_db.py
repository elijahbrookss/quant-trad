"""Actual PostgreSQL proof of count-only allowance, rollback and writer fences."""
from dataclasses import replace
from datetime import timedelta
from threading import Thread

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, _ingest, _placement

pytestmark = pytest.mark.db
HEADER = "SELECT count(*) FROM market.fact_versions WHERE storage_day="
HOT = "SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day="


def seed(storage, monkeypatch):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    _ingest(storage)
    return day


def owner(storage, tmp_path, *, normal_ms=5000, check_budget=None):
    return PostgresCanonicalFactArchiveRepository(
        database=storage.database,
        object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
        temporary_directory=tmp_path / "staging", statement_timeout_ms=normal_ms,
        check_budget=check_budget,
    )


def state(storage, day):
    with storage.database.session() as s:
        return dict(s.execute(text("SELECT state,expected_rows,sealed_at FROM "
            "market.fact_retention_partitions WHERE storage_day=:day"), {"day": day}).mappings().one())


def pool_timeout(storage):
    with storage.database.session() as s:
        return s.execute(text("SELECT pg_backend_pid(), setting::integer FROM pg_settings "
                              "WHERE name='statement_timeout'")).one()


def observe(storage, callback):
    engine = storage.database._engine
    event.listen(engine, "before_cursor_execute", callback, retval=True)
    return engine


def test_exact_counts_restore_before_following_sql_and_pooled_session(storage, tmp_path, monkeypatch):
    day = seed(storage, monkeypatch)
    original = pool_timeout(storage)
    seen = []

    def capture(conn, cursor, sql, params, context, many):
        if (sql.startswith((HEADER, HOT, "SELECT pg_total_relation_size"))
                or "SET state='sealed'" in sql):
            cursor.execute("SHOW statement_timeout")
            seen.append((sql, cursor.fetchone()[0], cursor.connection.get_backend_pid()))
        return sql, params

    engine = observe(storage, capture)
    try:
        result = owner(storage, tmp_path).seal_partition(
            day, count_statement_timeout_ms=60_000, remaining_run_seconds=lambda: 600,
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    assert result["state"] == "sealed" and result["expected_rows"] == 1
    assert [timeout for _, timeout, _ in seen] == ["1min", "1min", "5s", "5s"]
    assert {pid for _, _, pid in seen} == {original[0]}
    assert pool_timeout(storage) == original
    # Existing immutable-history admission still rejects a late source writer.
    with pytest.raises(RuntimeError, match="fact_hot_partition_not_open"):
        _ingest(storage, replace(storage.fact, observation_key="late"))


@pytest.mark.parametrize("target", [HEADER, HOT])
def test_scoped_allowance_succeeds_where_original_statement_limit_cancels(
    storage, tmp_path, monkeypatch, target,
):
    day = seed(storage, monkeypatch)
    before = pool_timeout(storage)

    def slow(conn, cursor, sql, params, context, many):
        if sql.startswith(target):
            sql = sql.replace("SELECT count(*)", "SELECT count(*) + (SELECT 0 FROM pg_sleep(0.35))", 1)
        return sql, params

    engine = observe(storage, slow)
    archive = owner(storage, tmp_path, normal_ms=250)
    try:
        with pytest.raises(DBAPIError) as exc:
            archive.seal_partition(day)
        assert exc.value.orig.pgcode == "57014"
        assert state(storage, day)["state"] == "open"
        assert archive.seal_partition(day, count_statement_timeout_ms=1500,
            remaining_run_seconds=lambda: 600)["expected_rows"] == 1
    finally:
        event.remove(engine, "before_cursor_execute", slow)
    assert pool_timeout(storage) == before


@pytest.mark.parametrize("target", [HEADER, HOT])
def test_count_timeout_rolls_back_sealing_and_restores_pool(storage, tmp_path, monkeypatch, target):
    day = seed(storage, monkeypatch)
    before = pool_timeout(storage)

    def slow(conn, cursor, sql, params, context, many):
        if sql.startswith(target):
            sql = sql.replace("SELECT count(*)", "SELECT count(*) + (SELECT 0 FROM pg_sleep(0.4))", 1)
        return sql, params

    engine = observe(storage, slow)
    try:
        with pytest.raises(DBAPIError) as exc:
            owner(storage, tmp_path).seal_partition(day, count_statement_timeout_ms=150,
                                                    remaining_run_seconds=lambda: 600)
        assert exc.value.orig.pgcode == "57014"
    finally:
        event.remove(engine, "before_cursor_execute", slow)
    assert state(storage, day) == {"state": "open", "expected_rows": None, "sealed_at": None}
    assert pool_timeout(storage) == before


def test_second_count_reclamps_to_declining_remaining_budget(storage, tmp_path, monkeypatch):
    day = seed(storage, monkeypatch)
    remaining = [600]
    seen = []

    def slow(conn, cursor, sql, params, context, many):
        if sql.startswith(HEADER):
            cursor.execute("SHOW statement_timeout")
            seen.append(cursor.fetchone()[0])
            remaining[0] = 0.06
        elif sql.startswith(HOT):
            cursor.execute("SHOW statement_timeout")
            seen.append(cursor.fetchone()[0])
            sql = sql.replace("SELECT count(*)", "SELECT count(*) + (SELECT 0 FROM pg_sleep(0.4))", 1)
        return sql, params

    engine = observe(storage, slow)
    archive = owner(storage, tmp_path)
    # Work under the already acquired writer/policy fences consumes time before
    # the first count. Neither count may reuse the action-entry 600 seconds.
    archive.partition_guard = lambda session, partition: remaining.__setitem__(0, 0.2)
    try:
        with pytest.raises(DBAPIError) as exc:
            archive.seal_partition(day, count_statement_timeout_ms=60_000,
                                   remaining_run_seconds=lambda: remaining[0])
        assert exc.value.orig.pgcode == "57014"
    finally:
        event.remove(engine, "before_cursor_execute", slow)
    assert seen == ["200ms", "60ms"]
    assert state(storage, day)["state"] == "open"
    assert pool_timeout(storage)[1] == 0


@pytest.mark.parametrize("boundary", ["between_counts", "after_update"])
def test_original_run_deadline_or_cancellation_rolls_back_complete_seal(
    storage, tmp_path, monkeypatch, boundary,
):
    day = seed(storage, monkeypatch)
    clock = [599.0]

    def budget():
        if clock[0] >= 600:
            raise RuntimeError("original_run_budget_or_cancellation")

    def expire(conn, cursor, sql, params, context, many):
        if ((boundary == "between_counts" and sql.startswith(HEADER))
                or (boundary == "after_update" and "SET state='sealed'" in sql)):
            clock[0] = 601

    engine = storage.database._engine
    event.listen(engine, "after_cursor_execute", expire)
    try:
        with pytest.raises(RuntimeError, match="original_run_budget_or_cancellation"):
            owner(storage, tmp_path, check_budget=budget).seal_partition(
                day, count_statement_timeout_ms=60_000, remaining_run_seconds=lambda: 600 - clock[0],
            )
    finally:
        event.remove(engine, "after_cursor_execute", expire)
    assert state(storage, day) == {"state": "open", "expected_rows": None, "sealed_at": None}
    assert pool_timeout(storage)[1] == 0


def test_mismatched_exact_counts_fail_closed_with_larger_allowance(storage, tmp_path, monkeypatch):
    day = seed(storage, monkeypatch)

    def mismatch(conn, cursor, sql, params, context, many):
        if sql.startswith(HOT):
            sql = sql.replace("count(*)", "count(*)+1", 1)
        return sql, params

    engine = observe(storage, mismatch)
    try:
        with pytest.raises(RuntimeError, match="canonical_archive_source_incomplete"):
            owner(storage, tmp_path).seal_partition(day, count_statement_timeout_ms=60_000,
                                                   remaining_run_seconds=lambda: 600)
    finally:
        event.remove(engine, "before_cursor_execute", mismatch)
    assert state(storage, day)["state"] == "open"
    assert pool_timeout(storage)[1] == 0


def test_count_allowance_does_not_expand_writer_lock_deadline(storage, tmp_path, monkeypatch):
    day = seed(storage, monkeypatch)
    archive = owner(storage, tmp_path, normal_ms=150)
    failures = []

    def seal():
        try:
            archive.seal_partition(day, count_statement_timeout_ms=60_000,
                                   remaining_run_seconds=lambda: 600)
        except Exception as exc:
            failures.append(exc)

    with storage.database.session() as writer:
        storage.repo.ingest_facts_in_session(writer, series_id=storage.series_id,
            source_id=storage.source_id, facts=[replace(storage.fact, observation_key="inflight")])
        thread = Thread(target=seal)
        thread.start()
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert len(failures) == 1 and failures[0].orig.pgcode == "57014"
    result = archive.seal_partition(day, count_statement_timeout_ms=60_000,
                                    remaining_run_seconds=lambda: 600)
    assert result["state"] == "sealed" and result["expected_rows"] == 2


@pytest.mark.parametrize("limit", [0, -1, True, 60_001, 1.5])
def test_invalid_count_allowance_refuses_before_transaction(storage, tmp_path, limit):
    with pytest.raises(ValueError, match="canonical_archive_seal_count_timeout_invalid"):
        owner(storage, tmp_path).seal_partition(storage.today - timedelta(days=2),
                                               count_statement_timeout_ms=limit)
