"""Online copy yields to live intake; it never certifies a final switch."""
from dataclasses import replace
from datetime import timedelta
import os

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from scripts.db import fact_header_v2_online as online
from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as raw
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_header_copy_placement_db import placed, source
from tests.test_market_data.test_fact_header_copy_db import _insert, _frozen_records
from tests.test_market_data.test_fact_storage_tiers_db import storage
from tests.test_market_data.test_raw_mapping_copy_db import _rows
from tests.test_market_data.test_fact_raw_lineage_db import _raw_trade_fixture, _raw_book_fixture
from tests.test_market_data.test_archive_reference_placement_db import _options

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                      reason="requires owned SSD/HDD storage-demo topology"),
]


def _prepare(placed, tmp_path, monkeypatch):
    _raw_trade_fixture(placed, tmp_path, monkeypatch)
    engine = placed.database._engine
    placed.copy_plan = replace(placed.copy_plan, history_before=placed.today-timedelta(days=30))
    with engine.begin() as conn:
        headers.prepare_copy(conn, placement=placed.copy_plan)
        raw.prepare_copy(conn)
    options = _options(placed)
    options["policy"] = replace(options["policy"], movement_enabled=True, backup_enabled=True)
    return engine, dict(placement=placed.copy_plan, page_rows=2,
                        max_pages=2, max_duration_seconds=60, **options)


def _advance(engine, options):
    for _ in range(32):
        result = online.copy_pass(engine, **options)
        assert sum(result["committed_pages"].values()) <= options["max_pages"]
        assert not result["migration_ready"] and not result["final_switch_authorized"]
        if result["outcome"] == "identity_relocation_required":
            with engine.begin() as conn:
                headers.place_identity_on_history(conn)
        elif result["outcome"] == "raw_relocation_required":
            with engine.begin() as conn:
                raw.place_on_history(conn)
        elif result["outcome"] == "both_tails_observed_empty":
            return result
    pytest.fail("bounded fixture passes did not converge")


def test_online_pass_commits_progress_with_concurrent_intake_and_preserves_source(
        placed, tmp_path, monkeypatch):
    engine, options = _prepare(placed, tmp_path, monkeypatch)
    with engine.connect() as conn:
        original_start = conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture"))
        frozen = _frozen_records(conn)
    inserted = [False]

    def during_copy(conn, cursor, statement, parameters, context, executemany):
        if not inserted[0] and statement.startswith("INSERT INTO "+SCHEMA+".fact_versions "):
            inserted[0] = True
            # Publish in another transaction while the copy transaction is open.
            # The insert is captured in its own transaction while source reads
            # and the private copy transaction remain open.
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                _insert(writer, placed, "online-late-commit")

    event.listen(engine, "after_cursor_execute", during_copy)
    try:
        first = online.copy_pass(engine, **options)
    finally:
        event.remove(engine, "after_cursor_execute", during_copy)
    assert inserted[0]
    assert first["committed_pages"]["headers"] == 2
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT after_id FROM {headers.STATE}")) is not None
        assert not conn.scalar(text(f"SELECT baseline_complete FROM {headers.STATE}"))
        assert _frozen_records(conn) == frozen
    _raw_book_fixture(placed, tmp_path, monkeypatch, definition_id="online-late-raw")
    _advance(engine, options)
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture")) == original_start
        assert _rows(conn) == _rows(conn, raw.TARGET)
        assert _frozen_records(conn) == frozen
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions "
                                "WHERE observation_key='online-late-commit'")) == 1
    # A new batch after an empty observation invalidates any inference of readiness.
    with engine.begin() as conn:
        _insert(conn, placed, "after-empty-observation")
    result = _advance(engine, options)
    assert not result["final_switch_authorized"]
    assert placed.archive_path.read_bytes() == placed.archive_bytes


def test_online_pass_interrupted_page_rolls_back_without_losing_earlier_pages(
        placed, tmp_path, monkeypatch):
    engine, options = _prepare(placed, tmp_path, monkeypatch)
    copied = [0]

    def kill_second(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO "+SCHEMA+".fact_versions "):
            copied[0] += 1
            if copied[0] == 2:
                with engine.begin() as killer:
                    assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                        {"pid": conn.connection.driver_connection.get_backend_pid()})
                conn.exec_driver_sql("SELECT 1")

    event.listen(engine, "after_cursor_execute", kill_second)
    try:
        with pytest.raises(DBAPIError):
            online.copy_pass(engine, **options)
    finally:
        event.remove(engine, "after_cursor_execute", kill_second)
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT verified_rows FROM {headers.STATE}")) == 2
        before = conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture"))
    with pytest.raises(RuntimeError, match="storage_move_cancelled"):
        online.copy_pass(engine, **options, cancelled=lambda: True)
    _advance(engine, options)
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture")) == before
        assert _rows(conn) == _rows(conn, raw.TARGET)


def test_online_pass_refuses_expired_original_attempt(placed, tmp_path, monkeypatch):
    engine, options = _prepare(placed, tmp_path, monkeypatch)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"UPDATE {SCHEMA}.capture SET prepared_at=clock_timestamp()-interval '5 days'")
    with pytest.raises(RuntimeError, match="attempt_expired"):
        online.copy_pass(engine, **options)
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT verified_rows FROM {headers.STATE}")) == 0


def test_online_empty_observation_does_not_lose_late_commit_or_starve_raw(
        placed, tmp_path, monkeypatch):
    engine, options = _prepare(placed, tmp_path, monkeypatch)
    _advance(engine, options)
    # Allocated sequence order is not commit order. The lower sequence remains
    # invisible through a complete catch-up pass; its queue entry commits later.
    with engine.begin() as early:
        early.exec_driver_sql("SET LOCAL statement_timeout='2s'")
        first = _insert(early, placed, "earlier-sequence-later-commit")
        with engine.begin() as later:
            second = _insert(later, placed, "later-sequence-earlier-commit")
        assert first["market_commit_seq"] < second["market_commit_seq"]
        observed = _advance(engine, options)
        assert observed["outcome"] == "both_tails_observed_empty"
        assert not observed["final_switch_authorized"]
    _advance(engine, options)
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions WHERE id=:id"),
                           {"id": first["id"]}) == 1

    with engine.begin() as conn:
        _insert(conn, placed, "tail-seed")
    published = [0]

    def growing_header_tail(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO "+SCHEMA+".fact_versions "):
            published[0] += 1
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                _insert(writer, placed, "growing-tail-"+str(published[0]))

    event.listen(engine, "after_cursor_execute", growing_header_tail)
    try:
        report = online.copy_pass(engine, **(options | {"max_pages": 4}))
    finally:
        event.remove(engine, "after_cursor_execute", growing_header_tail)
    assert report["outcome"] == "page_budget_reached"
    assert report["committed_pages"] == {"headers": 2, "raw": 2}
    assert published[0] == 2
    _advance(engine, options)


def test_online_pass_refuses_changed_recent_window_and_placement(placed, tmp_path, monkeypatch):
    engine, options = _prepare(placed, tmp_path, monkeypatch)
    with pytest.raises(RuntimeError, match="recent_window_on_history"):
        online.copy_pass(engine, **(options | {
            "policy": replace(options["policy"], recent_days=60)}))
    with pytest.raises(RuntimeError, match="placement_changed"):
        online.copy_pass(engine, **(options | {
            "placement": replace(placed.copy_plan,
                                 history_before=placed.copy_plan.history_before-timedelta(days=1))}))
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT verified_rows FROM {headers.STATE}")) == 0
