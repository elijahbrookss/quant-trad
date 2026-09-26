"""Atomic preparation on the owned QT SSD/HDD fixture; no host cutover."""
from datetime import timedelta
import os

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_online as online
from scripts.db import fact_header_v2_copy as headers
from scripts.db import raw_mapping_v2_copy as raw
from scripts.db import fact_header_v2_online_proof as protection
from scripts.db import archive_root_v2_online as archives
from tests.test_market_data.test_archive_online_copy_db import _prepare
from tests.test_market_data.test_fact_header_copy_db import _frozen_records
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import BASE, storage

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                reason="requires owned SSD/HDD storage-demo topology")]


def _setup(storage, tmp_path, monkeypatch):
    engine, options, source, _ = _prepare(
        storage, tmp_path, monkeypatch, prepare_captures=False)
    options.pop("page_rows")
    options.pop("max_page_bytes")
    return engine, dict(placement=storage.copy_plan, attempt_seconds=180, **options), source


def _saved(conn):
    return conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c"))


def test_online_preparation_retry_preserves_original_clock_and_live_publication(
        storage, tmp_path, monkeypatch):
    engine, options, source = _setup(storage, tmp_path, monkeypatch)
    with engine.connect() as conn:
        frozen = _frozen_records(conn)
    result = online.prepare_attempt(engine, **options)
    assert not result["migration_ready"] and not result["final_switch_authorized"]
    with engine.begin() as conn:
        original = _saved(conn)
        assert original["attempt_seconds"] == 180
        protection.inspect_protection(conn)
        assert conn.scalar(text(f"SELECT verified_rows FROM {headers.STATE}")) == 0
        assert conn.scalar(text(f"SELECT verified_rows FROM {raw.STATE}")) == 0
    _raw_book_fixture(storage, source, monkeypatch,
                      definition_id="online-atomic-live",
                      provider_product_id="BTC-USD-ATOMIC",
                      event_start=BASE+timedelta(hours=2))
    retry = online.prepare_attempt(engine, **(options | {"attempt_seconds": 96*3600}))
    assert retry["started_at"] == result["started_at"]
    with engine.begin() as conn:
        assert _saved(conn) == original
        assert _frozen_records(conn) == frozen
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {archives.QUEUE})"))
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {raw.QUEUE})"))
    # An early cancellation cannot change the admitted original attempt.
    with pytest.raises(RuntimeError, match="storage_move_cancelled"):
        online.prepare_attempt(engine, **options, cancelled=lambda: True)
    with engine.connect() as conn:
        assert _saved(conn) == original


def test_online_preparation_killed_transaction_leaves_no_partial_capture(
        storage, tmp_path, monkeypatch):
    engine, options, source = _setup(storage, tmp_path, monkeypatch)
    killed = []
    def terminate(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("CREATE TABLE "+archives.STATE+"("):
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid": conn.connection.driver_connection.get_backend_pid()})
            killed.append(True)
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", terminate)
    try:
        with pytest.raises(DBAPIError):
            online.prepare_attempt(engine, **options)
    finally:
        event.remove(engine, "after_cursor_execute", terminate)
    assert killed
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT to_regnamespace(:name)"),
                           {"name": capture.SCHEMA}) is None
    # Normal source publication still works after the complete DDL rollback.
    _raw_book_fixture(storage, source, monkeypatch,
                      definition_id="online-atomic-after-kill",
                      provider_product_id="BTC-USD-ATOMIC-KILL",
                      event_start=BASE+timedelta(hours=3))
    online.prepare_attempt(engine, **options)


def test_online_preparation_lost_commit_reply_reuses_exact_captures(
        storage, tmp_path, monkeypatch):
    engine, options, _ = _setup(storage, tmp_path, monkeypatch)
    prepared = set()
    def observe(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("CREATE TABLE "+archives.STATE+"("):
            prepared.add(id(conn))
    commit = Connection._commit_impl
    def lose_reply(conn):
        commit(conn)
        if id(conn) in prepared:
            raise RuntimeError("atomic preparation reply lost after actual commit")
    event.listen(engine, "after_cursor_execute", observe)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(Connection, "_commit_impl", lose_reply)
            with pytest.raises(RuntimeError, match="reply lost"):
                online.prepare_attempt(engine, **options)
    finally:
        event.remove(engine, "after_cursor_execute", observe)
    with engine.connect() as conn:
        original = _saved(conn)
        oid = conn.scalar(text("SELECT to_regclass(:name)::oid"),
                          {"name": archives.STATE})
    online.prepare_attempt(engine, **options)
    with engine.connect() as conn:
        assert _saved(conn) == original
        assert conn.scalar(text("SELECT to_regclass(:name)::oid"),
                           {"name": archives.STATE}) == oid


def test_online_preparation_refuses_busy_source_without_partial_capture(
        storage, tmp_path, monkeypatch):
    engine, options, _ = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as writer:
        writer.exec_driver_sql("LOCK TABLE market.fact_versions IN ROW EXCLUSIVE MODE")
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            online.prepare_attempt(engine, **options)
        # The source transaction was neither canceled nor replaced by admission.
        assert writer.scalar(text("SELECT 1")) == 1
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT to_regnamespace(:name)"),
                           {"name": capture.SCHEMA}) is None
    online.prepare_attempt(engine, **options)
