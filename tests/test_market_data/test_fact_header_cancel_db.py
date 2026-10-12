"""Terminal cancellation preserves source intake and all partial evidence."""
from datetime import timedelta
import os

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from scripts.db import fact_header_v2_cancel as cancel
from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_copy as headers
from scripts.db import fact_header_v2_references as references
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db import fact_header_v2_online_proof as proof
from scripts.db import raw_mapping_v2_copy as raw
from tests.test_market_data.test_archive_online_copy_db import _prepare, online, _synthetic_descriptor
from tests.test_market_data.test_archive_root_copy_db import _hashes
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import BASE
from tests.test_market_data.test_fact_header_copy_db import _frozen_records, _insert
from tests.test_market_data.test_fact_header_online_proof_db import _protected
from tests.test_market_data.test_fact_header_online_references_db import _ordinary, _prepare_slots
from tests.test_market_data.test_fact_header_copy_placement_db import placed, source
from tests.test_market_data.test_fact_storage_tiers_db import storage
from tests.test_market_data.tiered_v1_fixture import ensure_v1_payload_partition

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                reason="requires owned SSD/HDD storage-demo topology")]


def _counts(conn):
    return {name: conn.scalar(text(f"SELECT count(*) FROM {name}"))
            for name in (capture.QUEUE, raw.QUEUE, online.QUEUE,
                         capture.SCHEMA+".fact_identities", capture.SCHEMA+".fact_versions",
                         raw.TARGET)}


def test_cancel_expired_archive_attempt_preserves_rows_and_stops_dead_queues(
        storage, tmp_path, monkeypatch):
    engine, options, source_root, _ = _prepare(storage, tmp_path, monkeypatch)
    roots = {k: options[k] for k in ("source_root", "destination_root")}
    with engine.begin() as conn:
        proof.prepare(conn)
    handoff.stage_handoff(engine, placement=storage.copy_plan,
                          max_duration_seconds=120, **options)
    # Simulate an expired fixture by shortening only its saved budget. Original
    # timestamp and archive capture binding remain intact; cleanup never edits
    # either field. This mutation is test setup, not an operator escape hatch.
    with engine.begin() as conn:
        conn.exec_driver_sql(f"UPDATE {capture.STATE} SET attempt_seconds=1")
        saved = dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())
        with pytest.raises(RuntimeError, match="attempt_expired"):
            capture.capture_remaining_seconds(conn)
        expected = saved["prepared_at"].isoformat()
        frozen = _frozen_records(conn)
        before = _counts(conn)
    source_hashes, destination_hashes = _hashes(roots["source_root"]), _hashes(roots["destination_root"])
    killed = [False]
    def kill(conn, cursor, statement, parameters, context, executemany):
        if not killed[0] and statement.startswith("DROP TRIGGER trg_qt_header_v2_capture "):
            killed[0] = True
            with engine.begin() as killer:
                killer.execute(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid": conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", kill)
    try:
        with pytest.raises(DBAPIError), engine.begin() as conn:
            cancel.cancel_attempt(conn, expected_started_at=expected, **roots)
    finally:
        event.remove(engine, "after_cursor_execute", kill)
    assert killed[0]
    with engine.begin() as conn:
        online._inspect(conn, **roots)
        assert references._states(conn, references._inventory(conn))[references.PARENT]
        assert _counts(conn) == before
        assert not cancel._exists(conn, capture.CANCELLED)
    original_commit = Connection._commit_impl
    def lost_reply(conn):
        original_commit(conn)
        raise RuntimeError("cancel commit reply lost")
    with monkeypatch.context() as lost:
        lost.setattr(Connection, "_commit_impl", lost_reply)
        with pytest.raises(RuntimeError, match="commit reply lost"), engine.begin() as conn:
            receipt = cancel.cancel_attempt(conn, expected_started_at=expected, **roots)
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        assert conn.scalar(text(f"SELECT receipt FROM {capture.CANCELLED}")) == receipt
        assert dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one()) == saved
        assert _counts(conn) == before and _frozen_records(conn) == frozen
    # A late publisher can commit after cancellation; its manifest no longer
    # appends to the abandoned queue. All existing original bytes stay intact.
    with engine.begin() as conn:
        _synthetic_descriptor(conn, source_root, "!after-cancel")
        raw_before = conn.scalar(text(f"SELECT count(*) FROM {raw.SOURCE}"))
    _raw_book_fixture(storage, source_root, monkeypatch,
                      definition_id="cancel-live-publication",
                      provider_product_id="BTC-USD-CANCEL",
                      event_start=BASE+timedelta(hours=1))
    with engine.begin() as conn:
        assert conn.scalar(text(f"SELECT count(*) FROM {raw.SOURCE}")) > raw_before
        assert _counts(conn) == before and _frozen_records(conn) == frozen
        for operation in (lambda: headers.prepare_copy(conn, placement=storage.copy_plan),
                          lambda: raw.copy_page(conn), lambda: proof.prepare(conn),
                          lambda: online.prepare(conn, **roots),
                          lambda: references.inspect_references(conn),
                          lambda: capture.capture_remaining_seconds(conn)):
            with pytest.raises(RuntimeError, match="attempt_cancelled"):
                operation()
    assert all(_hashes(roots["source_root"])[key] == value for key, value in source_hashes.items())
    assert _hashes(roots["destination_root"]) == destination_hashes


def test_cancel_removes_inherited_staged_references_before_identity_mirror(
        placed, tmp_path, monkeypatch):
    engine, _ = _protected(placed, tmp_path, monkeypatch)
    ordinary = _ordinary(engine)
    _prepare_slots(engine, ordinary)
    with engine.begin() as conn:
        for relation in ordinary:
            references.validate_reference(conn, relation=relation)
        references.adopt_payload_references(conn)
    placed.open_day += timedelta(days=3)
    with engine.begin() as conn:
        leaf = ensure_v1_payload_partition(conn, placed.open_day)
        _insert(conn, placed, "before-cancel-new-leaf")
        assert references._existing(conn, references._inventory(conn)[leaf])["parent_oid"]
        expected = conn.scalar(text(f"SELECT prepared_at FROM {capture.STATE}")).isoformat()
        counts = [conn.scalar(text(f"SELECT count(*) FROM {name}"))
                  for name in (capture.QUEUE, capture.SCHEMA+".fact_identities")]
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="binding_changed"):
            cancel.cancel_attempt(conn, expected_started_at="different attempt")
        with conn.begin_nested() as rollback:
            cancel.cancel_attempt(conn, expected_started_at=expected)
            rollback.rollback()
        assert references.inspect_references(conn)["references_complete"]
    # A source writer already holding the table refuses cleanup without waiting
    # or disturbing that writer's eventual commit.
    with engine.begin() as writer:
        _insert(writer, placed, "cancel-busy-source")
        with pytest.raises(DBAPIError), engine.begin() as conn:
            cancel.cancel_attempt(conn, expected_started_at=expected)
    with engine.begin() as conn:
        counts = [conn.scalar(text(f"SELECT count(*) FROM {name}"))
                  for name in (capture.QUEUE, capture.SCHEMA+".fact_identities")]
        cancel.cancel_attempt(conn, expected_started_at=expected)
    with engine.begin() as writer:
        row = _insert(writer, placed, "after-cancel-source-still-serves")
        writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
        with pytest.raises(DBAPIError), writer.begin_nested():
            writer.execute(text("DELETE FROM market.fact_versions WHERE id=:id"), {"id": row["id"]})
    placed.open_day += timedelta(days=1)
    with engine.begin() as conn:
        leaf = ensure_v1_payload_partition(conn, placed.open_day)
        _insert(conn, placed, "after-cancel-next-day")
        assert [conn.scalar(text(f"SELECT count(*) FROM {name}"))
                for name in (capture.QUEUE, capture.SCHEMA+".fact_identities")] == counts
        assert conn.scalar(text("""SELECT count(*) FROM pg_constraint
            WHERE conrelid=to_regclass(:leaf) AND contype='f'
              AND confrelid='market.fact_versions'::regclass"""), {"leaf": leaf}) == 1
        assert conn.scalar(text("""SELECT count(*) FROM pg_constraint
            WHERE conrelid=to_regclass(:leaf) AND conname=:name"""),
            {"leaf": leaf, "name": references.STAGED}) == 0
        assert _frozen_records(conn) == placed.frozen_before


def test_capture_only_cancel_after_natural_expiry_is_bounded_and_refuses_drift(source):
    from time import sleep
    engine = source.database._engine
    with engine.begin() as conn:
        capture.install_capture(conn, attempt_seconds=1)
        expected = capture.inspect_capture(conn)["started_at"]
    sleep(1.1)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="attempt_expired"):
            capture.capture_remaining_seconds(conn)
        with conn.begin_nested() as rollback:
            conn.exec_driver_sql("ALTER TABLE market.fact_versions "
                                 "DISABLE TRIGGER trg_qt_header_v2_capture")
            with pytest.raises(RuntimeError, match="trigger_changed"):
                cancel.cancel_attempt(conn, expected_started_at=expected)
            assert not cancel._exists(conn, capture.CANCELLED)
            rollback.rollback()
    injected = [False]
    def exceed_budget(conn, cursor, statement, parameters, context, executemany):
        if not injected[0] and statement.startswith("DROP TRIGGER trg_qt_header_v2_capture "):
            injected[0] = True
            conn.exec_driver_sql("SELECT pg_sleep(2)")
    event.listen(engine, "after_cursor_execute", exceed_budget)
    try:
        with pytest.raises((RuntimeError, DBAPIError), match="statement timeout|step_timeout"):
            with engine.begin() as conn:
                cancel.cancel_attempt(conn, expected_started_at=expected, timeout_seconds=1)
    finally:
        event.remove(engine, "after_cursor_execute", exceed_budget)
    assert injected[0]
    with engine.begin() as conn:
        assert capture.inspect_capture(conn)["started_at"] == expected
        _insert(conn, source, "expired-attempt-still-captures-before-cancel")
        queued = conn.scalar(text(f"SELECT count(*) FROM {capture.QUEUE}"))
        receipt = cancel.cancel_attempt(conn, expected_started_at=expected)
        assert receipt["capture"]["attempt_seconds"] == 1
    with engine.begin() as conn:
        _insert(conn, source, "expired-attempt-source-serves-after-cancel")
        assert conn.scalar(text(f"SELECT count(*) FROM {capture.QUEUE}")) == queued
        with pytest.raises(RuntimeError, match="attempt_cancelled"):
            capture.install_capture(conn, attempt_seconds=96*3600)
        assert _frozen_records(conn) == source.frozen_before
