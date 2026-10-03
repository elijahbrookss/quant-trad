"""Native online key preparation after an exactly reconciled cancellation."""

import pytest
from sqlalchemy import event, text

from scripts.db import fact_header_forward_keys as forward
from scripts.db import fact_header_v2_capture as capture, fact_header_v2_copy as copy
from scripts.db import fact_header_v2_cancel as cancellation
from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK
from tests.test_market_data.test_fact_header_copy_db import source, _headers, _frozen_records, _insert
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


@pytest.fixture
def cancelled(source):
    engine = source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
        original = cancellation._capture_binding(conn)
        cancellation.cancel_attempt(conn, expected_started_at=capture.inspect_capture(conn)["started_at"],
            expected_capture=original, intent_sha256="a"*64)
    source.cancelled_capture = original
    return source


def prepare(source, **kwargs):
    return forward.prepare_keys(source.database._engine,
        expected_capture=source.cancelled_capture, intent_sha256="a"*64, **kwargs)


def state(source):
    with source.database._engine.connect() as conn:
        return forward._read_state(conn)


def test_forward_keys_preserve_heap_search_indexes_capture_and_native_v1_writes(cancelled):
    source = cancelled
    engine = source.database._engine
    with engine.begin() as conn:
        original = forward._source_binding(conn, source.cancelled_capture, "a"*64)
        frozen = _frozen_records(conn)
        old_queue = conn.execute(text("SELECT id FROM "+capture.QUEUE+" ORDER BY id")).scalars().all()
    inserted = []
    def writer(conn,cursor,statement,parameters,context,executemany):
        if statement.startswith("CREATE UNIQUE INDEX CONCURRENTLY") and not inserted:
            # The restored source is v1; current repository ingestion requires
            # the v2 identity parent, which is deliberately absent before handoff.
            with engine.begin() as publisher:
                inserted.append(_insert(publisher,source,"during-forward-key-preparation")["id"])
    event.listen(engine,"before_cursor_execute",writer)
    try:
        result = prepare(source)
    finally:
        event.remove(engine,"before_cursor_execute",writer)
    assert len(inserted) == 1
    assert result["keys_prepared"] and not result["migration_ready"] and not result["final_switch_authorized"]
    initial = state(source)
    assert initial["complete"] and set(initial["index_oids"]) == set(forward.KEYS)
    assert prepare(source)["reused"]
    assert state(source) == initial
    with engine.begin() as conn:
        assert forward._source_binding(conn,source.cancelled_capture,"a"*64) == original
        assert _frozen_records(conn) == frozen
        assert conn.execute(text("SELECT id FROM "+capture.QUEUE+" ORDER BY id")).scalars().all() == old_queue
        assert cancellation._capture_binding(conn) == source.cancelled_capture
        assert len(_headers(conn,'market.fact_versions')) == len(source.source_before)+1
        assert conn.scalar(text("SELECT to_regclass('market.fact_header_legacy')")) is None
        assert conn.scalar(text("SELECT count(*) FROM pg_trigger WHERE tgrelid='market.fact_versions'::regclass "
            "AND tgname LIKE 'trg_qt_header_v2_%'")) == 0


def test_valid_concurrent_index_commit_is_reconciled_without_rebuild_or_deadline_reset(cancelled):
    engine = cancelled.database._engine
    def lose_reply(conn,cursor,statement,parameters,context,executemany):
        if statement.startswith("CREATE UNIQUE INDEX CONCURRENTLY "+next(iter(forward.KEYS))):
            raise RuntimeError("lost concurrent index reply")
    event.listen(engine,"after_cursor_execute",lose_reply)
    try:
        with pytest.raises(RuntimeError,match="lost concurrent index reply"):
            prepare(cancelled)
    finally:
        event.remove(engine,"after_cursor_execute",lose_reply)
    partial = state(cancelled)
    with engine.begin() as conn:
        first_oid = forward.inspect_keys(conn)[next(iter(forward.KEYS))]
    assert first_oid and not partial["complete"]
    assert prepare(cancelled)["keys_prepared"]
    complete = state(cancelled)
    assert complete["started_at"] == partial["started_at"]
    assert complete["expires_at"] == partial["expires_at"]
    assert complete["index_oids"][next(iter(forward.KEYS))] == first_oid


def test_foreign_controller_and_uncommitted_cancellation_refuse(source):
    engine = source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
        original = cancellation._capture_binding(conn)
    with pytest.raises(RuntimeError,match="committed_cancellation_required"):
        forward.prepare_keys(engine,expected_capture=original,intent_sha256="a"*64)
    with engine.begin() as owner:
        owner.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"),{"name":CONTROLLER_LOCK})
        with pytest.raises(RuntimeError,match="controller_active"):
            forward.prepare_keys(engine,expected_capture=original,intent_sha256="a"*64)
    with engine.begin() as conn:
        assert conn.scalar(text("SELECT to_regnamespace(:name)"),{"name":forward.SCHEMA}) is None


@pytest.mark.parametrize("damage",["foreign_key","invalid_key","replaced_key","clock_expired"])
def test_changed_or_expired_preparation_refuses_without_repair(cancelled,damage):
    engine=cancelled.database._engine
    first=next(iter(forward.KEYS))
    if damage=="foreign_key":
        with engine.begin() as conn:
            conn.exec_driver_sql("CREATE UNIQUE INDEX "+first+" ON market.fact_versions(id,storage_day)")
        expected="unowned_preparation_key"
    else:
        prepare(cancelled)
        with engine.begin() as conn:
            if damage=="invalid_key":
                conn.exec_driver_sql("UPDATE pg_index SET indisvalid=false WHERE indexrelid='market."+first+"'::regclass")
                expected="key_changed_or_incomplete"
            elif damage=="replaced_key":
                conn.exec_driver_sql("DROP INDEX market."+first)
                conn.exec_driver_sql("CREATE UNIQUE INDEX "+first+" ON market.fact_versions(id,storage_day)")
                expected="index_replaced"
            else:
                # Owned-fixture fault injection, never a production clock edit.
                conn.exec_driver_sql("UPDATE "+forward.STATE+" SET complete=false,started_at=started_at-interval '2 hours',expires_at=expires_at-interval '2 hours'")
                expected="preparation_expired"
    with pytest.raises(RuntimeError,match=expected):
        prepare(cancelled)
    with engine.begin() as conn:
        assert conn.scalar(text("SELECT to_regclass('market."+first+"')")) is not None
        assert cancellation._capture_binding(conn)==cancelled.cancelled_capture
        assert _headers(conn,'market.fact_versions') == cancelled.source_before
