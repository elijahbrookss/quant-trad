"""Real transaction, owner exclusion and capture continuity during amendment."""
from datetime import datetime

import pytest
from sqlalchemy import event, text

from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_deadline as amendment
from tests.test_market_data.test_fact_header_capture_db import capture_source, insert, ids

pytestmark = pytest.mark.db


def setup_attempt(engine, seconds=60*3600):
    with engine.begin() as conn:
        capture.install_capture(conn, attempt_seconds=seconds)
        return conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c"))


def change(conn, original, seconds=96*3600):
    return amendment.amend_capture_deadline(conn, expected_capture=original,
        attempt_seconds=seconds, intent_sha256="a"*64)


def test_explicit_amendment_preserves_start_queue_and_concurrent_collection(capture_source):
    engine = capture_source
    original = setup_attempt(engine)
    with engine.begin() as conn:
        insert(conn, "before")
        receipt = change(conn, original)
        # An independent collector transaction can still commit while the
        # amendment transaction is open; no source table writer lock is held.
        with engine.begin() as writer:
            insert(writer, "during", 3)
    engine.dispose()
    with engine.begin() as conn:
        assert amendment.inspect_amendment(conn) == receipt
        actual = conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c"))
        assert actual == {**original, "attempt_seconds": 96*3600}
        resumed_start = capture.install_capture(conn, attempt_seconds=10)["started_at"]
        assert datetime.fromisoformat(resumed_start) == datetime.fromisoformat(original["prepared_at"])
        with pytest.raises(RuntimeError, match="capture_changed"):
            change(conn, original)
    assert ids(engine, capture.QUEUE) == ["before", "during"]
    assert ids(engine, capture.SOURCE) == ["before", "during", "existing"]


def test_amendment_cannot_race_live_controller_or_capture_page(capture_source):
    engine = capture_source
    original = setup_attempt(engine)
    for name, error in [(amendment.CONTROLLER_LOCK, "controller_active"), (capture.LOCK, "migration_busy")]:
        with engine.begin() as owner:
            owner.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"), {"name": name})
            with engine.begin() as conn:
                with pytest.raises(RuntimeError, match=error):
                    change(conn, original)
                assert amendment.inspect_amendment(conn) is None
                assert conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c")) == original


@pytest.mark.parametrize("state,error", [("expired", "attempt_expired"), ("future", "start_time_invalid"), ("cancelled", "attempt_cancelled")])
def test_invalid_attempt_never_revives(capture_source, state, error):
    engine = capture_source
    setup_attempt(engine)
    with engine.begin() as conn:
        if state == "cancelled":
            conn.exec_driver_sql(f"CREATE TABLE {capture.CANCELLED}(id integer)")
        else:
            interval = "- interval '61 hours'" if state == "expired" else "+ interval '1 hour'"
            conn.exec_driver_sql(f"UPDATE {capture.STATE} SET prepared_at=clock_timestamp() {interval}")
        original = conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c"))
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match=error):
            change(conn, original)
        assert amendment.inspect_amendment(conn) is None
        assert conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c")) == original
        insert(conn, "still-collecting")


def test_interrupted_amendment_rolls_back_duration_and_audit_together(capture_source):
    engine = capture_source
    original = setup_attempt(engine)
    def fail(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith(f"UPDATE {capture.STATE} SET attempt_seconds"):
            raise RuntimeError("injected after duration change")
    event.listen(engine, "after_cursor_execute", fail)
    try:
        with engine.begin() as conn:
            with pytest.raises(RuntimeError, match="injected"):
                change(conn, original)
            assert amendment.inspect_amendment(conn) is None
            assert conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c")) == original
    finally:
        event.remove(engine, "after_cursor_execute", fail)
    with engine.begin() as conn:
        assert change(conn, original)["before"] == original


@pytest.mark.parametrize("seconds", [True, 0, 60*3600, 97*3600])
def test_bounds_do_not_change_ordinary_retry_semantics(capture_source, seconds):
    original = setup_attempt(capture_source)
    with capture_source.begin() as conn:
        with pytest.raises(ValueError, match="amendment_invalid"):
            change(conn, original, seconds)
        assert amendment.inspect_amendment(conn) is None
        assert conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c")) == original
