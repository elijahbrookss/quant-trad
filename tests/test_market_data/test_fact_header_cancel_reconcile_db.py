"""Native terminal receipt reconciliation; no operator or scale qualification."""
from copy import deepcopy

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from scripts.db import fact_header_v2_cancel as cancel
from scripts.db import fact_header_v2_capture as capture
from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK
from tests.test_market_data.test_fact_header_copy_db import source, _insert, _frozen_records
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db
INTENT = "a" * 64


@pytest.fixture
def captured(source, request):
    engine = source.database._engine
    with engine.begin() as conn:
        capture.install_capture(conn, attempt_seconds=getattr(request, "param", 86400))
        started = capture.inspect_capture(conn)["started_at"]
        original = cancel._capture_binding(conn)
    if getattr(request, "param", 86400) == 1:
        from time import sleep
        sleep(1.1)
        with pytest.raises(RuntimeError, match="attempt_expired"), engine.begin() as conn:
            capture.capture_remaining_seconds(conn)
    return source, engine, dict(expected_started_at=started,
                               expected_capture=original, intent_sha256=INTENT)


def inspect(conn, args):
    return cancel.inspect_cancellation(conn, **{k:args[k] for k in ("expected_capture", "intent_sha256")})


@pytest.mark.parametrize("captured", [86400, 1], indirect=True)
def test_lost_commit_reply_is_reconciled_read_only_and_never_replayed(captured, monkeypatch):
    source, engine, args = captured
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        assert inspect(conn, args) is None
        assert capture.inspect_capture(conn)["capture_active"]
    original_commit = Connection._commit_impl
    def lost_reply(conn):
        original_commit(conn)
        raise RuntimeError("terminal commit reply lost")
    with monkeypatch.context() as lost:
        lost.setattr(Connection, "_commit_impl", lost_reply)
        with pytest.raises(RuntimeError, match="commit reply lost"), engine.begin() as conn:
            receipt = cancel.cancel_attempt(conn, **args)
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        assert inspect(conn, args) == receipt
        assert receipt["schema_version"] == "qt.fact_header_cancel.v2"
        assert _frozen_records(conn) == source.frozen_before
        queued = conn.scalar(text(f"SELECT count(*) FROM {capture.QUEUE}"))
    with engine.begin() as conn:
        _insert(conn, source, "new-fact-after-terminal-commit")
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        assert inspect(conn, args) == receipt
        assert conn.scalar(text(f"SELECT count(*) FROM {capture.QUEUE}")) == queued
        assert cancel._capture_binding(conn) == args["expected_capture"]
    with pytest.raises(RuntimeError, match="attempt_cancelled"), engine.begin() as conn:
        cancel.cancel_attempt(conn, **args)


@pytest.mark.parametrize("drift", ["intent", "capture", "trigger", "receipt"])
def test_reconciliation_refuses_foreign_intent_and_database_drift(captured, drift):
    _, engine, args = captured
    with engine.begin() as conn:
        cancel.cancel_attempt(conn, **args)
    supplied = deepcopy(args)
    with engine.begin() as conn:
        if drift == "intent":
            supplied["intent_sha256"] = "b" * 64
        elif drift == "capture":
            conn.exec_driver_sql(f"UPDATE {capture.STATE} SET attempt_seconds=attempt_seconds+1")
        elif drift == "trigger":
            conn.exec_driver_sql("ALTER TABLE market.fact_versions DISABLE TRIGGER trg_reject_mutation_fact_versions")
        else:
            conn.exec_driver_sql(f"UPDATE {capture.CANCELLED} SET receipt=jsonb_set(receipt,'{{source_retained}}','false')")
    with pytest.raises(RuntimeError, match="changed"), engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        inspect(conn, supplied)


def test_changed_preimage_refuses_before_cancellation(captured):
    _, engine, args = captured
    altered = deepcopy(args)
    altered["expected_capture"]["attempt_seconds"] += 1
    with pytest.raises(RuntimeError, match="preimage_changed"), engine.begin() as conn:
        cancel.cancel_attempt(conn, **altered)
    with engine.begin() as conn:
        assert capture.inspect_capture(conn)["capture_active"]
        assert not cancel._exists(conn, capture.CANCELLED)


def test_idle_controller_excludes_terminal_execution_and_inspection(captured):
    _, engine, args = captured
    with engine.connect() as owner:
        owner.execute(text("SELECT pg_advisory_lock(hashtextextended(:name,0))"), {"name":CONTROLLER_LOCK})
        owner.commit()
        try:
            for operation in (lambda c: cancel.cancel_attempt(c, **args), lambda c: inspect(c, args)):
                with pytest.raises(RuntimeError, match="controller_active"), engine.begin() as conn:
                    operation(conn)
        finally:
            owner.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"), {"name":CONTROLLER_LOCK})
            owner.commit()
    with engine.begin() as conn:
        assert capture.inspect_capture(conn)["capture_active"]
        assert not cancel._exists(conn, capture.CANCELLED)
