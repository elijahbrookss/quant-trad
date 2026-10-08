"""Native post-handoff movement; tiny fixtures do not admit production duration."""
import os
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from core.storage_targets import StoragePolicy
from portal.backend.service.storage.recovery_copies import _snapshot_layout
from scripts.db import raw_mapping_v2_placement as move
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db import fact_header_v2_placement as physical
from scripts.db import raw_mapping_v2_copy as raw
from tests.test_market_data.test_archive_forward_copy_db import (
    native_storage, storage,
    test_forward_archives_preserve_canceled_work_and_fence_final_switch as prepare_handoff,
)
from tests.test_market_data.test_archive_reference_placement_db import _rows, _options
from tests.test_market_data.test_fact_header_copy_db import _frozen_records

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
    reason="requires owned two-filesystem storage-demo topology")]


def test_retained_raw_move_is_atomic_reconciles_duplicates_and_keeps_frozen_history(storage, tmp_path, monkeypatch):
    prepare_handoff(storage, tmp_path, monkeypatch, successor="retain_raw")
    engine = storage.database._engine
    with engine.begin() as conn:
        evidence = move._state(conn)
        receipt = move._handoff(evidence)
        before = move._files(conn)
        source_rows = _rows(conn, raw.SOURCE)
        assert source_rows
        retained_rows = _rows(conn, handoff.RETAINED + "." + raw.NAME)
        frozen = _frozen_records(conn)
        recovery_layout_before = _snapshot_layout(conn)
        policy = physical._restore(receipt["binding"]["plan"])
        assert policy.recent_lookup_indexes
        saved_policy = StoragePolicy.from_dict(conn.scalar(text("SELECT policy FROM public.portal_storage_policy WHERE id=1")))
    options = _options(storage)
    options["policy"] = saved_policy
    options["resource_limits"]["movement_timeout_seconds"] = 180
    # The actual cutover activated the same policy; no invented journal or
    # hand-edited receipt is used to enter the new operation.
    request_id = "native-raw-history-" + uuid4().hex
    args = dict(request_id=request_id, handoff_sha256=move._digest(receipt), **options)
    plan_id = move._plan_id(request_id)

    observed = move.inspect_retained_raw_history(engine, **args)
    assert observed["state"] == "not_started" and observed["copy_bytes"] > 0
    assert observed["inspection_only"] and not observed["execution_admitted"]
    assert observed["started_at"] is None and observed["expires_at"] is None
    assert observed["retained_ssd_indexes"] == ["pk_market_raw_archive_record_mapping"]
    with engine.begin() as conn:
        assert move._plan(conn, plan_id) is None
        assert move._files(conn) == before and _snapshot_layout(conn) == recovery_layout_before

    # A real reader prevents the exclusive move. The durable intent may exist,
    # but no bytes or authoritative relation identities may change.
    with engine.connect() as reader, reader.begin():
        reader.exec_driver_sql("LOCK TABLE " + raw.SOURCE + " IN ACCESS SHARE MODE")
        with pytest.raises(DBAPIError):
            move.move_retained_raw_history(engine, **args)
    with engine.begin() as conn:
        assert move._files(conn) == before
        original_plan = move._plan(conn, plan_id)
        assert original_plan["state"] == "running"
    with pytest.raises(RuntimeError, match="other_storage_plan_active"):
        move.move_retained_raw_history(engine, **{**args, "request_id":request_id + "x"})
    with pytest.raises(RuntimeError, match="intent_changed"):
        move.move_retained_raw_history(engine, **{**args, "resource_limits":{
            **options["resource_limits"], "wal_bytes":options["resource_limits"]["wal_bytes"] + 1}})

    # Kill only the disposable mover backend after the native heap move. The
    # aborted transaction must retain the old files and all rows, not a half move.
    def terminate(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER TABLE " + raw.SOURCE + " SET TABLESPACE"):
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid":conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", terminate)
    try:
        with pytest.raises(DBAPIError):
            move.move_retained_raw_history(engine, **args)
    finally:
        event.remove(engine, "after_cursor_execute", terminate)
    with engine.begin() as conn:
        assert move._files(conn) == before
        assert _rows(conn, raw.SOURCE) == source_rows
        assert move._plan(conn, plan_id) == original_plan

    # Cancellation is tied to the actual watcher, after at least one real DDL.
    cancel = [False]
    def request_cancel(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER TABLE " + raw.SOURCE + " SET TABLESPACE"):
            cancel[0] = True
    event.listen(engine, "after_cursor_execute", request_cancel)
    try:
        with pytest.raises(RuntimeError, match="storage_move_cancelled"):
            move.move_retained_raw_history(engine, cancelled=lambda:cancel[0], **args)
    finally:
        event.remove(engine, "after_cursor_execute", request_cancel)
    # Simulate elapsed time in this disposable journal while preserving the
    # admitted duration. Reentry must not mint a new attempt clock.
    with engine.begin() as conn:
        conn.execute(text("UPDATE " + move.PLANS + " SET progress=progress||jsonb_build_object("
            "'started_at',CAST(progress->>'started_at' AS timestamptz)-interval '1 hour',"
            "'expires_at',CAST(progress->>'expires_at' AS timestamptz)-interval '1 hour') WHERE id=:id"),
            {"id":plan_id})
        expired_plan = move._plan(conn, plan_id)
    with pytest.raises(RuntimeError, match="step_timeout"):
        move.move_retained_raw_history(engine, **args)
    with engine.begin() as conn:
        assert move._plan(conn, plan_id) == expired_plan
    closed = move.cancel_retained_raw_history(engine, request_id=request_id, handoff_sha256=args["handoff_sha256"])
    assert closed["state"] == "cancelled" and closed["source_preserved"]
    assert move.cancel_retained_raw_history(engine, request_id=request_id,
        handoff_sha256=args["handoff_sha256"])["reused"]
    with pytest.raises(RuntimeError, match="intent_changed"):
        move.move_retained_raw_history(engine, **args)

    # A distinct explicitly supplied request may follow that verified rollback.
    args = {**args, "request_id":"native-raw-history-" + uuid4().hex}
    plan_id = move._plan_id(args["request_id"])
    original_commit = Connection._commit_impl
    lost = [False]
    def lose_reply(conn):
        finished = conn.in_transaction() and conn.scalar(text("SELECT state FROM " + move.PLANS + " WHERE id=:id"),
                                                        {"id":plan_id}) == "completed"
        original_commit(conn)
        if finished and not lost[0]:
            lost[0] = True
            raise RuntimeError("lost raw placement commit reply")
    with monkeypatch.context() as patch:
        patch.setattr(Connection, "_commit_impl", lose_reply)
        with pytest.raises(RuntimeError, match="lost raw placement commit reply"):
            move.move_retained_raw_history(engine, **args)
    assert lost[0]
    def prohibit_replay(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("ALTER "):
            raise AssertionError("completed raw placement was replayed")
    event.listen(engine, "before_cursor_execute", prohibit_replay)
    try:
        inspected = move.inspect_retained_raw_history(engine, **args)
        assert inspected["state"] == "completed" and inspected["inspection_only"]
        assert inspected["recovery_verified"] is False
        result = move.move_retained_raw_history(engine, **args)
        assert result["reused"] and not result["raw_history_placement_pending"]
        assert result["recovery_verified"] is False
    finally:
        event.remove(engine, "before_cursor_execute", prohibit_replay)
    with engine.begin() as conn:
        current = move._state(conn)
        # The existing recovery owner sees this physical layout as changed and
        # cannot treat the previous encrypted pair as current merely by age.
        recovery_layout_after = _snapshot_layout(conn)
        assert recovery_layout_after != recovery_layout_before
        assert {k:v for k,v in current.items() if k != move.POINTER} == evidence
        assert _rows(conn, raw.SOURCE) == source_rows
        assert _rows(conn, handoff.RETAINED + "." + raw.NAME) == retained_rows
        assert _frozen_records(conn) == frozen
        after = move._files(conn)
        assert {r["oid"] for r in after} == {r["oid"] for r in before}
        assert [r for r in after if r["relname"] == "pk_market_raw_archive_record_mapping"] == [
            r for r in before if r["relname"] == "pk_market_raw_archive_record_mapping"]
        assert all(r["tablespace_oid"] == policy.history_tablespace_oid for r in after
                   if r["relname"] != "pk_market_raw_archive_record_mapping")
        observed = handoff.inspect_handoff(conn, policy=options["policy"],
            source_root=receipt["source_root"], destination_root=receipt["destination_root"])
        assert observed["database_handoff_committed"]
        assert observed["receipt"] == receipt
        # Existing INSERT semantics still work without touching the retained
        # private copy. Roll back this fixture-only probe after inspection.
        with conn.begin_nested() as appended:
            projection = ["'raw-post-history-probe'" if c == "raw_record_id" else
                "object_row_index+1000000" if c == "object_row_index" else c for c in raw.COLUMNS]
            conn.exec_driver_sql("INSERT INTO " + raw.SOURCE + "(" + ",".join(raw.COLUMNS) +
                ") SELECT " + ",".join(projection) + " FROM " + raw.SOURCE + " LIMIT 1")
            assert conn.scalar(text("SELECT count(*) FROM " + raw.SOURCE +
                " WHERE raw_record_id='raw-post-history-probe'")) == 1
            assert move.inspect_completed_raw_history(conn, receipt,
                pid=physical.verify(conn, receipt["binding"]))["reused"]
            assert _snapshot_layout(conn) == recovery_layout_after
            appended.rollback()
        with conn.begin_nested() as changed:
            conn.execute(text("UPDATE market.fact_storage_state SET evidence=evidence-:key WHERE layout_version=:layout"),
                         {"key":move.POINTER, "layout":move.LAYOUT})
            with pytest.raises(RuntimeError, match="wrong_tablespace"):
                handoff.inspect_handoff(conn, policy=options["policy"],
                    source_root=receipt["source_root"], destination_root=receipt["destination_root"])
            changed.rollback()
