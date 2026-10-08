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


@pytest.mark.parametrize("queue_slots,blocked_frames", [(16, 12), (64, 48)])
def test_continuous_capture_buffers_raw_lock_then_publishes_every_frame(
        storage, tmp_path, monkeypatch, queue_slots, blocked_frames):
    """Exercise the existing queue, not production duration or policy admission.

    Real PostgreSQL excludes raw publication; real runtime/spool/archive/fact
    writers continue afterward. Scripted transport uses byte-driven segment
    rotation to exercise many pending segments without a minutes-long sleep.
    """
    import asyncio
    from datetime import UTC, datetime, timedelta
    import json
    from pathlib import Path
    from threading import Event
    from time import monotonic

    from data_providers.streams.contracts import ProviderRawMessage
    from portal.backend.service.market.continuous_stream_collector import (
        CoinbaseContinuousTransportAdapter, CoinbaseMarketTradeProjectionAdapter,
        ContinuousStreamRuntime,
    )
    from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement
    from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
    from tests.test_market_data.test_fact_storage_tiers_db import _placement

    source = Path("/qt-source/pgdata") / ("raw-buffer-" + uuid4().hex)
    source.mkdir()
    storage.open_day = storage.today
    _configure_placement(storage, tmp_path, monkeypatch)
    book = _raw_book_fixture(storage, source, monkeypatch)
    _placement(monkeypatch, storage.today)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(source))
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(source))
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", storage.copy_plan.recent.filesystem_uuid)
    monkeypatch.setenv("QT_MARKET_DATA_WORKING_EXPECTED_UUID", storage.copy_plan.recent.filesystem_uuid)
    structures = book.structures
    engine = storage.database._engine
    definition = "raw-buffer-" + uuid4().hex
    series = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type="market.trade",
        contract_version="market.trade.v1", timeframe_seconds=None)
    structures.upsert_stream_definition(
        definition_id=definition, source_id=book.source_id, series_id=series,
        provider=book.source.provider, venue=book.source.venue,
        provider_product_id="BTC-USD", channels=("market_trades", "heartbeats"),
        auth_mode="public", contract_version="market.trade.v1",
        max_spool_bytes=16 * 1024**2, max_segment_bytes=8192,
        config={
            "product_definition_version_id": book.claim.config["product_definition_version_id"],
            "runtime_policy": {
                "max_inflight_segments": queue_slots, "heartbeat_seconds": 1,
            },
        })
    stop, produced, unlock, closed, raw_waiting = (Event() for _ in range(5))
    frames, queue_sizes, queue_capacities = [], [], []
    heartbeat_started, heartbeat_completed = [], []
    received_start = datetime.now(UTC)
    connections = []
    original_heartbeat = structures.heartbeat

    def heartbeat(*args, **kwargs):
        heartbeat_started.append(monotonic())
        result = original_heartbeat(*args, **kwargs)
        heartbeat_completed.append(monotonic())
        return result

    monkeypatch.setattr(structures, "heartbeat", heartbeat)

    class Stream:
        def __init__(self, **kwargs):
            self.session_id = kwargs["stream_session_id"]

        async def connect(self):
            connections.append(0)
            return 0

        async def subscribe(self, subscriptions):
            assert subscriptions

        async def raw_messages(self):
            for ordinal in range(1, blocked_frames + 2):
                if ordinal == 3:
                    while not raw_waiting.is_set():
                        await asyncio.sleep(0.01)
                if 3 <= ordinal <= blocked_frames:
                    # Cross multiple real heartbeat intervals after publication
                    # is known to be waiting; a quick burst alone is insufficient.
                    await asyncio.sleep(3.0 / (blocked_frames - 2))
                if ordinal == blocked_frames + 1:
                    # This is reached only after the previous frame was consumed.
                    produced.set()
                    while not unlock.is_set():
                        await asyncio.sleep(0.01)
                timestamp = datetime.now(UTC).isoformat()
                message = ProviderRawMessage.build(
                    provider=book.source.provider, venue=book.source.venue,
                    stream_session_id=self.session_id, connection_epoch=0,
                    receive_ordinal=ordinal, received_at=timestamp,
                    raw_frame=json.dumps({
                        "channel": "market_trades", "timestamp": timestamp,
                        "sequence_num": ordinal, "fixture_padding": "x" * 4096,
                        "events": [{"type": "update", "trades": [{
                            "product_id": "BTC-USD", "trade_id": f"buffer-{ordinal}",
                            "price": "100", "size": "0.01", "side": "BUY",
                            "time": timestamp,
                        }]}],
                    }))
                frames.append(message)
                yield message
            stop.set()

        async def close(self):
            closed.set()

    original_enqueue = ContinuousStreamRuntime._enqueue_checkpoint

    def observe_queue(queue, **kwargs):
        original_enqueue(queue, **kwargs)
        queue_sizes.append(queue.qsize())
        queue_capacities.append(queue.maxsize)

    monkeypatch.setattr(ContinuousStreamRuntime, "_enqueue_checkpoint", staticmethod(observe_queue))
    transport = CoinbaseContinuousTransportAdapter(stream_factory=Stream)
    runtime = ContinuousStreamRuntime(repository=structures)
    spool = source / "spool" / definition
    observation = {}

    async def exercise(blocker):
        task = asyncio.create_task(runtime.run(
            definition_id=definition, owner_id="native-raw-buffer",
            stop_requested=stop.is_set, bounded_validation=True,
            storage_root=source, projection=CoinbaseMarketTradeProjectionAdapter(),
            transport=transport))
        try:
            # Waiting for native publication is observed through pg_locks, never
            # by reading the relation we intentionally hold exclusively.
            deadline = monotonic() + 15
            waiting = False
            while not waiting and monotonic() < deadline:
                with engine.begin() as conn:
                    conn.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                    waiting = conn.scalar(text(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE "
                        "relation='market.raw_archive_record_mappings'::regclass "
                        "AND mode IN ('AccessShareLock','RowExclusiveLock') AND NOT granted)"))
                if not waiting:
                    await asyncio.sleep(0.02)
            assert waiting, "fixture publisher never reached the real raw relation lock"
            blocked_at = monotonic()
            raw_waiting.set()
            captured = await asyncio.to_thread(produced.wait, 45)
            if not captured:
                with engine.begin() as conn:
                    graph = [dict(row) for row in conn.execute(text(
                        "SELECT pid,wait_event_type,wait_event,pg_blocking_pids(pid) AS blockers "
                        "FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid LIMIT 32"
                    )).mappings()]
                print(json.dumps({
                    "fixture_incomplete_capture": len(frames), "queue_slots": queue_slots,
                    "heartbeat_started": len(heartbeat_started),
                    "heartbeat_completed": len(heartbeat_completed), "wait_graph": graph,
                }))
            assert captured, "fixture capture did not finish across native blocked-publication heartbeats"
            assert not task.done() and not closed.is_set()
            assert len(frames) == blocked_frames
            assert len([at for at in heartbeat_completed if at >= blocked_at]) >= 2
            pending = [p for p in spool.rglob("*") if p.suffix in (".open", ".sealed")]
            assert len(pending) == blocked_frames
            assert max(queue_sizes) > 4 and set(queue_capacities) == {queue_slots}
            with engine.begin() as conn:
                assert conn.scalar(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:series"),
                                   {"series": series}) == 0
                observation.update(
                    released_at=conn.scalar(text("SELECT clock_timestamp()")),
                    pending_segments=len(pending),
                    pending_spool_bytes=sum(p.stat().st_size for p in pending),
                    queue_high_water=max(queue_sizes))
            blocker.commit()
            unlock.set()
            return await asyncio.wait_for(task, 120)
        finally:
            if blocker.in_transaction():
                blocker.rollback()
            unlock.set()
            stop.set()
            if not task.done():
                await asyncio.wait_for(task, 120)

    with engine.connect() as blocker:
        blocker.begin()
        blocker.exec_driver_sql(
            "LOCK TABLE market.raw_archive_record_mappings IN ACCESS EXCLUSIVE MODE NOWAIT")
        result = asyncio.run(exercise(blocker))

    assert connections == [0] and result["raw_records"] == blocked_frames + 1
    assert not [p for p in spool.rglob("*") if p.suffix in (".open", ".sealed")]
    with engine.begin() as conn:
        assert conn.scalar(text(
            "SELECT count(*) FROM market.raw_archive_record_mappings m "
            "JOIN market.raw_archive_manifests a ON a.id=m.manifest_id "
            "WHERE a.definition_id=:definition"), {"definition": definition}) == blocked_frames + 1
    facts = storage.repo.read_facts(
        series_id=series, start=received_start - timedelta(seconds=1),
        end=datetime.now(UTC) + timedelta(seconds=1))
    assert len(facts) == blocked_frames + 1
    assert len({row.fact_version_id for row in facts}) == len(facts)
    assert all(row.fact.known_at >= observation["released_at"] for row in facts)
    print(json.dumps({
        "proof": "native_raw_lock_continuous_capture",
        "queue_slots": queue_slots, "captured_while_blocked": blocked_frames,
        "captured_after_release": 1, "queue_high_water": observation["queue_high_water"],
        "pending_spool_bytes": observation["pending_spool_bytes"],
        "transport_connections": len(connections), "canonical_facts": len(facts),
        "production_duration_or_policy_admitted": False,
    }))


def test_stream_fences_recheck_wall_clock_expiry_after_waiting_for_raw(storage, tmp_path, monkeypatch):
    """A transaction's old now() must not admit ownership that expired waiting."""
    from concurrent.futures import ThreadPoolExecutor
    from pathlib import Path
    from threading import Event
    from time import monotonic, sleep

    from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement
    from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture

    source = Path("/qt-source/pgdata") / ("raw-fence-" + uuid4().hex)
    source.mkdir()
    storage.open_day = storage.today
    _configure_placement(storage, tmp_path, monkeypatch)
    book = _raw_book_fixture(storage, source, monkeypatch)
    structures = book.structures
    engine = storage.database._engine

    for kind in ("archive", "canonical"):
        claim = (book.claim if kind == "archive" else structures.claim_stream(
            definition_id=book.claim.definition_id, owner_id="native-expiry",
            lease_seconds=90, bounded=True))
        entered = Event()

        def attempt():
            with storage.database.session() as session:
                session.execute(text("SET LOCAL statement_timeout='5s'"))
                session.execute(text("SELECT current_timestamp"))
                entered.set()
                if kind == "archive":
                    return structures._require_fence(session, claim, raw_mapping_access=True)
                return storage.repo._assert_collection_fence(
                    session, series_id=claim.series_id,
                    collection_fence=structures._collection_fence(claim))

        with ThreadPoolExecutor(max_workers=1) as pool, engine.connect() as blocker:
            blocker.begin()
            blocker.exec_driver_sql(
                "LOCK TABLE market.raw_archive_record_mappings IN ACCESS EXCLUSIVE MODE NOWAIT")
            future = pool.submit(attempt)
            try:
                assert entered.wait(3)
                # Set expiry after the waiting transaction has begun. An early
                # lease-row lock would also block this bounded update.
                with engine.begin() as conn:
                    conn.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                    conn.execute(text(
                        "UPDATE market.stream_lease_state "
                        "SET expires_at=clock_timestamp()+interval '0.1 second' "
                        "WHERE definition_id=:definition"),
                        {"definition": claim.definition_id})
                deadline = monotonic() + 2
                expired = False
                while not expired and monotonic() < deadline:
                    with engine.begin() as conn:
                        expired = conn.scalar(text(
                            "SELECT expires_at<=clock_timestamp() FROM market.stream_lease_state "
                            "WHERE definition_id=:definition"),
                            {"definition": claim.definition_id})
                    if not expired:
                        sleep(0.02)
                assert expired and not future.done()
                blocker.commit()
                with pytest.raises(RuntimeError, match="ownership_lost"):
                    future.result(timeout=5)
            finally:
                if blocker.in_transaction():
                    blocker.rollback()
