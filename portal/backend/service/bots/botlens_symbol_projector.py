"""Symbol-level projector for the BotLens telemetry pipeline.

SymbolProjector is the sole owner of canonical state for one (run_id, symbol_key).
It runs as an asyncio task and is the only writer of symbol detail persistence.

Ownership:
  Reads from:  SymbolMailbox (fact_queue + bootstrap_slot)
  Writes to:   symbol detail storage, run_notifications queue, fanout_channel

Invariants:
  - Facts are applied in order within (run_id, symbol_key, bridge_session_id).
  - A bootstrap resets state, drains stale-session facts, and resumes cleanly.
  - Facts whose bridge_session_id doesn't match the current session are rejected.
  - SymbolProjector never writes run-summary or open-trades state.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ..observability import BackendObserver, QueueStateMetricOwner, normalize_failure_mode
from ..storage.storage import (
    record_bot_runtime_event,
    upsert_bot_run_view_state,
    get_latest_bot_run_view_state,
)
from .botlens_contract import (
    EVENT_TYPE_RUNTIME_BOOTSTRAP,
    EVENT_TYPE_RUNTIME_FACTS,
    _event_id,
    _sanitize_json,
    normalize_bridge_seq,
    normalize_bridge_session_id,
    normalize_fact_entries,
    normalize_series_key,
)
from .botlens_mailbox import FanoutTypedDelta, SymbolMailbox
from .botlens_mailbox import FanoutEnvelope, QueueEnvelope
from .botlens_state import apply_fact_batch, empty_symbol_detail, read_symbol_detail_state, serialize_symbol_detail_state
from .botlens_typed_deltas import SymbolTypedDeltaBuilder

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_symbol_projector", event_logger=logger)


# ---------------------------------------------------------------------------
# Notification types sent upward to RunProjector
# ---------------------------------------------------------------------------

@dataclass
class SymbolSummaryNotification:
    """Sent from SymbolProjector to RunProjector after each state update."""
    run_id: str
    symbol_key: str
    detail_state: Dict[str, Any]   # copy of projected symbol state for summary building
    trade_upserts: List[Dict[str, Any]]
    trade_removals: List[str]
    seq: int
    runtime: Optional[Dict[str, Any]]
    event_time: Any
    known_at: Any


# ---------------------------------------------------------------------------
# SymbolProjector
# ---------------------------------------------------------------------------

class SymbolProjector:
    """
    Owns canonical state for one (run_id, symbol_key).

    Runs as a persistent asyncio task. Processes its SymbolMailbox in a loop:
    - Bootstrap arrivals (via bootstrap_slot) take priority and reset state.
    - Incremental facts are applied in order within the current bridge session.
    - State changes are persisted then emitted to RunProjector and the fanout channel.
    """

    def __init__(
        self,
        *,
        run_id: str,
        bot_id: str,
        symbol_key: str,
        mailbox: SymbolMailbox,
        run_notifications: "asyncio.Queue[QueueEnvelope]",
        fanout_channel: "asyncio.Queue[Any]",
        run_notification_queue_metrics: QueueStateMetricOwner,
        fanout_queue_metrics: QueueStateMetricOwner,
    ) -> None:
        self._run_id = run_id
        self._bot_id = bot_id
        self._symbol_key = symbol_key
        self._mailbox = mailbox
        self._run_notifications = run_notifications
        self._fanout_channel = fanout_channel
        self._run_notification_queue_metrics = run_notification_queue_metrics
        self._fanout_queue_metrics = fanout_queue_metrics

        self._state: Dict[str, Any] = empty_symbol_detail(symbol_key)
        self._current_session_id: Optional[str] = None
        self._active = True

    @property
    def symbol_key(self) -> str:
        return self._symbol_key

    def get_snapshot(self) -> Dict[str, Any]:
        """
        Return a synchronous copy of the current canonical symbol state.

        Safe to call from outside the projector task (asyncio cooperative
        scheduling ensures no concurrent state mutation during a sync call).
        """
        return dict(self._state)

    async def _await_persistence(
        self,
        *,
        storage_target: str,
        pipeline_stage: str,
        func: Any,
        args: tuple[Any, ...] = (),
        **kwargs: Any,
    ) -> Any:
        started = time.perf_counter()
        try:
            result = await asyncio.to_thread(func, *args, **kwargs)
        except Exception as exc:
            _OBSERVER.observe(
                "persistence_wait_ms",
                max((time.perf_counter() - started) * 1000.0, 0.0),
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                storage_target=storage_target,
                pipeline_stage=pipeline_stage,
                failure_mode=normalize_failure_mode(exc),
            )
            raise
        _OBSERVER.observe(
            "persistence_wait_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            storage_target=storage_target,
            pipeline_stage=pipeline_stage,
        )
        return result

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main projector loop. Runs until cancelled."""
        await self._load_initial_state()

        # Persistent tasks for waiting on either source. Re-created after each use.
        fact_task: asyncio.Task[Any] = asyncio.create_task(
            self._mailbox.fact_queue.get(), name=f"sym-fact-{self._symbol_key}"
        )
        bootstrap_task: asyncio.Task[None] = asyncio.create_task(
            self._mailbox.bootstrap_slot.event.wait(),
            name=f"sym-boot-{self._symbol_key}",
        )

        try:
            while self._active:
                # Fast path: bootstrap is already pending from a previous signal.
                if self._mailbox.bootstrap_slot.pending:
                    fact_task, bootstrap_task = await self._handle_bootstrap(
                        fact_task, bootstrap_task
                    )
                    continue

                done, _ = await asyncio.wait(
                    {fact_task, bootstrap_task},
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=5.0,
                )

                if bootstrap_task in done:
                    # Bootstrap event fired during wait.
                    fact_task, bootstrap_task = await self._handle_bootstrap(
                        fact_task, bootstrap_task
                    )
                    continue

                if fact_task in done:
                    try:
                        envelope = fact_task.result()
                        payload = envelope.payload if isinstance(envelope, QueueEnvelope) else envelope
                        queue_wait_ms = (
                            max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)
                            if isinstance(envelope, QueueEnvelope)
                            else 0.0
                        )
                        _OBSERVER.observe(
                            "symbol_fact_queue_wait_ms",
                            queue_wait_ms,
                            bot_id=self._bot_id,
                            run_id=self._run_id,
                            series_key=self._symbol_key,
                            queue_name="symbol_fact_queue",
                            message_kind="facts",
                        )
                        self._mailbox._emit_fact_gauges()
                        await self._apply_facts(payload)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        _OBSERVER.event(
                            "symbol_projector_failed",
                            level=logging.ERROR,
                            bot_id=self._bot_id,
                            run_id=self._run_id,
                            series_key=self._symbol_key,
                            failure_mode="fact_apply_failed",
                            error=str(exc),
                        )
                    fact_task = asyncio.create_task(
                        self._mailbox.fact_queue.get(),
                        name=f"sym-fact-{self._symbol_key}",
                    )

        except asyncio.CancelledError:
            pass
        finally:
            for t in (fact_task, bootstrap_task):
                if not t.done():
                    t.cancel()

    # ------------------------------------------------------------------
    # Bootstrap handling
    # ------------------------------------------------------------------

    async def _handle_bootstrap(
        self,
        fact_task: "asyncio.Task[Any]",
        bootstrap_task: "asyncio.Task[None]",
    ) -> "Tuple[asyncio.Task[Any], asyncio.Task[None]]":
        """Take and apply the latest pending bootstrap, drain stale facts, return fresh tasks."""
        bootstrap_delay_ms = self._mailbox.bootstrap_slot.pending_age_ms
        bootstrap = self._mailbox.bootstrap_slot.take()

        # Cancel the fact task so the queue slot isn't held by an awaiter
        # during the drain. asyncio.Queue.get() cancellation leaves the item
        # in the queue, so nothing is lost.
        if not fact_task.done():
            fact_task.cancel()
            try:
                await fact_task
            except (asyncio.CancelledError, Exception):
                pass

        if bootstrap is not None:
            new_session_id = normalize_bridge_session_id(bootstrap)
            self._drain_stale_session_facts(new_session_id)
            _OBSERVER.observe(
                "bootstrap_apply_delay_ms",
                bootstrap_delay_ms,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                message_kind="bootstrap",
            )
            await self._apply_bootstrap(bootstrap)
        else:
            # Slot was taken by another concurrent reader (shouldn't happen with
            # single-task ownership, but guard defensively).
            _OBSERVER.event(
                "symbol_projector_failed",
                level=logging.WARN,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                failure_mode="bootstrap_slot_empty",
            )

        # Cancel and re-create the bootstrap wait task (event was cleared by take()).
        if not bootstrap_task.done():
            bootstrap_task.cancel()
            try:
                await bootstrap_task
            except (asyncio.CancelledError, Exception):
                pass

        return (
            asyncio.create_task(
                self._mailbox.fact_queue.get(), name=f"sym-fact-{self._symbol_key}"
            ),
            asyncio.create_task(
                self._mailbox.bootstrap_slot.event.wait(), name=f"sym-boot-{self._symbol_key}"
            ),
        )

    def _drain_stale_session_facts(self, new_session_id: str) -> int:
        """
        Remove facts from the queue that belong to sessions other than new_session_id.
        Facts for new_session_id are valid continuations of the bootstrap and are kept.
        """
        stale: List[QueueEnvelope] = []
        fresh: List[QueueEnvelope] = []
        while True:
            try:
                item = self._mailbox.fact_queue.get_nowait()
                payload = item.payload if isinstance(item, QueueEnvelope) else item
                if normalize_bridge_session_id(payload) == new_session_id:
                    fresh.append(item)
                else:
                    stale.append(item)
            except asyncio.QueueEmpty:
                break
        # Re-queue fresh items (valid continuations of the new session).
        for item in fresh:
            try:
                self._mailbox.fact_queue.put_nowait(item)
            except asyncio.QueueFull:
                _OBSERVER.increment(
                    "symbol_fact_dropped_total",
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    series_key=self._symbol_key,
                    queue_name="symbol_fact_queue",
                    message_kind="facts",
                    failure_mode="queue_full",
                )
                _OBSERVER.event(
                    "symbol_fact_queue_overflow",
                    level=logging.WARN,
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    series_key=self._symbol_key,
                    queue_name="symbol_fact_queue",
                    failure_mode="queue_full",
                    overflow_policy="drop_new",
                )
        if stale:
            _OBSERVER.increment(
                "symbol_projector_stale_facts_drained_total",
                value=float(len(stale)),
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                message_kind="facts",
            )
            _OBSERVER.event(
                "symbol_stale_facts_drained",
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                stale_count=len(stale),
                fresh_count=len(fresh),
            )
        return len(stale)

    # ------------------------------------------------------------------
    # Core projection
    # ------------------------------------------------------------------

    async def _apply_bootstrap(self, payload: Mapping[str, Any]) -> None:
        started = time.perf_counter()
        session_id = normalize_bridge_session_id(payload)
        facts = normalize_fact_entries(payload.get("facts"))
        if not facts:
            _OBSERVER.event(
                "symbol_projector_failed",
                level=logging.WARN,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                failure_mode="empty_bootstrap",
            )
            return

        # Reset to empty state before applying the snapshot.
        self._state = empty_symbol_detail(self._symbol_key)
        self._current_session_id = session_id

        seq = int(payload.get("run_seq") or 0)
        known_at = payload.get("known_at") or payload.get("event_time")
        event_time = payload.get("event_time") or known_at

        applied = apply_fact_batch(self._state, facts=facts, seq=seq, event_time=known_at)
        self._state = dict(applied["detail"])

        _OBSERVER.increment(
            "symbol_projector_bootstrap_apply_total",
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind="bootstrap",
        )
        _OBSERVER.observe(
            "symbol_projector_batch_size",
            float(len(facts)),
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind="bootstrap",
        )
        await self._emit_typed_deltas(delta=applied["delta"], seq=seq, event_time=event_time)
        await self._persist_detail_state(seq=seq, event_time=event_time, known_at=known_at)
        await self._record_raw_event(
            event_type=EVENT_TYPE_RUNTIME_BOOTSTRAP,
            seq=seq,
            raw_payload=payload,
            event_time=event_time,
            known_at=known_at,
        )
        await self._notify_run_projector(
            delta=applied["delta"],
            seq=seq,
            event_time=event_time,
            known_at=known_at,
        )
        _OBSERVER.observe(
            "symbol_projector_apply_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind="bootstrap",
        )
        _OBSERVER.event(
            "symbol_bootstrap_applied",
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            seq=seq,
            bridge_session_id=session_id,
        )

    async def _apply_facts(self, payload: Mapping[str, Any]) -> None:
        started = time.perf_counter()
        session_id = normalize_bridge_session_id(payload)

        # Reject facts from a superseded session.
        if self._current_session_id and session_id and session_id != self._current_session_id:
            _OBSERVER.increment(
                "symbol_projector_stale_session_reject_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                message_kind="facts",
            )
            _OBSERVER.event(
                "symbol_stale_session_rejected",
                level=logging.WARN,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                expected_session_id=self._current_session_id,
                observed_session_id=session_id,
            )
            return

        facts = normalize_fact_entries(payload.get("facts"))
        if not facts:
            return

        # Accept the session on first facts message if no bootstrap arrived yet.
        if not self._current_session_id and session_id:
            self._current_session_id = session_id

        seq = int(payload.get("run_seq") or 0)
        known_at = payload.get("known_at") or payload.get("event_time")
        event_time = payload.get("event_time") or known_at

        applied = apply_fact_batch(self._state, facts=facts, seq=seq, event_time=known_at)
        self._state = dict(applied["detail"])

        _OBSERVER.increment(
            "symbol_projector_fact_apply_total",
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind="facts",
        )
        _OBSERVER.observe(
            "symbol_projector_batch_size",
            float(len(facts)),
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind="facts",
        )
        await self._emit_typed_deltas(delta=applied["delta"], seq=seq, event_time=event_time)
        await self._persist_detail_state(seq=seq, event_time=event_time, known_at=known_at)
        await self._record_raw_event(
            event_type=EVENT_TYPE_RUNTIME_FACTS,
            seq=seq,
            raw_payload=payload,
            event_time=event_time,
            known_at=known_at,
        )
        await self._notify_run_projector(
            delta=applied["delta"],
            seq=seq,
            event_time=event_time,
            known_at=known_at,
        )
        _OBSERVER.observe(
            "symbol_projector_apply_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind="facts",
        )

    # ------------------------------------------------------------------
    # Fanout emission (non-blocking — projection must not wait on delivery)
    # ------------------------------------------------------------------

    async def _emit_typed_deltas(
        self,
        *,
        delta: Dict[str, Any],
        seq: int,
        event_time: Any,
    ) -> Dict[str, Any]:
        prepared = SymbolTypedDeltaBuilder.build(
            run_id=self._run_id,
            symbol_key=self._symbol_key,
            seq=seq,
            event_time=event_time,
            delta=_sanitize_json(delta),
        )
        if prepared:
            try:
                payload_bytes = sum(int(item.payload_bytes) for item in prepared)
                self._fanout_channel.put_nowait(
                    FanoutEnvelope(
                        run_id=self._run_id,
                        item=FanoutTypedDelta(run_id=self._run_id, prepared_deltas=prepared),
                        message_kind="typed_delta",
                        payload_bytes=payload_bytes,
                    )
                )
                _OBSERVER.increment(
                    "fanout_enqueued_total",
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    series_key=self._symbol_key,
                    queue_name="fanout_channel",
                    message_kind="typed_delta",
                )
                _OBSERVER.observe(
                    "fanout_payload_bytes",
                    float(payload_bytes),
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    series_key=self._symbol_key,
                    queue_name="fanout_channel",
                    message_kind="typed_delta",
                )
                self._emit_fanout_gauges()
            except asyncio.QueueFull:
                _OBSERVER.increment(
                    "fanout_dropped_total",
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    series_key=self._symbol_key,
                    queue_name="fanout_channel",
                    message_kind="typed_delta",
                    failure_mode="queue_full",
                )
                _OBSERVER.event(
                    "fanout_channel_overflow",
                    level=logging.WARN,
                    log_to_logger=False,
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    queue_name="fanout_channel",
                    operation="typed_delta",
                    failure_mode="queue_full",
                    overflow_policy="drop_new",
                )
                self._emit_fanout_gauges()
        return {}

    # ------------------------------------------------------------------
    # Run projector notification
    # ------------------------------------------------------------------

    async def _notify_run_projector(
        self,
        *,
        delta: Dict[str, Any],
        seq: int,
        event_time: Any,
        known_at: Any,
    ) -> None:
        trade_upserts = [
            dict(t) for t in (delta.get("trade_upserts") or []) if isinstance(t, Mapping)
        ]
        trade_removals = [
            str(r) for r in (delta.get("trade_removals") or []) if str(r).strip()
        ]
        notification = SymbolSummaryNotification(
            run_id=self._run_id,
            symbol_key=self._symbol_key,
            detail_state=dict(self._state),
            trade_upserts=trade_upserts,
            trade_removals=trade_removals,
            seq=seq,
            runtime=dict(self._state.get("runtime") or {}),
            event_time=event_time,
            known_at=known_at,
        )
        try:
            self._run_notifications.put_nowait(
                QueueEnvelope(payload=notification)
            )
            _OBSERVER.increment(
                "run_notification_enqueued_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                queue_name="run_notification_queue",
                message_kind="notification",
            )
            self._emit_run_notification_gauges()
        except asyncio.QueueFull:
            _OBSERVER.increment(
                "run_notification_dropped_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                queue_name="run_notification_queue",
                message_kind="notification",
                failure_mode="queue_full",
            )
            _OBSERVER.event(
                "run_notification_queue_overflow",
                level=logging.WARN,
                log_to_logger=False,
                run_id=self._run_id,
                queue_name="run_notification_queue",
                failure_mode="queue_full",
                overflow_policy="drop_new",
            )
            self._emit_run_notification_gauges()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _persist_detail_state(
        self, *, seq: int, event_time: Any, known_at: Any
    ) -> None:
        await self._await_persistence(
            storage_target="bot_run_view_state",
            pipeline_stage="detail_state_persist",
            func=upsert_bot_run_view_state,
            args=(
                {
                    "run_id": self._run_id,
                    "bot_id": self._bot_id,
                    "series_key": self._symbol_key,
                    "seq": int(seq),
                    "schema_version": int(self._state.get("schema_version") or 4),
                    "payload": serialize_symbol_detail_state(self._state),
                    "event_time": event_time,
                    "known_at": known_at,
                    "updated_at": known_at,
                },
            ),
        )
    async def _record_raw_event(
        self,
        *,
        event_type: str,
        seq: int,
        raw_payload: Mapping[str, Any],
        event_time: Any,
        known_at: Any,
    ) -> None:
        bridge_session_id = normalize_bridge_session_id(raw_payload)
        bridge_seq = normalize_bridge_seq(raw_payload)
        event_payload: Dict[str, Any] = {
            "series_key": self._symbol_key,
            "run_seq": int(seq),
            "bridge_session_id": bridge_session_id,
            "bridge_seq": bridge_seq,
        }
        facts = normalize_fact_entries(raw_payload.get("facts"))
        if facts:
            event_payload["facts"] = facts
        await self._await_persistence(
            storage_target="bot_runtime_events",
            pipeline_stage="raw_event_persist",
            func=record_bot_runtime_event,
            args=(
                {
                    "event_id": _event_id(
                        bot_id=self._bot_id,
                        run_id=self._run_id,
                        event_type=event_type,
                        symbol_key=self._symbol_key,
                        bridge_session_id=bridge_session_id,
                        bridge_seq=bridge_seq,
                        seq=seq,
                    ),
                    "bot_id": self._bot_id,
                    "run_id": self._run_id,
                    "seq": int(seq),
                    "event_type": event_type,
                    "critical": bool(event_type == EVENT_TYPE_RUNTIME_BOOTSTRAP),
                    "schema_version": 4,
                    "event_time": event_time,
                    "known_at": known_at,
                    "payload": event_payload,
                },
            ),
        )

    # ------------------------------------------------------------------
    # Initial state load (on startup / backend restart)
    # ------------------------------------------------------------------

    async def _load_initial_state(self) -> None:
        """
        Eagerly load the latest persisted symbol state from storage.
        Gives viewers a meaningful snapshot even before the first new bootstrap.
        """
        started = time.perf_counter()
        try:
            row = await asyncio.to_thread(
                get_latest_bot_run_view_state,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
            )
            if row:
                payload = (row or {}).get("payload")
                if payload:
                    self._state = read_symbol_detail_state(payload, symbol_key=self._symbol_key)
                    load_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
                    _OBSERVER.observe(
                        "db_initial_load_ms",
                        load_ms,
                        bot_id=self._bot_id,
                        run_id=self._run_id,
                        series_key=self._symbol_key,
                        storage_target="bot_run_view_state",
                    )
                    _OBSERVER.event(
                        "db_initial_state_load_completed",
                        bot_id=self._bot_id,
                        run_id=self._run_id,
                        series_key=self._symbol_key,
                        storage_target="bot_run_view_state",
                        load_ms=round(load_ms, 6),
                    )
        except Exception as exc:
            load_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
            _OBSERVER.observe(
                "db_initial_load_ms",
                load_ms,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                storage_target="bot_run_view_state",
                failure_mode="load_failed",
            )
            _OBSERVER.event(
                "db_initial_state_load_failed",
                level=logging.WARN,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                storage_target="bot_run_view_state",
                load_ms=round(load_ms, 6),
                failure_mode="load_failed",
                error=str(exc),
            )

    def _emit_run_notification_gauges(self) -> None:
        queue = self._run_notifications
        oldest_age_ms = 0.0
        if queue.qsize() > 0:
            try:
                envelope = queue._queue[0]
                if isinstance(envelope, QueueEnvelope):
                    oldest_age_ms = max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)
            except Exception:
                oldest_age_ms = 0.0
        self._run_notification_queue_metrics.emit(
            depth=queue.qsize(),
            capacity=max(int(queue.maxsize or 1), 1),
            oldest_age_ms=oldest_age_ms,
        )

    def _emit_fanout_gauges(self) -> None:
        oldest_age_ms = 0.0
        if self._fanout_channel.qsize() > 0:
            try:
                envelope = self._fanout_channel._queue[0]
                if isinstance(envelope, FanoutEnvelope):
                    oldest_age_ms = max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)
            except Exception:
                oldest_age_ms = 0.0
        self._fanout_queue_metrics.emit(
            depth=self._fanout_channel.qsize(),
            capacity=max(int(self._fanout_channel.maxsize or 1), 1),
            oldest_age_ms=oldest_age_ms,
        )


__all__ = ["SymbolProjector", "SymbolSummaryNotification"]
