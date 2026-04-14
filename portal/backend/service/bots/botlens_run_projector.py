"""Run-level projector for the BotLens telemetry pipeline.

RunProjector is the sole owner of canonical run-level state:
  - run summary (health, lifecycle, symbol_index, open_trades_index, run_meta)
  - run lifecycle persistence
  - run-level fanout emission (summary delta, open-trades delta)

It processes from two sources concurrently:
  - lifecycle_channel: lifecycle event payloads from the intake router
  - _symbol_notifications: SymbolSummaryNotification from SymbolProjectors

Ownership invariants:
  - RunProjector is the only writer of run summary state and run-level persistence.
  - SymbolProjector never writes run-level state.
  - RunProjector never writes symbol detail state.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from typing import Any, Callable, Coroutine, Dict, List, Optional

from ..observability import BackendObserver, QueueStateMetricOwner, normalize_failure_mode, payload_size_bytes
from ..storage.storage import (
    get_bot_run,
    get_latest_bot_run_view_state,
    get_latest_bot_runtime_event,
    record_bot_runtime_event,
    upsert_bot_run_view_state,
)
from .botlens_contract import (
    EVENT_TYPE_LIFECYCLE,
    RUN_SCOPE_KEY,
    normalize_lifecycle_payload,
    normalize_series_key,
)
from .botlens_mailbox import (
    FanoutEnvelope,
    FanoutOpenTradesDelta,
    FanoutSummaryDelta,
    QueueEnvelope,
    RunMailbox,
)
from .botlens_state import (
    build_symbol_summary,
    empty_run_summary,
    is_open_trade,
    read_run_summary_state,
    serialize_run_summary_state,
)
from .botlens_symbol_projector import SymbolSummaryNotification

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_run_projector", event_logger=logger)

_ACTIVE_RUN_TTL_S = 1800.0
_TERMINAL_RUN_TTL_S = 300.0
_LIFECYCLE_SEQ_OFFSET = 1_000_000_000
_INT32_MAX = 2_147_483_647
_TERMINAL_LIFECYCLE_PHASES = frozenset(
    {"completed", "stopped", "error", "failed", "crashed", "startup_failed"}
)
_TERMINAL_LIFECYCLE_STATUSES = frozenset(
    {"completed", "stopped", "error", "failed", "crashed", "startup_failed"}
)
_SYMBOL_NOTIFICATION_QUEUE_MAX = 1024


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


class RunProjector:
    """
    Owns run-level canonical state for one run_id.
    Runs as an asyncio task.

    Consumes:
      - mailbox.lifecycle_channel  → lifecycle events from the intake router
      - _symbol_notifications      → SymbolSummaryNotification from SymbolProjectors

    Emits:
      - FanoutSummaryDelta and FanoutOpenTradesDelta to fanout_channel
      - Persistence writes (run summary, lifecycle events)
      - publish_runtime_update / publish_projected_bot side-effects
    """

    def __init__(
        self,
        *,
        run_id: str,
        bot_id: str,
        mailbox: RunMailbox,
        fanout_channel: "asyncio.Queue[Any]",
        fanout_queue_metrics: QueueStateMetricOwner,
        on_evict: Callable[[str], Coroutine[Any, Any, None]],
    ) -> None:
        self._run_id = run_id
        self._bot_id = bot_id
        self._mailbox = mailbox
        self._fanout_channel = fanout_channel
        self._fanout_queue_metrics = fanout_queue_metrics
        self._on_evict = on_evict

        self._summary_state: Dict[str, Any] = empty_run_summary(
            bot_id=bot_id, run_id=run_id
        )
        self._latest_lifecycle: Dict[str, Any] = {}
        self._latest_lifecycle_seq: Optional[int] = None

        self._terminal = False
        self._terminal_at: Optional[float] = None
        self._last_activity: float = time.monotonic()

        # Notification queue from SymbolProjectors to this RunProjector.
        self._symbol_notifications: "asyncio.Queue[QueueEnvelope]" = asyncio.Queue(
            maxsize=_SYMBOL_NOTIFICATION_QUEUE_MAX
        )
        self._run_notification_queue_metrics = QueueStateMetricOwner(
            observer=_OBSERVER,
            key=f"run_notification_queue:{self._run_id}",
            depth_metric="run_notification_queue_depth",
            utilization_metric="run_notification_queue_utilization",
            oldest_age_metric="run_notification_queue_oldest_age_ms",
            labels={
                "bot_id": self._bot_id,
                "run_id": self._run_id,
                "queue_name": "run_notification_queue",
            },
        )
        self._active = True

    @property
    def symbol_notifications(self) -> "asyncio.Queue[QueueEnvelope]":
        """SymbolProjectors push to this queue to notify the run projector."""
        return self._symbol_notifications

    @property
    def run_notification_queue_metrics(self) -> QueueStateMetricOwner:
        return self._run_notification_queue_metrics

    @property
    def fanout_queue_metrics(self) -> QueueStateMetricOwner:
        return self._fanout_queue_metrics

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
            storage_target=storage_target,
            pipeline_stage=pipeline_stage,
        )
        return result

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main run projector loop. Exits when cancelled or when the run TTL expires."""
        await self._load_initial_state()

        lifecycle_task: asyncio.Task[Any] = asyncio.create_task(
            self._mailbox.lifecycle_channel.get(), name=f"run-lc-{self._run_id}"
        )
        notification_task: asyncio.Task[Any] = asyncio.create_task(
            self._symbol_notifications.get(), name=f"run-notif-{self._run_id}"
        )

        try:
            while self._active:
                done, _ = await asyncio.wait(
                    {lifecycle_task, notification_task},
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=2.0,
                )

                if lifecycle_task in done:
                    try:
                        envelope = lifecycle_task.result()
                        payload = envelope.payload if isinstance(envelope, QueueEnvelope) else envelope
                        queue_wait_ms = (
                            max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)
                            if isinstance(envelope, QueueEnvelope)
                            else 0.0
                        )
                        _OBSERVER.observe(
                            "run_lifecycle_queue_wait_ms",
                            queue_wait_ms,
                            bot_id=self._bot_id,
                            run_id=self._run_id,
                            queue_name="run_lifecycle_queue",
                            message_kind="lifecycle",
                        )
                        self._mailbox._emit_lifecycle_gauges()
                        await self._process_lifecycle(payload)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        _OBSERVER.event(
                            "run_projector_failed",
                            level=logging.ERROR,
                            bot_id=self._bot_id,
                            run_id=self._run_id,
                            failure_mode="lifecycle_apply_failed",
                            error=str(exc),
                        )
                    lifecycle_task = asyncio.create_task(
                        self._mailbox.lifecycle_channel.get(),
                        name=f"run-lc-{self._run_id}",
                    )

                if notification_task in done:
                    try:
                        envelope = notification_task.result()
                        notification = envelope.payload if isinstance(envelope, QueueEnvelope) else envelope
                        queue_wait_ms = (
                            max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)
                            if isinstance(envelope, QueueEnvelope)
                            else 0.0
                        )
                        _OBSERVER.observe(
                            "run_notification_queue_wait_ms",
                            queue_wait_ms,
                            bot_id=self._bot_id,
                            run_id=self._run_id,
                            queue_name="run_notification_queue",
                            message_kind="notification",
                        )
                        self._emit_notification_gauges()
                        await self._process_symbol_notification(notification)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        _OBSERVER.event(
                            "run_projector_failed",
                            level=logging.ERROR,
                            bot_id=self._bot_id,
                            run_id=self._run_id,
                            failure_mode="notification_apply_failed",
                            error=str(exc),
                        )
                    notification_task = asyncio.create_task(
                        self._symbol_notifications.get(),
                        name=f"run-notif-{self._run_id}",
                    )

                # Eviction check: terminal + TTL elapsed + no viewers.
                if self._should_evict():
                    break

        except asyncio.CancelledError:
            pass
        finally:
            for t in (lifecycle_task, notification_task):
                if not t.done():
                    t.cancel()
            asyncio.create_task(self._on_evict(self._run_id))

    def _should_evict(self) -> bool:
        if not self._terminal or self._terminal_at is None:
            return False
        return (time.monotonic() - self._terminal_at) > _TERMINAL_RUN_TTL_S

    # ------------------------------------------------------------------
    # Symbol summary notification processing
    # ------------------------------------------------------------------

    async def _process_symbol_notification(self, notification: SymbolSummaryNotification) -> None:
        started = time.perf_counter()
        self._last_activity = time.monotonic()
        seq = notification.seq
        symbol_key = notification.symbol_key

        # 1. Merge trade diffs into the run-level open-trades index.
        open_trades_delta = self._merge_open_trades(
            upserts=notification.trade_upserts,
            removals=notification.trade_removals,
        )

        # 2. Build updated symbol summary (now that open_trades_index is merged).
        open_trades_for_symbol = [
            t for t in self._summary_state.get("open_trades_index", {}).values()
            if normalize_series_key(t.get("symbol_key")) == symbol_key
            and isinstance(t, Mapping)
        ]
        symbol_summary = build_symbol_summary(
            notification.detail_state,
            open_trades=open_trades_for_symbol,
        )
        self._summary_state.setdefault("symbol_index", {})[symbol_key] = symbol_summary

        # 3. Refresh run health from symbol runtime payload.
        if notification.runtime:
            self._refresh_health_from_runtime(notification.runtime, known_at=notification.known_at)

        self._summary_state["seq"] = int(seq)

        # 4. Ensure run metadata is loaded.
        await self._ensure_run_meta()

        # 5. Persist summary state.
        await self._persist_summary_state(
            seq=seq,
            event_time=notification.event_time,
            known_at=notification.known_at,
        )

        # 6. Emit runtime update publish.
        if notification.runtime:
            await self._publish_runtime_update(
                runtime_payload=notification.runtime,
                seq=seq,
                known_at=notification.known_at,
            )

        # 7. Fanout summary delta.
        await self._emit_summary_delta(seq=seq, symbol_upserts=[symbol_summary])

        # 8. Fanout open-trades delta if there were changes.
        if open_trades_delta:
            upserts, removals = open_trades_delta
            await self._emit_open_trades_delta(
                seq=seq, upserts=upserts, removals=removals
            )
        _OBSERVER.increment(
            "run_projector_notification_apply_total",
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=symbol_key,
            message_kind="notification",
        )
        _OBSERVER.observe(
            "run_projector_apply_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=symbol_key,
            message_kind="notification",
        )

    # ------------------------------------------------------------------
    # Lifecycle processing
    # ------------------------------------------------------------------

    async def _process_lifecycle(self, payload: Mapping[str, Any]) -> None:
        started = time.perf_counter()
        self._last_activity = time.monotonic()
        lifecycle = normalize_lifecycle_payload(payload)
        if not lifecycle:
            return

        previous_phase = str(self._latest_lifecycle.get("phase") or "").strip().lower()
        previous_status = str(self._latest_lifecycle.get("status") or "").strip().lower()
        self._latest_lifecycle = dict(lifecycle)

        # Persist lifecycle event with its own monotonic seq.
        lifecycle_seq = await self._next_lifecycle_seq(lifecycle)
        await self._persist_lifecycle_event(lifecycle=lifecycle, lifecycle_seq=lifecycle_seq)

        # Update summary health from lifecycle.
        self._summary_state["lifecycle"] = dict(lifecycle)
        self._refresh_health_from_lifecycle(lifecycle, known_at=lifecycle.get("checkpoint_at"))

        summary_seq = max(
            int(self._summary_state.get("seq") or 0) + 1,
            _coerce_int(payload.get("seq"), default=0),
        )
        self._summary_state["seq"] = summary_seq

        await self._ensure_run_meta()
        await self._persist_summary_state(
            seq=summary_seq,
            event_time=lifecycle.get("checkpoint_at"),
            known_at=lifecycle.get("checkpoint_at"),
        )

        terminal = (
            str(lifecycle.get("phase") or "").strip().lower() in _TERMINAL_LIFECYCLE_PHASES
            or str(lifecycle.get("status") or "").strip().lower() in _TERMINAL_LIFECYCLE_STATUSES
        )
        if terminal and not self._terminal:
            self._terminal = True
            self._terminal_at = time.monotonic()
            _OBSERVER.increment("run_projector_terminal_total", bot_id=self._bot_id, run_id=self._run_id)
            _OBSERVER.event(
                "run_terminal_detected",
                bot_id=self._bot_id,
                run_id=self._run_id,
                message_kind="lifecycle",
                phase=lifecycle.get("phase"),
                status=lifecycle.get("status"),
            )

        await self._publish_projected_bot()
        await self._emit_summary_delta(
            seq=summary_seq,
            symbol_upserts=[],
            lifecycle=lifecycle,
        )
        if (
            previous_phase != str(lifecycle.get("phase") or "").strip().lower()
            or previous_status != str(lifecycle.get("status") or "").strip().lower()
        ):
            _OBSERVER.event(
                "run_phase_changed",
                bot_id=self._bot_id,
                run_id=self._run_id,
                message_kind="lifecycle",
                previous_phase=previous_phase or None,
                phase=lifecycle.get("phase"),
                previous_status=previous_status or None,
                status=lifecycle.get("status"),
            )
        _OBSERVER.increment(
            "run_projector_lifecycle_apply_total",
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind="lifecycle",
        )
        _OBSERVER.observe(
            "run_projector_apply_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind="lifecycle",
        )

    # ------------------------------------------------------------------
    # Open-trades merge (run projector is sole writer)
    # ------------------------------------------------------------------

    def _merge_open_trades(
        self,
        *,
        upserts: List[Dict[str, Any]],
        removals: List[str],
    ) -> Optional[tuple]:
        if not upserts and not removals:
            return None
        index = self._summary_state.setdefault("open_trades_index", {})
        effective_removals = []
        effective_upserts = []
        for trade_id in removals:
            if str(trade_id).strip() and str(trade_id) in index:
                index.pop(str(trade_id))
                effective_removals.append(str(trade_id))
        for trade in upserts:
            if not isinstance(trade, Mapping):
                continue
            trade_id = str(trade.get("trade_id") or "").strip()
            if not trade_id:
                continue
            if is_open_trade(trade):
                index[trade_id] = dict(trade)
                effective_upserts.append(dict(trade))
            else:
                if trade_id in index:
                    index.pop(trade_id)
                    effective_removals.append(trade_id)
        if not effective_upserts and not effective_removals:
            return None
        return effective_upserts, effective_removals

    # ------------------------------------------------------------------
    # Health refresh helpers
    # ------------------------------------------------------------------

    def _refresh_health_from_runtime(
        self, runtime: Mapping[str, Any], *, known_at: Any
    ) -> None:
        health = dict(self._summary_state.get("health") or {})
        health["status"] = str(runtime.get("status") or health.get("status") or "waiting")
        health["worker_count"] = int(runtime.get("worker_count") or health.get("worker_count") or 0)
        health["active_workers"] = int(runtime.get("active_workers") or health.get("active_workers") or 0)
        warnings = runtime.get("warnings")
        if isinstance(warnings, list):
            health["warning_count"] = len(warnings)
            health["warnings"] = [dict(w) for w in warnings if isinstance(w, Mapping)]
        health["last_event_at"] = known_at
        self._summary_state["health"] = health

    def _refresh_health_from_lifecycle(
        self, lifecycle: Mapping[str, Any], *, known_at: Any
    ) -> None:
        health = dict(self._summary_state.get("health") or {})
        health["phase"] = lifecycle.get("phase") or health.get("phase")
        health["status"] = str(lifecycle.get("status") or health.get("status") or "waiting")
        if known_at:
            health["last_event_at"] = known_at
        self._summary_state["health"] = health

    # ------------------------------------------------------------------
    # Fanout emission
    # ------------------------------------------------------------------

    async def _emit_summary_delta(
        self,
        *,
        seq: int,
        symbol_upserts: List[Dict[str, Any]],
        lifecycle: Optional[Dict[str, Any]] = None,
        symbol_removals: Optional[List[str]] = None,
    ) -> None:
        item = FanoutSummaryDelta(
            run_id=self._run_id,
            seq=seq,
            health=dict(self._summary_state.get("health") or {}),
            lifecycle=lifecycle or dict(self._latest_lifecycle),
            symbol_upserts=list(symbol_upserts),
            symbol_removals=symbol_removals,
        )
        try:
            payload_bytes = payload_size_bytes(
                {
                    "health": item.health,
                    "lifecycle": item.lifecycle,
                    "symbol_upserts": item.symbol_upserts,
                    "symbol_removals": item.symbol_removals,
                }
            )
            self._fanout_channel.put_nowait(
                FanoutEnvelope(
                    run_id=self._run_id,
                    item=item,
                    message_kind="summary_delta",
                    payload_bytes=payload_bytes,
                )
            )
            _OBSERVER.increment(
                "fanout_enqueued_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="summary_delta",
            )
            _OBSERVER.observe(
                "fanout_payload_bytes",
                float(payload_bytes),
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="summary_delta",
            )
            self._emit_fanout_gauges()
        except asyncio.QueueFull:
            _OBSERVER.increment(
                "fanout_dropped_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="summary_delta",
                failure_mode="queue_full",
            )
            _OBSERVER.event(
                "fanout_channel_overflow",
                level=logging.WARN,
                log_to_logger=False,
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                operation="summary_delta",
                failure_mode="queue_full",
                overflow_policy="drop_new",
            )
            self._emit_fanout_gauges()

    async def _emit_open_trades_delta(
        self,
        *,
        seq: int,
        upserts: List[Dict[str, Any]],
        removals: List[str],
    ) -> None:
        item = FanoutOpenTradesDelta(
            run_id=self._run_id,
            seq=seq,
            upserts=upserts,
            removals=removals,
        )
        try:
            payload_bytes = payload_size_bytes(
                {
                    "upserts": item.upserts,
                    "removals": item.removals,
                }
            )
            self._fanout_channel.put_nowait(
                FanoutEnvelope(
                    run_id=self._run_id,
                    item=item,
                    message_kind="open_trades_delta",
                    payload_bytes=payload_bytes,
                )
            )
            _OBSERVER.increment(
                "fanout_enqueued_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="open_trades_delta",
            )
            _OBSERVER.observe(
                "fanout_payload_bytes",
                float(payload_bytes),
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="open_trades_delta",
            )
            self._emit_fanout_gauges()
        except asyncio.QueueFull:
            _OBSERVER.increment(
                "fanout_dropped_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="open_trades_delta",
                failure_mode="queue_full",
            )
            _OBSERVER.event(
                "fanout_channel_overflow",
                level=logging.WARN,
                log_to_logger=False,
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                operation="open_trades_delta",
                failure_mode="queue_full",
                overflow_policy="drop_new",
            )
            self._emit_fanout_gauges()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _persist_summary_state(
        self, *, seq: int, event_time: Any, known_at: Any
    ) -> None:
        await self._await_persistence(
            storage_target="bot_run_view_state",
            pipeline_stage="summary_state_persist",
            func=upsert_bot_run_view_state,
            args=(
                {
                    "run_id": self._run_id,
                    "bot_id": self._bot_id,
                    "series_key": RUN_SCOPE_KEY,
                    "seq": int(seq),
                    "schema_version": int(self._summary_state.get("schema_version") or 4),
                    "payload": serialize_run_summary_state(self._summary_state),
                    "event_time": event_time,
                    "known_at": known_at,
                    "updated_at": known_at,
                },
            ),
        )

    async def _next_lifecycle_seq(self, lifecycle_payload: Mapping[str, Any]) -> int:
        if self._latest_lifecycle_seq is None:
            latest = await self._await_persistence(
                storage_target="bot_runtime_events",
                pipeline_stage="lifecycle_seq_load",
                func=get_latest_bot_runtime_event,
                bot_id=self._bot_id,
                run_id=self._run_id,
                event_types=[EVENT_TYPE_LIFECYCLE],
            )
            self._latest_lifecycle_seq = max(
                _LIFECYCLE_SEQ_OFFSET,
                _coerce_int((latest or {}).get("seq"), default=0),
            )
        raw_seq = _coerce_int(lifecycle_payload.get("seq"), default=0)
        candidate = _LIFECYCLE_SEQ_OFFSET + raw_seq if raw_seq > 0 else 0
        if candidate > _INT32_MAX:
            candidate = 0
        next_seq = max(self._latest_lifecycle_seq + 1, candidate or (_LIFECYCLE_SEQ_OFFSET + 1))
        if next_seq > _INT32_MAX:
            raise RuntimeError(
                f"lifecycle seq exceeded int32 | run_id={self._run_id} | seq={next_seq}"
            )
        self._latest_lifecycle_seq = next_seq
        return next_seq

    async def _persist_lifecycle_event(
        self, *, lifecycle: Mapping[str, Any], lifecycle_seq: int
    ) -> None:
        await self._await_persistence(
            storage_target="bot_runtime_events",
            pipeline_stage="lifecycle_event_persist",
            func=record_bot_runtime_event,
            args=(
                {
                    "event_id": f"{self._bot_id}:{self._run_id}:{EVENT_TYPE_LIFECYCLE}:{lifecycle_seq}",
                    "bot_id": self._bot_id,
                    "run_id": self._run_id,
                    "seq": lifecycle_seq,
                    "event_type": EVENT_TYPE_LIFECYCLE,
                    "critical": True,
                    "schema_version": 4,
                    "event_time": lifecycle.get("checkpoint_at") or lifecycle.get("updated_at"),
                    "known_at": lifecycle.get("checkpoint_at") or lifecycle.get("updated_at"),
                    "payload": dict(lifecycle),
                },
            ),
        )

    # ------------------------------------------------------------------
    # Run metadata
    # ------------------------------------------------------------------

    async def _ensure_run_meta(self) -> None:
        cached = _mapping(self._summary_state.get("run_meta"))
        if cached.get("run_id") == str(self._run_id):
            return
        try:
            row = await asyncio.to_thread(get_bot_run, str(self._run_id))
            row = _mapping(row)
            meta = {
                "run_id": str(self._run_id),
                "bot_id": str(row.get("bot_id") or self._bot_id),
                "strategy_id": row.get("strategy_id"),
                "strategy_name": row.get("strategy_name"),
                "run_type": row.get("run_type"),
                "datasource": row.get("datasource"),
                "exchange": row.get("exchange"),
                "symbols": list(row.get("symbols") or []) if isinstance(row.get("symbols"), list) else [],
                "started_at": row.get("started_at"),
                "ended_at": row.get("ended_at"),
            }
            self._summary_state["run_meta"] = meta
        except Exception as exc:
            logger.warning(
                "run_projector_meta_load_failed | run_id=%s | error=%s", self._run_id, exc
            )

    # ------------------------------------------------------------------
    # Side-effect publishes
    # ------------------------------------------------------------------

    async def _publish_runtime_update(
        self, *, runtime_payload: Mapping[str, Any], seq: int, known_at: Any
    ) -> None:
        try:
            from .bot_service import publish_runtime_update
            await asyncio.to_thread(
                publish_runtime_update,
                self._bot_id,
                {
                    **dict(runtime_payload),
                    "status": str(runtime_payload.get("status") or "running"),
                    "run_id": self._run_id,
                    "seq": seq,
                    "known_at": known_at,
                    "last_snapshot_at": known_at,
                    "warnings": list(runtime_payload.get("warnings") or []),
                },
            )
        except Exception as exc:
            logger.warning(
                "run_projector_runtime_publish_failed | run_id=%s | error=%s",
                self._run_id, exc,
            )

    async def _publish_projected_bot(self) -> None:
        try:
            from .bot_service import publish_projected_bot
            await asyncio.to_thread(publish_projected_bot, self._bot_id, inspect_container=False)
        except Exception as exc:
            logger.warning(
                "run_projector_projected_bot_failed | run_id=%s | error=%s", self._run_id, exc
            )

    # ------------------------------------------------------------------
    # Initial state load
    # ------------------------------------------------------------------

    async def _load_initial_state(self) -> None:
        """Load latest persisted run summary from storage on startup."""
        started = time.perf_counter()
        try:
            row = await asyncio.to_thread(
                get_latest_bot_run_view_state,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=RUN_SCOPE_KEY,
            )
            if row:
                payload = (row or {}).get("payload")
                if payload:
                    self._summary_state = read_run_summary_state(
                        payload, bot_id=self._bot_id, run_id=self._run_id
                    )
                    load_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
                    _OBSERVER.observe(
                        "run_projector_initial_state_load_ms",
                        load_ms,
                        bot_id=self._bot_id,
                        run_id=self._run_id,
                        storage_target="bot_run_view_state",
                    )
                    _OBSERVER.observe(
                        "db_initial_load_ms",
                        load_ms,
                        bot_id=self._bot_id,
                        run_id=self._run_id,
                        storage_target="bot_run_view_state",
                    )
                    _OBSERVER.event(
                        "db_initial_state_load_completed",
                        bot_id=self._bot_id,
                        run_id=self._run_id,
                        storage_target="bot_run_view_state",
                        load_ms=round(load_ms, 6),
                    )
        except Exception as exc:
            load_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
            _OBSERVER.observe(
                "run_projector_initial_state_load_ms",
                load_ms,
                bot_id=self._bot_id,
                run_id=self._run_id,
                storage_target="bot_run_view_state",
                failure_mode="load_failed",
            )
            _OBSERVER.observe(
                "db_initial_load_ms",
                load_ms,
                bot_id=self._bot_id,
                run_id=self._run_id,
                storage_target="bot_run_view_state",
                failure_mode="load_failed",
            )
            _OBSERVER.event(
                "db_initial_state_load_failed",
                level=logging.WARN,
                bot_id=self._bot_id,
                run_id=self._run_id,
                storage_target="bot_run_view_state",
                failure_mode="load_failed",
                load_ms=round(load_ms, 6),
                error=str(exc),
            )

    def _emit_notification_gauges(self) -> None:
        oldest_age_ms = 0.0
        if self._symbol_notifications.qsize() > 0:
            try:
                envelope = self._symbol_notifications._queue[0]
                if isinstance(envelope, QueueEnvelope):
                    oldest_age_ms = max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)
            except Exception:
                oldest_age_ms = 0.0
        self._run_notification_queue_metrics.emit(
            depth=self._symbol_notifications.qsize(),
            capacity=max(int(self._symbol_notifications.maxsize or 1), 1),
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


__all__ = ["RunProjector"]
