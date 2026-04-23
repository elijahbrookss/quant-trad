"""Symbol-level BotLens projector.

SymbolProjector coordinates concern-specific symbol projection for one
``(run_id, symbol_key)`` lane. It does not own run-level state and it does not
shape transport payloads.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Tuple

from ..observability import BackendObserver, QueueStateMetricOwner
from .botlens_event_replay import load_domain_projection_batches
from .botlens_mailbox import FanoutEnvelope, FanoutSymbolDeltaBatch, QueueEnvelope, SymbolMailbox
from .botlens_state import (
    ProjectionBatch,
    SymbolDiagnosticsState,
    SymbolProjectionSnapshot,
    SymbolConcernDelta,
    SymbolReadinessState,
    apply_symbol_batch,
    empty_symbol_projection_snapshot,
)

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_symbol_projector", event_logger=logger)


@dataclass(frozen=True)
class SymbolProjectorSnapshot:
    state: SymbolProjectionSnapshot


@dataclass(frozen=True)
class SymbolSummaryNotification:
    run_id: str
    bot_id: str
    symbol_key: str
    seq: int
    event_time: Any
    known_at: Any
    symbol_summary: dict[str, Any]
    trade_upserts: Tuple[dict[str, Any], ...]
    trade_removals: Tuple[str, ...]
    runtime: dict[str, Any]


class SymbolProjector:
    def __init__(
        self,
        *,
        run_id: str,
        bot_id: str,
        symbol_key: str,
        mailbox: SymbolMailbox,
        run_notifications: "asyncio.Queue[Any]",
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

        self._state = empty_symbol_projection_snapshot(symbol_key)
        self._current_session_id: Optional[str] = None
        self._seen_event_ids: set[str] = set()
        self._active = True
        self._ready = asyncio.Event()

    @property
    def symbol_key(self) -> str:
        return self._symbol_key

    def get_snapshot(self) -> SymbolProjectionSnapshot:
        return self._state

    async def wait_until_ready(self) -> None:
        await self._ready.wait()

    async def run(self) -> None:
        await self._load_initial_state()
        self._ready.set()

        event_task: asyncio.Task[Any] = asyncio.create_task(
            self._mailbox.event_queue.get(), name=f"sym-events-{self._symbol_key}"
        )
        bootstrap_task: asyncio.Task[None] = asyncio.create_task(
            self._mailbox.bootstrap_slot.event.wait(),
            name=f"sym-bootstrap-{self._symbol_key}",
        )

        try:
            while self._active:
                if self._mailbox.bootstrap_slot.pending:
                    event_task, bootstrap_task = await self._handle_bootstrap(event_task, bootstrap_task)
                    continue

                done, _ = await asyncio.wait(
                    {event_task, bootstrap_task},
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=5.0,
                )

                if bootstrap_task in done:
                    event_task, bootstrap_task = await self._handle_bootstrap(event_task, bootstrap_task)
                    continue

                if event_task in done:
                    try:
                        envelope = event_task.result()
                        batch = envelope.payload if isinstance(envelope, QueueEnvelope) else envelope
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
                            message_kind="domain_batch",
                        )
                        self._mailbox._emit_fact_gauges()
                        await self._apply_batch(batch)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        _OBSERVER.event(
                            "symbol_projector_failed",
                            level=logging.ERROR,
                            bot_id=self._bot_id,
                            run_id=self._run_id,
                            series_key=self._symbol_key,
                            failure_mode="batch_apply_failed",
                            error=str(exc),
                        )
                    event_task = asyncio.create_task(
                        self._mailbox.event_queue.get(),
                        name=f"sym-events-{self._symbol_key}",
                    )
        except asyncio.CancelledError:
            pass
        finally:
            for task in (event_task, bootstrap_task):
                if not task.done():
                    task.cancel()

    async def _handle_bootstrap(
        self,
        event_task: "asyncio.Task[Any]",
        bootstrap_task: "asyncio.Task[None]",
    ) -> Tuple[asyncio.Task[Any], asyncio.Task[None]]:
        bootstrap_delay_ms = self._mailbox.bootstrap_slot.pending_age_ms
        batch = self._mailbox.bootstrap_slot.take()

        if not event_task.done():
            event_task.cancel()
            try:
                await event_task
            except (asyncio.CancelledError, Exception):
                pass

        if batch is not None:
            self._drain_stale_session_batches(batch.bridge_session_id or "")
            _OBSERVER.observe(
                "bootstrap_apply_delay_ms",
                bootstrap_delay_ms,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                message_kind="bootstrap",
            )
            await self._apply_bootstrap(batch)

        if not bootstrap_task.done():
            bootstrap_task.cancel()
            try:
                await bootstrap_task
            except (asyncio.CancelledError, Exception):
                pass

        return (
            asyncio.create_task(self._mailbox.event_queue.get(), name=f"sym-events-{self._symbol_key}"),
            asyncio.create_task(self._mailbox.bootstrap_slot.event.wait(), name=f"sym-bootstrap-{self._symbol_key}"),
        )

    def _drain_stale_session_batches(self, new_session_id: str) -> int:
        stale = []
        fresh = []
        while True:
            try:
                item = self._mailbox.event_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            batch = item.payload if isinstance(item, QueueEnvelope) else item
            session_id = getattr(batch, "bridge_session_id", None)
            if session_id == new_session_id:
                fresh.append(item)
            else:
                stale.append(item)
        for item in fresh:
            try:
                self._mailbox.event_queue.put_nowait(item)
            except asyncio.QueueFull:
                _OBSERVER.increment(
                    "symbol_fact_dropped_total",
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    series_key=self._symbol_key,
                    queue_name="symbol_fact_queue",
                    message_kind="domain_batch",
                    failure_mode="queue_full",
                )
        if stale:
            _OBSERVER.increment(
                "symbol_projector_stale_facts_drained_total",
                value=float(len(stale)),
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                message_kind="domain_batch",
            )
        return len(stale)

    async def _apply_bootstrap(self, batch: ProjectionBatch) -> None:
        self._state = empty_symbol_projection_snapshot(self._symbol_key)
        self._current_session_id = batch.bridge_session_id
        await self._apply_projected_batch(batch, message_kind="bootstrap")

    async def _apply_batch(self, batch: ProjectionBatch) -> None:
        if self._current_session_id and batch.bridge_session_id and batch.bridge_session_id != self._current_session_id:
            _OBSERVER.increment(
                "symbol_projector_stale_session_reject_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                message_kind="domain_batch",
            )
            return
        if not self._current_session_id and batch.bridge_session_id:
            self._current_session_id = batch.bridge_session_id
        await self._apply_projected_batch(batch, message_kind="facts")

    async def _apply_facts(self, batch: ProjectionBatch) -> None:
        await self._apply_batch(batch)

    async def _apply_projected_batch(self, batch: ProjectionBatch, *, message_kind: str) -> None:
        started = time.perf_counter()
        if not batch.events:
            return
        events = tuple(
            event for event in batch.events
            if str(getattr(event, "event_id", "") or "").strip() not in self._seen_event_ids
        )
        if not events:
            return
        batch = replace(batch, events=events)
        next_state, deltas = apply_symbol_batch(self._state, batch=batch)
        self._state = next_state
        self._seen_event_ids.update(
            str(getattr(event, "event_id", "") or "").strip()
            for event in events
            if str(getattr(event, "event_id", "") or "").strip()
        )
        _OBSERVER.increment(
            f"symbol_projector_{'bootstrap' if message_kind == 'bootstrap' else 'fact'}_apply_total",
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind=message_kind,
        )
        _OBSERVER.observe(
            "symbol_projector_batch_size",
            float(len(batch.events)),
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind=message_kind,
        )
        await self._emit_run_notification(batch=batch, deltas=deltas)
        await self._emit_deltas(deltas=deltas)
        _OBSERVER.observe(
            "symbol_projector_apply_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            bot_id=self._bot_id,
            run_id=self._run_id,
            series_key=self._symbol_key,
            message_kind=message_kind,
        )

    async def _emit_run_notification(
        self,
        *,
        batch: ProjectionBatch,
        deltas: Tuple[SymbolConcernDelta, ...],
    ) -> None:
        trade_upserts: list[dict[str, Any]] = []
        trade_removals: list[str] = []
        for delta in deltas:
            if not hasattr(delta, "trade_upserts"):
                continue
            trade_upserts.extend(
                dict(entry)
                for entry in getattr(delta, "trade_upserts", ())
                if isinstance(entry, Mapping)
            )
            trade_removals.extend(
                str(entry)
                for entry in getattr(delta, "trade_removals", ())
                if str(entry).strip()
            )

        runtime_payload = self._runtime_payload_from_batch(batch)
        if not deltas and not runtime_payload:
            return

        notification = SymbolSummaryNotification(
            run_id=self._run_id,
            bot_id=self._bot_id,
            symbol_key=self._state.symbol_key,
            seq=int(self._state.seq),
            event_time=batch.event_time,
            known_at=batch.known_at,
            symbol_summary=self._symbol_summary_payload(),
            trade_upserts=tuple(trade_upserts),
            trade_removals=tuple(trade_removals),
            runtime=runtime_payload,
        )
        try:
            self._run_notifications.put_nowait(QueueEnvelope(payload=notification))
            _OBSERVER.increment(
                "run_notification_enqueued_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                queue_name="run_notification_queue",
                message_kind="symbol_summary",
            )
            self._emit_run_notification_gauges()
        except asyncio.QueueFull:
            _OBSERVER.increment(
                "run_notification_dropped_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                queue_name="run_notification_queue",
                message_kind="symbol_summary",
                failure_mode="queue_full",
            )
            _OBSERVER.event(
                "run_notification_queue_overflow",
                level=logging.WARN,
                log_to_logger=False,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                queue_name="run_notification_queue",
                message_kind="symbol_summary",
                depth=self._run_notifications.qsize(),
                overflow_policy="drop_new",
            )
            self._emit_run_notification_gauges()

    def _symbol_summary_payload(self) -> dict[str, Any]:
        identity = self._state.identity.to_dict()
        candles = self._state.candles.candles
        trades = self._state.trades.trades
        last_candle = candles[-1] if candles else {}
        last_trade = trades[-1] if trades else {}
        last_trade_at = (
            str(last_trade.get("updated_at") or last_trade.get("closed_at") or last_trade.get("opened_at") or "").strip()
            or None
        )
        return {
            "symbol_key": self._state.symbol_key,
            "instrument_id": identity.get("instrument_id"),
            "symbol": identity.get("symbol"),
            "timeframe": identity.get("timeframe"),
            "last_event_at": self._state.last_event_at,
            "last_bar_time": last_candle.get("time"),
            "last_price": last_candle.get("close"),
            "candle_count": len(candles),
            "last_trade_at": last_trade_at,
            "last_activity_at": self._state.last_event_at,
            "stats": dict(self._state.stats.stats),
            "readiness": self._state.readiness.to_dict(),
        }

    def _runtime_payload_from_batch(self, batch: ProjectionBatch) -> dict[str, Any]:
        for event in batch.events:
            context = event.context.to_dict() if hasattr(event.context, "to_dict") else {}
            if not context.get("status"):
                continue
            if "warning_count" not in context and "warnings" not in context:
                continue
            return dict(context)
        return {}

    async def _emit_deltas(self, *, deltas: Tuple[SymbolConcernDelta, ...]) -> None:
        if not deltas:
            return
        try:
            self._fanout_channel.put_nowait(
                FanoutEnvelope(
                    run_id=self._run_id,
                    item=FanoutSymbolDeltaBatch(run_id=self._run_id, deltas=deltas),
                    message_kind="symbol_projection_delta",
                    payload_bytes=0,
                )
            )
            self._emit_fanout_gauges()
        except asyncio.QueueFull:
            _OBSERVER.increment(
                "fanout_dropped_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                queue_name="fanout_channel",
                message_kind="symbol_projection_delta",
                failure_mode="queue_full",
            )
            _OBSERVER.event(
                "fanout_channel_overflow",
                level=logging.WARN,
                log_to_logger=False,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                queue_name="fanout_channel",
                message_kind="symbol_projection_delta",
                depth=self._fanout_channel.qsize(),
                overflow_policy="drop_new",
                failure_mode="queue_full",
            )
            self._emit_fanout_gauges()

    async def _load_initial_state(self) -> None:
        started = time.perf_counter()
        try:
            batches = await asyncio.to_thread(
                load_domain_projection_batches,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
            )
            for batch in batches:
                self._state, _ = apply_symbol_batch(self._state, batch=batch)
                self._seen_event_ids.update(
                    str(getattr(event, "event_id", "") or "").strip()
                    for event in batch.events
                    if str(getattr(event, "event_id", "") or "").strip()
                )
            if batches:
                load_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
                _OBSERVER.observe(
                    "ledger_rebuild_ms",
                    load_ms,
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    series_key=self._symbol_key,
                    storage_target="bot_runtime_events",
                )
        except Exception as exc:
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            error_text = str(exc)[:512]
            diagnostic = {
                "id": f"projection_error:{self._symbol_key}",
                "event_id": f"projection_error:{self._run_id}:{self._symbol_key}",
                "type": "projection_error",
                "severity": "error",
                "source": "botlens_symbol_projector",
                "message": "Symbol projection is unavailable because ledger rebuild failed.",
                "error": error_text,
                "timestamp": now,
                "created_at": now,
            }
            self._state = replace(
                self._state,
                diagnostics=SymbolDiagnosticsState(diagnostics=(diagnostic,)),
                readiness=SymbolReadinessState(snapshot_ready=False, symbol_live=False),
            )
            _OBSERVER.event(
                "ledger_rebuild_failed",
                level=logging.ERROR,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=self._symbol_key,
                storage_target="bot_runtime_events",
                failure_mode="load_failed",
                projection_state="projection_error",
                error=error_text,
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

    def _emit_run_notification_gauges(self) -> None:
        oldest_age_ms = 0.0
        if self._run_notifications.qsize() > 0:
            try:
                envelope = self._run_notifications._queue[0]
                if isinstance(envelope, QueueEnvelope):
                    oldest_age_ms = max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)
            except Exception:
                oldest_age_ms = 0.0
        self._run_notification_queue_metrics.emit(
            depth=self._run_notifications.qsize(),
            capacity=max(int(self._run_notifications.maxsize or 1), 1),
            oldest_age_ms=oldest_age_ms,
        )


__all__ = ["SymbolProjector", "SymbolProjectorSnapshot", "SymbolSummaryNotification"]
