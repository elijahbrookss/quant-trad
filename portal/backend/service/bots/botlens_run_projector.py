"""Run-level BotLens projector.

RunProjector coordinates concern-specific run projection for one ``run_id``.
It owns run lifecycle, runtime health, bounded fault state, the open-trades
index, and the run symbol catalog. It does not depend on SymbolProjector state.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from ..observability import BackendObserver, QueueStateMetricOwner
from .botlens_event_replay import load_domain_projection_batches, rebuild_run_projection_snapshot
from .botlens_mailbox import FanoutEnvelope, FanoutRunDeltaBatch, QueueEnvelope, RunMailbox
from .botlens_symbol_projector import SymbolSummaryNotification
from .botlens_state import (
    ProjectionBatch,
    RunConcernDelta,
    RunFaultsState,
    RunHealthDelta,
    RunHealthState,
    RunLifecycleDelta,
    RunOpenTradesDelta,
    RunProjectionSnapshot,
    RunReadinessState,
    RunSymbolCatalogDelta,
    RunOpenTradesState,
    RunSymbolCatalogState,
    _build_run_health_state,
    apply_run_batch,
    empty_run_projection_snapshot,
    is_open_trade,
)

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_run_projector", event_logger=logger)

_TERMINAL_RUN_TTL_S = 300.0
_TERMINAL_LIFECYCLE_PHASES = frozenset(
    {
        "completed",
        "stopped",
        "cancelled",
        "canceled",
        "error",
        "failed",
        "crashed",
        "startup_failed",
        "degraded_terminal",
    }
)
_TERMINAL_LIFECYCLE_STATUSES = frozenset(
    {
        "completed",
        "stopped",
        "cancelled",
        "canceled",
        "error",
        "failed",
        "crashed",
        "startup_failed",
        "degraded_terminal",
    }
)


def _batch_terminal_status(batch: ProjectionBatch) -> str | None:
    for event in batch.events:
        event_name = str(getattr(event.event_name, "value", event.event_name) or "").strip().upper()
        if event_name not in {"RUN_COMPLETED", "RUN_FAILED", "RUN_STOPPED", "RUN_CANCELLED"}:
            continue
        context = event.context.to_dict() if hasattr(event.context, "to_dict") else {}
        status = str(context.get("status") or "").strip().lower()
        phase = str(context.get("phase") or "").strip().lower()
        if status in _TERMINAL_LIFECYCLE_STATUSES:
            return status
        if phase in _TERMINAL_LIFECYCLE_PHASES:
            return phase
        return event_name.lower().replace("run_", "")
    return None


class RunProjector:
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

        self._state = empty_run_projection_snapshot(bot_id=bot_id, run_id=run_id)
        self._terminal = False
        self._terminal_at: float | None = None
        self._active = True
        self._ready = asyncio.Event()

    def get_snapshot(self) -> RunProjectionSnapshot:
        return self._state

    async def wait_until_ready(self) -> None:
        await self._ready.wait()

    async def run(self) -> None:
        await self._load_initial_state()
        self._ready.set()
        lifecycle_task: asyncio.Task[Any] = asyncio.create_task(
            self._mailbox.lifecycle_queue.get(), name=f"run-lifecycle-{self._run_id}"
        )
        notification_task: asyncio.Task[Any] = asyncio.create_task(
            self._mailbox.notification_queue.get(), name=f"run-notifications-{self._run_id}"
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
                        batch = envelope.payload if isinstance(envelope, QueueEnvelope) else envelope
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
                        await self._apply_lifecycle_batch(batch)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        _OBSERVER.event(
                            "run_projector_failed",
                            level=logging.ERROR,
                            bot_id=self._bot_id,
                            run_id=self._run_id,
                            failure_mode="batch_apply_failed",
                            error=str(exc),
                        )
                    lifecycle_task = asyncio.create_task(
                        self._mailbox.lifecycle_queue.get(),
                        name=f"run-lifecycle-{self._run_id}",
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
                        self._mailbox._emit_notification_gauges()
                        if isinstance(notification, SymbolSummaryNotification):
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
                        self._mailbox.notification_queue.get(),
                        name=f"run-notifications-{self._run_id}",
                    )

                if self._should_evict():
                    break
        except asyncio.CancelledError:
            pass
        finally:
            for task in (lifecycle_task, notification_task):
                if not task.done():
                    task.cancel()
            asyncio.create_task(self._on_evict(self._run_id))

    def _should_evict(self) -> bool:
        return bool(self._terminal and self._terminal_at is not None and (time.monotonic() - self._terminal_at) > _TERMINAL_RUN_TTL_S)

    async def _apply_lifecycle_batch(self, batch: ProjectionBatch) -> None:
        started = time.perf_counter()
        terminal_status = _batch_terminal_status(batch)
        if terminal_status and self._state.open_trades.entries:
            await self._reconcile_from_canonical_ledger(
                reason="terminal_lifecycle_with_open_projection_trades",
                prior_open_trade_ids=tuple(sorted(self._state.open_trades.entries)),
            )
        try:
            self._state, deltas = apply_run_batch(self._state, batch=batch)
        except RuntimeError as exc:
            if "completed run retains open trades" not in str(exc):
                raise
            await self._reconcile_from_canonical_ledger(
                reason="terminal_lifecycle_projection_invariant",
                prior_open_trade_ids=tuple(sorted(self._state.open_trades.entries)),
            )
            self._state, deltas = apply_run_batch(self._state, batch=batch)
        await self._emit_deltas(deltas=deltas)
        await self._publish_side_effects(deltas=deltas, batch=batch)
        self._refresh_terminal_state()
        elapsed_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
        _OBSERVER.observe(
            "run_projector_apply_ms",
            elapsed_ms,
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind=batch.batch_kind,
        )
        _OBSERVER.observe(
            "run_projector_event_count",
            float(len(batch.events)),
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind="lifecycle",
        )
        _OBSERVER.observe(
            "run_projector_delta_count",
            float(len(deltas)),
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind="lifecycle",
        )
        _OBSERVER.maybe_gauge(
            key=f"run_projector_projected_seq:{self._run_id}",
            name="run_projector_projected_seq",
            value=float(max(int(self._state.seq), int(batch.seq or 0))),
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind="lifecycle",
        )

    async def _process_symbol_notification(self, notification: SymbolSummaryNotification) -> None:
        started = time.perf_counter()
        deltas: list[RunConcernDelta] = []

        symbol_summary = dict(notification.symbol_summary)
        symbol_key = str(symbol_summary.get("symbol_key") or notification.symbol_key).strip()
        if symbol_key:
            next_catalog = dict(self._state.symbol_catalog.entries)
            current_entry = next_catalog.get(symbol_key)
            if current_entry != symbol_summary:
                next_catalog[symbol_key] = symbol_summary
                self._state = replace(
                    self._state,
                    seq=max(int(self._state.seq), int(notification.seq)),
                    symbol_catalog=RunSymbolCatalogState(entries=next_catalog),
                    readiness=RunReadinessState(
                        catalog_discovered=bool(next_catalog),
                        run_live=bool(self._state.readiness.run_live),
                    ),
                )
                deltas.append(
                    RunSymbolCatalogDelta(
                        seq=int(notification.seq),
                        event_time=notification.event_time,
                        symbol_upserts=(dict(symbol_summary),),
                    )
                )

        if notification.trade_upserts or notification.trade_removals:
            next_open_trades = dict(self._state.open_trades.entries)
            changed = False
            emitted_upserts: list[dict[str, Any]] = []
            emitted_removals: list[str] = []
            current_seq = int(self._state.seq or 0)
            notification_seq = int(notification.seq or 0)
            stale_notification = notification_seq < current_seq
            removal_ids = {
                str(entry).strip()
                for entry in notification.trade_removals
                if str(entry).strip()
            }
            for trade in notification.trade_upserts:
                trade_id = str(trade.get("trade_id") or "").strip()
                if not trade_id:
                    continue
                try:
                    open_trade = is_open_trade(trade)
                except RuntimeError:
                    open_trade = False
                if not open_trade:
                    removal_ids.add(trade_id)
                    continue
                if stale_notification and trade_id not in next_open_trades:
                    continue
                if next_open_trades.get(trade_id) != trade:
                    next_open_trades[trade_id] = dict(trade)
                    emitted_upserts.append(dict(trade))
                    changed = True
            for trade_id in sorted(removal_ids):
                if next_open_trades.pop(trade_id, None) is not None:
                    emitted_removals.append(trade_id)
                    changed = True
            if changed:
                self._state = replace(
                    self._state,
                    seq=max(int(self._state.seq), int(notification.seq)),
                    open_trades=RunOpenTradesState(entries=next_open_trades),
                )
                deltas.append(
                    RunOpenTradesDelta(
                        seq=int(notification.seq),
                        event_time=notification.event_time,
                        upserts=tuple(emitted_upserts),
                        removals=tuple(emitted_removals),
                    )
                )

        if notification.runtime:
            next_health = _build_run_health_state(
                self._state.health,
                status=notification.runtime.get("status"),
                phase=self._state.lifecycle.phase,
                warning_count=notification.runtime.get("warning_count"),
                warnings=notification.runtime.get("warnings"),
                last_event_at=notification.runtime.get("last_event_at") or notification.known_at,
                worker_count=notification.runtime.get("worker_count"),
                active_workers=notification.runtime.get("active_workers"),
                trigger_event=notification.runtime.get("trigger_event"),
                runtime_state=notification.runtime.get("runtime_state"),
                last_useful_progress_at=notification.runtime.get("last_useful_progress_at"),
                progress_state=notification.runtime.get("progress_state"),
                degraded=notification.runtime.get("degraded"),
                churn=notification.runtime.get("churn"),
                pressure=notification.runtime.get("pressure"),
                recent_transitions=notification.runtime.get("recent_transitions"),
                terminal=notification.runtime.get("terminal"),
            )
            if next_health != self._state.health:
                self._state = replace(
                    self._state,
                    seq=max(int(self._state.seq), int(notification.seq)),
                    health=next_health,
                )
                deltas.append(
                    RunHealthDelta(
                        seq=int(notification.seq),
                        event_time=notification.event_time,
                        health=next_health.to_dict(),
                    )
                )

        if not deltas:
            return

        await self._emit_deltas(deltas=tuple(deltas))
        await self._publish_side_effects(
            deltas=tuple(deltas),
            batch=ProjectionBatch(
                batch_kind="botlens_symbol_summary_notification",
                run_id=self._run_id,
                bot_id=self._bot_id,
                seq=int(notification.seq),
                event_time=notification.event_time,
                known_at=notification.known_at,
                symbol_key=notification.symbol_key,
                events=(),
            ),
        )
        self._refresh_terminal_state()
        _OBSERVER.observe(
            "run_projector_apply_ms",
            max((time.perf_counter() - started) * 1000.0, 0.0),
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind="notification",
        )
        _OBSERVER.observe(
            "run_projector_delta_count",
            float(len(deltas)),
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind="notification",
        )
        _OBSERVER.maybe_gauge(
            key=f"run_projector_projected_seq:{self._run_id}",
            name="run_projector_projected_seq",
            value=float(max(int(self._state.seq), int(notification.seq or 0))),
            bot_id=self._bot_id,
            run_id=self._run_id,
            message_kind="notification",
        )

    def _refresh_terminal_state(self) -> None:
        phase = str(self._state.lifecycle.phase or "").strip().lower()
        status = str(self._state.lifecycle.status or self._state.health.status or "").strip().lower()
        terminal = phase in _TERMINAL_LIFECYCLE_PHASES or status in _TERMINAL_LIFECYCLE_STATUSES
        if terminal and not self._terminal:
            self._terminal = True
            self._terminal_at = time.monotonic()

    async def _emit_deltas(self, *, deltas: tuple[RunConcernDelta, ...]) -> None:
        if not deltas:
            return
        enqueue_started = time.perf_counter()
        try:
            self._fanout_channel.put_nowait(
                FanoutEnvelope(
                    run_id=self._run_id,
                    item=FanoutRunDeltaBatch(run_id=self._run_id, state=self._state, deltas=deltas),
                    message_kind="run_projection_delta",
                    payload_bytes=0,
                )
            )
            _OBSERVER.observe(
                "live_transport_enqueue_ms",
                max((time.perf_counter() - enqueue_started) * 1000.0, 0.0),
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="run_projection_delta",
                pipeline_stage="run_projection",
            )
            self._emit_fanout_gauges()
        except asyncio.QueueFull:
            _OBSERVER.increment(
                "fanout_dropped_total",
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="run_projection_delta",
                failure_mode="queue_full",
            )
            _OBSERVER.increment(
                "live_transport_dropped_stale_count",
                value=float(len(deltas)),
                bot_id=self._bot_id,
                run_id=self._run_id,
                queue_name="fanout_channel",
                message_kind="run_projection_delta",
                pipeline_stage="run_projection",
                source_reason="fanout_queue_full",
            )

    async def _publish_side_effects(self, *, deltas: tuple[RunConcernDelta, ...], batch: ProjectionBatch) -> None:
        if any(isinstance(delta, RunHealthDelta) for delta in deltas):
            try:
                from .bot_service import publish_runtime_update

                health = self._state.health.to_dict()
                payload = {
                    **health,
                    "run_id": self._run_id,
                    "seq": int(self._state.seq),
                    "known_at": batch.known_at,
                    "last_snapshot_at": batch.known_at,
                }
                await asyncio.to_thread(publish_runtime_update, self._bot_id, payload)
            except Exception as exc:
                logger.warning("run_projector_runtime_publish_failed | run_id=%s | error=%s", self._run_id, exc)

        if any(isinstance(delta, RunLifecycleDelta) for delta in deltas):
            try:
                from .bot_service import publish_projected_bot

                await asyncio.to_thread(publish_projected_bot, self._bot_id, inspect_container=False)
            except Exception as exc:
                logger.warning("run_projector_projected_bot_failed | run_id=%s | error=%s", self._run_id, exc)

    async def _load_initial_state(self) -> None:
        started = time.perf_counter()
        try:
            batches = await asyncio.to_thread(
                load_domain_projection_batches,
                bot_id=self._bot_id,
                run_id=self._run_id,
                series_key=None,
            )
            for batch in batches:
                self._state, _ = apply_run_batch(self._state, batch=batch)
            if batches:
                load_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
                _OBSERVER.observe(
                    "ledger_rebuild_ms",
                    load_ms,
                    bot_id=self._bot_id,
                    run_id=self._run_id,
                    storage_target="bot_runtime_events",
                )
            self._refresh_terminal_state()
        except Exception as exc:
            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            error_text = str(exc)[:512]
            warning = {
                "warning_id": f"projection_error::{self._run_id}",
                "warning_type": "projection_error",
                "severity": "error",
                "source": "botlens_run_projector",
                "message": "Run projection is unavailable because ledger rebuild failed.",
                "error": error_text,
                "first_seen_at": now,
                "last_seen_at": now,
                "updated_at": now,
                "count": 1,
            }
            fault = {
                "event_id": f"projection_error:{self._run_id}",
                "fault_code": "projection_error",
                "severity": "error",
                "source": "botlens_run_projector",
                "message": "Run projection is unavailable because ledger rebuild failed.",
                "error": error_text,
                "observed_at": now,
            }
            self._state = replace(
                self._state,
                health=RunHealthState(
                    status="projection_error",
                    phase=self._state.lifecycle.phase,
                    warning_count=1,
                    warnings=(warning,),
                    last_event_at=now,
                    worker_count=self._state.health.worker_count,
                    active_workers=self._state.health.active_workers,
                    warning_types=("projection_error",),
                    highest_warning_severity="error",
                    trigger_event="ledger_rebuild_failed",
                    runtime_state="projection_error",
                    terminal=self._state.health.terminal,
                ),
                faults=RunFaultsState(faults=(fault, *self._state.faults.faults)),
                readiness=RunReadinessState(catalog_discovered=False, run_live=False),
            )
            _OBSERVER.event(
                "ledger_rebuild_failed",
                level=logging.ERROR,
                bot_id=self._bot_id,
                run_id=self._run_id,
                storage_target="bot_runtime_events",
                failure_mode="load_failed",
                projection_state="projection_error",
                error=error_text,
            )

    async def _reconcile_from_canonical_ledger(
        self,
        *,
        reason: str,
        prior_open_trade_ids: tuple[str, ...],
    ) -> None:
        started = time.perf_counter()
        snapshot = await asyncio.to_thread(
            rebuild_run_projection_snapshot,
            bot_id=self._bot_id,
            run_id=self._run_id,
        )
        elapsed_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
        if snapshot is None:
            _OBSERVER.event(
                "run_projector_reconcile_failed",
                level=logging.ERROR,
                bot_id=self._bot_id,
                run_id=self._run_id,
                failure_mode="canonical_replay_unavailable",
                reason=reason,
                prior_open_trade_count=len(prior_open_trade_ids),
                prior_open_trade_ids=list(prior_open_trade_ids)[:20],
                replay_required=True,
                reconciliation_source="canonical_runtime_events",
            )
            raise RuntimeError(
                "botlens_run_projection_reconcile_failed: canonical replay unavailable "
                f"bot_id={self._bot_id} run_id={self._run_id}"
            )
        self._state = snapshot
        self._refresh_terminal_state()
        open_trade_ids = tuple(sorted(str(trade_id) for trade_id in self._state.open_trades.entries))
        _OBSERVER.observe(
            "run_projector_reconcile_ms",
            elapsed_ms,
            bot_id=self._bot_id,
            run_id=self._run_id,
            storage_target="bot_runtime_events",
            message_kind="lifecycle",
        )
        _OBSERVER.event(
            "run_projector_reconciled",
            level=logging.WARN if prior_open_trade_ids else logging.INFO,
            bot_id=self._bot_id,
            run_id=self._run_id,
            reason=reason,
            prior_open_trade_count=len(prior_open_trade_ids),
            open_trade_count=len(open_trade_ids),
            prior_open_trade_ids=list(prior_open_trade_ids)[:20],
            open_trade_ids=list(open_trade_ids)[:20],
            replay_required=False,
            reconciliation_source="canonical_runtime_events",
            projection_state="reconciled" if not open_trade_ids else "replayed_with_open_trades",
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
