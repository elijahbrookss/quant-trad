"""Projector registry for the BotLens telemetry pipeline.

ProjectorRegistry creates, holds, and evicts per-run projector infrastructure.
It is the single source of truth for which runs are currently active and what
their associated projectors and mailboxes are.

Responsibilities:
  - Create RunProjector + SymbolProjectors on first message for a run/symbol.
  - Start the run projector task, per-symbol projector tasks, and fanout delivery task.
  - Expose lookup APIs for mailboxes and symbol projectors (used by snapshot delivery).
  - Evict stale or terminated run contexts when the run projector signals completion.

Not responsible for:
  - Projection logic (that lives in RunProjector / SymbolProjector).
  - Fanout delivery logic (that lives in _fanout_delivery_loop).
  - State mutation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ..observability import BackendObserver, QueueStateMetricOwner
from .botlens_mailbox import (
    FanoutEnvelope,
    FanoutOpenTradesDelta,
    FanoutSummaryDelta,
    FanoutTypedDelta,
    RunMailbox,
    SymbolMailbox,
    _FANOUT_CHANNEL_MAX,
    _FANOUT_STOP,
)
from .botlens_run_projector import RunProjector
from .botlens_symbol_projector import SymbolProjector

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_projector_registry", event_logger=logger)


# ---------------------------------------------------------------------------
# Per-run context bundle
# ---------------------------------------------------------------------------

@dataclass
class RunProjectorContext:
    run_id: str
    bot_id: str
    mailbox: RunMailbox
    run_projector: RunProjector
    run_projector_task: asyncio.Task
    fanout_channel: "asyncio.Queue[Any]"
    fanout_queue_metrics: QueueStateMetricOwner
    fanout_task: asyncio.Task
    symbol_projectors: Dict[str, SymbolProjector] = field(default_factory=dict)
    symbol_projector_tasks: Dict[str, asyncio.Task] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class ProjectorRegistry:
    """
    Creates and holds active RunProjectorContext instances keyed by run_id.

    Thread safety: access is serialized through an asyncio.Lock because
    multiple concurrent ingest coroutines may race to create the same run.
    """

    def __init__(self, run_stream: Any) -> None:
        # run_stream: BotLensRunStream — used by the fanout delivery loop
        self._run_stream = run_stream
        self._contexts: Dict[str, RunProjectorContext] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API used by IntakeRouter
    # ------------------------------------------------------------------

    async def ensure_run(self, run_id: str, bot_id: str) -> RunMailbox:
        """
        Ensure a run projector context exists for run_id and return its mailbox.
        Creates the full context (run projector + fanout delivery loop) on first call.
        """
        async with self._lock:
            if run_id in self._contexts:
                return self._contexts[run_id].mailbox
            context = self._create_run_context(run_id=run_id, bot_id=bot_id)
            self._contexts[run_id] = context
            return context.mailbox

    async def ensure_symbol(
        self, run_id: str, bot_id: str, symbol_key: str
    ) -> SymbolMailbox:
        """
        Ensure a symbol projector exists for (run_id, symbol_key).
        Creates the run context first if needed.
        """
        async with self._lock:
            if run_id not in self._contexts:
                context = self._create_run_context(run_id=run_id, bot_id=bot_id)
                self._contexts[run_id] = context
            context = self._contexts[run_id]

            symbol_mailbox = context.mailbox.get_or_create_symbol_mailbox(symbol_key)

            if symbol_key not in context.symbol_projectors:
                self._create_symbol_projector(context, symbol_key, symbol_mailbox)

            return symbol_mailbox

    def get_symbol_projector(
        self, run_id: str, symbol_key: str
    ) -> Optional[SymbolProjector]:
        """Return the live symbol projector for (run_id, symbol_key) or None."""
        context = self._contexts.get(run_id)
        if context is None:
            return None
        return context.symbol_projectors.get(symbol_key)

    def get_bot_id(self, run_id: str) -> Optional[str]:
        context = self._contexts.get(run_id)
        return context.bot_id if context else None

    def active_run_count(self) -> int:
        return len(self._contexts)

    def active_symbol_count(self) -> int:
        return sum(len(ctx.symbol_projectors) for ctx in self._contexts.values())

    # ------------------------------------------------------------------
    # Context creation (must be called with self._lock held)
    # ------------------------------------------------------------------

    def _create_run_context(self, run_id: str, bot_id: str) -> RunProjectorContext:
        mailbox = RunMailbox(run_id=run_id, bot_id=bot_id)
        fanout_channel: asyncio.Queue[Any] = asyncio.Queue(maxsize=_FANOUT_CHANNEL_MAX)
        fanout_queue_metrics = QueueStateMetricOwner(
            observer=_OBSERVER,
            key=f"fanout_channel:{run_id}",
            depth_metric="fanout_queue_depth",
            utilization_metric="fanout_queue_utilization",
            oldest_age_metric="fanout_queue_oldest_age_ms",
            labels={
                "bot_id": bot_id,
                "run_id": run_id,
                "queue_name": "fanout_channel",
            },
        )

        run_projector = RunProjector(
            run_id=run_id,
            bot_id=bot_id,
            mailbox=mailbox,
            fanout_channel=fanout_channel,
            fanout_queue_metrics=fanout_queue_metrics,
            on_evict=self._evict,
        )

        run_projector_task = asyncio.create_task(
            run_projector.run(), name=f"botlens-run-projector-{run_id}"
        )
        fanout_task = asyncio.create_task(
            _fanout_delivery_loop(
                run_id=run_id,
                fanout_channel=fanout_channel,
                fanout_queue_metrics=fanout_queue_metrics,
                run_stream=self._run_stream,
            ),
            name=f"botlens-fanout-{run_id}",
        )

        _OBSERVER.event(
            "run_projector_created",
            bot_id=bot_id,
            run_id=run_id,
        )
        return RunProjectorContext(
            run_id=run_id,
            bot_id=bot_id,
            mailbox=mailbox,
            run_projector=run_projector,
            run_projector_task=run_projector_task,
            fanout_channel=fanout_channel,
            fanout_queue_metrics=fanout_queue_metrics,
            fanout_task=fanout_task,
        )

    def _create_symbol_projector(
        self,
        context: RunProjectorContext,
        symbol_key: str,
        symbol_mailbox: SymbolMailbox,
    ) -> None:
        projector = SymbolProjector(
            run_id=context.run_id,
            bot_id=context.bot_id,
            symbol_key=symbol_key,
            mailbox=symbol_mailbox,
            run_notifications=context.run_projector.symbol_notifications,
            fanout_channel=context.fanout_channel,
            run_notification_queue_metrics=context.run_projector.run_notification_queue_metrics,
            fanout_queue_metrics=context.fanout_queue_metrics,
        )
        task = asyncio.create_task(
            projector.run(),
            name=f"botlens-sym-projector-{context.run_id}-{symbol_key}",
        )
        context.symbol_projectors[symbol_key] = projector
        context.symbol_projector_tasks[symbol_key] = task
        _OBSERVER.event(
            "symbol_projector_created",
            bot_id=context.bot_id,
            run_id=context.run_id,
            series_key=symbol_key,
        )

    # ------------------------------------------------------------------
    # Eviction (called by RunProjector via on_evict callback)
    # ------------------------------------------------------------------

    async def _evict(self, run_id: str) -> None:
        async with self._lock:
            context = self._contexts.pop(run_id, None)

        if context is None:
            return

        # Signal fanout delivery loop to stop.
        try:
            context.fanout_channel.put_nowait(_FANOUT_STOP)
        except asyncio.QueueFull:
            context.fanout_task.cancel()

        # Cancel symbol projector tasks.
        for task in context.symbol_projector_tasks.values():
            task.cancel()

        # Evict from run stream (closes viewer connections).
        await self._run_stream.evict_run(run_id=run_id)

        _OBSERVER.event(
            "run_evicted",
            bot_id=context.bot_id,
            run_id=run_id,
            symbol_count=len(context.symbol_projectors),
        )


# ---------------------------------------------------------------------------
# Fanout delivery loop (one per run, downstream of projection)
# ---------------------------------------------------------------------------

async def _fanout_delivery_loop(
    *,
    run_id: str,
    fanout_channel: "asyncio.Queue[Any]",
    fanout_queue_metrics: QueueStateMetricOwner,
    run_stream: Any,
) -> None:
    """
    Reads fanout items from the channel and delivers them to WebSocket viewers.

    This loop is the ONLY place where BotLensRunStream.broadcast_* is called.
    Projectors put items into fanout_channel without awaiting delivery.
    """
    while True:
        try:
            item = await fanout_channel.get()
        except asyncio.CancelledError:
            break

        if item is _FANOUT_STOP:
            break

        try:
            if isinstance(item, FanoutEnvelope):
                envelope = item
            else:
                envelope = FanoutEnvelope(
                    run_id=run_id,
                    item=item,
                    message_kind="legacy",
                    payload_bytes=0,
                )
            queue_wait_ms = max((time.monotonic() - envelope.enqueued_monotonic) * 1000.0, 0.0)
            deliver_started = time.perf_counter()

            if isinstance(envelope.item, FanoutTypedDelta):
                _OBSERVER.increment(
                    "fanout_delivery_items_total",
                    value=float(len(envelope.item.prepared_deltas)),
                    run_id=envelope.run_id,
                    queue_name="fanout_channel",
                    message_kind=envelope.message_kind,
                )
                for prepared in envelope.item.prepared_deltas:
                    await run_stream.broadcast_typed_delta(prepared)

            elif isinstance(envelope.item, FanoutSummaryDelta):
                _OBSERVER.increment(
                    "fanout_delivery_items_total",
                    run_id=envelope.run_id,
                    queue_name="fanout_channel",
                    message_kind=envelope.message_kind,
                )
                await run_stream.broadcast_summary_delta(
                    run_id=envelope.item.run_id,
                    seq=envelope.item.seq,
                    health=envelope.item.health,
                    lifecycle=envelope.item.lifecycle,
                    symbol_upserts=envelope.item.symbol_upserts,
                    symbol_removals=envelope.item.symbol_removals,
                )

            elif isinstance(envelope.item, FanoutOpenTradesDelta):
                _OBSERVER.increment(
                    "fanout_delivery_items_total",
                    run_id=envelope.run_id,
                    queue_name="fanout_channel",
                    message_kind=envelope.message_kind,
                )
                await run_stream.broadcast_open_trades_delta(
                    run_id=envelope.item.run_id,
                    seq=envelope.item.seq,
                    upserts=envelope.item.upserts,
                    removals=envelope.item.removals,
                )
            _OBSERVER.observe(
                "fanout_queue_wait_ms",
                queue_wait_ms,
                run_id=envelope.run_id,
                queue_name="fanout_channel",
                message_kind=envelope.message_kind,
            )
            _OBSERVER.observe(
                "fanout_delivery_ms",
                max((time.perf_counter() - deliver_started) * 1000.0, 0.0),
                run_id=envelope.run_id,
                queue_name="fanout_channel",
                message_kind=envelope.message_kind,
            )

        except asyncio.CancelledError:
            break
        except Exception as exc:
            _OBSERVER.increment(
                "fanout_delivery_error_total",
                run_id=run_id,
                queue_name="fanout_channel",
                failure_mode="delivery_error",
            )
            _OBSERVER.event(
                "fanout_delivery_error",
                level=logging.ERROR,
                run_id=run_id,
                queue_name="fanout_channel",
                failure_mode="delivery_error",
                error=str(exc),
            )
        finally:
            fanout_channel.task_done()
            oldest_age_ms = 0.0
            if fanout_channel.qsize() > 0:
                try:
                    oldest = fanout_channel._queue[0]
                    if isinstance(oldest, FanoutEnvelope):
                        oldest_age_ms = max((time.monotonic() - oldest.enqueued_monotonic) * 1000.0, 0.0)
                except Exception:
                    oldest_age_ms = 0.0
            fanout_queue_metrics.emit(
                depth=fanout_channel.qsize(),
                capacity=max(int(fanout_channel.maxsize or 1), 1),
                oldest_age_ms=oldest_age_ms,
            )


__all__ = ["ProjectorRegistry", "RunProjectorContext"]
