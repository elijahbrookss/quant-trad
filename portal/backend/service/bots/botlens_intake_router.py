"""Intake router for the BotLens telemetry pipeline.

IntakeRouter is responsible for:
  - Receiving raw ingest payloads from the WebSocket ingest endpoint.
  - Validating the envelope (kind, run_id, bot_id present).
  - Extracting routing keys (run_id, symbol_key, kind).
  - Dispatching to the correct mailbox slot or channel.

IntakeRouter is NOT responsible for:
  - Projection.
  - Persistence.
  - Fanout.
  - Recovery policy beyond routing the bootstrap to the correct slot.

After dispatch this function returns immediately. All processing is asynchronous
and happens inside the projector tasks.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Any, Dict

from ..observability import BackendObserver
from ..storage.storage import record_bot_runtime_events_batch
from .botlens_contract import (
    BRIDGE_BOOTSTRAP_KIND,
    BRIDGE_FACTS_KIND,
    LIFECYCLE_KIND,
    PROJECTION_REFRESH_KIND,
    RUN_SCOPE_KEY,
    normalize_bridge_seq,
    normalize_bridge_session_id,
    normalize_ingest_kind,
    normalize_series_key,
)
from .botlens_candle_continuity import (
    CandleContinuityAccumulator,
    continuity_candles_from_fact_payload,
    continuity_summary_from_fact_payload,
    emit_candle_continuity_summary,
    expected_interval_seconds,
)
from .botlens_domain_events import (
    botlens_domain_event_type,
    build_botlens_domain_events_from_fact_batch,
    build_botlens_domain_events_from_lifecycle,
    serialize_botlens_domain_event,
)
from .botlens_projection_batches import projection_batch_from_payload, runtime_event_rows_from_batch, split_fact_events
from .botlens_projector_registry import ProjectorRegistry
from .botlens_runtime_state import BotLensRuntimeState, startup_bootstrap_admission
from .botlens_state import ProjectionBatch

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_intake_router", event_logger=logger)
_PERSIST_BATCH_MAX_ROWS = 128
_PERSIST_BATCH_MAX_DELAY_MS = 10
_TERMINAL_LIFECYCLE_STATES = frozenset({"completed", "stopped", "cancelled", "canceled", "error", "failed", "crashed", "startup_failed"})


def _ingest_source_reason(*, kind: str, payload: Mapping[str, Any]) -> str:
    explicit = str(payload.get("source_reason") or "").strip().lower()
    if explicit in {"ingest", "replay", "retry", "bootstrap", "projector", "transport", "unknown"}:
        return explicit
    if kind == BRIDGE_BOOTSTRAP_KIND:
        return "bootstrap"
    if "replay" in explicit:
        return "replay"
    if "retry" in explicit:
        return "retry"
    if "projector" in explicit:
        return "projector"
    if "transport" in explicit:
        return "transport"
    return "ingest"


@dataclass
class _PendingPersistBatch:
    rows: list[Dict[str, Any]] = field(default_factory=list)
    waiters: list[asyncio.Future[int]] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)
    flushing: bool = False
    flush_task: asyncio.Task[None] | None = None


class IntakeRouter:
    """
    Validates and routes ingest payloads to the correct mailbox/slot.

    One instance is shared across all ingest connections for the process.
    All methods are non-blocking (no await on projection or persistence).
    """

    def __init__(
        self,
        registry: ProjectorRegistry,
        *,
        persist_batch_max_rows: int = _PERSIST_BATCH_MAX_ROWS,
        persist_batch_max_delay_ms: int = _PERSIST_BATCH_MAX_DELAY_MS,
    ) -> None:
        self._registry = registry
        self._persist_batch_max_rows = max(int(persist_batch_max_rows), 1)
        self._persist_batch_max_delay_s = max(float(persist_batch_max_delay_ms) / 1000.0, 0.0)
        self._persist_lock = asyncio.Lock()
        self._pending_persist_batches: dict[tuple[Any, ...], _PendingPersistBatch] = {}
        self._continuity_accumulators: dict[tuple[str, str], CandleContinuityAccumulator] = {}

    @staticmethod
    def _persist_context_key(context: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            str(context.get("bot_id") or "").strip(),
            str(context.get("run_id") or "").strip(),
            str(context.get("series_key") or "").strip(),
            str(context.get("worker_id") or "").strip(),
            str(context.get("message_kind") or "").strip(),
            str(context.get("pipeline_stage") or "").strip(),
            str(context.get("source_emitter") or "").strip(),
            str(context.get("source_reason") or "").strip(),
        )

    async def _persist_rows(
        self,
        *,
        rows: list[Dict[str, Any]],
        context: Mapping[str, Any],
    ) -> int:
        if not rows:
            return 0
        key = self._persist_context_key(context)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[int] = loop.create_future()
        immediate_flush = False
        async with self._persist_lock:
            pending = self._pending_persist_batches.get(key)
            if pending is None:
                pending = _PendingPersistBatch(context=dict(context))
                self._pending_persist_batches[key] = pending
            pending.rows.extend(dict(row) for row in rows)
            pending.waiters.append(future)
            if len(pending.rows) >= self._persist_batch_max_rows and not pending.flushing:
                pending.flushing = True
                if pending.flush_task is not None:
                    pending.flush_task.cancel()
                    pending.flush_task = None
                immediate_flush = True
            elif pending.flush_task is None and not pending.flushing:
                pending.flush_task = asyncio.create_task(self._delayed_flush_rows(key))
        if immediate_flush:
            await self._flush_rows(key)
        return await future

    async def _delayed_flush_rows(self, key: tuple[Any, ...]) -> None:
        try:
            await asyncio.sleep(self._persist_batch_max_delay_s)
            async with self._persist_lock:
                pending = self._pending_persist_batches.get(key)
                if pending is None or pending.flushing:
                    return
                pending.flushing = True
                pending.flush_task = None
            await self._flush_rows(key)
        except asyncio.CancelledError:
            return

    async def _flush_rows(self, key: tuple[Any, ...]) -> None:
        async with self._persist_lock:
            pending = self._pending_persist_batches.pop(key, None)
        if pending is None:
            return
        rows = [dict(row) for row in pending.rows]
        context = {
            **dict(pending.context),
            "batch_size": len(rows),
        }
        try:
            inserted = int(
                await asyncio.to_thread(
                    record_bot_runtime_events_batch,
                    rows,
                    context=context,
                )
            )
        except Exception as exc:  # noqa: BLE001
            for waiter in pending.waiters:
                if not waiter.done():
                    waiter.set_exception(exc)
            raise
        for waiter in pending.waiters:
            if not waiter.done():
                waiter.set_result(inserted)

    def _accumulate_continuity(
        self,
        *,
        run_id: str,
        series_key: str,
        facts: Any,
        source_reason: str,
        gap_classification: Any = None,
    ) -> None:
        candles = continuity_candles_from_fact_payload(facts if isinstance(facts, list) else [])
        if not candles:
            return
        key = (str(run_id), str(series_key))
        accumulator = self._continuity_accumulators.get(key)
        if accumulator is None:
            accumulator = CandleContinuityAccumulator(
                expected_interval_seconds_value=expected_interval_seconds(series_key=series_key),
            )
            self._continuity_accumulators[key] = accumulator
        accumulator.add(
            candles,
            source_reason=source_reason,
            gap_classification=gap_classification,
        )

    def _emit_final_continuity_summaries(self, *, run_id: str, bot_id: str, reason: str) -> None:
        prefix = str(run_id)
        keys = sorted(key for key in self._continuity_accumulators if key[0] == prefix)
        for key in keys:
            _, series_key = key
            accumulator = self._continuity_accumulators.pop(key)
            summary = accumulator.summary()
            emit_candle_continuity_summary(
                _OBSERVER,
                stage="botlens_run_final",
                summary=summary,
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
                message_kind=LIFECYCLE_KIND,
                source_reason=reason,
                boundary_name="run_final",
                extra={
                    "scope": "run_final",
                    "final_status": summary.final_status,
                },
            )

    @staticmethod
    def _is_terminal_lifecycle(payload: Mapping[str, Any]) -> bool:
        phase = str(payload.get("phase") or "").strip().lower()
        status = str(payload.get("status") or "").strip().lower()
        return phase in _TERMINAL_LIFECYCLE_STATES or status in _TERMINAL_LIFECYCLE_STATES

    async def route(self, raw_payload: Any) -> None:
        """
        Validate and dispatch one ingest payload.

        This is the only entry point from the WebSocket ingest layer.
        Returns quickly after enqueueing; does not wait for processing.
        """
        started = time.perf_counter()
        if not isinstance(raw_payload, Mapping):
            _OBSERVER.increment("ingest_messages_invalid_total", failure_mode="invalid_envelope")
            _OBSERVER.event(
                "intake_invalid_envelope",
                level=logging.WARN,
                failure_mode="invalid_envelope",
                envelope_type=type(raw_payload).__name__,
            )
            return

        kind = normalize_ingest_kind(raw_payload.get("kind"))
        run_id = str(raw_payload.get("run_id") or "").strip()
        bot_id = str(raw_payload.get("bot_id") or "").strip()
        worker_id = str(raw_payload.get("worker_id") or "").strip() or None
        source_reason = _ingest_source_reason(kind=kind, payload=raw_payload)
        base_context = {
            "bot_id": bot_id or None,
            "run_id": run_id or None,
            "series_key": normalize_series_key(raw_payload.get("series_key")) or None,
            "worker_id": worker_id,
            "message_kind": kind or None,
            "source_reason": source_reason,
        }

        if not kind:
            _OBSERVER.increment("ingest_messages_invalid_total", failure_mode="missing_kind", **base_context)
            _OBSERVER.event(
                "intake_missing_required_field",
                level=logging.WARN,
                failure_mode="missing_kind",
                field="kind",
                **base_context,
            )
            return
        if not run_id:
            _OBSERVER.increment("ingest_messages_invalid_total", failure_mode="missing_run_id", **base_context)
            _OBSERVER.event(
                "intake_missing_required_field",
                level=logging.WARN,
                failure_mode="missing_run_id",
                field="run_id",
                **base_context,
            )
            return
        if not bot_id:
            _OBSERVER.increment("ingest_messages_invalid_total", failure_mode="missing_bot_id", **base_context)
            _OBSERVER.event(
                "intake_missing_required_field",
                level=logging.WARN,
                failure_mode="missing_bot_id",
                field="bot_id",
                **base_context,
            )
            return

        _OBSERVER.increment(
            "ingest_messages_total",
            bot_id=bot_id,
            run_id=run_id,
            worker_id=worker_id,
            series_key=base_context["series_key"],
            message_kind=kind,
            source_reason=source_reason,
        )
        try:
            if kind == BRIDGE_FACTS_KIND:
                await self._route_facts(run_id=run_id, bot_id=bot_id, payload=raw_payload)

            elif kind == BRIDGE_BOOTSTRAP_KIND:
                await self._route_bootstrap(run_id=run_id, bot_id=bot_id, payload=raw_payload)

            elif kind == LIFECYCLE_KIND:
                await self._route_lifecycle(run_id=run_id, bot_id=bot_id, payload=raw_payload)

            elif kind == PROJECTION_REFRESH_KIND:
                _OBSERVER.event(
                    "intake_unknown_kind",
                    level=logging.WARN,
                    bot_id=bot_id,
                    run_id=run_id,
                    worker_id=worker_id,
                    message_kind=kind,
                    failure_mode="projection_refresh_deprecated",
                )

            else:
                _OBSERVER.increment(
                    "ingest_messages_unknown_kind_total",
                    bot_id=bot_id,
                    run_id=run_id,
                    worker_id=worker_id,
                    message_kind=kind,
                    failure_mode="unknown_kind",
                )
                _OBSERVER.event(
                    "intake_unknown_kind",
                    level=logging.WARN,
                    bot_id=bot_id,
                    run_id=run_id,
                    worker_id=worker_id,
                    message_kind=kind,
                    failure_mode="unknown_kind",
                )
        finally:
            _OBSERVER.observe(
                "ingest_route_ms",
                max((time.perf_counter() - started) * 1000.0, 0.0),
                bot_id=bot_id,
                run_id=run_id,
                worker_id=worker_id,
                series_key=base_context["series_key"],
                message_kind=kind,
            )

    # ------------------------------------------------------------------
    # Per-kind routing
    # ------------------------------------------------------------------

    async def _startup_bootstrap_allowed(
        self,
        *,
        run_id: str,
        bot_id: str,
    ) -> tuple[bool, str | None]:
        snapshot = await self._registry.ensure_run_snapshot(
            run_id=run_id,
            bot_id=bot_id,
        )
        admission = startup_bootstrap_admission(
            runtime_state=snapshot.health.runtime_state,
            lifecycle_phase=snapshot.lifecycle.phase,
            projection_seq=snapshot.seq,
        )
        return admission.allowed, admission.runtime_state

    @staticmethod
    def _projection_batch_from_payload(
        *,
        batch_kind: str,
        run_id: str,
        bot_id: str,
        symbol_key: str | None,
        payload: Mapping[str, Any],
        events: list[Any],
    ) -> ProjectionBatch:
        return projection_batch_from_payload(
            batch_kind=batch_kind,
            run_id=str(run_id),
            bot_id=str(bot_id),
            symbol_key=symbol_key,
            payload=payload,
            events=tuple(events),
        )

    @staticmethod
    def _event_rows_from_batch(*, batch: ProjectionBatch) -> list[Dict[str, Any]]:
        return runtime_event_rows_from_batch(batch=batch)

    async def _route_facts(
        self, *, run_id: str, bot_id: str, payload: Mapping[str, Any]
    ) -> None:
        symbol_key = normalize_series_key(payload.get("series_key"))
        if not symbol_key:
            _OBSERVER.increment(
                "ingest_messages_invalid_total",
                bot_id=bot_id,
                run_id=run_id,
                message_kind=BRIDGE_FACTS_KIND,
                failure_mode="missing_series_key",
            )
            _OBSERVER.event(
                "intake_missing_required_field",
                level=logging.WARN,
                bot_id=bot_id,
                run_id=run_id,
                message_kind=BRIDGE_FACTS_KIND,
                failure_mode="missing_series_key",
                field="series_key",
            )
            return
        continuity_summary = continuity_summary_from_fact_payload(
            facts=payload.get("facts") if isinstance(payload.get("facts"), list) else [],
            series_key=symbol_key,
            source_reason=_ingest_source_reason(kind=BRIDGE_FACTS_KIND, payload=payload),
            gap_classification=payload.get("gap_classification"),
        )
        self._accumulate_continuity(
            run_id=run_id,
            series_key=symbol_key,
            facts=payload.get("facts") if isinstance(payload.get("facts"), list) else [],
            source_reason=_ingest_source_reason(kind=BRIDGE_FACTS_KIND, payload=payload),
            gap_classification=payload.get("gap_classification"),
        )
        if continuity_summary.candle_count > 1 or continuity_summary.detected_gap_count > 0:
            emit_candle_continuity_summary(
                _OBSERVER,
                stage="botlens_source_facts",
                summary=continuity_summary,
                bot_id=bot_id,
                run_id=run_id,
                series_key=symbol_key,
                message_kind=BRIDGE_FACTS_KIND,
                source_reason=_ingest_source_reason(kind=BRIDGE_FACTS_KIND, payload=payload),
                boundary_name="source_facts",
                extra={
                    "bridge_session_id": normalize_bridge_session_id(payload),
                    "bridge_seq": normalize_bridge_seq(payload),
                    "run_seq": int(payload.get("run_seq") or payload.get("seq") or 0),
                },
            )
        events = build_botlens_domain_events_from_fact_batch(
            bot_id=bot_id,
            run_id=run_id,
            payload=payload,
        )
        batch = self._projection_batch_from_payload(
            batch_kind=BRIDGE_FACTS_KIND,
            run_id=run_id,
            bot_id=bot_id,
            symbol_key=symbol_key,
            payload=payload,
            events=events,
        )
        _canonical_events, derived_events = split_fact_events(events)
        rows = runtime_event_rows_from_batch(batch=batch, events=derived_events)
        if rows:
            await self._persist_rows(
                rows=rows,
                context={
                    "bot_id": bot_id,
                    "run_id": run_id,
                    "series_key": symbol_key,
                    "worker_id": payload.get("worker_id"),
                    "message_kind": BRIDGE_FACTS_KIND,
                    "pipeline_stage": "botlens_ingest_facts",
                    "source_emitter": str(payload.get("source_emitter") or "container_runtime"),
                    "source_reason": _ingest_source_reason(kind=BRIDGE_FACTS_KIND, payload=payload),
                },
            )
        symbol_mailbox = await self._registry.ensure_symbol(
            run_id=run_id, bot_id=bot_id, symbol_key=symbol_key
        )
        enqueued = symbol_mailbox.enqueue_batch(batch)
        if not enqueued:
            return

    async def _route_bootstrap(
        self, *, run_id: str, bot_id: str, payload: Mapping[str, Any]
    ) -> None:
        symbol_key = normalize_series_key(payload.get("series_key"))
        if not symbol_key:
            _OBSERVER.increment(
                "ingest_messages_invalid_total",
                bot_id=bot_id,
                run_id=run_id,
                message_kind=BRIDGE_BOOTSTRAP_KIND,
                failure_mode="missing_series_key",
            )
            _OBSERVER.event(
                "intake_missing_required_field",
                level=logging.WARN,
                bot_id=bot_id,
                run_id=run_id,
                message_kind=BRIDGE_BOOTSTRAP_KIND,
                failure_mode="missing_series_key",
                field="series_key",
            )
            return
        continuity_summary = continuity_summary_from_fact_payload(
            facts=payload.get("facts") if isinstance(payload.get("facts"), list) else [],
            series_key=symbol_key,
            source_reason=_ingest_source_reason(kind=BRIDGE_BOOTSTRAP_KIND, payload=payload),
            gap_classification=payload.get("gap_classification"),
        )
        self._accumulate_continuity(
            run_id=run_id,
            series_key=symbol_key,
            facts=payload.get("facts") if isinstance(payload.get("facts"), list) else [],
            source_reason=_ingest_source_reason(kind=BRIDGE_BOOTSTRAP_KIND, payload=payload),
            gap_classification=payload.get("gap_classification"),
        )
        if continuity_summary.candle_count > 0:
            emit_candle_continuity_summary(
                _OBSERVER,
                stage="botlens_source_bootstrap",
                summary=continuity_summary,
                bot_id=bot_id,
                run_id=run_id,
                series_key=symbol_key,
                message_kind=BRIDGE_BOOTSTRAP_KIND,
                source_reason=_ingest_source_reason(kind=BRIDGE_BOOTSTRAP_KIND, payload=payload),
                boundary_name="source_bootstrap",
                extra={
                    "bridge_session_id": normalize_bridge_session_id(payload),
                    "bridge_seq": normalize_bridge_seq(payload),
                    "run_seq": int(payload.get("run_seq") or payload.get("seq") or 0),
                },
            )
        events = build_botlens_domain_events_from_fact_batch(
            bot_id=bot_id,
            run_id=run_id,
            payload=payload,
        )
        batch = self._projection_batch_from_payload(
            batch_kind=BRIDGE_BOOTSTRAP_KIND,
            run_id=run_id,
            bot_id=bot_id,
            symbol_key=symbol_key,
            payload=payload,
            events=events,
        )
        bootstrap_allowed, runtime_state = await self._startup_bootstrap_allowed(run_id=run_id, bot_id=bot_id)
        if not bootstrap_allowed:
            _OBSERVER.event(
                "startup_bootstrap_rejected",
                level=logging.ERROR,
                bot_id=bot_id,
                run_id=run_id,
                series_key=symbol_key,
                runtime_state=runtime_state or BotLensRuntimeState.LIVE.value,
                message_kind=BRIDGE_BOOTSTRAP_KIND,
                failure_mode="post_live_bootstrap_rejected",
            )
            return
        _canonical_events, derived_events = split_fact_events(events)
        rows = runtime_event_rows_from_batch(batch=batch, events=derived_events)
        if rows:
            await self._persist_rows(
                rows=rows,
                context={
                    "bot_id": bot_id,
                    "run_id": run_id,
                    "series_key": symbol_key,
                    "worker_id": payload.get("worker_id"),
                    "message_kind": BRIDGE_BOOTSTRAP_KIND,
                    "pipeline_stage": "botlens_ingest_bootstrap",
                    "source_emitter": str(payload.get("source_emitter") or "container_runtime"),
                    "source_reason": _ingest_source_reason(kind=BRIDGE_BOOTSTRAP_KIND, payload=payload),
                },
            )
        symbol_mailbox = await self._registry.ensure_symbol(
            run_id=run_id, bot_id=bot_id, symbol_key=symbol_key
        )
        # last-writer-wins: replaces any existing pending bootstrap.
        symbol_mailbox.set_bootstrap(batch)
        logger.debug(
            "botlens_intake_bootstrap_routed | run_id=%s | symbol_key=%s", run_id, symbol_key
        )

    async def _route_lifecycle(
        self, *, run_id: str, bot_id: str, payload: Mapping[str, Any]
    ) -> None:
        events = build_botlens_domain_events_from_lifecycle(
            bot_id=bot_id,
            run_id=run_id,
            lifecycle=payload,
        )
        batch = self._projection_batch_from_payload(
            batch_kind=LIFECYCLE_KIND,
            run_id=run_id,
            bot_id=bot_id,
            symbol_key=None,
            payload=payload,
            events=events,
        )
        mailbox = await self._registry.ensure_run(run_id=run_id, bot_id=bot_id)
        mailbox.enqueue_lifecycle(batch)
        if self._is_terminal_lifecycle(payload):
            self._emit_final_continuity_summaries(
                run_id=run_id,
                bot_id=bot_id,
                reason=str(payload.get("status") or payload.get("phase") or "terminal").strip().lower(),
            )


__all__ = ["IntakeRouter"]
