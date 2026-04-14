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

import logging
import time
from collections.abc import Mapping
from typing import Any, Dict

from ..observability import BackendObserver
from .botlens_contract import (
    BRIDGE_BOOTSTRAP_KIND,
    BRIDGE_FACTS_KIND,
    LIFECYCLE_KIND,
    PROJECTION_REFRESH_KIND,
    normalize_ingest_kind,
    normalize_series_key,
)
from .botlens_projector_registry import ProjectorRegistry

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_intake_router", event_logger=logger)


class IntakeRouter:
    """
    Validates and routes ingest payloads to the correct mailbox/slot.

    One instance is shared across all ingest connections for the process.
    All methods are non-blocking (no await on projection or persistence).
    """

    def __init__(self, registry: ProjectorRegistry) -> None:
        self._registry = registry

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
        base_context = {
            "bot_id": bot_id or None,
            "run_id": run_id or None,
            "series_key": normalize_series_key(raw_payload.get("series_key")) or None,
            "worker_id": worker_id,
            "message_kind": kind or None,
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
                message_kind=kind,
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
                message_kind=kind,
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
        symbol_mailbox = await self._registry.ensure_symbol(
            run_id=run_id, bot_id=bot_id, symbol_key=symbol_key
        )
        enqueued = symbol_mailbox.enqueue_facts(dict(payload))
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
        symbol_mailbox = await self._registry.ensure_symbol(
            run_id=run_id, bot_id=bot_id, symbol_key=symbol_key
        )
        # last-writer-wins: replaces any existing pending bootstrap.
        symbol_mailbox.set_bootstrap(dict(payload))
        logger.debug(
            "botlens_intake_bootstrap_routed | run_id=%s | symbol_key=%s", run_id, symbol_key
        )

    async def _route_lifecycle(
        self, *, run_id: str, bot_id: str, payload: Mapping[str, Any]
    ) -> None:
        mailbox = await self._registry.ensure_run(run_id=run_id, bot_id=bot_id)
        mailbox.enqueue_lifecycle(dict(payload))


__all__ = ["IntakeRouter"]
