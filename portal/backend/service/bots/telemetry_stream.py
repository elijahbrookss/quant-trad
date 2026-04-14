"""BotTelemetryHub — thin coordinator for the BotLens telemetry pipeline.

This module is the public API surface for the ingest and viewer-subscription paths.
All projection, persistence, and fanout logic lives in the components below.

Component responsibilities:
  IntakeRouter         — validates and routes ingest payloads to mailboxes
  ProjectorRegistry    — creates/holds/evicts per-run projector contexts
  RunProjector         — owns run-level canonical state (per run_id asyncio task)
  SymbolProjector      — owns symbol-level canonical state (per symbol asyncio task)
  BotLensRunStream     — WebSocket viewer connections and snapshot delivery
  _fanout_delivery_loop — downstream delivery loop (one per run)

This module no longer contains:
  - A global ingest queue
  - A single ingest worker serializing all runs
  - Inline projection, persistence, or fanout
  - _process_bridge_bootstrap / _process_bridge_facts / _process_lifecycle_event
  - _process_ingest
  - A global asyncio.Lock protecting all run state

Public API (unchanged from caller perspective):
  telemetry_hub.ingest(payload)
  telemetry_hub.add_run_viewer(run_id, ws, selected_symbol_key)
  telemetry_hub.update_run_viewer(run_id, ws, payload)
  telemetry_hub.remove_run_viewer(run_id, ws)
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from typing import Any, Dict, Optional

from fastapi import WebSocket

from ..observability import BackendObserver, normalize_failure_mode
from ..storage.storage import (
    get_bot_run,
    get_latest_bot_run_view_state,
)
from .botlens_contract import normalize_series_key
from .botlens_intake_router import IntakeRouter
from .botlens_projector_registry import ProjectorRegistry
from .botlens_run_stream import BotLensRunStream
from .botlens_state import detail_snapshot_contract, read_symbol_detail_state

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_telemetry_hub", event_logger=logger)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


class BotTelemetryHub:
    """
    Thin coordinator.  Delegates all processing to owned components.
    Exposes the same external API as before so callers require no changes.
    """

    def __init__(self) -> None:
        self._run_stream = BotLensRunStream()
        self._registry = ProjectorRegistry(run_stream=self._run_stream)
        self._router = IntakeRouter(registry=self._registry)

    # ------------------------------------------------------------------
    # Ingest — called by the WebSocket ingest endpoint per message
    # ------------------------------------------------------------------

    async def ingest(self, payload: Dict[str, Any]) -> None:
        """
        Validate and dispatch one telemetry payload.
        Returns immediately after routing; processing is async in projector tasks.
        """
        await self._router.route(payload)

    # ------------------------------------------------------------------
    # Viewer subscription — called by the botlens/live WebSocket endpoint
    # ------------------------------------------------------------------

    async def add_run_viewer(
        self,
        *,
        run_id: str,
        ws: WebSocket,
        selected_symbol_key: Optional[str] = None,
    ) -> None:
        normalized_symbol_key = normalize_series_key(selected_symbol_key)
        await self._run_stream.add_run_viewer(
            run_id=str(run_id),
            ws=ws,
            selected_symbol_key=normalized_symbol_key or None,
        )
        if normalized_symbol_key:
            await self._send_viewer_symbol_snapshot(
                run_id=str(run_id), ws=ws, symbol_key=normalized_symbol_key
            )

    async def update_run_viewer(
        self,
        *,
        run_id: str,
        ws: WebSocket,
        payload: Mapping[str, Any],
    ) -> None:
        await self._run_stream.update_viewer_subscription(
            run_id=str(run_id), ws=ws, payload=payload
        )
        if str(payload.get("type") or "").strip().lower() == "set_selected_symbol":
            normalized_symbol_key = normalize_series_key(payload.get("symbol_key"))
            if normalized_symbol_key:
                await self._send_viewer_symbol_snapshot(
                    run_id=str(run_id), ws=ws, symbol_key=normalized_symbol_key
                )

    async def remove_run_viewer(self, *, run_id: str, ws: WebSocket) -> None:
        await self._run_stream.remove_run_viewer(run_id=str(run_id), ws=ws)

    # ------------------------------------------------------------------
    # Snapshot delivery for connecting viewers
    # ------------------------------------------------------------------

    async def _send_viewer_symbol_snapshot(
        self, *, run_id: str, ws: WebSocket, symbol_key: str
    ) -> None:
        normalized_symbol_key = normalize_series_key(symbol_key)
        if not normalized_symbol_key:
            return
        total_started = time.perf_counter()
        _OBSERVER.event(
            "viewer_snapshot_started",
            run_id=str(run_id),
            series_key=normalized_symbol_key,
        )
        try:
            load_started = time.perf_counter()
            detail_state, load_labels = await self._load_symbol_state(
                run_id=run_id, symbol_key=normalized_symbol_key
            )
            load_elapsed_ms = max((time.perf_counter() - load_started) * 1000.0, 0.0)
            _OBSERVER.observe(
                "viewer_snapshot_load_ms",
                load_elapsed_ms,
                run_id=str(run_id),
                series_key=normalized_symbol_key,
                **load_labels,
            )
            snapshot = detail_snapshot_contract(run_id=run_id, detail=detail_state)
            _OBSERVER.observe(
                "viewer_snapshot_total_ms",
                max((time.perf_counter() - total_started) * 1000.0, 0.0),
                run_id=str(run_id),
                series_key=normalized_symbol_key,
                **load_labels,
            )
            await self._run_stream.deliver_symbol_snapshot(
                run_id=run_id, ws=ws, snapshot=snapshot
            )
        except Exception as exc:
            failure_mode = normalize_failure_mode(exc)
            _OBSERVER.event(
                "viewer_snapshot_load_failed",
                level=logging.WARN,
                run_id=str(run_id),
                series_key=normalized_symbol_key,
                failure_mode=failure_mode,
                error=str(exc),
            )
            logger.warning(
                "botlens_viewer_symbol_snapshot_failed | run_id=%s | symbol_key=%s | error=%s",
                run_id, normalized_symbol_key, exc,
            )

    async def _load_symbol_state(self, *, run_id: str, symbol_key: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Return current canonical symbol state.

        Prefers in-memory state from the live SymbolProjector; falls back to
        storage when no projector exists (run not yet live, backend restart, etc.).
        """
        projector = self._registry.get_symbol_projector(run_id, symbol_key)
        if projector is not None:
            return projector.get_snapshot(), {"pipeline_stage": "in_memory"}

        # Storage fallback.
        bot_id = self._registry.get_bot_id(run_id)
        if not bot_id:
            row = await asyncio.to_thread(get_bot_run, str(run_id))
            bot_id = str(_mapping(row).get("bot_id") or "").strip()

        if not bot_id:
            return read_symbol_detail_state(None, symbol_key=symbol_key), {
                "pipeline_stage": "empty_fallback",
            }

        row = await asyncio.to_thread(
            get_latest_bot_run_view_state,
            bot_id=bot_id,
            run_id=run_id,
            series_key=symbol_key,
        )
        return read_symbol_detail_state(_mapping(row).get("payload"), symbol_key=symbol_key), {
            "pipeline_stage": "storage_fallback",
            "storage_target": "bot_run_view_state",
        }


# Module-level singleton — matches existing import pattern in other modules.
telemetry_hub = BotTelemetryHub()
