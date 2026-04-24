"""BotTelemetryHub — thin coordinator for the BotLens telemetry pipeline.

This module is the public API surface for the ingest and viewer-subscription paths.
All projection, persistence, and fanout logic lives in the components below.

Component responsibilities:
  IntakeRouter         — validates and routes ingest payloads to mailboxes
  ProjectorRegistry    — creates/holds/evicts per-run projector contexts
  RunProjector         — owns run-level canonical state (per run_id asyncio task)
  SymbolProjector      — owns symbol-level canonical state (per symbol asyncio task)
  BotLensRunStream     — WebSocket viewer connections and live delta delivery
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

import logging
from collections.abc import Mapping
from typing import Any, Dict, Optional

from core.settings import get_settings

try:
    from fastapi import WebSocket
except ModuleNotFoundError:  # pragma: no cover - test environments may not install FastAPI
    class WebSocket:  # type: ignore[override]
        pass

from .botlens_contract import normalize_series_key
from .botlens_intake_router import IntakeRouter
from .botlens_projector_registry import ProjectorRegistry
from .botlens_run_stream import BotLensRunStream

logger = logging.getLogger(__name__)
_BOTLENS_SETTINGS = get_settings().bot_runtime.botlens


class BotTelemetryHub:
    """
    Thin coordinator.  Delegates all processing to owned components.
    Exposes the same external API as before so callers require no changes.
    """

    def __init__(self) -> None:
        self._run_stream = BotLensRunStream(ring_size=int(_BOTLENS_SETTINGS.ring_size))
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
        resume_from_seq: int = 0,
        stream_session_id: Optional[str] = None,
    ) -> None:
        normalized_symbol_key = normalize_series_key(selected_symbol_key)
        await self._run_stream.add_run_viewer(
            run_id=str(run_id),
            ws=ws,
            selected_symbol_key=normalized_symbol_key or None,
            resume_from_seq=max(int(resume_from_seq or 0), 0),
            stream_session_id=str(stream_session_id or "").strip() or None,
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

    async def remove_run_viewer(self, *, run_id: str, ws: WebSocket) -> None:
        await self._run_stream.remove_run_viewer(run_id=str(run_id), ws=ws)

    async def ensure_run_snapshot(self, *, run_id: str, bot_id: str) -> Any:
        return await self._registry.ensure_run_snapshot(run_id=str(run_id), bot_id=str(bot_id))

    async def ensure_symbol_snapshot(self, *, run_id: str, bot_id: str, symbol_key: str) -> Any:
        return await self._registry.ensure_symbol_snapshot(
            run_id=str(run_id),
            bot_id=str(bot_id),
            symbol_key=normalize_series_key(symbol_key) or str(symbol_key),
        )

    def get_run_snapshot(self, *, run_id: str) -> Any:
        return self._registry.get_run_snapshot(str(run_id))

    def get_symbol_snapshot(self, *, run_id: str, symbol_key: str) -> Any:
        return self._registry.get_symbol_snapshot(str(run_id), normalize_series_key(symbol_key) or str(symbol_key))

    async def current_cursor(self, *, run_id: str, bot_id: str | None = None) -> Dict[str, Any]:
        return await self._run_stream.current_cursor(run_id=str(run_id), bot_id=str(bot_id or "").strip() or None)

    async def current_symbol_cursor(
        self,
        *,
        run_id: str,
        symbol_key: str,
        bot_id: str | None = None,
    ) -> Dict[str, Any]:
        return await self._run_stream.current_symbol_cursor(
            run_id=str(run_id),
            symbol_key=normalize_series_key(symbol_key) or str(symbol_key),
            bot_id=str(bot_id or "").strip() or None,
        )


# Module-level singleton — matches existing import pattern in other modules.
telemetry_hub = BotTelemetryHub()
