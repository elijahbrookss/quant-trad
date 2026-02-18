from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from typing import Any, DefaultDict, Dict, Set

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class BotTelemetryHub:
    def __init__(self) -> None:
        self._viewers: DefaultDict[str, Set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def add_viewer(self, *, bot_id: str, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._viewers[bot_id].add(ws)

    async def remove_viewer(self, *, bot_id: str, ws: WebSocket) -> None:
        async with self._lock:
            viewers = self._viewers.get(bot_id)
            if not viewers:
                return
            viewers.discard(ws)

    async def broadcast(self, *, bot_id: str, payload: Dict[str, Any]) -> None:
        async with self._lock:
            viewers = list(self._viewers.get(bot_id) or set())
        for ws in viewers:
            try:
                await ws.send_text(json.dumps(payload))
            except Exception:
                logger.warning("bot_telemetry_viewer_send_failed | bot_id=%s", bot_id)


telemetry_hub = BotTelemetryHub()
