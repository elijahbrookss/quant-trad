"""Bot read-model service.

Container runtime is authoritative; unsupported legacy read paths fail loudly.
"""

from __future__ import annotations

from typing import Any, Dict


class BotReadModelService:
    @staticmethod
    def performance(bot_id: str) -> Dict[str, object]:
        _ = bot_id
        raise RuntimeError("Performance API from in-process runtime is removed for container runtime")

    @staticmethod
    def regime_overlays(bot_id: str) -> Dict[str, Any]:
        _ = bot_id
        raise RuntimeError("Regime overlay debug endpoint is unavailable for container runtime")
