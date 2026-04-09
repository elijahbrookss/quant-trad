from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, Dict

from .botlens_contract import LIFECYCLE_KIND


def emit_lifecycle_event(payload: Mapping[str, Any]) -> None:
    from .telemetry_stream import telemetry_hub

    event: Dict[str, Any] = {
        "kind": LIFECYCLE_KIND,
        "bot_id": str(payload.get("bot_id") or "").strip(),
        "run_id": str(payload.get("run_id") or "").strip(),
        "seq": int(payload.get("seq") or 0),
        "phase": str(payload.get("phase") or "").strip(),
        "owner": str(payload.get("owner") or "").strip() or None,
        "message": str(payload.get("message") or "").strip() or None,
        "status": str(payload.get("status") or "").strip() or None,
        "metadata": dict(payload.get("metadata") or {}),
        "failure": dict(payload.get("failure") or {}),
        "checkpoint_at": payload.get("checkpoint_at") or payload.get("updated_at"),
        "updated_at": payload.get("updated_at") or payload.get("checkpoint_at"),
        "known_at": payload.get("checkpoint_at") or payload.get("updated_at"),
    }
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(telemetry_hub.ingest(event))
        return
    loop.create_task(telemetry_hub.ingest(event))


__all__ = ["emit_lifecycle_event"]
