"""Service helpers for bot CRUD and runtime orchestration."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, List, Optional

from .bot_runtime import BotRuntime, DEFAULT_RISK
from .storage import delete_bot, load_bots, upsert_bot


_RUNTIME: Dict[str, BotRuntime] = {}


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO format."""

    return datetime.utcnow().isoformat() + "Z"


def _normalise_risk(risk: Optional[Dict[str, object]]) -> Dict[str, object]:
    """Merge user overrides with the default ladder template."""

    config = dict(DEFAULT_RISK)
    if isinstance(risk, dict):
        config.update({k: risk[k] for k in risk if risk[k] is not None})
    return config


def _runtime_for(bot_id: str, config: Dict[str, object]) -> BotRuntime:
    """Return a cached runtime for the bot, creating one if required."""

    runtime = _RUNTIME.get(bot_id)
    if runtime is None:
        runtime = BotRuntime(bot_id, config)
        _RUNTIME[bot_id] = runtime
    else:
        runtime.config.update(config)
    return runtime


def list_bots() -> List[Dict[str, object]]:
    """Return all bot configs enriched with runtime status."""

    bots = load_bots()
    for bot in bots:
        runtime = _RUNTIME.get(bot["id"])
        if runtime:
            bot["runtime"] = runtime.snapshot()
    return bots


def create_bot(name: str, **payload: object) -> Dict[str, object]:
    """Persist a new bot configuration."""

    bot_id = payload.get("id") or str(uuid.uuid4())
    record = {
        "id": bot_id,
        "name": name,
        "strategy_id": payload.get("strategy_id"),
        "datasource": payload.get("datasource"),
        "exchange": payload.get("exchange"),
        "timeframe": payload.get("timeframe") or "15m",
        "mode": (payload.get("mode") or "instant").lower(),
        "fetch_seconds": max(int(payload.get("fetch_seconds") or 1), 0),
        "risk": _normalise_risk(payload.get("risk")),
        "status": "idle",
        "last_stats": {},
    }
    upsert_bot(record)
    return record


def update_bot(bot_id: str, **payload: object) -> Dict[str, object]:
    """Update mutable fields for a bot."""

    bots = {bot["id"]: bot for bot in load_bots()}
    if bot_id not in bots:
        raise KeyError(f"Bot {bot_id} was not found")
    record = bots[bot_id]
    record.update({k: v for k, v in payload.items() if v is not None})
    if "risk" in payload:
        record["risk"] = _normalise_risk(payload.get("risk"))
    upsert_bot(record)
    runtime = _RUNTIME.get(bot_id)
    if runtime:
        runtime.config.update(record)
    return record


def delete_bot_record(bot_id: str) -> None:
    """Delete a bot and stop its runtime if needed."""

    runtime = _RUNTIME.pop(bot_id, None)
    if runtime:
        runtime.stop()
    delete_bot(bot_id)


def start_bot(bot_id: str) -> Dict[str, object]:
    """Start the runtime for the requested bot."""

    bots = {bot["id"]: bot for bot in load_bots()}
    if bot_id not in bots:
        raise KeyError(f"Bot {bot_id} was not found")
    bot = bots[bot_id]
    runtime = _runtime_for(bot_id, bot)
    runtime.start()
    bot["status"] = "running"
    bot["last_run_at"] = _now_iso()
    upsert_bot(bot)
    return bot


def stop_bot(bot_id: str) -> Dict[str, object]:
    """Stop a running bot."""

    runtime = _RUNTIME.get(bot_id)
    if runtime:
        runtime.stop()
    bots = {bot["id"]: bot for bot in load_bots()}
    if bot_id not in bots:
        raise KeyError(f"Bot {bot_id} was not found")
    bot = bots[bot_id]
    bot["status"] = "stopped"
    upsert_bot(bot)
    return bot


def get_bot(bot_id: str) -> Dict[str, object]:
    """Return a single bot configuration."""

    for bot in load_bots():
        if bot["id"] == bot_id:
            runtime = _RUNTIME.get(bot_id)
            if runtime:
                bot["runtime"] = runtime.snapshot()
            return bot
    raise KeyError(f"Bot {bot_id} was not found")


def runtime_status(bot_id: str) -> Dict[str, object]:
    """Return live runtime data for a bot."""

    runtime = _RUNTIME.get(bot_id)
    if not runtime:
        raise KeyError(f"Bot {bot_id} has not been started")
    return runtime.snapshot()


def performance(bot_id: str) -> Dict[str, object]:
    """Return candle, trade, and stat payloads for the lens chart."""

    runtime = _RUNTIME.get(bot_id)
    if not runtime:
        # allow fetching from persisted config even if runtime not initialised
        runtime = _runtime_for(bot_id, get_bot(bot_id))
    runtime.warm_up()
    return runtime.chart_payload()


__all__ = [
    "create_bot",
    "delete_bot_record",
    "get_bot",
    "list_bots",
    "performance",
    "runtime_status",
    "start_bot",
    "stop_bot",
    "update_bot",
]
