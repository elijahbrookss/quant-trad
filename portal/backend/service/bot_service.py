"""Service helpers for bot CRUD and runtime orchestration."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from .bot_runtime import BotRuntime, DEFAULT_RISK
from .storage import delete_bot, load_bots, load_strategies, upsert_bot


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


def _validate_strategy_ids(
    strategy_ids: Optional[Iterable[str]],
    fallback: Optional[str] = None,
) -> List[str]:
    """Ensure at least one referenced strategy exists."""

    candidates: List[str] = []
    if strategy_ids:
        for strategy_id in strategy_ids:
            if strategy_id:
                candidates.append(str(strategy_id))
    if not candidates and fallback:
        candidates = [fallback]
    deduped: List[str] = []
    seen = set()
    for strategy_id in candidates:
        trimmed = strategy_id.strip()
        if not trimmed or trimmed in seen:
            continue
        deduped.append(trimmed)
        seen.add(trimmed)
    if not deduped:
        raise ValueError("Bots require at least one strategy.")
    available = {strategy["id"] for strategy in load_strategies()}
    missing = [strategy_id for strategy_id in deduped if strategy_id not in available]
    if missing:
        raise ValueError(
            "The following strategies do not exist: " + ", ".join(sorted(missing))
        )
    return deduped


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
    strategy_ids = _validate_strategy_ids(
        payload.get("strategy_ids"), payload.get("strategy_id")
    )
    record = {
        "id": bot_id,
        "name": name,
        "strategy_id": strategy_ids[0],
        "strategy_ids": strategy_ids,
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
    if "strategy_ids" in payload or "strategy_id" in payload:
        strategy_ids = _validate_strategy_ids(
            payload.get("strategy_ids"), payload.get("strategy_id")
        )
        record["strategy_ids"] = strategy_ids
        record["strategy_id"] = strategy_ids[0]
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
    strategy_ids = bot.get("strategy_ids") or ([bot.get("strategy_id")] if bot.get("strategy_id") else [])
    bot["strategy_ids"] = _validate_strategy_ids(strategy_ids)
    bot["strategy_id"] = bot["strategy_ids"][0]
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


def _indicator_meta(strategy: Dict[str, object]) -> List[Dict[str, object]]:
    """Return indicator metadata suitable for UI display."""

    indicators: List[Dict[str, object]] = []
    for link in strategy.get("indicator_links", []) or []:
        if not isinstance(link, dict):
            continue
        snapshot = dict(link.get("indicator_snapshot") or {})
        indicator_id = link.get("indicator_id") or snapshot.get("id")
        indicators.append(
            {
                "id": indicator_id,
                "name": snapshot.get("name") or snapshot.get("type") or indicator_id,
                "type": snapshot.get("type"),
                "color": snapshot.get("color"),
                "datasource": snapshot.get("datasource"),
                "exchange": snapshot.get("exchange"),
                "params": dict(snapshot.get("params") or {}),
            }
        )
    return indicators


def _performance_meta(bot: Dict[str, object]) -> Dict[str, object]:
    """Assemble descriptive metadata for bot strategies and symbols."""

    strategy_index = {strategy["id"]: strategy for strategy in load_strategies()}
    selected: List[Dict[str, object]] = []
    for strategy_id in bot.get("strategy_ids", []) or []:
        strategy = strategy_index.get(strategy_id)
        if not strategy:
            continue
        selected.append(
            {
                "id": strategy["id"],
                "name": strategy.get("name"),
                "symbols": list(strategy.get("symbols") or []),
                "timeframe": strategy.get("timeframe"),
                "datasource": strategy.get("datasource") or bot.get("datasource"),
                "exchange": strategy.get("exchange") or bot.get("exchange"),
                "indicators": _indicator_meta(strategy),
            }
        )
    return {
        "bot": {
            "id": bot.get("id"),
            "name": bot.get("name"),
            "mode": bot.get("mode"),
            "timeframe": bot.get("timeframe"),
            "datasource": bot.get("datasource"),
            "exchange": bot.get("exchange"),
            "risk": bot.get("risk"),
        },
        "strategies": selected,
    }


def performance(bot_id: str) -> Dict[str, object]:
    """Return candle, trade, stat, and metadata payloads for the lens chart."""

    bot = get_bot(bot_id)
    runtime = _RUNTIME.get(bot_id)
    if not runtime:
        # allow fetching from persisted config even if runtime not initialised
        runtime = _runtime_for(bot_id, bot)
    runtime.warm_up()
    payload = runtime.chart_payload()
    payload["meta"] = _performance_meta(bot)
    return payload


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
