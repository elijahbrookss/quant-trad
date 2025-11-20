"""Service helpers for bot CRUD and runtime orchestration."""

from __future__ import annotations

import uuid
from datetime import datetime
from queue import Queue
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

import logging

from . import instrument_service
from .atm import DEFAULT_ATM_TEMPLATE, merge_templates, template_metrics
from .bot_runtime import BotRuntime, DEFAULT_RISK
from .bot_stream import BotStreamManager
from .storage import delete_bot, load_bots, load_strategies, upsert_bot


logger = logging.getLogger(__name__)

_RUNTIME: Dict[str, BotRuntime] = {}
_BOT_STREAM_MANAGER = BotStreamManager()


def _broadcast_bot_stream(event: str, payload: Mapping[str, Any]) -> None:
    """Fan out bot-level updates to SSE subscribers."""

    _BOT_STREAM_MANAGER.broadcast(event, payload)


def _persist_runtime_patch(bot_id: str, patch: Mapping[str, Any]) -> None:
    """Persist runtime-driven status/stat updates for *bot_id*."""

    if not patch:
        return
    runtime_payload = patch.get("runtime")
    if runtime_payload:
        _broadcast_bot_stream(
            "bot_runtime",
            {"bot_id": bot_id, "runtime": dict(runtime_payload)},
        )
    bots = {bot["id"]: bot for bot in load_bots()}
    record = bots.get(bot_id)
    if not record:
        return
    mutable = dict(record)
    updates = {key: patch[key] for key in ("status", "last_stats", "last_run_at") if key in patch}
    if not updates:
        return
    mutable.update(updates)
    upsert_bot(mutable)
    snapshot = dict(mutable)
    runtime = _RUNTIME.get(bot_id)
    if runtime:
        snapshot["runtime"] = runtime.snapshot()
    _broadcast_bot_stream("bot_status", {"bot": snapshot})


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO format."""

    return datetime.utcnow().isoformat() + "Z"


def _normalise_risk(risk: Optional[Dict[str, object]]) -> Dict[str, object]:
    """Merge user overrides with the default ladder template."""

    if not risk:
        return {}
    return merge_templates(risk)


def _coerce_playback_speed(value: Optional[object]) -> float:
    """Normalise playback speed factors into non-negative floats."""

    try:
        numeric = float(value) if value is not None else 10.0
    except (TypeError, ValueError):
        numeric = 10.0
    return numeric if numeric >= 0 else 0.0


def _coerce_isoformat(value: Optional[object]) -> Optional[str]:
    """Normalise datetime inputs into ISO8601 strings."""

    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat() + "Z"
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        return text
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.replace(tzinfo=None).isoformat() + "Z"
    except ValueError:
        return text


def _validate_backtest_window(record: Mapping[str, object]) -> None:
    """Ensure backtests include explicit start/end bounds."""

    run_type = str(record.get("run_type") or "backtest").lower()
    if run_type != "backtest":
        return
    if not record.get("backtest_start") or not record.get("backtest_end"):
        raise ValueError("Backtests require both start and end timestamps.")


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


def _strategy_index() -> Dict[str, Dict[str, object]]:
    """Return strategies keyed by id for quick lookup."""

    return {entry["id"]: entry for entry in load_strategies() if entry.get("id")}


def _attach_strategy_meta(bot: Dict[str, object]) -> None:
    """Populate strategy metadata on the bot config for runtime prep."""

    ids = bot.get("strategy_ids") or []
    index = _strategy_index()
    meta: List[Dict[str, object]] = []
    for strategy_id in ids:
        strategy = index.get(strategy_id)
        if strategy:
            meta.append(strategy)
    if not meta:
        raise ValueError("Bots require at least one valid strategy before starting.")
    bot["strategies_meta"] = meta


def _attach_instrument_meta(bot: Dict[str, object]) -> None:
    """Populate instrument metadata for the bot's strategy symbols."""

    strategies: List[Dict[str, Any]] = bot.get("strategies_meta") or []  # type: ignore[assignment]
    instrument_map: Dict[str, Dict[str, Any]] = {}
    for strategy in strategies:
        datasource = strategy.get("datasource") or bot.get("datasource")
        exchange = strategy.get("exchange") or bot.get("exchange")
        instruments: List[Dict[str, Any]] = []
        for symbol in strategy.get("symbols") or []:
            record = instrument_service.resolve_instrument(datasource, exchange, symbol)
            if not record:
                continue
            keys = {
                instrument_service.instrument_key(datasource, exchange, symbol),
                instrument_service.instrument_key(datasource, None, symbol),
                instrument_service.instrument_key(None, exchange, symbol),
                instrument_service.instrument_key(None, None, symbol),
            }
            for key in keys:
                instrument_map[key] = record
            enriched = dict(record)
            enriched.setdefault("symbol", symbol)
            instruments.append(enriched)
        if instruments:
            strategy["instruments"] = instruments
    if instrument_map:
        bot["instrument_index"] = instrument_map


def _runtime_for(bot_id: str, config: Dict[str, object]) -> BotRuntime:
    """Return a cached runtime for the bot, creating one if required."""

    runtime = _RUNTIME.get(bot_id)
    payload = dict(config)
    if runtime is None:
        runtime = BotRuntime(
            bot_id,
            payload,
            state_callback=lambda patch, *, _bot_id=bot_id: _persist_runtime_patch(_bot_id, patch),
        )
        _RUNTIME[bot_id] = runtime
    else:
        runtime.apply_config(payload)
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
    run_type = str(payload.get("run_type") or "backtest").lower()
    playback_input = payload.get("playback_speed")
    if playback_input is None:
        playback_input = payload.get("fetch_seconds")

    record = {
        "id": bot_id,
        "name": name,
        "strategy_id": strategy_ids[0],
        "strategy_ids": strategy_ids,
        "datasource": payload.get("datasource"),
        "exchange": payload.get("exchange"),
        "timeframe": payload.get("timeframe") or "15m",
        "mode": (payload.get("mode") or "instant").lower(),
        "run_type": run_type,
        "playback_speed": _coerce_playback_speed(playback_input),
        "backtest_start": _coerce_isoformat(payload.get("backtest_start")),
        "backtest_end": _coerce_isoformat(payload.get("backtest_end")),
        "risk": _normalise_risk(payload.get("risk")),
        "status": "idle",
        "last_stats": {},
    }
    _validate_backtest_window(record)
    upsert_bot(record)
    logger.info("[BotService] bot created", extra={"bot_id": bot_id, "run_type": run_type})
    _broadcast_bot_stream("bot", {"bot": record})
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
    if "name" in payload and payload["name"] is not None:
        record["name"] = payload["name"]
    if "run_type" in payload and payload["run_type"] is not None:
        record["run_type"] = str(payload["run_type"]).lower()
    if "mode" in payload and payload["mode"] is not None:
        record["mode"] = str(payload["mode"]).lower()
    if "playback_speed" in payload and payload["playback_speed"] is not None:
        record["playback_speed"] = _coerce_playback_speed(payload["playback_speed"])
    elif "fetch_seconds" in payload and payload["fetch_seconds"] is not None:
        record["playback_speed"] = _coerce_playback_speed(payload["fetch_seconds"])
    if "datasource" in payload and payload["datasource"] is not None:
        record["datasource"] = payload["datasource"]
    if "exchange" in payload and payload["exchange"] is not None:
        record["exchange"] = payload["exchange"]
    if "timeframe" in payload and payload["timeframe"] is not None:
        record["timeframe"] = payload["timeframe"]
    if "backtest_start" in payload:
        record["backtest_start"] = _coerce_isoformat(payload.get("backtest_start"))
    if "backtest_end" in payload:
        record["backtest_end"] = _coerce_isoformat(payload.get("backtest_end"))
    if "risk" in payload:
        record["risk"] = _normalise_risk(payload.get("risk"))
    _validate_backtest_window(record)
    upsert_bot(record)
    runtime = _RUNTIME.get(bot_id)
    if runtime:
        runtime.apply_config(record)
    logger.info("[BotService] bot updated", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot", {"bot": record})
    return record


def delete_bot_record(bot_id: str) -> None:
    """Delete a bot and stop its runtime if needed."""

    runtime = _RUNTIME.pop(bot_id, None)
    if runtime:
        runtime.stop()
    delete_bot(bot_id)
    logger.info("[BotService] bot deleted", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot_deleted", {"bot_id": bot_id})


def start_bot(bot_id: str) -> Dict[str, object]:
    """Start the runtime for the requested bot."""

    bots = {bot["id"]: bot for bot in load_bots()}
    if bot_id not in bots:
        raise KeyError(f"Bot {bot_id} was not found")
    bot = bots[bot_id]
    strategy_ids = bot.get("strategy_ids") or ([bot.get("strategy_id")] if bot.get("strategy_id") else [])
    bot["strategy_ids"] = _validate_strategy_ids(strategy_ids)
    bot["strategy_id"] = bot["strategy_ids"][0]
    _validate_backtest_window(bot)
    _attach_strategy_meta(bot)
    _attach_instrument_meta(bot)
    runtime = _runtime_for(bot_id, bot)
    runtime.reset_if_finished()
    runtime.start()
    bot["status"] = "running"
    bot["last_run_at"] = _now_iso()
    bot["runtime"] = runtime.snapshot()
    upsert_bot(bot)
    logger.info("[BotService] bot started", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot", {"bot": bot})
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
    logger.info("[BotService] bot stopped", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot", {"bot": bot})
    return bot


def pause_bot(bot_id: str) -> Dict[str, object]:
    """Pause a running bot and persist status."""

    runtime = _RUNTIME.get(bot_id)
    if runtime is None:
        raise KeyError(f"Bot {bot_id} has not been started")
    runtime.pause()
    bots = {bot["id"]: bot for bot in load_bots()}
    if bot_id not in bots:
        raise KeyError(f"Bot {bot_id} was not found")
    bot = bots[bot_id]
    bot["status"] = "paused"
    upsert_bot(bot)
    logger.info("[BotService] bot paused", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot", {"bot": bot})
    return bot


def resume_bot(bot_id: str) -> Dict[str, object]:
    """Resume a paused bot."""

    runtime = _RUNTIME.get(bot_id)
    if runtime is None:
        raise KeyError(f"Bot {bot_id} has not been started")
    runtime.resume()
    bots = {bot["id"]: bot for bot in load_bots()}
    if bot_id not in bots:
        raise KeyError(f"Bot {bot_id} was not found")
    bot = bots[bot_id]
    bot["status"] = "running"
    upsert_bot(bot)
    logger.info("[BotService] bot resumed", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot", {"bot": bot})
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


def runtime_logs(bot_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    """Return recent runtime log entries for a bot."""

    bot = get_bot(bot_id)
    _attach_strategy_meta(bot)
    _attach_instrument_meta(bot)
    runtime = _runtime_for(bot_id, bot)
    runtime.warm_up()
    return runtime.logs(limit)


def stream(bot_id: str) -> Tuple[Callable[[], None], Queue, Dict[str, Any]]:
    """Return a release callback, queue, and initial payload for SSE streaming."""

    bot = get_bot(bot_id)
    _attach_strategy_meta(bot)
    _attach_instrument_meta(bot)
    status = str(bot.get("status") or "idle").lower()
    runtime = _RUNTIME.get(bot_id)
    if runtime is None:
        if status == "idle":
            raise ValueError("Bot has not been started yet.")
        runtime = _runtime_for(bot_id, bot)
    else:
        runtime.apply_config(bot)
    runtime.warm_up()
    token, channel = runtime.subscribe()

    def _release() -> None:
        runtime.unsubscribe(token)

    initial = runtime.chart_payload()
    initial.setdefault("meta", _performance_meta(bot))
    initial.setdefault("type", "snapshot")
    return _release, channel, initial


def bots_stream() -> Tuple[Callable[[], None], Queue, Dict[str, Any]]:
    """Return a release callback, queue, and initial payload for all-bots SSE."""

    return _BOT_STREAM_MANAGER.subscribe_all(list_bots)


def _indicator_meta(strategy: Dict[str, object]) -> List[Dict[str, object]]:
    """Return indicator metadata suitable for UI display."""

    indicators: List[Dict[str, object]] = []
    sources = strategy.get("indicator_links") or strategy.get("indicators") or []
    for link in sources or []:
        if not isinstance(link, dict):
            continue
        snapshot = dict(
            link.get("indicator_snapshot")
            or link.get("meta")
            or link.get("snapshot")
            or {}
        )
        indicator_id = link.get("indicator_id") or link.get("id") or snapshot.get("id")
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
    runtime_meta = {
        entry.get("id"): entry for entry in bot.get("strategies_meta") or [] if entry.get("id")
    }
    selected: List[Dict[str, object]] = []
    for strategy_id in bot.get("strategy_ids", []) or []:
        stored = strategy_index.get(strategy_id) or {}
        runtime_entry = runtime_meta.get(strategy_id) or {}
        if not stored and not runtime_entry:
            continue
        merged = dict(stored)
        merged.update(runtime_entry)
        bot_override = bot.get("risk") or {}
        override_payload = bot_override if bot_override and bot_override != DEFAULT_ATM_TEMPLATE else None
        atm_template = merge_templates(
            merged.get("atm_template"),
            override_payload,
        )
        instruments = (
            runtime_entry.get("instruments")
            or stored.get("instruments")
            or []
        )
        selected.append(
            {
                "id": merged.get("id"),
                "name": merged.get("name"),
                "symbols": list(merged.get("symbols") or []),
                "timeframe": merged.get("timeframe"),
                "datasource": merged.get("datasource") or bot.get("datasource"),
                "exchange": merged.get("exchange") or bot.get("exchange"),
                "indicators": _indicator_meta(merged),
                "instruments": instruments,
                "atm_template": atm_template,
                "atm_metrics": template_metrics(atm_template),
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
        "instrument_index": bot.get("instrument_index") or {},
    }


def performance(bot_id: str) -> Dict[str, object]:
    """Return candle, trade, stat, and metadata payloads for the lens chart."""

    bot = get_bot(bot_id)
    _attach_strategy_meta(bot)
    _attach_instrument_meta(bot)
    runtime = _RUNTIME.get(bot_id)
    status = str(bot.get("status") or "idle").lower()
    payload: Dict[str, Any]
    if runtime is not None:
        runtime.apply_config(bot)
        runtime.warm_up()
        payload = runtime.chart_payload()
        payload.setdefault("logs", runtime.logs())
    elif status == "idle":
        payload = {
            "candles": [],
            "trades": [],
            "stats": bot.get("last_stats") or {},
            "overlays": [],
            "logs": [],
            "inactive": True,
            "message": "Start this bot to stream performance data.",
            "runtime": {
                "status": status,
                "progress": 0.0,
                "paused": False,
                "next_bar_in_seconds": None,
            },
        }
    else:
        runtime = _runtime_for(bot_id, bot)
        runtime.apply_config(bot)
        runtime.warm_up()
        payload = runtime.chart_payload()
        payload.setdefault("logs", runtime.logs())
    payload["meta"] = _performance_meta(bot)
    if runtime is not None:
        payload["runtime"] = runtime.snapshot()
    else:
        payload.setdefault("runtime", {"status": status})
    return payload


__all__ = [
    "create_bot",
    "delete_bot_record",
    "get_bot",
    "list_bots",
    "pause_bot",
    "performance",
    "runtime_logs",
    "resume_bot",
    "runtime_status",
    "stream",
    "start_bot",
    "stop_bot",
    "update_bot",
]
