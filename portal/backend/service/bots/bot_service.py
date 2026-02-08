"""Service helpers for bot CRUD and runtime orchestration."""

from __future__ import annotations

import uuid
from datetime import datetime
from queue import Queue
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

import logging

from ..market import instrument_service
from ..risk.atm import merge_templates, template_metrics
from .bot_runtime import BotRuntime
from .bot_watchdog import get_watchdog
from .bot_stream import BotStreamManager
from ..storage.storage import delete_bot, load_bots, load_strategies, upsert_bot


logger = logging.getLogger(__name__)

_RUNTIME: Dict[str, BotRuntime] = {}
_BOT_STREAM_MANAGER = BotStreamManager()
MIN_STARTING_WALLET = 10.0
_DERIVATIVE_TYPES = {"perp", "perps", "swap", "future", "futures", "derivative", "derivatives"}


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
    status = str(updates.get("status") or "").lower()
    if status in {"completed", "stopped", "error"}:
        get_watchdog().unregister_bot(bot_id)
        mutable["runner_id"] = None
        mutable["heartbeat_at"] = None
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



def _coerce_playback_speed(value: Optional[object]) -> float:
    """Normalise playback speed factors into non-negative floats."""

    return 0.0


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


def _normalize_instrument_policy(value: Optional[object]) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text == "spot":
        return "spot"
    if text in _DERIVATIVE_TYPES:
        return "derivatives"
    raise ValueError(f"Unsupported instrument_type '{value}'")


def _instrument_policy_from_bot(bot: Mapping[str, object]) -> Optional[str]:
    direct = bot.get("instrument_type")
    if direct:
        return _normalize_instrument_policy(direct)
    risk = bot.get("risk")
    if isinstance(risk, Mapping):
        return _normalize_instrument_policy(risk.get("instrument_type"))
    return None


def _apply_instrument_policy(record: Dict[str, object], value: Optional[object]) -> None:
    policy = _normalize_instrument_policy(value)
    if policy is None:
        return
    record["instrument_type"] = policy
    risk = dict(record.get("risk") or {})
    risk["instrument_type"] = policy
    record["risk"] = risk


def _validate_backtest_window(record: Mapping[str, object]) -> None:
    """Ensure backtests include explicit start/end bounds."""

    run_type = str(record.get("run_type") or "backtest").lower()
    if run_type != "backtest":
        return
    if not record.get("backtest_start") or not record.get("backtest_end"):
        raise ValueError("Backtests require both start and end timestamps.")


def _validate_wallet_config(wallet_config: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(wallet_config, Mapping):
        raise ValueError("wallet_config is required and must be an object")
    balances = wallet_config.get("balances")
    if not isinstance(balances, Mapping) or not balances:
        raise ValueError("wallet_config.balances is required and cannot be empty")
    normalized: Dict[str, float] = {}
    total = 0.0
    for currency, amount in balances.items():
        code = str(currency).strip().upper()
        if not code:
            raise ValueError("wallet_config.balances contains an empty currency key")
        try:
            numeric = float(amount)
        except (TypeError, ValueError):
            raise ValueError(f"wallet_config.balances[{code}] must be numeric")
        if numeric < 0:
            raise ValueError(f"wallet_config.balances[{code}] must be non-negative")
        normalized[code] = numeric
        total += numeric
    if total < MIN_STARTING_WALLET:
        raise ValueError(
            f"wallet_config balances must sum to at least {MIN_STARTING_WALLET}"
        )
    return {"balances": normalized}


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


def _validate_strategy_existence(bot: Dict[str, object]) -> None:
    """Validate that all strategy IDs exist in the database.

    Args:
        bot: Bot configuration dict with strategy_ids

    Raises:
        ValueError: If any strategy doesn't exist

    The runtime will load strategies fresh from DB using strategy_ids.
    """
    from .bot_runtime.strategy import StrategyLoader

    ids = bot.get("strategy_ids") or []
    for strategy_id in ids:
        if not StrategyLoader.strategy_exists(strategy_id):
            raise ValueError(f"Strategy not found: {strategy_id}")

    logger.debug("[BotService] Validated %d strategy IDs (DB-based loading)", len(ids))


def _validate_instrument_policy(bot: Mapping[str, object]) -> None:
    """Validate instrument type policy against attached strategy instruments."""

    policy = _instrument_policy_from_bot(bot)
    if not policy:
        return
    from .bot_runtime.strategy import StrategyLoader
    from ..market import instrument_service

    strategy_ids = bot.get("strategy_ids") or []
    for strategy_id in strategy_ids:
        strategy = StrategyLoader.fetch_strategy(strategy_id)
        for link in strategy.instrument_links:
            snapshot = link.instrument_snapshot or {}
            instrument_type = str(snapshot.get("instrument_type") or "").lower()
            symbol = snapshot.get("symbol") or link.symbol
            if not instrument_type:
                resolved = instrument_service.resolve_instrument(
                    strategy.datasource,
                    strategy.exchange,
                    symbol or "",
                )
                instrument_type = str((resolved or {}).get("instrument_type") or "").lower()
            if not instrument_type:
                raise ValueError(
                    f"Instrument type missing for {symbol or link.instrument_id}. "
                    "Validate the instrument before running this bot."
                )
            is_spot = instrument_type == "spot"
            if policy == "derivatives" and is_spot:
                raise ValueError(
                    f"Derivatives-only bot cannot run on spot instrument {symbol or link.instrument_id}."
                )
            if policy == "spot" and not is_spot:
                raise ValueError(
                    f"Spot-only bot cannot run on derivatives instrument {symbol or link.instrument_id}."
                )


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
        bot["instrument_type"] = _instrument_policy_from_bot(bot)
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
    wallet_config = _validate_wallet_config(payload.get("wallet_config"))

    record = {
        "id": bot_id,
        "name": name,
        "strategy_id": strategy_ids[0],
        "strategy_ids": strategy_ids,
        # runtime context should come from strategies, not the bot row
        "timeframe": None,
        "mode": (payload.get("mode") or "instant").lower(),
        "run_type": run_type,
        "playback_speed": _coerce_playback_speed(playback_input),
        "backtest_start": _coerce_isoformat(payload.get("backtest_start")),
        "backtest_end": _coerce_isoformat(payload.get("backtest_end")),
        "risk": dict(payload.get("risk") or {}),
        "wallet_config": wallet_config,
        "status": "idle",
        "last_stats": {},
    }
    _apply_instrument_policy(record, payload.get("instrument_type"))
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
    if "instrument_type" in payload:
        _apply_instrument_policy(record, payload.get("instrument_type"))
    if "run_type" in payload and payload["run_type"] is not None:
        record["run_type"] = str(payload["run_type"]).lower()
    if "mode" in payload and payload["mode"] is not None:
        record["mode"] = str(payload["mode"]).lower()
    if "playback_speed" in payload and payload["playback_speed"] is not None:
        record["playback_speed"] = _coerce_playback_speed(payload["playback_speed"])
    elif "fetch_seconds" in payload and payload["fetch_seconds"] is not None:
        record["playback_speed"] = _coerce_playback_speed(payload["fetch_seconds"])
    if "focus_symbol" in payload:
        record["focus_symbol"] = payload.get("focus_symbol") or None
    if "datasource" in payload and payload["datasource"] is not None:
        # Ignore attempts to set peripheral runtime context on bots; derive from strategies
        pass
    if "backtest_start" in payload:
        record["backtest_start"] = _coerce_isoformat(payload.get("backtest_start"))
    if "backtest_end" in payload:
        record["backtest_end"] = _coerce_isoformat(payload.get("backtest_end"))
    if "wallet_config" in payload and payload["wallet_config"] is not None:
        record["wallet_config"] = _validate_wallet_config(payload.get("wallet_config"))
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
    get_watchdog().unregister_bot(bot_id)
    delete_bot(bot_id)
    logger.info("[BotService] bot deleted", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot_deleted", {"bot_id": bot_id})


def start_bot(bot_id: str) -> Dict[str, object]:
    """Start the runtime for the requested bot."""

    bots = {bot["id"]: bot for bot in load_bots()}
    if bot_id not in bots:
        raise KeyError(f"Bot {bot_id} was not found")
    bot = bots[bot_id]
    bot["wallet_config"] = _validate_wallet_config(bot.get("wallet_config"))
    strategy_ids = bot.get("strategy_ids") or ([bot.get("strategy_id")] if bot.get("strategy_id") else [])
    bot["strategy_ids"] = _validate_strategy_ids(strategy_ids)
    bot["strategy_id"] = bot["strategy_ids"][0]
    _validate_backtest_window(bot)
    _validate_strategy_existence(bot)
    _validate_instrument_policy(bot)

    logger.info(
        "[BotService] starting bot with strategies",
        extra={"bot_id": bot_id, "strategy_ids": bot["strategy_ids"]},
    )
    logger.info("[BotService] bot runtime start requested", extra={"bot_id": bot_id})
    runtime = _runtime_for(bot_id, bot)
    runtime.reset_if_finished()
    get_watchdog().register_bot(bot_id)
    runtime.start()
    logger.info("[BotService] bot runtime start dispatched", extra={"bot_id": bot_id})
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
    get_watchdog().unregister_bot(bot_id)
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
            bot["instrument_type"] = _instrument_policy_from_bot(bot)
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
    runtime = _runtime_for(bot_id, bot)
    runtime.warm_up()
    return runtime.logs(limit)


def stream(bot_id: str) -> Tuple[Callable[[], None], Queue, Dict[str, Any]]:
    """Return a release callback, queue, and initial payload for SSE streaming."""

    bot = get_bot(bot_id)
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
        # Use meta field (loaded fresh from DB) instead of snapshot
        meta = dict(
            link.get("meta")
            or {}
        )
        indicator_id = link.get("indicator_id") or link.get("id") or meta.get("id")
        indicators.append(
            {
                "id": indicator_id,
                "name": meta.get("name") or meta.get("type") or indicator_id,
                "type": meta.get("type"),
                "color": meta.get("color"),
                "datasource": meta.get("datasource"),
                "exchange": meta.get("exchange"),
                "params": dict(meta.get("params") or {}),
            }
        )
    return indicators


def _performance_meta(bot: Dict[str, object]) -> Dict[str, object]:
    """Assemble descriptive metadata for bot strategies and symbols.

    Loads strategy data fresh from the database to avoid config drift.
    """
    from .bot_runtime.strategy import StrategyLoader

    selected: List[Dict[str, object]] = []
    for strategy_id in bot.get("strategy_ids", []) or []:
        try:
            strategy = StrategyLoader.fetch_strategy(strategy_id)
        except ValueError:
            # Strategy doesn't exist, skip it
            continue

        # Convert to dict for backward compatibility with UI
        strategy_dict = strategy.to_dict()
        atm_template = merge_templates(strategy_dict.get("atm_template"))

        # Extract instruments from instrument_links
        instruments = [
            {
                "id": link.instrument_id,
                "symbol": link.symbol,
                "risk_multiplier": link.risk_multiplier,
                **link.instrument_snapshot,
            }
            for link in strategy.instrument_links
        ]

        selected.append(
            {
                "id": strategy.id,
                "name": strategy.name,
                "symbols": [link.symbol for link in strategy.instrument_links if link.symbol],
                "timeframe": strategy.timeframe,
                "datasource": strategy.datasource,
                "exchange": strategy.exchange,
                "indicators": _indicator_meta(strategy_dict),
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
            # Bot-level runtime context is derived from its primary strategy.
            "timeframe": (selected[0]["timeframe"] if selected else None),
            "datasource": (selected[0]["datasource"] if selected else None),
            "exchange": (selected[0]["exchange"] if selected else None),
            "risk": bot.get("risk"),
        },
        "strategies": selected,
    }


def performance(bot_id: str) -> Dict[str, object]:
    """Return candle, trade, stat, and metadata payloads for the lens chart."""

    bot = get_bot(bot_id)
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
            "warnings": [],
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


def regime_overlays(bot_id: str) -> Dict[str, Any]:
    """Return raw vs visible regime overlays for debugging."""

    bot = get_bot(bot_id)
    runtime = _RUNTIME.get(bot_id) or _runtime_for(bot_id, bot)
    runtime.apply_config(bot)
    runtime.warm_up()
    return runtime.regime_overlay_dump()


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
