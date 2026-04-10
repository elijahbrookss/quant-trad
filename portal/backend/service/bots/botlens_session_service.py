from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, Optional

from . import bot_service
from .botlens_contract import RUN_SCOPE_KEY, normalize_series_key
from .botlens_state import read_run_summary_state, run_bootstrap_contract, select_default_symbol_key
from .botlens_symbol_service import get_symbol_detail
from ..storage.storage import get_bot_run, get_latest_bot_run_view_state

_ACTIVE_STATUSES = {"starting", "running", "paused", "degraded", "telemetry_degraded"}


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _run_meta(*, run_id: str, projected_bot: Mapping[str, Any], summary_state: Mapping[str, Any]) -> Dict[str, Any]:
    row = _mapping(get_bot_run(run_id))
    existing = _mapping(summary_state.get("run_meta"))
    summary_health = _mapping(summary_state.get("health"))
    return {
        "run_id": str(run_id),
        "bot_id": str(row.get("bot_id") or projected_bot.get("id") or "").strip() or None,
        "status": str(summary_health.get("status") or row.get("status") or projected_bot.get("status") or "").strip() or None,
        "started_at": row.get("started_at"),
        "ended_at": row.get("ended_at"),
        "strategy_id": row.get("strategy_id") or existing.get("strategy_id"),
        "strategy_name": row.get("strategy_name") or existing.get("strategy_name"),
        "run_type": row.get("run_type") or existing.get("run_type"),
        "datasource": row.get("datasource") or existing.get("datasource"),
        "exchange": row.get("exchange") or existing.get("exchange"),
        "symbols": list(row.get("symbols") or []) if isinstance(row.get("symbols"), list) else list(existing.get("symbols") or []),
        "summary": dict(row.get("summary") or {}) if isinstance(row.get("summary"), Mapping) else {},
    }


def get_active_botlens_session(*, bot_id: str, symbol_key: Optional[str] = None, limit: int = 320) -> Dict[str, Any]:
    projected_bot = _mapping(bot_service.get_bot(str(bot_id)))
    active_run_id = str(projected_bot.get("active_run_id") or "").strip() or None
    lifecycle = _mapping(projected_bot.get("lifecycle"))
    bot_status = str(projected_bot.get("status") or "").strip().lower()

    if not active_run_id:
        return {
            "schema_version": 4,
            "bot_id": str(bot_id),
            "state": "inactive",
            "live": False,
            "message": "No active runtime is attached to this bot.",
            "run_meta": None,
            "lifecycle": lifecycle,
            "health": {},
            "symbol_summaries": [],
            "open_trades": [],
            "selected_symbol_key": None,
            "detail": None,
            "seq": 0,
        }

    summary_row = get_latest_bot_run_view_state(bot_id=str(bot_id), run_id=active_run_id, series_key=RUN_SCOPE_KEY)
    summary_state = read_run_summary_state(_mapping(summary_row).get("payload"), bot_id=str(bot_id), run_id=active_run_id)
    symbol_index = summary_state.get("symbol_index") if isinstance(summary_state.get("symbol_index"), Mapping) else {}
    open_trades_index = summary_state.get("open_trades_index") if isinstance(summary_state.get("open_trades_index"), Mapping) else {}
    requested_symbol_key = normalize_series_key(symbol_key)
    if requested_symbol_key and requested_symbol_key not in symbol_index:
        return {
            "schema_version": 4,
            "bot_id": str(bot_id),
            "state": "symbol_unavailable",
            "live": bot_status in _ACTIVE_STATUSES,
            "message": "The requested symbol is not available for the current active runtime.",
            "run_meta": _run_meta(run_id=active_run_id, projected_bot=projected_bot, summary_state=summary_state),
            "lifecycle": lifecycle,
            "health": dict(summary_state.get("health") or {}),
            "symbol_summaries": [dict(value) for value in symbol_index.values() if isinstance(value, Mapping)],
            "open_trades": [dict(value) for value in open_trades_index.values() if isinstance(value, Mapping)],
            "selected_symbol_key": None,
            "detail": None,
            "seq": int(summary_state.get("seq") or 0),
        }

    if not symbol_index:
        return {
            "schema_version": 4,
            "bot_id": str(bot_id),
            "state": "waiting_for_symbols",
            "live": bot_status in _ACTIVE_STATUSES,
            "message": "Active runtime is booting. BotLens has not received the first symbol snapshot yet.",
            "run_meta": _run_meta(run_id=active_run_id, projected_bot=projected_bot, summary_state=summary_state),
            "lifecycle": lifecycle,
            "health": dict(summary_state.get("health") or {}),
            "symbol_summaries": [],
            "open_trades": [dict(value) for value in open_trades_index.values() if isinstance(value, Mapping)],
            "selected_symbol_key": None,
            "detail": None,
            "seq": int(summary_state.get("seq") or 0),
        }

    selected_symbol_key = requested_symbol_key or select_default_symbol_key(
        symbol_index=symbol_index,
        open_trades_index=open_trades_index,
    )
    if not selected_symbol_key:
        raise ValueError(f"BotLens could not resolve a default symbol for run_id={active_run_id}")

    detail_contract = get_symbol_detail(run_id=active_run_id, symbol_key=selected_symbol_key, limit=limit)
    return run_bootstrap_contract(
        bot_id=str(bot_id),
        run_meta=_run_meta(run_id=active_run_id, projected_bot=projected_bot, summary_state=summary_state),
        lifecycle=_mapping(summary_state.get("lifecycle")) or lifecycle,
        health=_mapping(summary_state.get("health")),
        symbol_index=symbol_index,
        open_trades_index=open_trades_index,
        selected_symbol_key=selected_symbol_key,
        detail=_mapping(detail_contract.get("detail")),
        state="ready",
        live=bot_status in _ACTIVE_STATUSES,
        message="BotLens active runtime session ready.",
        seq=int(summary_state.get("seq") or 0),
    )


def resolve_active_botlens_stream(*, bot_id: str, symbol_key: Optional[str], limit: int = 320) -> Dict[str, Any]:
    session = get_active_botlens_session(bot_id=bot_id, symbol_key=symbol_key, limit=limit)
    if str(session.get("state") or "") != "ready":
        raise ValueError(str(session.get("message") or "BotLens live session is unavailable"))
    run_meta = _mapping(session.get("run_meta"))
    run_id = str(run_meta.get("run_id") or "").strip()
    selected_symbol_key = normalize_series_key(session.get("selected_symbol_key"))
    if not run_id:
        raise ValueError("BotLens active session is missing run_id")
    return {
        "run_id": run_id,
        "selected_symbol_key": selected_symbol_key or None,
        "session": session,
    }


__all__ = ["get_active_botlens_session", "resolve_active_botlens_stream"]
