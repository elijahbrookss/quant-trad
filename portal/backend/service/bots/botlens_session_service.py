from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from . import bot_service
from .botlens_contract import continuity_only, projection_only
from .botlens_projection import find_series, normalize_series_key
from .botlens_series_service import get_series_window
from ..storage.storage import get_latest_bot_run_view_state, get_bot_run, list_bot_run_view_states

_ACTIVE_STATUSES = {"starting", "running", "paused", "degraded", "telemetry_degraded"}
_WAITING_PHASES = {
    "start_requested",
    "validating_configuration",
    "resolving_strategy",
    "resolving_runtime_dependencies",
    "preparing_run",
    "stamping_starting_state",
    "launching_container",
    "container_launched",
    "awaiting_container_boot",
    "container_booting",
    "loading_bot_config",
    "claiming_run",
    "loading_strategy_snapshot",
    "preparing_wallet",
    "planning_series_workers",
    "spawning_series_workers",
    "waiting_for_series_bootstrap",
    "warming_up_runtime",
    "runtime_subscribing",
    "awaiting_first_snapshot",
}


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _series_label(*, symbol: str, timeframe: str, series_key: str) -> str:
    display_symbol = str(symbol or "").strip() or str(series_key.split("|", 1)[0] if "|" in series_key else series_key).strip()
    display_timeframe = str(timeframe or "").strip()
    if display_timeframe:
        return f"{display_symbol} · {display_timeframe}"
    return display_symbol or "Unknown series"


def _series_catalog_for_run(*, bot_id: str, run_id: str) -> list[Dict[str, Any]]:
    rows = list_bot_run_view_states(bot_id=bot_id, run_id=run_id)
    catalog: list[Dict[str, Any]] = []
    for row in rows:
        normalized_key = normalize_series_key(row.get("series_key"))
        if not normalized_key:
            continue
        projection = projection_only(row.get("payload"))
        selected = find_series(projection, normalized_key) or {}
        continuity = continuity_only(row.get("payload"))
        symbol = str(selected.get("symbol") or "").strip().upper()
        timeframe = str(selected.get("timeframe") or "").strip().lower()
        instrument_id = str(selected.get("instrument_id") or "").strip() or str(normalized_key.split("|", 1)[0])
        runtime_payload = _mapping(projection.get("runtime"))
        catalog.append(
            {
                "series_key": normalized_key,
                "instrument_id": instrument_id,
                "symbol": symbol or None,
                "timeframe": timeframe or None,
                "display_label": _series_label(symbol=symbol, timeframe=timeframe, series_key=normalized_key),
                "status": str(runtime_payload.get("status") or "").strip() or None,
                "continuity_status": str(continuity.get("status") or "").strip() or None,
                "last_snapshot_at": row.get("event_time") or row.get("known_at"),
                "known_at": row.get("known_at"),
                "seq": int(row.get("seq") or 0),
            }
        )
    catalog.sort(
        key=lambda entry: (
            str(entry.get("symbol") or "").strip() == "",
            str(entry.get("symbol") or "").strip(),
            str(entry.get("timeframe") or "").strip(),
            str(entry.get("series_key") or "").strip(),
        )
    )
    return catalog


def _default_series_key(*, bot_id: str, run_id: str, catalog: list[Dict[str, Any]]) -> Optional[str]:
    latest = get_latest_bot_run_view_state(bot_id=bot_id, run_id=run_id, series_key=None)
    latest_key = normalize_series_key(_mapping(latest).get("series_key"))
    if latest_key and any(str(entry.get("series_key") or "") == latest_key for entry in catalog):
        return latest_key
    return str(catalog[0].get("series_key") or "").strip() or None if catalog else None


def _run_payload(*, run_id: str, projected_bot: Mapping[str, Any], snapshot: Any = None) -> Dict[str, Any]:
    row = _mapping(get_bot_run(run_id))
    runtime = _mapping((_mapping(snapshot).get("window") or {}).get("runtime"))
    summary = row.get("summary")
    if not isinstance(summary, Mapping):
        summary = runtime.get("stats") if isinstance(runtime.get("stats"), Mapping) else {}
    lifecycle = _mapping(projected_bot.get("lifecycle"))
    return {
        "run_id": run_id,
        "status": str(runtime.get("status") or row.get("status") or projected_bot.get("status") or "").strip() or None,
        "phase": str(lifecycle.get("phase") or "").strip() or None,
        "started_at": row.get("started_at"),
        "ended_at": row.get("ended_at"),
        "strategy_id": row.get("strategy_id"),
        "strategy_name": row.get("strategy_name"),
        "run_type": row.get("run_type"),
        "datasource": row.get("datasource"),
        "exchange": row.get("exchange"),
        "symbols": list(row.get("symbols") or []) if isinstance(row.get("symbols"), list) else [],
        "summary": dict(summary or {}) if isinstance(summary, Mapping) else {},
    }


def get_active_botlens_session(*, bot_id: str, series_key: Optional[str] = None, limit: int = 320) -> Dict[str, Any]:
    projected_bot = _mapping(bot_service.get_bot(str(bot_id)))
    active_run_id = str(projected_bot.get("active_run_id") or "").strip() or None
    lifecycle = _mapping(projected_bot.get("lifecycle"))
    bot_status = _normalize_status(projected_bot.get("status") or lifecycle.get("status"))
    bot_phase = str(lifecycle.get("phase") or "").strip().lower()

    if not active_run_id:
        return {
            "bot_id": str(bot_id),
            "state": "inactive",
            "live": False,
            "message": "No active runtime is attached to this bot.",
            "run": None,
            "series_catalog": [],
            "selected_series_key": None,
            "snapshot": None,
            "lifecycle": lifecycle,
        }

    catalog = _series_catalog_for_run(bot_id=str(bot_id), run_id=active_run_id)
    requested_series_key = normalize_series_key(series_key)
    if requested_series_key and not any(str(entry.get("series_key") or "") == requested_series_key for entry in catalog):
        return {
            "bot_id": str(bot_id),
            "state": "series_unavailable",
            "live": bot_status in _ACTIVE_STATUSES,
            "message": "The requested series is not available for the current active runtime.",
            "run": _run_payload(run_id=active_run_id, projected_bot=projected_bot),
            "series_catalog": catalog,
            "selected_series_key": None,
            "snapshot": None,
            "lifecycle": lifecycle,
        }

    if not catalog:
        waiting_message = (
            "Active runtime is booting. BotLens has not received the first series snapshot yet."
            if bot_status in _ACTIVE_STATUSES or bot_phase in _WAITING_PHASES
            else "Active runtime does not currently expose any BotLens series."
        )
        return {
            "bot_id": str(bot_id),
            "state": "waiting_for_series",
            "live": bot_status in _ACTIVE_STATUSES,
            "message": waiting_message,
            "run": _run_payload(run_id=active_run_id, projected_bot=projected_bot),
            "series_catalog": [],
            "selected_series_key": None,
            "snapshot": None,
            "lifecycle": lifecycle,
        }

    selected_series_key = requested_series_key or _default_series_key(
        bot_id=str(bot_id),
        run_id=active_run_id,
        catalog=catalog,
    )
    if not selected_series_key:
        return {
            "bot_id": str(bot_id),
            "state": "waiting_for_series",
            "live": bot_status in _ACTIVE_STATUSES,
            "message": "BotLens could not resolve a default series for the active runtime.",
            "run": _run_payload(run_id=active_run_id, projected_bot=projected_bot),
            "series_catalog": catalog,
            "selected_series_key": None,
            "snapshot": None,
            "lifecycle": lifecycle,
        }

    snapshot = get_series_window(run_id=active_run_id, series_key=selected_series_key, to="now", limit=limit)
    run_payload = _run_payload(run_id=active_run_id, projected_bot=projected_bot, snapshot=snapshot)
    return {
        "bot_id": str(bot_id),
        "state": "ready",
        "live": bot_status in _ACTIVE_STATUSES,
        "message": "BotLens active runtime session ready.",
        "run": run_payload,
        "series_catalog": catalog,
        "selected_series_key": selected_series_key,
        "snapshot": snapshot,
        "lifecycle": _mapping(snapshot.get("lifecycle")) or lifecycle,
    }


def resolve_active_botlens_stream(*, bot_id: str, series_key: Optional[str], limit: int = 320) -> Dict[str, Any]:
    session = get_active_botlens_session(bot_id=bot_id, series_key=series_key, limit=limit)
    if str(session.get("state") or "") != "ready":
        raise ValueError(str(session.get("message") or "BotLens live session is unavailable"))
    run = _mapping(session.get("run"))
    run_id = str(run.get("run_id") or "").strip()
    selected_series_key = normalize_series_key(session.get("selected_series_key"))
    if not run_id or not selected_series_key:
        raise ValueError("BotLens active session is missing run_id or selected_series_key")
    return {
        "run_id": run_id,
        "series_key": selected_series_key,
        "session": session,
    }


__all__ = ["get_active_botlens_session", "resolve_active_botlens_stream"]
