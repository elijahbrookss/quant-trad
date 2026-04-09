from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from portal.backend.service.bots.botlens_contract import (
    EVENT_TYPE_LIFECYCLE,
    EVENT_TYPE_RUNTIME_FACTS,
    EVENT_TYPE_RUNTIME_BOOTSTRAP,
    build_window_payload,
    normalize_lifecycle_payload,
    projection_only,
)
from ..storage.storage import (
    get_bot_run,
    get_latest_bot_run_view_state,
    get_latest_bot_runtime_event,
    list_bot_run_view_states,
    list_bot_runtime_events,
)
from .botlens_projection import (
    candle_facts,
    canonicalize_candle,
    find_series,
    normalize_series_key,
)

_SCHEMA_VERSION = 3
_MAX_SCAN_EVENTS = 5000
_BOTLENS_EVENT_TYPES = (EVENT_TYPE_RUNTIME_BOOTSTRAP, EVENT_TYPE_RUNTIME_FACTS)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _run_bot_id(*, run_id: str) -> str:
    run_row = get_bot_run(str(run_id)) or {}
    bot_id = str(run_row.get("bot_id") or "").strip()
    if not bot_id:
        raise ValueError(f"bot_id missing for run_id={run_id}")
    return bot_id


def _latest_series_view_state(*, run_id: str, series_key: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    bot_id = _run_bot_id(run_id=run_id)
    row = get_latest_bot_run_view_state(
        bot_id=bot_id,
        run_id=str(run_id),
        series_key=normalize_series_key(series_key),
    )
    return bot_id, dict(row) if isinstance(row, Mapping) else None


def _latest_lifecycle(*, bot_id: str, run_id: str) -> Dict[str, Any]:
    row = get_latest_bot_runtime_event(
        bot_id=bot_id,
        run_id=run_id,
        event_types=[EVENT_TYPE_LIFECYCLE],
    )
    payload = row.get("payload") if isinstance(row, Mapping) else {}
    return normalize_lifecycle_payload(payload)


def get_series_window(*, run_id: str, series_key: str, to: Optional[str], limit: int) -> Dict[str, Any]:
    _ = to
    series_key = normalize_series_key(series_key)
    bot_id, row = _latest_series_view_state(run_id=run_id, series_key=series_key)
    projection = projection_only((row or {}).get("payload"))
    if row and find_series(projection, series_key):
        window = build_window_payload(
            run_id=run_id,
            series_key=series_key,
            seq=_coerce_int(row.get("seq"), 0),
            event_time=row.get("event_time") or row.get("known_at"),
            payload=row.get("payload"),
            limit=limit,
        )
        latest_lifecycle = _latest_lifecycle(bot_id=bot_id, run_id=run_id)
        if latest_lifecycle:
            window["lifecycle"] = latest_lifecycle
        return window

    rows = list_bot_run_view_states(bot_id=bot_id, run_id=str(run_id))
    if not rows:
        return {
            "run_id": str(run_id),
            "series_key": str(series_key),
            "schema_version": _SCHEMA_VERSION,
            "seq": 0,
            "cursor": {"projection_seq": 0},
            "continuity": {"status": "bootstrap_required", "last_bridge_seq": 0, "details": {}},
            "lifecycle": _latest_lifecycle(bot_id=bot_id, run_id=run_id),
            "window": {"candles": [], "trades": [], "markers": [], "status": "waiting"},
        }
    raise ValueError(f"series {series_key!r} was not found for run_id={run_id}")


def _merge_projection_candles(candle_map: Dict[int, Dict[str, Any]], projection: Mapping[str, Any], series_key: str) -> None:
    selected = find_series(projection, series_key) or {}
    candles = selected.get("candles") if isinstance(selected.get("candles"), list) else []
    for candle in candles:
        normalized = canonicalize_candle(candle)
        if normalized is None:
            continue
        candle_map[int(normalized["time"])] = normalized


def get_series_history(*, run_id: str, series_key: str, before_ts: Optional[str], limit: int) -> Dict[str, Any]:
    series_key = normalize_series_key(series_key)
    bot_id, latest_row = _latest_series_view_state(run_id=run_id, series_key=series_key)
    latest_projection = projection_only((latest_row or {}).get("payload"))
    before_dt = _to_datetime(before_ts)
    if latest_row is None:
        return {
            "run_id": str(run_id),
            "series_key": str(series_key),
            "schema_version": _SCHEMA_VERSION,
            "before_ts": before_ts,
            "next_before_ts": None,
            "has_more": False,
            "history": {"candles": [], "trades": [], "markers": []},
        }

    candle_map: Dict[int, Dict[str, Any]] = {}
    rows = list_bot_runtime_events(
        bot_id=bot_id,
        run_id=str(run_id),
        after_seq=0,
        limit=max(1, min(int(limit or 320) * 20, _MAX_SCAN_EVENTS)),
        event_types=list(_BOTLENS_EVENT_TYPES),
    )
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
        if normalize_series_key(payload.get("series_key")) != normalize_series_key(series_key):
            continue
        for candle in candle_facts(payload.get("facts"), series_key=series_key):
            candle_map[int(candle["time"])] = candle

    if not candle_map:
        _merge_projection_candles(candle_map, latest_projection, series_key)

    ordered = [candle_map[key] for key in sorted(candle_map.keys())]
    if before_dt is not None:
        cutoff = int(before_dt.timestamp())
        ordered = [candle for candle in ordered if int(candle.get("time") or 0) < cutoff]
    page = ordered[-max(1, int(limit)) :]
    oldest = page[0]["time"] if page else None
    has_more = len(ordered) > len(page)
    return {
        "run_id": str(run_id),
        "series_key": str(series_key),
        "schema_version": _SCHEMA_VERSION,
        "before_ts": before_ts,
        "next_before_ts": oldest,
        "has_more": has_more,
        "history": {"candles": page, "trades": [], "markers": []},
    }


def list_series_keys(*, run_id: str) -> Dict[str, Any]:
    bot_id = _run_bot_id(run_id=run_id)
    rows = list_bot_run_view_states(bot_id=bot_id, run_id=str(run_id))
    keys: List[str] = []
    for row in rows:
        series_key = normalize_series_key(row.get("series_key"))
        if series_key and series_key not in keys:
            keys.append(series_key)
    return {"run_id": str(run_id), "series": keys}
