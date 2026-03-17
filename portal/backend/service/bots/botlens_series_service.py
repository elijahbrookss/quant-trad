from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from engines.bot_runtime.runtime.event_types import BOTLENS_SERIES_BOOTSTRAP, BOTLENS_SERIES_DELTA
from ..storage.storage import (
    get_bot_run,
    get_latest_bot_run_view_state,
    list_bot_run_view_states,
    list_bot_runtime_events,
)
from .botlens_projection import (
    apply_series_runtime_delta,
    bounded_projection,
    canonicalize_candle,
    canonicalize_projection,
    find_series,
    normalize_series_key,
)

_SCHEMA_VERSION = 1
_MAX_SCAN_EVENTS = 5000
_BOTLENS_EVENT_TYPES = (BOTLENS_SERIES_BOOTSTRAP, BOTLENS_SERIES_DELTA)


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


def _window_from_projection(
    *,
    run_id: str,
    series_key: str,
    projection: Mapping[str, Any],
    seq: int,
    event_time: Any,
    limit: int,
) -> Dict[str, Any]:
    bounded = bounded_projection(projection, candle_limit=limit)
    selected_series = find_series(bounded, series_key) or {}
    candles = list(selected_series.get("candles") or []) if isinstance(selected_series.get("candles"), list) else []
    trades = [dict(trade) for trade in bounded.get("trades") if isinstance(trade, Mapping)] if isinstance(bounded.get("trades"), list) else []
    logs = list(bounded.get("logs") or []) if isinstance(bounded.get("logs"), list) else []
    decisions = list(bounded.get("decisions") or []) if isinstance(bounded.get("decisions"), list) else []
    warnings = list(bounded.get("warnings") or []) if isinstance(bounded.get("warnings"), list) else []
    runtime = dict(bounded.get("runtime") or {}) if isinstance(bounded.get("runtime"), Mapping) else {}
    return {
        "run_id": str(run_id),
        "series_key": str(series_key),
        "schema_version": _SCHEMA_VERSION,
        "seq": int(seq),
        "event_time": event_time,
        "window": {
            "projection": bounded,
            "selected_series": dict(selected_series) if isinstance(selected_series, Mapping) else {},
            "candles": candles,
            "trades": trades,
            "logs": logs,
            "decisions": decisions,
            "warnings": warnings,
            "runtime": runtime,
            "markers": [],
            "status": str(runtime.get("status") or "waiting"),
        },
    }


def _latest_series_view_state(*, run_id: str, series_key: str) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    bot_id = _run_bot_id(run_id=run_id)
    row = get_latest_bot_run_view_state(
        bot_id=bot_id,
        run_id=str(run_id),
        series_key=normalize_series_key(series_key),
    )
    projection = canonicalize_projection(dict(row.get("payload") or {})) if isinstance(row, Mapping) else {}
    return bot_id, dict(row) if isinstance(row, Mapping) else None, projection


def get_series_window(*, run_id: str, series_key: str, to: Optional[str], limit: int) -> Dict[str, Any]:
    _ = to
    series_key = normalize_series_key(series_key)
    _bot_id, row, projection = _latest_series_view_state(run_id=run_id, series_key=series_key)
    if row and find_series(projection, series_key):
        return _window_from_projection(
            run_id=run_id,
            series_key=series_key,
            projection=projection,
            seq=_coerce_int(row.get("seq"), 0),
            event_time=row.get("event_time") or row.get("known_at"),
            limit=limit,
        )

    rows = list_bot_run_view_states(bot_id=_bot_id, run_id=str(run_id))
    if not rows:
        return {
            "run_id": str(run_id),
            "series_key": str(series_key),
            "schema_version": _SCHEMA_VERSION,
            "seq": 0,
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
    bot_id, latest_row, latest_projection = _latest_series_view_state(run_id=run_id, series_key=series_key)
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
        projection = payload.get("projection") if isinstance(payload.get("projection"), Mapping) else None
        if isinstance(projection, Mapping):
            _merge_projection_candles(candle_map, canonicalize_projection(projection), series_key)
            continue
        runtime_delta = payload.get("runtime_delta") if isinstance(payload.get("runtime_delta"), Mapping) else None
        if not isinstance(runtime_delta, Mapping):
            continue
        projection = apply_series_runtime_delta(
            {"series": []},
            series_key=series_key,
            seq=_coerce_int(payload.get("series_seq"), 0),
            runtime_delta=runtime_delta,
        )
        _merge_projection_candles(candle_map, projection, series_key)

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
