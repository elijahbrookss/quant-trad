from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..storage.storage import get_bot_run, get_latest_bot_run_view_state, list_bot_runtime_events

_SCHEMA_VERSION = 1
_MAX_SCAN_EVENTS = 5000


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


def _series_identity(entry: Mapping[str, Any]) -> str:
    symbol = str(entry.get("symbol") or "").strip().upper()
    timeframe = str(entry.get("timeframe") or "").strip().lower()
    return f"{symbol}|{timeframe}"


def _find_series(snapshot: Mapping[str, Any], series_key: str) -> Optional[Dict[str, Any]]:
    series = snapshot.get("series")
    if not isinstance(series, list):
        return None
    target = str(series_key or "").strip().upper()
    for entry in series:
        if not isinstance(entry, Mapping):
            continue
        if _series_identity(entry).upper() == target:
            return dict(entry)
    return None


def _event_rows_for_run(*, run_id: str, limit: int = _MAX_SCAN_EVENTS) -> Tuple[str, List[Dict[str, Any]]]:
    run_row = get_bot_run(str(run_id)) or {}
    bot_id = str(run_row.get("bot_id") or "").strip()
    if not bot_id:
        raise ValueError(f"bot_id missing for run_id={run_id}")
    rows = list_bot_runtime_events(bot_id=bot_id, run_id=str(run_id), after_seq=0, limit=max(1, min(int(limit), _MAX_SCAN_EVENTS)))
    return bot_id, rows


def _view_state_for_run(*, run_id: str) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    run_row = get_bot_run(str(run_id)) or {}
    bot_id = str(run_row.get("bot_id") or "").strip()
    if not bot_id:
        raise ValueError(f"bot_id missing for run_id={run_id}")
    view_row = get_latest_bot_run_view_state(bot_id=bot_id, run_id=str(run_id), series_key="bot")
    snapshot = dict(view_row.get("payload") or {}) if isinstance(view_row, Mapping) else {}
    return bot_id, dict(view_row) if isinstance(view_row, Mapping) else None, snapshot


def _extract_snapshot(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), Mapping) else {}
    return dict(snapshot)


def _extract_series_point(candle: Mapping[str, Any]) -> Optional[Tuple[int, Dict[str, Any]]]:
    t = candle.get("time")
    try:
        key = int(t)
    except (TypeError, ValueError):
        return None
    return key, dict(candle)


def _window_from_snapshot(
    *,
    run_id: str,
    series_key: str,
    snapshot: Mapping[str, Any],
    seq: int,
    event_time: Any,
    limit: int,
) -> Dict[str, Any]:
    series = _find_series(snapshot, series_key) or {}
    candles = series.get("candles") if isinstance(series.get("candles"), list) else []
    bounded = [dict(c) for c in candles[-max(1, int(limit)): ] if isinstance(c, Mapping)]
    symbol = str(series.get("symbol") or "").strip().upper()

    trades = []
    for trade in snapshot.get("trades") if isinstance(snapshot.get("trades"), list) else []:
        if not isinstance(trade, Mapping):
            continue
        if symbol and str(trade.get("symbol") or "").strip().upper() != symbol:
            continue
        trades.append(dict(trade))

    return {
        "run_id": str(run_id),
        "series_key": str(series_key),
        "schema_version": _SCHEMA_VERSION,
        "seq": int(seq),
        "event_time": event_time,
        "window": {
            "candles": bounded,
            "trades": trades[-max(1, int(limit)):],
            "markers": [],
            "status": str((snapshot.get("runtime") or {}).get("status") or "running"),
        },
    }


def _series_keys_from_snapshot(snapshot: Mapping[str, Any]) -> List[str]:
    series = snapshot.get("series") if isinstance(snapshot.get("series"), list) else []
    keys: List[str] = []
    for entry in series:
        if not isinstance(entry, Mapping):
            continue
        key = _series_identity(entry)
        if key and key not in keys:
            keys.append(key)
    return keys


def _merge_snapshot_candles(
    *,
    candle_map: Dict[int, Dict[str, Any]],
    snapshot: Mapping[str, Any],
    series_key: str,
    before_dt: Optional[datetime],
) -> None:
    series = _find_series(snapshot, series_key) or {}
    candles = series.get("candles") if isinstance(series.get("candles"), list) else []
    for candle in candles:
        if not isinstance(candle, Mapping):
            continue
        point = _extract_series_point(candle)
        if point is None:
            continue
        ts_int, candle_value = point
        if before_dt is not None:
            candle_dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
            if candle_dt >= before_dt:
                continue
        candle_map[ts_int] = candle_value


def get_series_window(*, run_id: str, series_key: str, to: Optional[str], limit: int) -> Dict[str, Any]:
    _bot_id, rows = _event_rows_for_run(run_id=run_id)
    if not rows:
        _view_bot_id, view_row, snapshot = _view_state_for_run(run_id=run_id)
        if snapshot:
            return _window_from_snapshot(
                run_id=run_id,
                series_key=series_key,
                snapshot=snapshot,
                seq=_coerce_int((view_row or {}).get("seq"), 0),
                event_time=(view_row or {}).get("event_time") or (view_row or {}).get("known_at"),
                limit=limit,
            )
        return {
            "run_id": str(run_id),
            "series_key": str(series_key),
            "schema_version": _SCHEMA_VERSION,
            "seq": 0,
            "window": {"candles": [], "trades": [], "markers": [], "status": "waiting"},
        }

    to_dt = _to_datetime(to) if to and str(to).lower() != "now" else None
    selected_row: Optional[Dict[str, Any]] = None
    for row in reversed(rows):
        event_time = _to_datetime(row.get("event_time") or row.get("known_at") or row.get("created_at"))
        if to_dt is not None and event_time is not None and event_time > to_dt:
            continue
        selected_row = dict(row)
        break
    if selected_row is None:
        selected_row = dict(rows[0])

    snapshot = _extract_snapshot(selected_row)
    if not _find_series(snapshot, series_key):
        _bot_id, view_row, fallback_snapshot = _view_state_for_run(run_id=run_id)
        if fallback_snapshot:
            snapshot = fallback_snapshot
            selected_row = {
                **selected_row,
                "seq": _coerce_int((view_row or {}).get("seq"), _coerce_int(selected_row.get("seq"), 0)),
                "event_time": (view_row or {}).get("event_time") or (view_row or {}).get("known_at") or selected_row.get("event_time"),
                "known_at": (view_row or {}).get("known_at") or selected_row.get("known_at"),
            }
    return _window_from_snapshot(
        run_id=run_id,
        series_key=series_key,
        snapshot=snapshot,
        seq=_coerce_int(selected_row.get("seq"), 0),
        event_time=selected_row.get("event_time") or selected_row.get("known_at"),
        limit=limit,
    )


def get_series_history(*, run_id: str, series_key: str, before_ts: Optional[str], limit: int) -> Dict[str, Any]:
    _bot_id, rows = _event_rows_for_run(run_id=run_id)
    before_dt = _to_datetime(before_ts)
    candle_map: Dict[int, Dict[str, Any]] = {}
    _view_bot_id, _view_row, fallback_snapshot = _view_state_for_run(run_id=run_id)

    if fallback_snapshot:
        _merge_snapshot_candles(
            candle_map=candle_map,
            snapshot=fallback_snapshot,
            series_key=series_key,
            before_dt=before_dt,
        )

    for row in rows:
        snapshot = _extract_snapshot(row)
        _merge_snapshot_candles(
            candle_map=candle_map,
            snapshot=snapshot,
            series_key=series_key,
            before_dt=before_dt,
        )

    ordered = [candle_map[key] for key in sorted(candle_map.keys())]
    page = ordered[-max(1, int(limit)):]
    oldest = page[0]["time"] if page else None
    has_more = len(ordered) > len(page)
    return {
        "run_id": str(run_id),
        "series_key": str(series_key),
        "schema_version": _SCHEMA_VERSION,
        "before_ts": before_ts,
        "next_before_ts": oldest,
        "has_more": has_more,
        "history": {
            "candles": page,
            "trades": [],
            "markers": [],
        },
    }


def build_live_tail_messages(
    *,
    run_id: str,
    series_key: str,
    seq: int,
    previous_snapshot: Optional[Mapping[str, Any]],
    current_snapshot: Mapping[str, Any],
    known_at: Any,
) -> List[Dict[str, Any]]:
    prev_series = _find_series(previous_snapshot or {}, series_key) if previous_snapshot else None
    curr_series = _find_series(current_snapshot, series_key)
    if not curr_series:
        return []

    messages: List[Dict[str, Any]] = []
    prev_candles = prev_series.get("candles") if isinstance(prev_series, Mapping) and isinstance(prev_series.get("candles"), list) else []
    curr_candles = curr_series.get("candles") if isinstance(curr_series.get("candles"), list) else []
    if curr_candles:
        curr_last = curr_candles[-1]
        prev_last = prev_candles[-1] if prev_candles else None
        if isinstance(curr_last, Mapping):
            envelope = {
                "type": "botlens_live_tail",
                "run_id": str(run_id),
                "series_key": str(series_key),
                "schema_version": _SCHEMA_VERSION,
                "seq": int(seq),
                "known_at": known_at,
            }
            if isinstance(prev_last, Mapping) and prev_last.get("time") == curr_last.get("time"):
                messages.append({**envelope, "message_type": "bar_update", "payload": {"bar": dict(curr_last)}})
            else:
                messages.append({**envelope, "message_type": "bar_append", "payload": {"bar": dict(curr_last)}})

    status = str((current_snapshot.get("runtime") or {}).get("status") or "")
    if status:
        messages.append(
            {
                "type": "botlens_live_tail",
                "run_id": str(run_id),
                "series_key": str(series_key),
                "schema_version": _SCHEMA_VERSION,
                "seq": int(seq),
                "known_at": known_at,
                "message_type": "status",
                "payload": {"status": status},
            }
        )
    return messages


def list_series_keys(*, run_id: str) -> Dict[str, Any]:
    _bot_id, rows = _event_rows_for_run(run_id=run_id)
    if rows:
        keys = _series_keys_from_snapshot(_extract_snapshot(rows[-1]))
        if keys:
            return {"run_id": str(run_id), "series": keys}
    _view_bot_id, _view_row, snapshot = _view_state_for_run(run_id=run_id)
    return {"run_id": str(run_id), "series": _series_keys_from_snapshot(snapshot)}
