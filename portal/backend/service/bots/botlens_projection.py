from __future__ import annotations

import json
import math
from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def normalize_candle_time(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        if abs(numeric) > 2e10:
            numeric /= 1000.0
        return int(math.floor(numeric))
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
        if not math.isfinite(numeric):
            return None
        if abs(numeric) > 2e10:
            numeric /= 1000.0
        return int(math.floor(numeric))
    except ValueError:
        pass
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        return None


def canonical_series_key(symbol: Any, timeframe: Any) -> str:
    return f"{str(symbol or '').strip().upper()}|{str(timeframe or '').strip().lower()}"


def canonical_series_key_from_entry(entry: Mapping[str, Any]) -> str:
    return canonical_series_key(entry.get("symbol"), entry.get("timeframe"))


def canonicalize_candle(candle: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(candle, Mapping):
        return None
    time_value = normalize_candle_time(candle.get("time"))
    if time_value is None:
        return None
    normalized = dict(candle)
    normalized["time"] = time_value
    for key in ("open", "high", "low", "close"):
        if key not in normalized:
            continue
        try:
            numeric = float(normalized.get(key))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric):
            return None
        normalized[key] = numeric
    return normalized


def merge_candle_streams(*streams: Any) -> List[Dict[str, Any]]:
    by_time: Dict[int, Dict[str, Any]] = {}
    for stream in streams:
        values = stream if isinstance(stream, list) else []
        for candle in values:
            normalized = canonicalize_candle(candle)
            if normalized is None:
                continue
            by_time[int(normalized["time"])] = normalized
    return [by_time[key] for key in sorted(by_time.keys())]


def overlay_identity(overlay: Any, index: int) -> str:
    if not isinstance(overlay, Mapping):
        return f"index:{index}"
    for key in ("overlay_id", "id", "name", "key", "slug", "indicator_id", "type"):
        value = str(overlay.get(key) or "").strip()
        if value:
            return f"{key}:{value}"
    return f"index:{index}"


def overlay_revision(overlay: Any) -> str:
    return json.dumps(_normalize_scalar_value(overlay), sort_keys=True, separators=(",", ":"))


def project_overlay_state(overlays: Any) -> List[Dict[str, Any]]:
    projected: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for index, overlay in enumerate(overlays if isinstance(overlays, list) else []):
        if not isinstance(overlay, Mapping):
            continue
        identity = overlay_identity(overlay, index)
        normalized = dict(overlay)
        normalized["overlay_id"] = identity
        normalized["overlay_revision"] = overlay_revision(
            {key: value for key, value in normalized.items() if key not in {"overlay_revision"}}
        )
        projected[identity] = normalized
    return list(projected.values())


def canonicalize_series_entry(entry: Any, index: int = 0) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, Mapping):
        return None
    symbol = str(entry.get("symbol") or "").strip().upper()
    timeframe = str(entry.get("timeframe") or "").strip().lower()
    series = dict(entry)
    series["symbol"] = symbol
    series["timeframe"] = timeframe
    series["series_key"] = canonical_series_key(symbol, timeframe)
    series["candles"] = merge_candle_streams(entry.get("candles"))
    series["overlays"] = project_overlay_state(entry.get("overlays"))
    series["stats"] = dict(entry.get("stats") or {}) if isinstance(entry.get("stats"), Mapping) else {}
    if not series["series_key"]:
        series["series_key"] = f"series_index:{index}"
    return series


def canonicalize_projection(snapshot: Any) -> Dict[str, Any]:
    source = snapshot if isinstance(snapshot, Mapping) else {}
    series_entries = source.get("series") if isinstance(source.get("series"), list) else []
    series_by_key: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for index, entry in enumerate(series_entries):
        normalized = canonicalize_series_entry(entry, index=index)
        if normalized is None:
            continue
        series_by_key[str(normalized.get("series_key") or f"series_index:{index}")] = normalized
    return {
        "series": list(series_by_key.values()),
        "trades": [dict(entry) for entry in source.get("trades") if isinstance(entry, Mapping)] if isinstance(source.get("trades"), list) else [],
        "logs": list(source.get("logs") or []) if isinstance(source.get("logs"), list) else [],
        "decisions": list(source.get("decisions") or []) if isinstance(source.get("decisions"), list) else [],
        "runtime": dict(source.get("runtime") or {}) if isinstance(source.get("runtime"), Mapping) else {},
        "warnings": list(source.get("warnings") or []) if isinstance(source.get("warnings"), list) else [],
    }


def bounded_projection(snapshot: Any, *, candle_limit: int) -> Dict[str, Any]:
    projection = canonicalize_projection(snapshot)
    limit = max(1, int(candle_limit))
    bounded_series: List[Dict[str, Any]] = []
    for entry in projection.get("series") or []:
        row = dict(entry)
        candles = row.get("candles") if isinstance(row.get("candles"), list) else []
        row["candles"] = list(candles[-limit:])
        bounded_series.append(row)
    next_projection = dict(projection)
    next_projection["series"] = bounded_series
    return next_projection


def find_series(snapshot: Any, series_key: str) -> Optional[Dict[str, Any]]:
    projection = snapshot if isinstance(snapshot, Mapping) else {}
    target = str(series_key or "").strip().upper()
    for entry in projection.get("series") if isinstance(projection.get("series"), list) else []:
        if not isinstance(entry, Mapping):
            continue
        candidate_key = str(entry.get("series_key") or canonical_series_key_from_entry(entry)).strip().upper()
        if candidate_key == target:
            return dict(entry)
    return None


def overlay_projection_delta(*, previous: Any, current: Any) -> Dict[str, Any]:
    previous_projection = canonicalize_projection(previous)
    current_projection = canonicalize_projection(current)

    previous_series: Dict[str, Dict[str, Any]] = {
        str(entry.get("series_key") or canonical_series_key_from_entry(entry)): dict(entry)
        for entry in previous_projection.get("series") or []
        if isinstance(entry, Mapping)
    }
    deltas: List[Dict[str, Any]] = []
    for entry in current_projection.get("series") or []:
        if not isinstance(entry, Mapping):
            continue
        series_key = str(entry.get("series_key") or canonical_series_key_from_entry(entry))
        current_overlays = list(entry.get("overlays") or []) if isinstance(entry.get("overlays"), list) else []
        previous_entry = previous_series.get(series_key)
        if previous_entry is None:
            deltas.append(
                {
                    "series_key": series_key,
                    "mode": "replace",
                    "removed": [],
                    "overlays": current_overlays,
                }
            )
            continue
        previous_overlays = list(previous_entry.get("overlays") or []) if isinstance(previous_entry.get("overlays"), list) else []
        previous_map = {
            str(overlay.get("overlay_id") or overlay_identity(overlay, index)): dict(overlay)
            for index, overlay in enumerate(previous_overlays)
            if isinstance(overlay, Mapping)
        }
        current_ids: List[str] = []
        changed: List[Dict[str, Any]] = []
        for index, overlay in enumerate(current_overlays):
            if not isinstance(overlay, Mapping):
                continue
            overlay_id = str(overlay.get("overlay_id") or overlay_identity(overlay, index))
            current_ids.append(overlay_id)
            previous_overlay = previous_map.get(overlay_id)
            if previous_overlay is None or overlay_revision(previous_overlay) != overlay_revision(overlay):
                changed.append(dict(overlay))
        removed = [overlay_id for overlay_id in previous_map.keys() if overlay_id not in set(current_ids)]
        deltas.append(
            {
                "series_key": series_key,
                "mode": "delta",
                "removed": removed,
                "overlays": changed,
            }
        )
    return {"series": deltas}


def _normalize_scalar_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_scalar_value(entry) for key, entry in value.items()}
    if isinstance(value, list):
        return [_normalize_scalar_value(entry) for entry in value]
    if isinstance(value, tuple):
        return [_normalize_scalar_value(entry) for entry in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value
