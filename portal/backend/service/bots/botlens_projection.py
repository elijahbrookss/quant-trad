from __future__ import annotations

import json
import math
from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from engines.bot_runtime.core.series_identity import (
    canonical_series_key as build_canonical_series_key,
)
from engines.bot_runtime.core.series_identity import normalize_series_key as normalize_public_series_key


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


def canonical_series_key(instrument_id: Any, timeframe: Any) -> str:
    return build_canonical_series_key(instrument_id, timeframe)


def normalize_series_key(value: Any) -> str:
    return normalize_public_series_key(value)


def _instrument_id_from_entry(entry: Mapping[str, Any]) -> str:
    explicit = str(entry.get("instrument_id") or "").strip()
    if explicit:
        return explicit
    instrument = entry.get("instrument")
    if isinstance(instrument, Mapping):
        return str(instrument.get("id") or "").strip()
    return ""


def canonical_series_key_from_entry(entry: Mapping[str, Any]) -> str:
    explicit = normalize_series_key(entry.get("series_key"))
    if explicit:
        return explicit
    return canonical_series_key(_instrument_id_from_entry(entry), entry.get("timeframe"))


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
    explicit_overlay_id = str(overlay.get("overlay_id") or "").strip()
    if explicit_overlay_id:
        return explicit_overlay_id
    for key in ("id", "name", "key", "slug", "indicator_id", "type"):
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


def apply_overlay_delta(overlays: Any, delta: Any) -> List[Dict[str, Any]]:
    current = project_overlay_state(overlays)
    payload = delta if isinstance(delta, Mapping) else {}
    ops = payload.get("ops") if isinstance(payload.get("ops"), list) else []
    overlay_map: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for index, overlay in enumerate(current):
        if not isinstance(overlay, Mapping):
            continue
        overlay_id = str(overlay.get("overlay_id") or overlay_identity(overlay, index))
        normalized = dict(overlay)
        normalized["overlay_id"] = overlay_id
        overlay_map[overlay_id] = normalized
    for op in ops:
        if not isinstance(op, Mapping):
            continue
        op_name = str(op.get("op") or "").strip().lower()
        key = str(op.get("key") or "").strip()
        if not key:
            continue
        if op_name == "remove":
            overlay_map.pop(key, None)
            continue
        if op_name == "upsert":
            overlay = op.get("overlay")
            if not isinstance(overlay, Mapping):
                continue
            normalized = dict(overlay)
            normalized["overlay_id"] = key
            normalized["overlay_revision"] = overlay_revision(
                {entry_key: entry_value for entry_key, entry_value in normalized.items() if entry_key != "overlay_revision"}
            )
            overlay_map[key] = normalized
    return list(overlay_map.values())


def canonicalize_series_entry(entry: Any, index: int = 0) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, Mapping):
        return None
    instrument_id = _instrument_id_from_entry(entry)
    symbol = str(entry.get("symbol") or "").strip().upper()
    timeframe = str(entry.get("timeframe") or "").strip().lower()
    series = dict(entry)
    series["instrument_id"] = instrument_id
    series["symbol"] = symbol
    series["timeframe"] = timeframe
    series["series_key"] = canonical_series_key(instrument_id, timeframe)
    series["candles"] = merge_candle_streams(entry.get("candles"))
    series["overlays"] = project_overlay_state(entry.get("overlays"))
    series["stats"] = dict(entry.get("stats") or {}) if isinstance(entry.get("stats"), Mapping) else {}
    if not series["series_key"]:
        return None
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


def apply_series_runtime_delta(
    snapshot: Any,
    *,
    series_key: str,
    seq: int,
    runtime_delta: Any,
) -> Dict[str, Any]:
    projection = canonicalize_projection(snapshot)
    delta = runtime_delta if isinstance(runtime_delta, Mapping) else {}
    target_series_key = normalize_series_key(series_key)
    runtime_payload = delta.get("runtime") if isinstance(delta.get("runtime"), Mapping) else {}
    series_entries = delta.get("series") if isinstance(delta.get("series"), list) else []
    series_delta = None
    for entry in series_entries:
        if not isinstance(entry, Mapping):
            continue
        candidate_key = canonical_series_key_from_entry(entry)
        if candidate_key == target_series_key:
            series_delta = dict(entry)
            break

    existing_series = find_series(projection, target_series_key) or {
        "series_key": target_series_key,
        "instrument_id": str(target_series_key.split("|")[0] if "|" in target_series_key else ""),
        "symbol": "",
        "timeframe": str(target_series_key.split("|")[1] if "|" in target_series_key else ""),
        "candles": [],
        "overlays": [],
        "stats": {},
    }
    next_series = dict(existing_series)
    if isinstance(series_delta, Mapping):
        for field in (
            "strategy_id",
            "instrument_id",
            "symbol",
            "timeframe",
            "datasource",
            "exchange",
            "instrument",
            "bar_index",
            "series_key",
        ):
            if field in series_delta:
                next_series[field] = series_delta.get(field)
        candle = series_delta.get("candle")
        if isinstance(candle, Mapping):
            next_series["candles"] = merge_candle_streams(next_series.get("candles"), [dict(candle)])
        if isinstance(series_delta.get("overlay_delta"), Mapping):
            next_series["overlays"] = apply_overlay_delta(next_series.get("overlays"), series_delta.get("overlay_delta"))
        if isinstance(series_delta.get("trades"), list):
            projection["trades"] = [dict(entry) for entry in series_delta.get("trades") if isinstance(entry, Mapping)]
        if isinstance(series_delta.get("stats"), Mapping):
            next_series["stats"] = dict(series_delta.get("stats") or {})

    if isinstance(delta.get("logs"), list):
        projection["logs"] = list(delta.get("logs") or [])
    if isinstance(delta.get("decisions"), list):
        projection["decisions"] = list(delta.get("decisions") or [])
    if isinstance(runtime_payload, Mapping):
        projection["runtime"] = dict(runtime_payload)
        warnings = runtime_payload.get("warnings")
        if isinstance(warnings, list):
            projection["warnings"] = list(warnings)

    projection["series"] = [
        entry
        for entry in projection.get("series") or []
        if canonical_series_key_from_entry(entry) != target_series_key
    ]
    projection["series"].append(canonicalize_series_entry(next_series) or next_series)
    projection["series"].sort(key=lambda entry: str(entry.get("series_key") or canonical_series_key_from_entry(entry)))
    projection["seq"] = int(seq)
    projection["series_key"] = target_series_key
    return projection


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
    target = normalize_series_key(series_key)
    for entry in projection.get("series") if isinstance(projection.get("series"), list) else []:
        if not isinstance(entry, Mapping):
            continue
        candidate_key = canonical_series_key_from_entry(entry)
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
