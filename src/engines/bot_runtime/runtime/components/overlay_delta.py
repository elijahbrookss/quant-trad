"""Pure helpers for runtime overlay identity, revisioning, and delta transport."""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


_OVERLAY_PAYLOAD_LIST_KEYS = (
    "price_lines",
    "markers",
    "touchPoints",
    "touch_points",
    "boxes",
    "segments",
    "polylines",
    "bubbles",
    "regime_blocks",
)
_OVERLAY_PAYLOAD_FALLBACK_POINT_LIMIT = 160
_OVERLAY_UI_TRANSPORT_KEYS = frozenset(
    {
        "label",
        "color",
        "lineColor",
        "fillColor",
        "style",
        "visible",
        "zIndex",
        "pane",
    }
)


def overlay_points_for_payload(payload: Mapping[str, Any]) -> int:
    points = 0
    for key in (
        "price_lines",
        "markers",
        "touchPoints",
        "touch_points",
        "boxes",
        "segments",
        "polylines",
        "bubbles",
        "regime_blocks",
    ):
        entries = payload.get(key)
        if isinstance(entries, list):
            points += len(entries)
    return points


def overlay_cache_key(overlay: Mapping[str, Any], ordinal: int) -> str:
    explicit_overlay_id = overlay.get("overlay_id")
    if explicit_overlay_id:
        return str(explicit_overlay_id)
    explicit = overlay.get("id")
    if explicit:
        return str(explicit)
    parts = [
        str(overlay.get("type") or "overlay"),
        str(overlay.get("strategy_id") or ""),
        str(overlay.get("symbol") or ""),
        str(overlay.get("timeframe") or ""),
        str(overlay.get("instrument_id") or ""),
        str(overlay.get("source") or ""),
        str(ordinal),
    ]
    return "|".join(parts)


def overlay_payload_fingerprint(overlay: Mapping[str, Any]) -> str:
    fingerprint_payload = {
        key: value
        for key, value in dict(overlay).items()
        if key
        not in {
            "indicator_commit_seq",
            "indicator_commit_seq_status",
            "overlay_commit_seq",
            "base_overlay_commit_seq",
            "overlay_commit_seq_status",
        }
    }
    try:
        return json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return str(fingerprint_payload)


def compact_overlay_payload(
    value: Any,
    *,
    max_items: int = _OVERLAY_PAYLOAD_FALLBACK_POINT_LIMIT,
    path: Tuple[str, ...] = (),
) -> Any:
    resolved_limit = max(int(max_items or _OVERLAY_PAYLOAD_FALLBACK_POINT_LIMIT), 1)
    if isinstance(value, Mapping):
        return {
            str(key): compact_overlay_payload(
                entry,
                max_items=resolved_limit,
                path=(*path, str(key)),
            )
            for key, entry in value.items()
        }
    if isinstance(value, list):
        polyline_history_limit = max(resolved_limit, resolved_limit * 4)
        preserve_polyline_history = bool(
            path
            and path[-1] == "points"
            and "polylines" in path
            and len(value) <= polyline_history_limit
        )
        subset = (
            value
            if preserve_polyline_history or len(value) <= resolved_limit
            else value[-resolved_limit:]
        )
        return [
            compact_overlay_payload(entry, max_items=resolved_limit, path=path)
            for entry in subset
        ]
    return value


def overlay_payload_summary(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    counts: Dict[str, int] = {}
    geometry_keys: list[str] = []
    point_count = 0
    for key in _OVERLAY_PAYLOAD_LIST_KEYS:
        entries = payload.get(key)
        if not isinstance(entries, list) or not entries:
            continue
        counts[key] = len(entries)
        geometry_keys.append(key)
        if key == "polylines":
            point_count += sum(
                len(entry.get("points") or [])
                for entry in entries
                if isinstance(entry, Mapping)
            )
    summary: Dict[str, Any] = {}
    if geometry_keys:
        summary["geometry_keys"] = geometry_keys
    if counts:
        summary["payload_counts"] = counts
    if point_count > 0:
        summary["point_count"] = int(point_count)
    return summary


def compact_overlay_for_transport(
    overlay: Mapping[str, Any],
    *,
    key: Optional[str] = None,
    max_payload_items: int = _OVERLAY_PAYLOAD_FALLBACK_POINT_LIMIT,
) -> Dict[str, Any]:
    mapping = dict(overlay)
    overlay_id = str(mapping.get("overlay_id") or mapping.get("id") or key or "").strip()
    payload_value = mapping.get("payload")
    compacted_payload = (
        compact_overlay_payload(payload_value, max_items=max_payload_items)
        if isinstance(payload_value, Mapping)
        else None
    )
    pane_views = [
        str(entry).strip()
        for entry in (mapping.get("pane_views") if isinstance(mapping.get("pane_views"), list) else [])
        if str(entry).strip()
    ]
    ui = {}
    if isinstance(mapping.get("ui"), Mapping):
        ui = {
            str(entry_key): entry_value
            for entry_key, entry_value in dict(mapping["ui"]).items()
            if str(entry_key) in _OVERLAY_UI_TRANSPORT_KEYS and entry_value not in (None, "", [], {}, ())
        }
    compacted: Dict[str, Any] = {
        "overlay_id": overlay_id or None,
        "type": mapping.get("type"),
        "strategy_id": mapping.get("strategy_id"),
        "source": mapping.get("source"),
        "pane_key": mapping.get("pane_key"),
        "pane_views": pane_views or None,
        "color": mapping.get("color"),
        "ind_id": mapping.get("ind_id"),
        "ui": ui or None,
        "detail_level": "bounded_render",
        "payload": compacted_payload,
        "payload_summary": overlay_payload_summary(compacted_payload),
    }
    for seq_key in (
        "indicator_commit_seq",
        "indicator_commit_seq_status",
        "overlay_commit_seq",
        "base_overlay_commit_seq",
        "overlay_commit_seq_status",
    ):
        value = mapping.get(seq_key)
        if value not in (None, "", [], {}, ()):
            compacted[seq_key] = value
    return {
        entry_key: entry_value
        for entry_key, entry_value in compacted.items()
        if entry_value not in (None, "", [], {}, ())
    }


def build_overlay_delta(
    cache: Dict[str, Any],
    overlays: Sequence[Mapping[str, Any]],
    *,
    max_payload_items: int = _OVERLAY_PAYLOAD_FALLBACK_POINT_LIMIT,
) -> Optional[Dict[str, Any]]:
    previous_entries = cache.get("overlay_entries")
    previous_fingerprints = cache.get("overlay_fingerprints")
    previous_order = cache.get("overlay_order")
    previous_seq = int(cache.get("overlay_commit_seq") or 0)
    if not isinstance(previous_entries, dict) or not isinstance(previous_fingerprints, dict) or not isinstance(previous_order, list):
        previous_entries = {}
        previous_fingerprints = {}
        previous_order = []

    next_entries: Dict[str, Dict[str, Any]] = {}
    next_fingerprints: Dict[str, str] = {}
    next_order: list[str] = []
    for idx, overlay in enumerate(overlays):
        if not isinstance(overlay, Mapping):
            continue
        key = overlay_cache_key(overlay, idx)
        compacted_overlay = compact_overlay_for_transport(
            overlay,
            key=key,
            max_payload_items=max_payload_items,
        )
        next_entries[key] = compacted_overlay
        next_fingerprints[key] = overlay_payload_fingerprint(compacted_overlay)
        next_order.append(key)

    if (
        len(previous_entries) == len(next_entries)
        and set(previous_entries.keys()) == set(next_entries.keys())
        and all(previous_fingerprints.get(key) == next_fingerprints.get(key) for key in next_entries.keys())
    ):
        return None

    next_seq = previous_seq + 1
    ops: list[Dict[str, Any]] = []
    removed_keys = [key for key in previous_order if key not in next_entries]
    for key in removed_keys:
        ops.append({"op": "remove", "key": key})
    for key in next_order:
        if previous_fingerprints.get(key) != next_fingerprints.get(key):
            ops.append({"op": "upsert", "key": key, "overlay": next_entries[key]})

    cache["overlay_entries"] = next_entries
    cache["overlay_fingerprints"] = next_fingerprints
    cache["overlay_order"] = next_order
    cache["overlay_commit_seq"] = next_seq
    return {
        "overlay_commit_seq": next_seq,
        "base_overlay_commit_seq": previous_seq,
        "overlay_commit_seq_status": "overlay_scoped",
        "ops": ops,
    }


def overlay_delta_op_counts(delta: Mapping[str, Any]) -> Dict[str, int]:
    ops = delta.get("ops")
    if not isinstance(ops, list):
        return {}
    counts: Dict[str, int] = {}
    for op in ops:
        if not isinstance(op, Mapping):
            continue
        key = str(op.get("op") or "unknown").lower()
        counts[key] = counts.get(key, 0) + 1
    return counts


def count_overlay_points(overlays: Sequence[Mapping[str, Any]]) -> int:
    points = 0
    for overlay in overlays or []:
        if not isinstance(overlay, Mapping):
            continue
        payload = overlay.get("payload")
        if isinstance(payload, Mapping):
            points += overlay_points_for_payload(payload)
    return points


def overlay_change_metrics(
    before: Sequence[Mapping[str, Any]],
    after: Sequence[Mapping[str, Any]],
) -> Tuple[float, float]:
    changed = 0
    before_len = len(before or [])
    after_len = len(after or [])
    min_len = min(before_len, after_len)
    for idx in range(min_len):
        prev = before[idx] if isinstance(before[idx], Mapping) else {}
        curr = after[idx] if isinstance(after[idx], Mapping) else {}
        prev_type = str(prev.get("type") or "")
        curr_type = str(curr.get("type") or "")
        prev_points = overlay_points_for_payload(prev.get("payload")) if isinstance(prev.get("payload"), Mapping) else 0
        curr_points = overlay_points_for_payload(curr.get("payload")) if isinstance(curr.get("payload"), Mapping) else 0
        if prev_type != curr_type or prev_points != curr_points:
            changed += 1
    changed += abs(before_len - after_len)
    points_changed = abs(count_overlay_points(after or []) - count_overlay_points(before or []))
    return float(changed), float(points_changed)


def overlay_payload_metrics(payload: Mapping[str, Any]) -> Tuple[int, int]:
    overlay_count = 0
    overlay_points = 0

    def consume(overlays: Any) -> None:
        nonlocal overlay_count, overlay_points
        if not isinstance(overlays, list):
            return
        for overlay in overlays:
            if not isinstance(overlay, Mapping):
                continue
            overlay_count += 1
            overlay_payload = overlay.get("payload")
            if isinstance(overlay_payload, Mapping):
                overlay_points += overlay_points_for_payload(overlay_payload)

    consume(payload.get("overlays"))
    series_list = payload.get("series")
    if isinstance(series_list, list):
        for series_entry in series_list:
            if not isinstance(series_entry, Mapping):
                continue
            consume(series_entry.get("overlays"))
    return overlay_count, overlay_points


def entry_fingerprint(entries: Sequence[Mapping[str, Any]]) -> Tuple[int, Optional[str], Optional[str]]:
    if not entries:
        return (0, None, None)
    last = entries[-1]
    marker: Optional[str] = None
    kind: Optional[str] = None
    if isinstance(last, Mapping):
        kind_value = last.get("type")
        kind = str(kind_value) if kind_value is not None else None
        for key in ("id", "event_id", "trade_id", "time", "created_at", "timestamp", "message"):
            value = last.get(key)
            if value is not None:
                marker = str(value)
                break
    return (len(entries), kind, marker)
