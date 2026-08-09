"""Pure helpers for runtime overlay identity, revisioning, and delta transport."""

from __future__ import annotations

import hashlib
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
_OVERLAY_TRANSPORT_STATIC_EXCLUDE = frozenset(
    {
        "payload",
        "payload_summary",
        "indicator_commit_seq",
        "indicator_commit_seq_status",
        "overlay_commit_seq",
        "base_overlay_commit_seq",
        "overlay_commit_seq_status",
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
    polylines = payload.get("polylines")
    if isinstance(polylines, list) and polylines:
        summary["polyline_fingerprint"] = _json_fingerprint(polylines)
    return summary


def _json_size(value: Any) -> int:
    try:
        return len(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8"))
    except (TypeError, ValueError):
        return len(str(value).encode("utf-8"))


def _json_fingerprint(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _polyline_tail_patch(
    previous: Any,
    current: Any,
) -> Optional[Dict[str, Any]]:
    if not isinstance(previous, list) or not isinstance(current, list):
        return None
    if len(previous) != len(current):
        return None
    entries: list[Dict[str, Any]] = []
    for index, (previous_line, current_line) in enumerate(zip(previous, current)):
        if not isinstance(previous_line, Mapping) or not isinstance(current_line, Mapping):
            return None
        previous_static = {str(key): value for key, value in previous_line.items() if str(key) != "points"}
        current_static = {str(key): value for key, value in current_line.items() if str(key) != "points"}
        if previous_static != current_static:
            return None
        previous_points = previous_line.get("points")
        current_points = current_line.get("points")
        if not isinstance(previous_points, list) or not isinstance(current_points, list):
            return None
        if previous_points == current_points:
            continue
        drop_prefix = None
        append: list[Any] = []
        for candidate in range(len(previous_points) + 1):
            retained = previous_points[candidate:]
            if len(retained) > len(current_points):
                continue
            if retained == current_points[: len(retained)]:
                drop_prefix = candidate
                append = list(current_points[len(retained) :])
                break
        if drop_prefix is None:
            return None
        entries.append(
            {
                "index": index,
                "expected_count": len(previous_points),
                "drop_prefix": drop_prefix,
                "append": append,
            }
        )
    if not entries:
        return None
    return {
        "expected_fingerprint": _json_fingerprint(previous),
        "result_fingerprint": _json_fingerprint(current),
        "entries": entries,
    }


def _overlay_static_payload(overlay: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        str(key): value
        for key, value in dict(overlay or {}).items()
        if str(key) not in _OVERLAY_TRANSPORT_STATIC_EXCLUDE
    }


def _overlay_payload_patch(
    previous_payload: Any,
    next_payload: Any,
) -> Optional[Dict[str, Any]]:
    if not isinstance(previous_payload, Mapping) or not isinstance(next_payload, Mapping):
        return None
    replace: Dict[str, Any] = {}
    remove: list[str] = []
    polyline_tail: Optional[Dict[str, Any]] = None
    previous_keys = {str(key) for key in previous_payload.keys()}
    next_keys = {str(key) for key in next_payload.keys()}
    for key in sorted(previous_keys - next_keys):
        remove.append(key)
    for raw_key, value in next_payload.items():
        key = str(raw_key)
        previous_value = previous_payload.get(raw_key)
        if previous_value == value or previous_payload.get(key) == value:
            continue
        if key == "polylines":
            polyline_tail = _polyline_tail_patch(previous_value, value)
            if polyline_tail is not None:
                continue
        replace[key] = value
    if not replace and not remove and polyline_tail is None:
        return None
    patch: Dict[str, Any] = {}
    if replace:
        patch["replace"] = replace
    if remove:
        patch["remove"] = remove
    if polyline_tail is not None:
        patch["polyline_tail"] = polyline_tail
    summary = overlay_payload_summary(next_payload)
    if summary:
        patch["payload_summary"] = summary
    return patch


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
    source_payload_summary = overlay_payload_summary(payload_value)
    compacted_payload_summary = overlay_payload_summary(compacted_payload)
    payload_truncated = bool(
        isinstance(payload_value, Mapping)
        and compacted_payload != payload_value
    )
    if compacted_payload_summary and payload_truncated:
        compacted_payload_summary["truncated"] = True
        compacted_payload_summary["source_payload_counts"] = dict(
            source_payload_summary.get("payload_counts") or {}
        )
        compacted_payload_summary["source_point_count"] = int(
            source_payload_summary.get("point_count") or 0
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
        "payload_summary": compacted_payload_summary,
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
    force: bool = False,
    force_full: bool = False,
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

    unchanged = (
        len(previous_entries) == len(next_entries)
        and set(previous_entries.keys()) == set(next_entries.keys())
        and all(previous_fingerprints.get(key) == next_fingerprints.get(key) for key in next_entries.keys())
    )
    if unchanged and not force and not force_full:
        return None

    next_seq = previous_seq + 1
    ops: list[Dict[str, Any]] = []
    if force_full:
        for key in next_order:
            ops.append({"op": "upsert", "key": key, "overlay": next_entries[key]})
    else:
        removed_keys = [key for key in previous_order if key not in next_entries]
        for key in removed_keys:
            ops.append({"op": "remove", "key": key})
        for key in next_order:
            if previous_fingerprints.get(key) == next_fingerprints.get(key):
                continue
            previous_overlay = previous_entries.get(key)
            next_overlay = next_entries[key]
            patch = None
            if (
                isinstance(previous_overlay, Mapping)
                and _overlay_static_payload(previous_overlay) == _overlay_static_payload(next_overlay)
            ):
                patch = _overlay_payload_patch(previous_overlay.get("payload"), next_overlay.get("payload"))
            if isinstance(patch, Mapping):
                patch_op = {"op": "patch", "key": key, "payload_patch": dict(patch)}
                if _json_size(patch_op) < _json_size({"op": "upsert", "key": key, "overlay": next_overlay}):
                    ops.append(patch_op)
                    continue
            ops.append({"op": "upsert", "key": key, "overlay": next_overlay})

    cache["overlay_entries"] = next_entries
    cache["overlay_fingerprints"] = next_fingerprints
    cache["overlay_order"] = next_order
    cache["overlay_commit_seq"] = next_seq
    delta = {
        "overlay_commit_seq": next_seq,
        "base_overlay_commit_seq": previous_seq,
        "overlay_commit_seq_status": "overlay_scoped",
        "ops": ops,
    }
    if force_full:
        delta["checkpoint_kind"] = "full_state"
    return delta


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
