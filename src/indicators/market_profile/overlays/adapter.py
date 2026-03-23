from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

from indicators.market_profile.manifest import DEFAULT_MERGE_THRESHOLD, DEFAULT_MIN_MERGE_SESSIONS
from overlays.transformers import overlay_transformer


log = logging.getLogger("MarketProfileOverlays")
_WARNED_TRANSFORMERS: set[str] = set()

_BREAKOUT_COLORS = {
    "above": "#16a34a",  # green
    "below": "#dc2626",  # red
}

_RETEST_COLORS = {
    "support": "#0ea5e9",  # sky blue
    "resistance": "#f97316",  # amber
}


def _signal_metadata(signal: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = signal.get("metadata")
    return metadata if isinstance(metadata, Mapping) else {}


def _signal_kind(signal: Mapping[str, Any]) -> str:
    return str(signal.get("type") or signal.get("event_key") or signal.get("key") or "").strip()


def _signal_time(signal: Mapping[str, Any]) -> Any:
    metadata = _signal_metadata(signal)
    return (
        signal.get("event_time")
        or signal.get("time")
        or metadata.get("signal_time")
        or metadata.get("time")
        or metadata.get("trigger_time")
    )


def _signal_symbol(signal: Mapping[str, Any]) -> str:
    text = str(signal.get("symbol") or _signal_metadata(signal).get("symbol") or "").strip()
    return text


def finite_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def format_duration(seconds: float) -> str:
    if seconds >= 1:
        return f"{seconds:.2f}s"
    return f"{seconds * 1000:.1f}ms"


def to_epoch_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return int(ts.timestamp())


def bias_label_from_direction(direction: Optional[str], fallback: Optional[str] = None) -> Optional[str]:
    hint = str(direction or fallback or "").strip().lower()
    if hint in {"above", "up", "long", "buy", "support"}:
        return "Long"
    if hint in {"below", "down", "short", "sell", "resistance"}:
        return "Short"
    return None


def rgba_from_hex(color: str, alpha: float) -> Optional[str]:
    value = str(color or "").strip().lstrip("#")
    if len(value) != 6:
        return None
    try:
        red = int(value[0:2], 16)
        green = int(value[2:4], 16)
        blue = int(value[4:6], 16)
    except ValueError:
        return None
    opacity = min(max(float(alpha), 0.0), 1.0)
    return f"rgba({red},{green},{blue},{opacity:.2f})"


def _variant_compact(variant: str) -> str:
    normalized = str(variant or "").strip().lower()
    mapping = {
        "breakout_up": "BO-UP",
        "breakout_down": "BO-DN",
        "breakin_from_above": "BI-ABV",
        "breakin_from_below": "BI-BLW",
    }
    return mapping.get(normalized, normalized.upper() if normalized else "")


def _short_indicator_id(value: Any, *, size: int = 4) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text[: max(1, int(size))]


def _direction_compact(
    *,
    direction: Any = None,
    breakout_direction: Any = None,
    pointer_direction: Any = None,
    bias: Any = None,
) -> str:
    direct = str(direction or "").strip().lower()
    if direct in {"long", "buy", "bullish"}:
        return "L"
    if direct in {"short", "sell", "bearish"}:
        return "S"
    breakout = str(breakout_direction or "").strip().lower()
    if breakout in {"above", "up", "bullish"}:
        return "L"
    if breakout in {"below", "down", "bearish"}:
        return "S"
    pointer = str(pointer_direction or "").strip().lower()
    if pointer in {"up", "above"}:
        return "L"
    if pointer in {"down", "below"}:
        return "S"
    bias_text = str(bias or "").strip().lower()
    if bias_text == "bullish":
        return "L"
    if bias_text == "bearish":
        return "S"
    return ""


def _normalize_marker_time(
    ts: Any,
    plot_df: Optional[pd.DataFrame],
    idx: Optional[int],
    marker_kind: str,
    require_exact_plot_match: bool = True,
) -> Optional[int]:
    """Normalize marker timestamps to epoch seconds, preferring plot_df index when available."""

    epoch_from_meta = to_epoch_seconds(ts)
    epoch_from_index: Optional[int] = None

    if plot_df is not None and idx is not None:
        try:
            index_value = plot_df.index[idx]
            epoch_from_index = to_epoch_seconds(index_value)
        except Exception as exc:
            log.warning(
                "Failed to read %s marker time from plot_df index | idx=%s | ts=%s | error=%s",
                marker_kind,
                idx,
                ts,
                exc,
            )

    if epoch_from_meta is not None and epoch_from_index is not None and epoch_from_meta != epoch_from_index:
        log.warning(
            "Marker time mismatch | kind=%s | idx=%s | ts=%s | meta_epoch=%s | index_epoch=%s",
            marker_kind,
            idx,
            ts,
            epoch_from_meta,
            epoch_from_index,
        )

    normalized = epoch_from_index if epoch_from_index is not None else epoch_from_meta
    if (
        require_exact_plot_match
        and normalized is not None
        and plot_df is not None
        and len(plot_df.index) > 0
    ):
        try:
            index_epochs = {
                value
                for value in (to_epoch_seconds(item) for item in plot_df.index)
                if value is not None
            }
        except Exception as exc:
            log.warning(
                "Failed to build index epoch set for exact match | kind=%s | error=%s",
                marker_kind,
                exc,
            )
            index_epochs = set()
        if normalized not in index_epochs:
            log.warning(
                "Skipping %s marker due to missing exact close-time match | signal_epoch=%s",
                marker_kind,
                normalized,
            )
            return None
    if normalized is None:
        log.warning(
            "Skipping %s marker due to invalid timestamp | ts=%s | idx=%s",
            marker_kind,
            ts,
            idx,
        )
    return normalized


def _resolve_level_price(metadata: Mapping[str, Any]) -> Optional[float]:
    price = finite_float(metadata.get("level_price"))
    if price is not None:
        return price
    price = finite_float(metadata.get("boundary_price"))
    if price is not None:
        return price

    level_type = str(metadata.get("level_type") or metadata.get("boundary_type") or "").upper()
    if level_type == "VAH":
        return finite_float(metadata.get("VAH"))
    if level_type == "VAL":
        return finite_float(metadata.get("VAL"))

    for key in ("VAH", "VAL"):
        price = finite_float(metadata.get(key))
        if price is not None:
            return price

    return None


def _level_label(metadata: Mapping[str, Any]) -> str:
    level_type = str(metadata.get("level_type") or metadata.get("boundary_type") or "").strip().upper()
    if level_type in {"VAH", "VAL"}:
        return level_type
    if level_type:
        return level_type.title()
    return "Value Area"


def _resolve_breakout_direction(metadata: Mapping[str, Any]) -> str:
    direction = str(metadata.get("breakout_direction") or "").strip().lower()
    if direction in {"above", "below"}:
        return direction
    variant = str(metadata.get("variant") or metadata.get("breakout_variant") or "").strip().lower()
    if variant in {"breakout_up", "breakin_from_below"}:
        return "above"
    if variant in {"breakout_down", "breakin_from_above"}:
        return "below"
    fallback = str(metadata.get("direction") or "").strip().lower()
    if fallback == "long":
        return "above"
    if fallback == "short":
        return "below"
    return ""


def _confidence_meta(metadata: Mapping[str, Any]) -> Optional[str]:
    confidence = finite_float(metadata.get("confidence"))
    if confidence is None:
        return None

    percent = max(0, min(100, round(confidence * 100)))
    return f"Confidence {percent}%"


def _va_span_meta(metadata: Mapping[str, Any]) -> Optional[str]:
    vah = finite_float(metadata.get("VAH"))
    val = finite_float(metadata.get("VAL"))
    if vah is None or val is None:
        return None
    return f"VAH {vah:.2f} / VAL {val:.2f}"


def _signal_diagnostics(metadata: Mapping[str, Any], *, signal_time: Any) -> List[str]:
    lines: List[str] = []
    rule_id = metadata.get("rule_id")
    level_type = metadata.get("level_type") or metadata.get("boundary_type")
    level_price = finite_float(metadata.get("level_price"))
    if level_price is None:
        level_price = finite_float(metadata.get("boundary_price"))
    trigger_close = finite_float(metadata.get("trigger_close"))
    direction = metadata.get("breakout_direction") or metadata.get("direction")
    value_area_id = metadata.get("value_area_id")
    known_at = metadata.get("known_at")
    formed_at = metadata.get("formed_at")
    bar_index = metadata.get("bar_index")
    streak_count = metadata.get("streak_count")
    run_length = metadata.get("run_length")
    confirm_bars = metadata.get("confirm_bars")
    variant = metadata.get("variant") or metadata.get("breakout_variant")
    trace_id = metadata.get("trace_id")
    event_signature = metadata.get("event_signature")
    started_bar_index = metadata.get("started_bar_index")
    confirm_streak = metadata.get("confirm_streak_at_emit")
    if rule_id:
        lines.append(f"rule={rule_id}")
    if trace_id:
        lines.append(f"trace_id={trace_id}")
    elif event_signature:
        lines.append(f"trace={event_signature}")
    if level_type or level_price is not None:
        lines.append(f"level={level_type or 'NA'}@{level_price:.2f}" if level_price is not None else f"level={level_type}")
    if variant:
        lines.append(f"variant={variant}")
    if trigger_close is not None:
        lines.append(f"trigger_close={trigger_close:.2f}")
    if direction:
        lines.append(f"direction={direction}")
    if bar_index is not None:
        lines.append(f"bar_index={bar_index}")
    if streak_count is not None:
        lines.append(f"streak={streak_count}")
    if run_length is not None:
        lines.append(f"run_len={run_length}")
    if confirm_bars is not None:
        lines.append(f"confirm_bars={confirm_bars}")
    if confirm_streak is not None:
        lines.append(f"confirm_streak_at_emit={confirm_streak}")
    if started_bar_index is not None:
        lines.append(f"started_bar_index={started_bar_index}")
    if value_area_id:
        lines.append(f"profile={value_area_id}")
    if known_at is not None:
        lines.append(f"known_at={known_at}")
    if formed_at is not None:
        lines.append(f"formed_at={formed_at}")
    if signal_time is not None:
        lines.append(f"signal_time={signal_time}")
    return lines


def _apply_collision_avoidance(
    bubbles: List[Dict[str, Any]],
    time_threshold_seconds: int = 3600,  # 1 hour
    price_threshold_percent: float = 0.5,  # 0.5% price difference
) -> None:
    """
    Adjust bubble positions to prevent overlapping signals.

    Modifies bubbles in-place to offset prices when signals are too close together.
    Uses a simple vertical stacking approach for overlapping bubbles.

    Args:
        bubbles: List of bubble dictionaries to adjust
        time_threshold_seconds: Max time difference to consider bubbles overlapping (in seconds)
        price_threshold_percent: Max price difference % to consider bubbles overlapping
    """
    if not bubbles:
        return

    # Sort bubbles by time, then price
    bubbles.sort(key=lambda b: (b.get("time", 0), b.get("price", 0)))

    # Track occupied positions (time, price_range) and their offset levels
    occupied_positions: List[Dict[str, Any]] = []

    for bubble in bubbles:
        bubble_time = bubble.get("time")
        bubble_price = bubble.get("price")
        lock_price = bool(bubble.get("lock_price"))

        if bubble_time is None or bubble_price is None:
            continue

        # Find overlapping bubbles
        offset_level = 0
        price_range = bubble_price * (price_threshold_percent / 100.0)

        for pos in occupied_positions:
            # Check if this bubble overlaps with an existing position
            time_overlap = abs(bubble_time - pos["time"]) <= time_threshold_seconds
            price_overlap = abs(bubble_price - pos["price"]) <= (pos["price_range"] + price_range)

            if time_overlap and price_overlap:
                # Found overlap - need to offset this bubble
                offset_level = max(offset_level, pos["offset_level"] + 1)

        # Apply offset if needed (shift price by small percentage per level).
        # Some bubbles intentionally represent exact levels (e.g. VAH/VAL labels);
        # preserve their y-anchor and skip vertical collision shifting.
        if offset_level > 0 and not lock_price:
            # Determine offset direction based on bubble direction or default to upward
            direction = bubble.get("direction", "above")
            if direction in ("below", "down"):
                # Shift downward for bubbles pointing down
                price_offset_multiplier = -1
            else:
                # Shift upward for bubbles pointing up
                price_offset_multiplier = 1

            # Apply 0.3% offset per level
            offset_percent = 0.3 * offset_level * price_offset_multiplier
            original_price = bubble_price
            bubble["price"] = bubble_price * (1 + offset_percent / 100.0)

            log.debug(
                "Applied collision avoidance | time=%s | original_price=%.2f | new_price=%.2f | offset_level=%d | direction=%s",
                bubble_time,
                original_price,
                bubble["price"],
                offset_level,
                direction,
            )

        # Record this bubble's position
        occupied_positions.append({
            "time": bubble_time,
            "price": bubble["price"],  # Use adjusted price
            "price_range": price_range,
            "offset_level": offset_level,
        })


__all__ = ["market_profile_overlay_transformer"]


@overlay_transformer(["market-profile", "market_profile", "mpf"])
def market_profile_overlay_transformer(
    overlay: Mapping[str, Any], current_epoch: int
) -> Optional[Mapping[str, Any]]:
    payload = overlay.get("payload")
    if not isinstance(payload, Mapping):
        return overlay
    profiles = payload.get("profiles")
    if not isinstance(profiles, list):
        # Runtime overlay projection can emit prebuilt boxes directly (without
        # profile collections). In that case, preserve payload as-is.
        has_prebuilt_payload = any(
            isinstance(payload.get(key), list) and len(payload.get(key) or []) > 0
            for key in ("boxes", "markers", "bubbles", "segments", "polylines", "touch_points")
        )
        if has_prebuilt_payload:
            return overlay
        if "market_profile_profiles_missing" not in _WARNED_TRANSFORMERS:
            log.error("market_profile_profiles_missing_for_walk_forward")
            _WARNED_TRANSFORMERS.add("market_profile_profiles_missing")
        trimmed = dict(overlay)
        trimmed["payload"] = dict(payload)
        trimmed["payload"]["boxes"] = []
        return trimmed

    raw_params = payload.get("profile_params") or {}
    params: Dict[str, Any] = (
        dict(raw_params) if isinstance(raw_params, Mapping) else {}
    )
    if bool(params.get("use_merged_value_areas")):
        if params.get("merge_threshold") is None:
            params["merge_threshold"] = float(DEFAULT_MERGE_THRESHOLD)
            log.warning(
                "event=market_profile_merge_param_defaulted param=merge_threshold default=%s",
                float(DEFAULT_MERGE_THRESHOLD),
            )
        if params.get("min_merge_sessions") is None:
            params["min_merge_sessions"] = int(DEFAULT_MIN_MERGE_SESSIONS)
            log.warning(
                "event=market_profile_merge_param_defaulted param=min_merge_sessions default=%s",
                int(DEFAULT_MIN_MERGE_SESSIONS),
            )
    extend_to_end = bool(params.get("extend_value_area_to_chart_end"))
    window_start_epoch = to_epoch_seconds(params.get("start"))
    window_end_epoch = to_epoch_seconds(params.get("end"))
    effective_window_end = min(
        int(current_epoch),
        int(window_end_epoch) if window_end_epoch is not None else int(current_epoch),
    )

    # Extract metadata for logging context
    bot_id = overlay.get("bot_id") or payload.get("bot_id")
    symbol = overlay.get("symbol") or payload.get("symbol")
    strategy_id = overlay.get("strategy_id") or payload.get("strategy_id")

    try:
        from indicators.market_profile.compute.internal.runtime_profiles import (
            profile_identity,
            resolve_effective_profiles,
        )
    except Exception:
        if "market_profile_runtime_profiles_import_failed" not in _WARNED_TRANSFORMERS:
            log.error("market_profile_runtime_profiles_import_failed")
            _WARNED_TRANSFORMERS.add("market_profile_runtime_profiles_import_failed")
        trimmed = dict(overlay)
        payload_copy = dict(payload)
        payload_copy["boxes"] = []
        payload_copy["transform_summary"] = {
            "known_profiles": 0,
            "merged_profiles": 0,
        }
        trimmed["payload"] = payload_copy
        return trimmed

    merged_profiles, transform_summary = resolve_effective_profiles(
        profiles_payload=profiles,
        profile_params=params if isinstance(params, Mapping) else {},
        current_epoch=int(current_epoch),
        bot_id=str(bot_id) if bot_id is not None else None,
        symbol=str(symbol) if symbol is not None else None,
        strategy_id=str(strategy_id) if strategy_id is not None else None,
    )

    if not merged_profiles and bool(params.get("use_merged_value_areas")):
        known_profiles = int(transform_summary.get("known_profiles", 0) or 0)
        if known_profiles > 0:
            missing_merge_param = (
                not isinstance(raw_params, Mapping)
                or raw_params.get("merge_threshold") is None
                or raw_params.get("min_merge_sessions") is None
            )
            if missing_merge_param and "market_profile_merge_params_missing" not in _WARNED_TRANSFORMERS:
                log.error("market_profile_merge_params_missing")
                _WARNED_TRANSFORMERS.add("market_profile_merge_params_missing")
            elif "market_profile_merge_resolved_empty" not in _WARNED_TRANSFORMERS:
                log.warning(
                    "event=market_profile_merge_resolved_empty known_profiles=%s merge_threshold=%s min_merge_sessions=%s",
                    known_profiles,
                    params.get("merge_threshold"),
                    params.get("min_merge_sessions"),
                )
                _WARNED_TRANSFORMERS.add("market_profile_merge_resolved_empty")

    boxes: List[Dict[str, Any]] = []
    debug_profiles: List[Dict[str, Any]] = []
    clipped_by_window = 0
    dropped_outside_window = 0
    for profile in merged_profiles:
        start_epoch = int(profile.start.timestamp())
        end_epoch = int(profile.end.timestamp())
        if end_epoch > current_epoch:
            continue
        box_start = start_epoch
        box_end = int(current_epoch) if extend_to_end else end_epoch

        if window_start_epoch is not None and box_end < int(window_start_epoch):
            dropped_outside_window += 1
            continue
        if window_end_epoch is not None and box_start > int(window_end_epoch):
            dropped_outside_window += 1
            continue

        if window_start_epoch is not None and box_start < int(window_start_epoch):
            box_start = int(window_start_epoch)
            clipped_by_window += 1
        if window_end_epoch is not None and box_end > int(window_end_epoch):
            box_end = int(window_end_epoch)
            clipped_by_window += 1
        if box_end > effective_window_end:
            box_end = int(effective_window_end)
        if box_start > box_end:
            dropped_outside_window += 1
            continue

        profile_key = profile_identity(profile)
        boxes.append(
            {
                "x1": box_start,
                "x2": box_end,
                "y1": float(profile.val),
                "y2": float(profile.vah),
                "profile_key": profile_key,
                "fillColor": "rgba(59, 130, 246, 0.1)",
                "borderColor": "#3b82f6",
                "borderWidth": 1,
                "borderStyle": 2,
            }
        )
        if len(debug_profiles) < 5:
            debug_profiles.append(
                {
                    "profile_key": profile_key,
                    "vah": float(profile.vah),
                    "val": float(profile.val),
                    "start": start_epoch,
                    "end": end_epoch,
                    "clipped_start": box_start,
                    "box_end": box_end,
                }
            )

    if window_start_epoch is not None or window_end_epoch is not None:
        log.debug(
            "event=market_profile_overlay_window_clip symbol=%s window_start=%s window_end=%s clipped=%s dropped=%s boxes=%s",
            symbol,
            window_start_epoch,
            window_end_epoch,
            clipped_by_window,
            dropped_outside_window,
            len(boxes),
        )

    trimmed = dict(overlay)
    payload_copy = dict(payload)
    payload_copy["boxes"] = boxes
    payload_copy["transform_summary"] = transform_summary
    window_min = int(window_start_epoch) if window_start_epoch is not None else None
    window_max = int(effective_window_end)

    def _entry_in_window(epoch: Optional[int]) -> bool:
        if epoch is None:
            return True
        if window_min is not None and epoch < window_min:
            return False
        if epoch > window_max:
            return False
        return True

    markers = payload_copy.get("markers")
    if isinstance(markers, list):
        filtered_markers: List[Dict[str, Any]] = []
        dropped_markers = 0
        for marker in markers:
            if not isinstance(marker, Mapping):
                continue
            marker_time = to_epoch_seconds(marker.get("time"))
            if not _entry_in_window(marker_time):
                dropped_markers += 1
                continue
            filtered_markers.append(dict(marker))
        payload_copy["markers"] = filtered_markers
        if dropped_markers > 0:
            log.debug(
                "event=market_profile_overlay_marker_window_filter symbol=%s dropped=%s kept=%s window_start=%s window_end=%s",
                symbol,
                dropped_markers,
                len(filtered_markers),
                window_min,
                window_max,
            )

    bubbles = payload_copy.get("bubbles")
    if isinstance(bubbles, list):
        filtered_bubbles: List[Dict[str, Any]] = []
        dropped_bubbles = 0
        dropped_nested_markers = 0
        for bubble in bubbles:
            if not isinstance(bubble, Mapping):
                continue
            bubble_time = to_epoch_seconds(bubble.get("time"))
            if not _entry_in_window(bubble_time):
                dropped_bubbles += 1
                continue
            bubble_copy = dict(bubble)
            nested_markers = bubble_copy.get("_markers")
            if isinstance(nested_markers, list):
                kept_nested: List[Dict[str, Any]] = []
                for nested in nested_markers:
                    if not isinstance(nested, Mapping):
                        continue
                    nested_time = to_epoch_seconds(nested.get("time"))
                    if not _entry_in_window(nested_time):
                        dropped_nested_markers += 1
                        continue
                    kept_nested.append(dict(nested))
                bubble_copy["_markers"] = kept_nested
            filtered_bubbles.append(bubble_copy)
        payload_copy["bubbles"] = filtered_bubbles
        if dropped_bubbles > 0 or dropped_nested_markers > 0:
            log.debug(
                "event=market_profile_overlay_bubble_window_filter symbol=%s dropped_bubbles=%s dropped_nested_markers=%s kept=%s window_start=%s window_end=%s",
                symbol,
                dropped_bubbles,
                dropped_nested_markers,
                len(filtered_bubbles),
                window_min,
                window_max,
            )
        bubbles = filtered_bubbles

    if isinstance(bubbles, list):
        if not boxes and bubbles:
            sample_bubbles = []
            for bubble in bubbles[:5]:
                if not isinstance(bubble, Mapping):
                    continue
                sample_bubbles.append(
                    {
                        "time": bubble.get("time"),
                        "profile_key": bubble.get("profile_key"),
                        "boundary_type": bubble.get("boundary_type"),
                        "boundary_price": bubble.get("boundary_price"),
                        "label": bubble.get("label"),
                        "meta": bubble.get("meta"),
                    }
                )
            log.info(
                "event=market_profile_overlay_bubbles_without_boxes symbol=%s bubbles=%s boxes=%s window_start=%s window_end=%s sample=%s",
                symbol,
                len(bubbles),
                len(boxes),
                window_min,
                window_max,
                sample_bubbles,
            )
    if isinstance(bubbles, list) and boxes:
        box_by_profile: Dict[str, Dict[str, Any]] = {}
        box_by_boundary: Dict[tuple[str, float], List[Dict[str, Any]]] = {}
        for box in boxes:
            if not isinstance(box, Mapping):
                continue
            profile_key = str(box.get("profile_key") or "")
            if profile_key:
                box_by_profile[profile_key] = dict(box)
            y1 = finite_float(box.get("y1"))
            y2 = finite_float(box.get("y2"))
            if y1 is not None:
                box_by_boundary.setdefault(("VAL", round(float(y1), 8)), []).append(dict(box))
            if y2 is not None:
                box_by_boundary.setdefault(("VAH", round(float(y2), 8)), []).append(dict(box))

        reconciliation = {
            "bubbles_total": 0,
            "matched_by_profile": 0,
            "matched_by_boundary_only": 0,
            "unmatched_profile_key": 0,
            "missing_profile_key": 0,
            "missing_boundary": 0,
            "boundary_mismatch": 0,
        }
        reconciliation_samples: List[Dict[str, Any]] = []

        for bubble in bubbles:
            if not isinstance(bubble, Mapping):
                continue
            reconciliation["bubbles_total"] += 1
            profile_key = str(bubble.get("profile_key") or "")
            boundary_type = str(bubble.get("boundary_type") or "").upper()
            boundary_price = finite_float(bubble.get("boundary_price"))
            boundary_key = (
                boundary_type,
                round(float(boundary_price), 8),
            ) if boundary_type in {"VAH", "VAL"} and boundary_price is not None else None
            boundary_matches = box_by_boundary.get(boundary_key, []) if boundary_key is not None else []
            matched_box = box_by_profile.get(profile_key) if profile_key else None

            if not profile_key:
                reconciliation["missing_profile_key"] += 1
                if boundary_matches:
                    reconciliation["matched_by_boundary_only"] += 1
                continue
            if matched_box is None:
                reconciliation["unmatched_profile_key"] += 1
                if boundary_matches:
                    reconciliation["matched_by_boundary_only"] += 1
                    if len(reconciliation_samples) < 5:
                        reconciliation_samples.append(
                            {
                                "reason": "profile_key_unmatched_boundary_present",
                                "profile_key": profile_key,
                                "boundary_type": boundary_type,
                                "boundary_price": boundary_price,
                                "label": bubble.get("label"),
                            }
                        )
                log.warning(
                    "event=market_profile_overlay_bubble_profile_unmatched symbol=%s profile_key=%s boundary_type=%s boundary_price=%s",
                    symbol,
                    profile_key,
                    boundary_type,
                    boundary_price,
                )
                continue
            reconciliation["matched_by_profile"] += 1
            expected = None
            if boundary_type == "VAH":
                expected = finite_float(matched_box.get("y2"))
            elif boundary_type == "VAL":
                expected = finite_float(matched_box.get("y1"))
            if expected is None or boundary_price is None:
                reconciliation["missing_boundary"] += 1
                continue
            if abs(float(expected) - float(boundary_price)) > 1e-9:
                reconciliation["boundary_mismatch"] += 1
                if len(reconciliation_samples) < 5:
                    reconciliation_samples.append(
                        {
                            "reason": "boundary_mismatch",
                            "profile_key": profile_key,
                            "boundary_type": boundary_type,
                            "boundary_price": boundary_price,
                            "box_expected": expected,
                            "label": bubble.get("label"),
                        }
                    )
                log.warning(
                    "event=market_profile_overlay_bubble_boundary_mismatch symbol=%s profile_key=%s boundary_type=%s boundary_price=%.8f box_expected=%.8f",
                    symbol,
                    profile_key,
                    boundary_type,
                    float(boundary_price),
                    float(expected),
                )
        log.debug(
            "event=market_profile_overlay_bubble_box_reconciliation symbol=%s boxes=%s bubbles=%s stats=%s sample=%s",
            symbol,
            len(boxes),
            len(bubbles),
            reconciliation,
            reconciliation_samples,
        )
    if debug_profiles:
        log.debug(
            "event=market_profile_overlay_boxes_resolved symbol=%s current_epoch=%s boxes=%s sample=%s",
            symbol,
            current_epoch,
            len(boxes),
            debug_profiles,
        )
    trimmed["payload"] = payload_copy
    return trimmed
