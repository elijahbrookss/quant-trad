from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.base import BaseSignal
from signals.engine.signal_generator import overlay_adapter
from signals.overlays.schema import build_overlay
from signals.overlays.registry import overlay_type
from signals.overlays.transformers import overlay_transformer, normalize_overlay_epoch
from signals.rules.common.utils import (
    bias_label_from_direction,
    finite_float,
    format_duration,
    rgba_from_hex,
    to_epoch_seconds,
)


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


def _normalize_marker_time(
    ts: Any,
    plot_df: Optional[pd.DataFrame],
    idx: Optional[int],
    marker_kind: str,
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

    level_type = str(metadata.get("level_type", "")).upper()
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
    level_type = str(metadata.get("level_type", "")).strip().upper()
    if level_type in {"VAH", "VAL"}:
        return level_type
    if level_type:
        return level_type.title()
    return "Value Area"


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

        # Apply offset if needed (shift price by small percentage per level)
        if offset_level > 0:
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


@overlay_type(
    ["market_profile", "market-profile", "mpf"],
    label="Market Profile",
    pane_views=("va_box", "touch"),
    description="Market profile value area boxes and touch markers.",
    renderers={"lightweight": "va_box", "mpl": "box"},
    payload_keys=("boxes", "markers", "bubbles"),
    ui_color="#38bdf8",
)
@overlay_adapter("market_profile")
def market_profile_overlay_adapter(
    signals: Sequence[BaseSignal],
    plot_df: pd.DataFrame,
    **_: Any,
) -> List[Dict[str, Any]]:
    log.info(
        "🚀 OVERLAY ADAPTER CALLED | signals=%d | has_plot_df=%s | plot_df_len=%s",
        len(signals),
        plot_df is not None,
        len(plot_df) if plot_df is not None else 0,
    )
    start_time = perf_counter()
    bubbles: List[Dict[str, Any]] = []
    summary = {
        "total": len(signals),
        "converted_breakout": 0,
        "converted_retest": 0,
        "skipped_source": 0,
        "skipped_price": 0,
        "skipped_time": 0,
    }

    for sig in signals:
        metadata = sig.metadata or {}
        log.info(
            "Processing signal | type=%s | source=%s | sig.time=%s | has_confirm_indices=%s | confirm_indices=%s | confirm_times=%s",
            sig.type,
            metadata.get("source"),
            sig.time,
            "confirm_indices" in metadata,
            metadata.get("confirm_indices", []),
            metadata.get("confirm_times", []),
        )
        if metadata.get("source") != "MarketProfile":
            summary["skipped_source"] += 1
            continue

        breakout_index = metadata.get("bar_index") or metadata.get("trigger_index")

        # Use the signal time directly instead of the origin time
        signal_time = metadata.get("time") or metadata.get("trigger_time") or sig.time
        marker_time = _normalize_marker_time(signal_time, plot_df, breakout_index, "bubble")
        log.debug(
            "Normalized bubble time | signal_time=%s | epoch=%s | idx=%s",
            signal_time,
            marker_time,
            breakout_index,
        )
        if marker_time is None:
            summary["skipped_time"] += 1
            continue

        level_price = _resolve_level_price(metadata)
        if level_price is None:
            summary["skipped_price"] += 1
            continue

        level_label = _level_label(metadata)

        if sig.type in ("retest", "retest_v2"):
            retest_role = str(metadata.get("retest_role", "retest")).lower()
            color = _RETEST_COLORS.get(retest_role, "#38bdf8")
            # Position at retest close instead of level price
            anchor_price = finite_float(metadata.get("retest_close")) or finite_float(metadata.get("trigger_close")) or level_price
            bars_since = metadata.get("bars_since_breakout")
            if bars_since is not None:
                detail = f"Retest after {int(bars_since)} bars near {level_label} {float(level_price):.2f}"
            else:
                detail = f"Retest near {level_label} {float(level_price):.2f}"

            meta_bits = []
            meta_label = _confidence_meta(metadata)
            if meta_label:
                meta_bits.append(meta_label)
            va_span = _va_span_meta(metadata)
            if va_span:
                meta_bits.append(va_span)
            meta_text = " · ".join(meta_bits) if meta_bits else None
            pointer_hint = str(
                metadata.get("pointer_direction")
                or metadata.get("breakout_direction")
                or metadata.get("direction")
                or ""
            ).lower()
            if pointer_hint in {"above", "up"}:
                bubble_direction = "above"
            elif pointer_hint in {"below", "down"}:
                bubble_direction = "below"
            else:
                bubble_direction = "above" if retest_role == "resistance" else "below"

            bias_label = bias_label_from_direction(
                metadata.get("direction"), fallback=pointer_hint or retest_role
            )

            bubbles.append(
                {
                    "time": marker_time,
                    "price": float(anchor_price),
                    "label": f"{level_label} retest",
                    "detail": detail,
                    "meta": meta_text,
                    "accentColor": color,
                    "backgroundColor": rgba_from_hex(color, 0.18) or "rgba(14,165,233,0.25)",
                    "textColor": "#ffffff",
                    "direction": metadata.get("pointer_direction")
                    or metadata.get("direction")
                    or bubble_direction,
                    "bias": bias_label,
                    "subtype": "bubble",
                }
            )
            summary["converted_retest"] += 1
            continue

        breakout_direction = str(metadata.get("breakout_direction", "")).lower()
        color = _BREAKOUT_COLORS.get(breakout_direction, "#6b7280")

        # Get trigger candle OHLC from plot_df using breakout_index
        trigger_close = None
        if breakout_index is not None and plot_df is not None:
            try:
                if isinstance(breakout_index, int) and 0 <= breakout_index < len(plot_df):
                    row = plot_df.iloc[breakout_index]
                    trigger_close = finite_float(row.get("close"))
            except Exception as exc:
                log.warning(
                    "Failed to read trigger close from plot_df | idx=%s | error=%s",
                    breakout_index,
                    exc,
                )

        # Fallback to metadata or level price
        if trigger_close is None:
            trigger_close = finite_float(metadata.get("trigger_close"))
        anchor_price = trigger_close or level_price

        # Position bubble at the close of the signal candle instead of level price
        bubble_price = float(anchor_price)

        if breakout_direction == "above":
            label = f"{level_label} breakout"
            detail_prefix = "Closed above"
        elif breakout_direction == "below":
            label = f"{level_label} breakdown"
            detail_prefix = "Closed below"
        else:
            label = f"{level_label} breakout"
            detail_prefix = "Closed near"

        detail = f"{detail_prefix} {level_label} {float(level_price):.2f}"
        meta_bits = []
        meta_label = _confidence_meta(metadata)
        if meta_label:
            meta_bits.append(meta_label)
        # Don't include value_area_id in meta text - it's an ISO timestamp that's confusing to users
        # value_area_id is still in metadata for matching breakouts to retests
        va_span = _va_span_meta(metadata)
        if va_span:
            meta_bits.append(va_span)
        meta_text = " · ".join(meta_bits) if meta_bits else None

        bias_label = bias_label_from_direction(
            breakout_direction or metadata.get("direction")
        )

        pointer_hint = metadata.get("pointer_direction") or breakout_direction or metadata.get("direction")

        # Confirmation markers (checkmarks)
        confirm_indices = metadata.get("confirm_indices") or []
        confirm_times = metadata.get("confirm_times") or []
        confirm_markers: List[Dict[str, Any]] = []

        log.debug(
            "Processing breakout | confirm_indices=%s | confirm_times=%s | has_plot_df=%s",
            confirm_indices,
            confirm_times,
            plot_df is not None,
        )

        if confirm_times and plot_df is not None:
            for ts_idx, ts in enumerate(confirm_times):
                confirm_idx = confirm_indices[ts_idx] if ts_idx < len(confirm_indices) else None
                normalized_time = _normalize_marker_time(
                    ts,
                    plot_df=plot_df,
                    idx=confirm_idx,
                    marker_kind="confirm",
                )
                if normalized_time is None:
                    continue
                try:
                    if isinstance(confirm_idx, int) and 0 <= confirm_idx < len(plot_df):
                        row = plot_df.iloc[confirm_idx]
                        ts_val = plot_df.index[confirm_idx]
                    else:
                        ts_val = pd.Timestamp(ts)
                        row = plot_df.loc[ts_val]
                    body_high = max(float(row.get("open", row.get("close"))), float(row.get("close")))
                    body_low = min(float(row.get("open", row.get("close"))), float(row.get("close")))

                    marker_point = {
                        "time": normalized_time,
                        "price": (body_high + body_low) / 2.0,
                        "shape": "square",
                        "color": color,
                        "text": "✓",
                        "position": "inBar",
                        "subtype": "marker",
                    }
                    confirm_markers.append(marker_point)
                    log.debug(
                        "Created confirmation marker | ts=%s | epoch=%s | price=%.5f",
                        ts,
                        normalized_time,
                        marker_point["price"],
                    )
                except Exception as e:
                    log.warning(
                        "Failed to create confirmation marker | ts=%s | epoch=%s | idx=%s | error=%s",
                        ts,
                        normalized_time,
                        confirm_idx,
                        e,
                    )
                    continue

        # Prior markers (circles)
        prior_indices = metadata.get("prior_indices") or []
        prior_times = metadata.get("prior_times") or []
        prior_markers: List[Dict[str, Any]] = []

        log.debug(
            "Processing prior window | prior_indices=%s | prior_times=%s | has_plot_df=%s",
            prior_indices,
            prior_times,
            plot_df is not None,
        )

        if prior_times and plot_df is not None:
            for position, ts in enumerate(prior_times):
                prior_idx = prior_indices[position] if position < len(prior_indices) else None
                normalized_time = _normalize_marker_time(
                    ts,
                    plot_df=plot_df,
                    idx=prior_idx,
                    marker_kind="prior",
                )
                if normalized_time is None:
                    continue
                try:
                    if isinstance(prior_idx, int) and 0 <= prior_idx < len(plot_df):
                        row = plot_df.iloc[prior_idx]
                        ts_val = plot_df.index[prior_idx]
                    else:
                        ts_val = pd.Timestamp(ts)
                        row = plot_df.loc[ts_val]
                    body_high = max(float(row.get("open", row.get("close"))), float(row.get("close")))
                    body_low = min(float(row.get("open", row.get("close"))), float(row.get("close")))

                    # Circle with numbered text or bullet
                    marker_text = str(position + 1) if len(prior_times) > 1 else "•"

                    prior_marker = {
                        "time": normalized_time,
                        "price": (body_high + body_low) / 2.0,
                        "shape": "circle",
                        "color": color,
                        "text": marker_text,
                        "position": "inBar",
                        "subtype": "marker",
                    }
                    prior_markers.append(prior_marker)
                    log.debug(
                        "Created prior marker | ts=%s | epoch=%s | price=%.5f | text=%s",
                        ts,
                        normalized_time,
                        prior_marker["price"],
                        marker_text,
                    )
                except Exception as e:
                    log.warning(
                        "Failed to create prior marker | ts=%s | epoch=%s | idx=%s | error=%s",
                        ts,
                        normalized_time,
                        prior_idx,
                        e,
                    )
                    continue

        # Combine both marker types (prior first, then confirm)
        marker_points = prior_markers + confirm_markers

        bubble = {
            "time": marker_time,
            "price": bubble_price,
            "label": label,
            "detail": detail,
            "meta": meta_text,
            "accentColor": color,
            "backgroundColor": rgba_from_hex(color, 0.2) or "rgba(30,41,59,0.75)",
            "textColor": "#ffffff",
            "direction": pointer_hint,
            "bias": bias_label,
            "subtype": "bubble",
        }
        if marker_points:
            bubble["_markers"] = marker_points

        # Debug logging for bubble placement
        log.info(
            "Created bubble | signal_time=%s (epoch=%s) | idx=%s | price=%.2f (close=%.2f, level=%.2f) | label=%s | markers=%d",
            signal_time,
            marker_time,
            breakout_index,
            bubble_price,
            trigger_close or 0,
            level_price,
            label,
            len(marker_points) if marker_points else 0,
        )

        bubbles.append(bubble)
        summary["converted_breakout"] += 1

    duration = perf_counter() - start_time
    symbol = next((sig.symbol for sig in signals if getattr(sig, "symbol", None)), None)

    log.info(
        "Market profile overlays | symbol=%s | total=%d | converted=%d (breakout=%d, retest=%d) | "
        "skipped[source=%d, price=%d, time=%d] | duration=%s",
        symbol,
        summary["total"],
        len(bubbles),
        summary["converted_breakout"],
        summary["converted_retest"],
        summary["skipped_source"],
        summary["skipped_price"],
        summary["skipped_time"],
        format_duration(duration),
    )

    if not bubbles:
        return []

    # Apply collision avoidance to prevent overlapping bubbles
    _apply_collision_avoidance(bubbles)

    markers: List[Dict[str, Any]] = []
    for b in bubbles:
        extra = b.pop("_markers", None)
        if extra:
            markers.extend(extra)

    # Count marker types for logging
    confirm_marker_count = sum(1 for m in markers if m.get("shape") == "square")
    prior_marker_count = sum(1 for m in markers if m.get("shape") == "circle")

    log.info(
        "Market profile overlays final | bubbles=%d | markers=%d (confirm=%d, prior=%d)",
        len(bubbles),
        len(markers),
        confirm_marker_count,
        prior_marker_count,
    )
    if markers:
        log.debug("Sample marker: %s", markers[0] if markers else None)

    marker_times = [m.get("time") for m in markers if isinstance(m.get("time"), (int, float))]
    bubble_times = [b.get("time") for b in bubbles if isinstance(b.get("time"), (int, float))]
    log.debug(
        "Overlay time bounds | marker_min=%s | marker_max=%s | bubble_min=%s | bubble_max=%s | marker_count=%d | bubble_count=%d",
        min(marker_times) if marker_times else None,
        max(marker_times) if marker_times else None,
        min(bubble_times) if bubble_times else None,
        max(bubble_times) if bubble_times else None,
        len(marker_times),
        len(bubble_times),
    )

    payload = {
        "price_lines": [],
        "markers": markers,
        "bubbles": bubbles,
    }

    return [build_overlay(MarketProfileIndicator.NAME, payload)]


__all__ = ["market_profile_overlay_adapter"]


@overlay_transformer(["market-profile", "market_profile", "mpf"])
def market_profile_overlay_transformer(
    overlay: Mapping[str, Any], current_epoch: int
) -> Optional[Mapping[str, Any]]:
    payload = overlay.get("payload")
    if not isinstance(payload, Mapping):
        return overlay
    profiles = payload.get("profiles")
    if not isinstance(profiles, list):
        if "market_profile_profiles_missing" not in _WARNED_TRANSFORMERS:
            log.error("market_profile_profiles_missing_for_walk_forward")
            _WARNED_TRANSFORMERS.add("market_profile_profiles_missing")
        trimmed = dict(overlay)
        trimmed["payload"] = dict(payload)
        trimmed["payload"]["boxes"] = []
        return trimmed

    params = payload.get("profile_params") or {}
    use_merged = bool(params.get("use_merged_value_areas"))
    merge_threshold = params.get("merge_threshold")
    min_merge_sessions = params.get("min_merge_sessions")
    extend_to_end = bool(params.get("extend_value_area_to_chart_end"))

    # Extract metadata for logging context
    bot_id = overlay.get("bot_id") or payload.get("bot_id")
    symbol = overlay.get("symbol") or payload.get("symbol")
    strategy_id = overlay.get("strategy_id") or payload.get("strategy_id")

    known_profiles = []
    for entry in profiles:
        if not isinstance(entry, Mapping):
            continue
        end_epoch = normalize_overlay_epoch(entry.get("end"))
        if end_epoch is None or end_epoch > current_epoch:
            continue
        profile = _profile_from_payload(entry)
        if profile is not None:
            known_profiles.append(profile)

    if not known_profiles:
        trimmed = dict(overlay)
        payload_copy = dict(payload)
        payload_copy["boxes"] = []
        payload_copy["transform_summary"] = {
            "known_profiles": 0,
            "merged_profiles": 0,
        }
        trimmed["payload"] = payload_copy
        return trimmed

    if use_merged:
        try:
            from indicators.market_profile._internal.merging import merge_profiles
        except Exception:
            merge_profiles = None
        if merge_profiles is None:
            if "market_profile_merge_import_failed" not in _WARNED_TRANSFORMERS:
                log.error("market_profile_merge_import_failed")
                _WARNED_TRANSFORMERS.add("market_profile_merge_import_failed")
            trimmed = dict(overlay)
            payload_copy = dict(payload)
            payload_copy["boxes"] = []
            payload_copy["transform_summary"] = {
                "known_profiles": len(known_profiles),
                "merged_profiles": 0,
            }
            trimmed["payload"] = payload_copy
            return trimmed
        if merge_threshold is None or min_merge_sessions is None:
            if "market_profile_merge_params_missing" not in _WARNED_TRANSFORMERS:
                log.error("market_profile_merge_params_missing")
                _WARNED_TRANSFORMERS.add("market_profile_merge_params_missing")
            trimmed = dict(overlay)
            payload_copy = dict(payload)
            payload_copy["boxes"] = []
            payload_copy["transform_summary"] = {
                "known_profiles": len(known_profiles),
                "merged_profiles": 0,
            }
            trimmed["payload"] = payload_copy
            return trimmed
        merged_profiles = merge_profiles(
            known_profiles,
            float(merge_threshold),
            int(min_merge_sessions),
            bot_id=bot_id,
            symbol=symbol,
            strategy_id=strategy_id,
        )
    else:
        merged_profiles = known_profiles

    boxes: List[Dict[str, Any]] = []
    for profile in merged_profiles:
        start_epoch = int(profile.start.timestamp())
        end_epoch = int(profile.end.timestamp())
        if end_epoch > current_epoch:
            continue
        box_end = current_epoch if extend_to_end else end_epoch
        boxes.append(
            {
                "x1": start_epoch,
                "x2": box_end,
                "y1": float(profile.val),
                "y2": float(profile.vah),
                "fillColor": "rgba(59, 130, 246, 0.1)",
                "borderColor": "#3b82f6",
                "borderWidth": 1,
                "borderStyle": 2,
            }
        )

    trimmed = dict(overlay)
    payload_copy = dict(payload)
    payload_copy["boxes"] = boxes
    payload_copy["transform_summary"] = {
        "known_profiles": len(known_profiles),
        "merged_profiles": len(merged_profiles),
    }
    trimmed["payload"] = payload_copy
    return trimmed


def _profile_from_payload(entry: Mapping[str, Any]) -> Optional["Profile"]:
    try:
        from indicators.market_profile.domain import Profile, ValueArea
    except Exception:
        return None
    start_epoch = normalize_overlay_epoch(entry.get("start"))
    end_epoch = normalize_overlay_epoch(entry.get("end"))
    if start_epoch is None or end_epoch is None:
        return None
    try:
        vah = float(entry.get("VAH"))
        val = float(entry.get("VAL"))
        poc = float(entry.get("POC"))
    except (TypeError, ValueError):
        return None
    session_count = int(entry.get("session_count") or 1)
    precision = int(entry.get("precision") or 4)
    value_area = ValueArea(vah=vah, val=val, poc=poc)
    return Profile(
        start=pd.Timestamp(start_epoch, unit="s"),
        end=pd.Timestamp(end_epoch, unit="s"),
        value_area=value_area,
        session_count=session_count,
        precision=precision,
    )
