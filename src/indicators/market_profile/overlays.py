from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.base import BaseSignal
from signals.engine.signal_generator import overlay_adapter
from signals.overlays.schema import build_overlay
from signals.rules.common.utils import (
    bias_label_from_direction,
    finite_float,
    format_duration,
    rgba_from_hex,
    to_epoch_seconds,
)


log = logging.getLogger("MarketProfileOverlays")

_BREAKOUT_COLORS = {
    "above": "#16a34a",  # green
    "below": "#dc2626",  # red
}

_RETEST_COLORS = {
    "support": "#0ea5e9",  # sky blue
    "resistance": "#f97316",  # amber
}


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

        level_price = _resolve_level_price(metadata)
        if level_price is None:
            summary["skipped_price"] += 1
            continue

        marker_time = to_epoch_seconds(sig.time)
        if marker_time is None:
            summary["skipped_time"] += 1
            continue

        level_label = _level_label(metadata)

        if sig.type == "retest":
            retest_role = str(metadata.get("retest_role", "retest")).lower()
            color = _RETEST_COLORS.get(retest_role, "#38bdf8")
            anchor_price = finite_float(metadata.get("retest_close")) or level_price
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
        anchor_price = finite_float(metadata.get("trigger_close")) or level_price
        trigger_high = finite_float(metadata.get("trigger_high")) or anchor_price
        trigger_low = finite_float(metadata.get("trigger_low")) or anchor_price

        # Position bubble at the level price
        bubble_price = float(level_price)

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
        value_area_id = metadata.get("value_area_id")
        if value_area_id:
            meta_bits.append(str(value_area_id))
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
            for ts in confirm_times:
                normalized_time = to_epoch_seconds(ts)
                if normalized_time is None:
                    log.warning("Skipping confirmation marker due to invalid timestamp | ts=%s", ts)
                    continue
                try:
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
                        "Failed to create confirmation marker | ts=%s | epoch=%s | error=%s",
                        ts,
                        normalized_time,
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
                normalized_time = to_epoch_seconds(ts)
                if normalized_time is None:
                    log.warning("Skipping prior marker due to invalid timestamp | ts=%s", ts)
                    continue
                try:
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
                        "Failed to create prior marker | ts=%s | epoch=%s | error=%s",
                        ts,
                        normalized_time,
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
            "Created bubble | time=%s (epoch=%s) | price=%.2f | level_price=%.2f | label=%s | markers=%d",
            sig.time, marker_time, bubble_price, level_price, label, len(marker_points) if marker_points else 0
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

    payload = {
        "price_lines": [],
        "markers": markers,
        "bubbles": bubbles,
    }

    return [build_overlay(MarketProfileIndicator.NAME, payload)]


__all__ = ["market_profile_overlay_adapter"]
