from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from signals.base import BaseSignal
from signals.engine.signal_generator import overlay_adapter
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


@overlay_adapter("market_profile")
def market_profile_overlay_adapter(
    signals: Sequence[BaseSignal],
    plot_df: pd.DataFrame,
    **_: Any,
) -> List[Dict[str, Any]]:
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

            meta_label = _confidence_meta(metadata)
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
                    "meta": meta_label,
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

        level_gap = abs(float(anchor_price) - float(level_price))
        wick_gap_above = max(0.0, float(trigger_high) - float(anchor_price))
        wick_gap_below = max(0.0, float(anchor_price) - float(trigger_low))
        base_offset = max(abs(float(anchor_price)) * 0.001, 0.1)

        if breakout_direction == "above":
            offset = max(level_gap * 0.25, wick_gap_above * 0.5, base_offset)
            bubble_price = float(anchor_price) + offset
            label = f"{level_label} breakout"
            detail_prefix = "Closed above"
        elif breakout_direction == "below":
            offset = max(level_gap * 0.25, wick_gap_below * 0.5, base_offset)
            bubble_price = float(anchor_price) - offset
            label = f"{level_label} breakdown"
            detail_prefix = "Closed below"
        else:
            bubble_price = float(anchor_price) + base_offset
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
        meta_text = " · ".join(meta_bits) if meta_bits else None

        bias_label = bias_label_from_direction(
            breakout_direction or metadata.get("direction")
        )

        pointer_hint = metadata.get("pointer_direction") or breakout_direction or metadata.get("direction")
        bubbles.append(
            {
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
        )
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
    payload = {
        "price_lines": [],
        "markers": [],
        "bubbles": bubbles,
    }

    return [
        {
            "type": MarketProfileIndicator.NAME,
            "payload": payload,
        }
    ]


__all__ = ["market_profile_overlay_adapter"]
