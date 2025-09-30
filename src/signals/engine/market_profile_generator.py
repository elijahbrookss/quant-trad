from typing import List, Dict, Optional, Sequence, Mapping, Any, Tuple
import logging
import math

import pandas as pd
from indicators.market_profile import MarketProfileIndicator

from signals.base import BaseSignal
from signals.engine.signal_generator import (
    build_signal_overlays,
    register_indicator_rules,
    run_indicator_rules,
)
from signals.rules.market_profile import (
    market_profile_breakout_rule,
    market_profile_retest_rule,
)

logger = logging.getLogger("MarketProfileSignalGenerator")


def _finite_float(value: Any) -> Optional[float]:
    """Return a finite float representation of ``value`` or ``None``."""

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    if math.isnan(numeric) or math.isinf(numeric):
        return None

    return numeric


def _clone_indicator_for_runtime(
    indicator: MarketProfileIndicator,
    df: pd.DataFrame,
    *,
    interval: Optional[str] = None,
) -> Optional[MarketProfileIndicator]:
    """Create a lightweight indicator instance for signal evaluation."""

    if df is None or df.empty:
        return None

    try:
        runtime = MarketProfileIndicator(
            df=df.copy(),
            bin_size=getattr(indicator, "bin_size", 0.1),
            mode=getattr(indicator, "mode", "tpo"),
            interval=interval or getattr(indicator, "interval", "30m"),
        )
    except Exception:
        logger.exception("Failed to initialise MarketProfileIndicator for signal payloads")
        return None

    return runtime


def build_value_area_payloads(
    indicator: MarketProfileIndicator,
    df: pd.DataFrame,
    *,
    interval: Optional[str] = None,
    use_merged: Optional[bool] = None,
    merge_threshold: Optional[float] = None,
    min_merge_sessions: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Derive value area payloads for market profile signal rules."""

    runtime = _clone_indicator_for_runtime(indicator, df, interval=interval)
    if runtime is None:
        return []

    use_merged = True if use_merged is None else bool(use_merged)

    if use_merged:
        threshold = 0.6 if merge_threshold is None else float(merge_threshold)
        min_merge = 2 if min_merge_sessions is None else int(min_merge_sessions)
        value_areas = runtime.merge_value_areas(threshold=threshold, min_merge=min_merge)
    else:
        value_areas = runtime.daily_profiles

    payloads: List[Dict[str, Any]] = []
    for area in value_areas or []:
        if isinstance(area, Mapping) and area.get("VAH") is not None and area.get("VAL") is not None:
            payloads.append(dict(area))

    return payloads


class MarketProfileSignalGenerator:
    def __init__(self, indicator: MarketProfileIndicator, symbol: Optional[str] = None):
        self.indicator = indicator
        self.symbol = symbol or getattr(indicator, "symbol", None)

    def generate_signals(
        self,
        df: pd.DataFrame,
        value_areas: Optional[Sequence[Mapping[str, Any]]] = None,
        **config: Any,
    ) -> List[BaseSignal]:
        """Run registered Market Profile rules and convert outputs into signals."""
        if self.symbol is None:
            raise ValueError("MarketProfileSignalGenerator requires a symbol for rule execution")

        payloads = (
            list(value_areas)
            if value_areas is not None
            else build_value_area_payloads(
                self.indicator,
                df,
                interval=getattr(self.indicator, "interval", None),
                use_merged=config.get("market_profile_use_merged_value_areas"),
                merge_threshold=config.get("market_profile_merge_threshold"),
                min_merge_sessions=config.get("market_profile_merge_min_sessions"),
            )
        )
        return run_indicator_rules(
            self.indicator,
            df,
            rule_payloads=payloads,
            symbol=self.symbol,
            **config,
        )

    @staticmethod
    def to_overlays(
        signals: List[BaseSignal],
        plot_df: pd.DataFrame,
        **kwargs,
    ) -> List[Dict]:
        return list(
            build_signal_overlays(
                MarketProfileIndicator.NAME,
                signals,
                plot_df,
                **kwargs,
            )
        )


def _to_epoch_seconds(value: Any) -> Optional[int]:
    """Best-effort conversion of timestamps into epoch seconds."""

    if value is None:
        return None

    if isinstance(value, (int,)):
        return int(value)

    if isinstance(value, (float,)):
        numeric = float(value)
        return int(numeric) if math.isfinite(numeric) else None

    if isinstance(value, pd.Timestamp):
        try:
            ts = value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")
        except (TypeError, ValueError):
            ts = value.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT") if value.tzinfo is None else value
        if pd.isna(ts):
            return None
        return int(ts.value // 10**9)

    try:
        candidate = pd.Timestamp(value)
    except Exception:
        return None

    if pd.isna(candidate):
        return None

    if candidate.tzinfo is None:
        candidate = candidate.tz_localize("UTC")
    else:
        candidate = candidate.tz_convert("UTC")

    return int(candidate.value // 10**9)


_BREAKOUT_COLORS = {
    "above": "#16a34a",  # green
    "below": "#dc2626",  # red
}

_RETEST_COLORS = {
    "support": "#0ea5e9",  # sky blue
    "resistance": "#f97316",  # amber
}


def _hex_to_rgb(color: str) -> Optional[Tuple[int, int, int]]:
    if not isinstance(color, str):
        return None

    value = color.strip().lstrip("#")
    if len(value) != 6:
        return None

    try:
        r = int(value[0:2], 16)
        g = int(value[2:4], 16)
        b = int(value[4:6], 16)
    except ValueError:
        return None

    return r, g, b


def _rgba_from_hex(color: str, alpha: float) -> Optional[str]:
    rgb = _hex_to_rgb(color)
    if rgb is None:
        return None

    r, g, b = rgb
    a = min(max(alpha, 0.0), 1.0)
    return f"rgba({r},{g},{b},{a:.2f})"


def _readable_text_color(color: str) -> str:
    rgb = _hex_to_rgb(color)
    if rgb is None:
        return "#0f172a"

    r, g, b = rgb
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return "#0f172a" if luminance > 0.55 else "#f8fafc"


def _resolve_level_price(metadata: Mapping[str, Any]) -> Optional[float]:
    price = _finite_float(metadata.get("level_price"))
    if price is not None:
        return price

    level_type = str(metadata.get("level_type", "")).upper()
    if level_type == "VAH":
        return _finite_float(metadata.get("VAH"))
    if level_type == "VAL":
        return _finite_float(metadata.get("VAL"))

    for key in ("VAH", "VAL"):
        price = _finite_float(metadata.get(key))
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
    confidence = _finite_float(metadata.get("confidence"))
    if confidence is None:
        return None

    percent = max(0, min(100, round(confidence * 100)))
    return f"Confidence {percent}%"


def _market_profile_overlay_adapter(
    signals: List[BaseSignal],
    plot_df: pd.DataFrame,
    **_: Any,
) -> List[Dict[str, Any]]:
    logger.info("Converting %d signals to bubble overlays", len(signals))
    bubbles: List[Dict[str, Any]] = []

    for idx, sig in enumerate(signals):
        metadata = sig.metadata or {}
        if metadata.get("source") != "MarketProfile":
            logger.debug("Skipping signal %d: not from MarketProfile source", idx)
            continue

        level_price = _resolve_level_price(metadata)
        if level_price is None:
            logger.debug("Skipping signal %d: unresolved level price", idx)
            continue

        marker_time = _to_epoch_seconds(sig.time)
        if marker_time is None:
            logger.debug("Skipping signal %d: invalid signal time %s", idx, sig.time)
            continue

        level_label = _level_label(metadata)

        if sig.type == "retest":
            retest_role = str(metadata.get("retest_role", "retest")).lower()
            color = _RETEST_COLORS.get(retest_role, "#38bdf8")
            anchor_price = _finite_float(metadata.get("retest_close")) or level_price
            bars_since = metadata.get("bars_since_breakout")
            if bars_since is not None:
                detail = f"Retest after {int(bars_since)} bars near {level_label} {float(level_price):.2f}"
            else:
                detail = f"Retest near {level_label} {float(level_price):.2f}"

            meta_label = _confidence_meta(metadata)
            direction = metadata.get("direction") or (
                "up" if str(metadata.get("breakout_direction")).lower() == "above" else "down"
            )

            bubbles.append(
                {
                    "time": marker_time,
                    "price": float(anchor_price),
                    "label": f"{level_label} retest",
                    "detail": detail,
                    "meta": meta_label,
                    "accentColor": color,
                    "backgroundColor": _rgba_from_hex(color, 0.18) or "rgba(14,165,233,0.25)",
                    "textColor": _readable_text_color(color),
                    "direction": direction,
                    "subtype": "bubble",
                }
            )
            logger.debug(
                "Signal %d converted to retest bubble | level=%s | price=%.2f | direction=%s",
                idx,
                level_label,
                float(anchor_price),
                direction,
            )
            continue

        breakout_direction = str(metadata.get("breakout_direction", "")).lower()
        color = _BREAKOUT_COLORS.get(breakout_direction, "#6b7280")
        anchor_price = _finite_float(metadata.get("trigger_close")) or level_price
        trigger_high = _finite_float(metadata.get("trigger_high")) or anchor_price
        trigger_low = _finite_float(metadata.get("trigger_low")) or anchor_price

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

        bubbles.append(
            {
                "time": marker_time,
                "price": bubble_price,
                "label": label,
                "detail": detail,
                "meta": meta_text,
                "accentColor": color,
                "backgroundColor": _rgba_from_hex(color, 0.2) or "rgba(30,41,59,0.75)",
                "textColor": _readable_text_color(color),
                "direction": breakout_direction or metadata.get("direction"),
                "subtype": "bubble",
            }
        )
        logger.debug(
            "Signal %d converted to breakout bubble | level=%s | price=%.2f | direction=%s",
            idx,
            level_label,
            bubble_price,
            breakout_direction,
        )

    if not bubbles:
        logger.info("Converted 0 signals to overlays")
        return []

    logger.info("Converted %d signals to overlays", len(bubbles))
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


register_indicator_rules(
    MarketProfileIndicator.NAME,
    rules=[market_profile_breakout_rule, market_profile_retest_rule],
    overlay_adapter=_market_profile_overlay_adapter,
)
