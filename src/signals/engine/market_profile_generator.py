from typing import List, Dict, Optional, Sequence, Mapping, Any, Tuple
import logging
import math
from time import perf_counter

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


def _format_duration(seconds: float) -> str:
    """Return a compact, human readable duration string."""

    if seconds >= 1:
        return f"{seconds:.2f}s"
    return f"{seconds * 1000:.1f}ms"


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
            extend_value_area_to_chart_end=getattr(
                indicator,
                "extend_value_area_to_chart_end",
                True,
            ),
            use_merged_value_areas=getattr(
                indicator,
                "use_merged_value_areas",
                True,
            ),
            merge_threshold=getattr(indicator, "merge_threshold", 0.6),
            min_merge_sessions=getattr(
                indicator,
                "min_merge_sessions",
                getattr(MarketProfileIndicator, "DEFAULT_MIN_MERGE_SESSIONS", 3),
            ),
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

    symbol = getattr(indicator, "symbol", None)

    if df is None or df.empty:
        logger.info(
            "Market profile payloads skipped | symbol=%s | reason=empty-data",
            symbol,
        )
        return []

    start_time = perf_counter()
    runtime = _clone_indicator_for_runtime(indicator, df, interval=interval)
    if runtime is None:
        logger.info(
            "Market profile payloads skipped | symbol=%s | reason=indicator-init",
            symbol,
        )
        return []

    if use_merged is None:
        use_merged = getattr(runtime, "use_merged_value_areas", True)
    else:
        use_merged = bool(use_merged)

    if use_merged:
        threshold = (
            getattr(runtime, "merge_threshold", 0.6)
            if merge_threshold is None
            else float(merge_threshold)
        )
        default_min_merge = getattr(
            runtime,
            "min_merge_sessions",
            getattr(MarketProfileIndicator, "DEFAULT_MIN_MERGE_SESSIONS", 3),
        )
        min_merge = default_min_merge if min_merge_sessions is None else int(min_merge_sessions)
        value_areas = runtime.merge_value_areas(threshold=threshold, min_merge=min_merge)
    else:
        value_areas = runtime.daily_profiles

    payloads: List[Dict[str, Any]] = []
    profile_labels: List[str] = []
    for idx, area in enumerate(value_areas or []):
        if isinstance(area, Mapping) and area.get("VAH") is not None and area.get("VAL") is not None:
            payload = dict(area)
            payloads.append(payload)
            label = _value_area_reference(payload, idx)
            profile_labels.append(label)

    elapsed = perf_counter() - start_time
    if profile_labels:
        preview = ", ".join(profile_labels[:5])
        if len(profile_labels) > 5:
            preview = f"{preview}, …"
        session_summary = f" | sessions={preview}"
    else:
        session_summary = ""

    logger.info(
        "Market profile payloads ready | symbol=%s | profiles=%d | merged=%s | duration=%s%s",
        symbol,
        len(payloads),
        use_merged,
        _format_duration(elapsed),
        session_summary,
    )

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

        start_time = perf_counter()

        if value_areas is not None:
            payloads = list(value_areas)
            payload_source = "provided"
            payload_duration = 0.0
        else:
            payload_start = perf_counter()
            payloads = build_value_area_payloads(
                self.indicator,
                df,
                interval=getattr(self.indicator, "interval", None),
                use_merged=config.get("market_profile_use_merged_value_areas"),
                merge_threshold=config.get("market_profile_merge_threshold"),
                min_merge_sessions=config.get("market_profile_merge_min_sessions"),
            )
            payload_duration = perf_counter() - payload_start
            payload_source = "computed"

        if payloads:
            logger.info(
                "Market profile signal payload summaries (%d):",
                len(payloads),
            )
            for idx, payload in enumerate(payloads, start=1):
                logger.info(
                    "  [%d] %s",
                    idx,
                    MarketProfileIndicator.describe_profile(payload),
                )
        else:
            logger.info("Market profile signal payload summaries: none")

        rules_start = perf_counter()
        signals = run_indicator_rules(
            self.indicator,
            df,
            rule_payloads=payloads,
            symbol=self.symbol,
            **config,
        )
        rules_duration = perf_counter() - rules_start
        total_duration = perf_counter() - start_time

        logger.info(
            "Market profile signals | symbol=%s | profiles=%d | signals=%d | payload_source=%s | "
            "durations[payloads=%s, rules=%s, total=%s]",
            self.symbol,
            len(payloads),
            len(signals),
            payload_source,
            "n/a" if payload_source == "provided" else _format_duration(payload_duration),
            _format_duration(rules_duration),
            _format_duration(total_duration),
        )

        return signals

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


def _bias_label_from_direction(direction: Optional[str], fallback: Optional[str] = None) -> Optional[str]:
    """Translate raw direction hints into a Long/Short bias label."""

    hint = direction or fallback
    if not hint:
        return None

    text = str(hint).strip().lower()
    if text in {"above", "up", "long", "buy", "support"}:
        return "Long"
    if text in {"below", "down", "short", "sell", "resistance"}:
        return "Short"
    return None


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


def _value_area_reference(area: Mapping[str, Any], index: int) -> str:
    """Return a human-readable label for a value area payload."""

    for key in (
        "session_label",
        "session",
        "session_start",
        "profile_start",
        "date",
        "value_area_id",
    ):
        value = area.get(key)
        if value not in (None, ""):
            return str(value)

    return f"profile-{index + 1}"


def _market_profile_overlay_adapter(
    signals: List[BaseSignal],
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

    for idx, sig in enumerate(signals):
        metadata = sig.metadata or {}
        if metadata.get("source") != "MarketProfile":
            summary["skipped_source"] += 1
            continue

        level_price = _resolve_level_price(metadata)
        if level_price is None:
            summary["skipped_price"] += 1
            continue

        marker_time = _to_epoch_seconds(sig.time)
        if marker_time is None:
            summary["skipped_time"] += 1
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

            bias_label = _bias_label_from_direction(metadata.get("direction"), fallback=pointer_hint or retest_role)

            bubbles.append(
                {
                    "time": marker_time,
                    "price": float(anchor_price),
                    "label": f"{level_label} retest",
                    "detail": detail,
                    "meta": meta_label,
                    "accentColor": color,
                    "backgroundColor": _rgba_from_hex(color, 0.18) or "rgba(14,165,233,0.25)",
                    "textColor": "#ffffff",
                    "direction": metadata.get("pointer_direction") or bubble_direction,
                    "bias": bias_label,
                    "subtype": "bubble",
                }
            )
            summary["converted_retest"] += 1
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

        bias_label = _bias_label_from_direction(breakout_direction or metadata.get("direction"))

        pointer_hint = metadata.get("pointer_direction") or breakout_direction or metadata.get("direction")
        bubbles.append(
            {
                "time": marker_time,
                "price": bubble_price,
                "label": label,
                "detail": detail,
                "meta": meta_text,
                "accentColor": color,
                "backgroundColor": _rgba_from_hex(color, 0.2) or "rgba(30,41,59,0.75)",
                "textColor": "#ffffff",
                "direction": pointer_hint,
                "bias": bias_label,
                "subtype": "bubble",
            }
        )
        summary["converted_breakout"] += 1

    duration = perf_counter() - start_time
    symbol = next((sig.symbol for sig in signals if getattr(sig, "symbol", None)), None)

    logger.info(
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
        _format_duration(duration),
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


register_indicator_rules(
    MarketProfileIndicator.NAME,
    rules=[market_profile_breakout_rule, market_profile_retest_rule],
    overlay_adapter=_market_profile_overlay_adapter,
)
