"""Pivot level based signal rules."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd

from indicators.pivot_level import Level, PivotLevelIndicator
from signals.base import BaseSignal


@dataclass(frozen=True)
class PivotBreakoutConfig:
    """Configuration for validating pivot level breakouts."""

    confirmation_bars: int = 1

    def __post_init__(self) -> None:  # pragma: no cover - dataclass guard
        if self.confirmation_bars < 1:
            raise ValueError("confirmation_bars must be >= 1")


log = logging.getLogger("PivotBreakoutRule")

_DEFAULT_CONFIG = PivotBreakoutConfig()


def _summarise_level(level: Level) -> str:
    return "|".join(
        [
            f"px={getattr(level, 'price', 'na'):.5f}" if hasattr(level, "price") else "px=na",
            f"kind={getattr(level, 'kind', 'na')}",
            f"lb={getattr(level, 'lookback', 'na')}",
            f"tf={getattr(level, 'timeframe', 'na')}",
        ]
    )


def _build_run_id(indicator: PivotLevelIndicator, df: pd.DataFrame, symbol: str) -> str:
    last_index = df.index[-1] if len(df.index) else "na"
    return "|".join(
        [
            "pivotbrk",
            f"symbol={symbol}",
            f"trace={getattr(indicator, 'trace_id', 'na')}",
            f"end={last_index}",
        ]
    )


def _to_datetime(value: Any) -> Any:
    """Return a native datetime for pandas timestamps."""

    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    return value


def _resolve_config(context: Mapping[str, Any]) -> PivotBreakoutConfig:
    config = context.get("pivot_breakout_config")
    if isinstance(config, PivotBreakoutConfig):
        return config

    confirmation_bars = context.get(
        "pivot_breakout_confirmation_bars",
        _DEFAULT_CONFIG.confirmation_bars,
    )
    try:
        confirmation_bars = int(confirmation_bars)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        confirmation_bars = _DEFAULT_CONFIG.confirmation_bars

    if confirmation_bars < 1:
        confirmation_bars = _DEFAULT_CONFIG.confirmation_bars

    resolved = PivotBreakoutConfig(confirmation_bars=confirmation_bars)
    log.debug(
        "pivotbrk | config_resolved | confirmation_bars=%d | context_keys=%s",
        resolved.confirmation_bars,
        sorted(context.keys()),
    )
    return resolved


def _ensure_indicator_levels(indicator: Any) -> Iterable[Level]:
    levels = getattr(indicator, "levels", None)
    if not levels:
        return []
    return levels


def _select_symbol(context: Mapping[str, Any], indicator: Any) -> Optional[str]:
    symbol = context.get("symbol")
    if symbol:
        return symbol
    return getattr(indicator, "symbol", None)


def _evaluate_level(
    df: pd.DataFrame,
    level: Level,
    confirmation_bars: int,
    *,
    mode: str = "backtest",
) -> Optional[Dict[str, Any]]:
    if "close" not in df.columns:
        raise KeyError("DataFrame must contain a 'close' column for pivot breakout rule")

    if len(df) <= confirmation_bars:
        log.debug(
            "pivotbrk | level_skip | reason=insufficient_bars | required=%d | available=%d",
            confirmation_bars + 1,
            len(df),
        )
        return None

    closes = df["close"]

    if level.kind == "support":
        in_range = closes >= level.price
        out_of_range_mask = closes < level.price
        breakout_side = "below"
    else:  # treat everything else as resistance
        in_range = closes <= level.price
        out_of_range_mask = closes > level.price
        breakout_side = "above"

    level_id = _summarise_level(level)
    last_idx_position = len(closes) - 1
    simulate_current_only = mode in {"sim", "live"}

    consecutive = 0
    for position, (index, is_out_of_range) in enumerate(out_of_range_mask.items()):
        if is_out_of_range:
            consecutive += 1
        else:
            consecutive = 0

        if consecutive < confirmation_bars:
            continue

        breakout_start_pos = position - confirmation_bars + 1
        prev_position = breakout_start_pos - 1

        if prev_position < 0:
            log.debug(
                "pivotbrk | level_skip | level=%s | reason=no_prior_bar | position=%d",
                level_id,
                position,
            )
            continue

        prev_index = closes.index[prev_position]
        if not bool(in_range.loc[prev_index]):
            log.debug(
                "pivotbrk | level_skip | level=%s | reason=never_in_range | prev_index=%s",
                level_id,
                prev_index,
            )
            continue

        if simulate_current_only and position != last_idx_position:
            # In live/sim modes only emit for the latest candle.
            continue

        breakout_start_idx = closes.index[breakout_start_pos]
        breakout_end_idx = index
        last_bar = df.loc[breakout_end_idx]

        meta: Dict[str, Any] = {
            "level_kind": level.kind,
            "level_price": float(level.price),
            "breakout_direction": breakout_side,
            "confirmation_bars_required": confirmation_bars,
            "bars_closed_beyond_level": confirmation_bars,
            "breakout_start": _to_datetime(breakout_start_idx),
            "level_lookback": getattr(level, "lookback", None),
            "level_timeframe": getattr(level, "timeframe", None),
            "level_first_touched": _to_datetime(getattr(level, "first_touched", None)),
            "trigger_close": float(last_bar["close"]),
            "trigger_time": _to_datetime(breakout_end_idx),
        }

        for column in ("open", "high", "low", "volume"):
            if column in df.columns:
                meta[f"trigger_{column}"] = float(last_bar[column])

        log.debug(
            "pivotbrk | level_breakout | level=%s | direction=%s | trigger_close=%.5f",
            level_id,
            breakout_side,
            last_bar["close"],
        )

        return meta

    log.debug(
        "pivotbrk | level_skip | level=%s | reason=no_breakout | confirmation_bars=%d",
        level_id,
        confirmation_bars,
    )
    return None


def pivot_breakout_rule(
    context: Mapping[str, Any],
    _: Any = None,
) -> List[Dict[str, Any]]:
    """Detect breakouts through pivot levels using closing price confirmation."""

    indicator = context.get("indicator")
    if not isinstance(indicator, PivotLevelIndicator):
        log.debug(
            "pivotbrk | guard_fail | reason=indicator_type | indicator=%r",
            type(indicator),
        )
        return []

    df = context.get("df")
    if df is None or df.empty:
        log.debug(
            "pivotbrk | guard_fail | reason=missing_df | indicator=%s",
            getattr(indicator, "NAME", type(indicator)),
        )
        return []

    levels = _ensure_indicator_levels(indicator)
    if not levels:
        log.debug(
            "pivotbrk | guard_fail | reason=no_levels | indicator_trace=%s",
            getattr(indicator, "trace_id", "unknown"),
        )
        return []

    symbol = _select_symbol(context, indicator)
    if symbol is None:
        log.debug(
            "pivotbrk | guard_fail | reason=no_symbol | indicator_trace=%s",
            getattr(indicator, "trace_id", "unknown"),
        )
        return []

    config = _resolve_config(context)
    confirmation_bars = config.confirmation_bars

    run_id = _build_run_id(indicator, df, symbol)
    log.debug(
        "%s | run_start | levels=%d | confirmation_bars=%d",
        run_id,
        len(levels),
        confirmation_bars,
    )

    mode = str(context.get("mode", "backtest")).lower()

    results: List[Dict[str, Any]] = []
    for level in levels:
        level_id = _summarise_level(level)
        log.debug("%s | level_eval | level=%s", run_id, level_id)
        meta = _evaluate_level(df, level, confirmation_bars, mode=mode)
        if not meta:
            log.debug("%s | level_eval_complete | level=%s | breakout=False", run_id, level_id)
            continue

        breakout_time = meta.get("trigger_time", df.index[-1])
        results.append(
            {
                "type": "breakout",
                "symbol": symbol,
                "time": _to_datetime(breakout_time),
                "source": getattr(indicator, "NAME", indicator.__class__.__name__),
                "direction": level.kind,
                **meta,
            }
        )
        log.debug("%s | level_eval_complete | level=%s | breakout=True", run_id, level_id)

    log.debug("%s | run_complete | signals=%d", run_id, len(results))
    return results


def _to_unix_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        ts = pd.Timestamp(value)
    except Exception:  # pragma: no cover - defensive guard
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.timestamp())


_BREAKOUT_COLORS = {
    "above": "#16a34a",  # green
    "below": "#dc2626",  # red
}


def pivot_signals_to_overlays(
    signals: Sequence[BaseSignal],
    plot_df: "pd.DataFrame",
    **ignored: Any,
) -> List[Dict[str, Any]]:
    """Convert pivot breakout signals into Lightweight Charts overlay payloads."""

    if not signals:
        return []

    markers: List[Dict[str, Any]] = []
    price_lines: List[Dict[str, Any]] = []

    index = getattr(plot_df, "index", None)

    for signal in signals:
        metadata = signal.metadata or {}
        level_price = metadata.get("level_price")
        if level_price is None:
            continue

        breakout_direction = metadata.get("breakout_direction")
        color = _BREAKOUT_COLORS.get(breakout_direction, "#6b7280")  # gray fallback

        marker_time = _to_unix_seconds(signal.time)
        level_kind = str(metadata.get("level_kind", "pivot")).capitalize()
        marker_position = "belowBar" if breakout_direction == "above" else "aboveBar"
        marker_shape = "triangleUp" if breakout_direction == "above" else "triangleDown"

        markers.append(
            {
                "time": marker_time,
                "price": float(level_price),
                "color": color,
                "position": marker_position,
                "shape": marker_shape,
                "text": f"{level_kind} breakout",
                "subtype": "signal",
            }
        )

        start_time = metadata.get("breakout_start")
        origin_time = _to_unix_seconds(start_time)
        if origin_time is None and index is not None and len(index):
            origin_time = _to_unix_seconds(index[0])
        if origin_time is None:
            origin_time = marker_time
        end_time = marker_time

        if origin_time and end_time and origin_time > end_time:
            origin_time, end_time = end_time, origin_time

        price_lines.append(
            {
                "price": float(level_price),
                "color": color,
                "lineStyle": 0,
                "lineWidth": 2,
                "axisLabelVisible": True,
                "title": f"{level_kind} lvl",
                "extend": "none",
                "originTime": origin_time,
                "endTime": end_time,
            }
        )

    if not markers and not price_lines:
        return []

    payload = {
        "price_lines": price_lines,
        "markers": markers,
    }

    return [
        {
            "type": PivotLevelIndicator.NAME,
            "payload": payload,
        }
    ]


def register_pivot_indicator(force: bool = False) -> None:
    """Ensure the pivot breakout rule and overlays are registered with the engine."""

    try:
        from signals.engine import signal_generator
    except ImportError:  # pragma: no cover - defensive guard
        return

    if not force and PivotLevelIndicator.NAME in signal_generator._REGISTRY:
        registration = signal_generator._REGISTRY[PivotLevelIndicator.NAME]
        if registration.overlay_adapter is not None:
            return

    from signals.engine.signal_generator import register_indicator_rules

    try:
        register_indicator_rules(
            PivotLevelIndicator.NAME,
            rules=[pivot_breakout_rule],
            overlay_adapter=pivot_signals_to_overlays,
        )
    except ValueError:
        if force:
            # Re-register by clearing and setting explicitly.
            signal_generator._REGISTRY[PivotLevelIndicator.NAME] = signal_generator.IndicatorRegistration(  # type: ignore[attr-defined]
                rules=(pivot_breakout_rule,),
                overlay_adapter=pivot_signals_to_overlays,
            )
        # otherwise keep existing registration


register_pivot_indicator()


__all__ = [
    "PivotBreakoutConfig",
    "pivot_breakout_rule",
    "pivot_signals_to_overlays",
    "register_pivot_indicator",
]

