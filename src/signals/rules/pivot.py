"""Pivot level based signal rules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional

import pandas as pd

from indicators.pivot_level import Level, PivotLevelIndicator


@dataclass(frozen=True)
class PivotBreakoutConfig:
    """Configuration for validating pivot level breakouts."""

    confirmation_bars: int = 1

    def __post_init__(self) -> None:  # pragma: no cover - dataclass guard
        if self.confirmation_bars < 1:
            raise ValueError("confirmation_bars must be >= 1")


_DEFAULT_CONFIG = PivotBreakoutConfig()


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

    return PivotBreakoutConfig(confirmation_bars=confirmation_bars)


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


def _level_out_of_range(mask: pd.Series) -> bool:
    return bool(mask.all()) if len(mask) else False


def _evaluate_level(
    df: pd.DataFrame,
    level: Level,
    confirmation_bars: int,
) -> Optional[Dict[str, Any]]:
    if "close" not in df.columns:
        raise KeyError("DataFrame must contain a 'close' column for pivot breakout rule")

    if len(df) <= confirmation_bars:
        return None

    closes = df["close"]
    recent = closes.iloc[-confirmation_bars:]
    prev_close = closes.iloc[-confirmation_bars - 1]

    if level.kind == "support":
        out_of_range = recent < level.price
        was_in_range = prev_close >= level.price
        breakout_side = "below"
    else:  # treat everything else as resistance
        out_of_range = recent > level.price
        was_in_range = prev_close <= level.price
        breakout_side = "above"

    if not was_in_range or not _level_out_of_range(out_of_range):
        return None

    breakout_start_idx = df.index[-confirmation_bars]
    breakout_end_idx = df.index[-1]
    last_bar = df.iloc[-1]

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
    }

    for column in ("open", "high", "low", "volume"):
        if column in df.columns:
            meta[f"trigger_{column}"] = float(last_bar[column])

    return meta


def pivot_breakout_rule(
    context: Mapping[str, Any],
    _: Any = None,
) -> List[Dict[str, Any]]:
    """Detect breakouts through pivot levels using closing price confirmation."""

    indicator = context.get("indicator")
    if not isinstance(indicator, PivotLevelIndicator):
        return []

    df = context.get("df")
    if df is None or df.empty:
        return []

    levels = _ensure_indicator_levels(indicator)
    if not levels:
        return []

    symbol = _select_symbol(context, indicator)
    if symbol is None:
        return []

    config = _resolve_config(context)
    confirmation_bars = config.confirmation_bars

    results: List[Dict[str, Any]] = []
    for level in levels:
        meta = _evaluate_level(df, level, confirmation_bars)
        if not meta:
            continue

        breakout_time = df.index[-1]
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

    return results


__all__ = ["PivotBreakoutConfig", "pivot_breakout_rule"]
