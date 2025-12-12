"""Configuration helpers for Market Profile breakouts."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

log = logging.getLogger("MarketProfileRules")


@dataclass(frozen=True)
class MarketProfileBreakoutConfig:
    """Configuration for Market Profile breakout confirmations."""

    confirmation_bars: int = 3
    early_confirmation_window: int = 3
    early_confirmation_distance_pct: float = 0.01
    require_full_candle_confirmation: bool = False
    accelerated_confirmation_min_bars: int = 2

    def __post_init__(self) -> None:  # pragma: no cover - dataclass guard
        if self.confirmation_bars < 1:
            raise ValueError("confirmation_bars must be >= 1")
        if self.early_confirmation_window < 1:
            raise ValueError("early_confirmation_window must be >= 1")
        if self.early_confirmation_distance_pct < 0:
            raise ValueError("early_confirmation_distance_pct must be >= 0")
        if self.accelerated_confirmation_min_bars < 1:
            raise ValueError("accelerated_confirmation_min_bars must be >= 1")
        object.__setattr__(
            self,
            "accelerated_confirmation_min_bars",
            min(self.confirmation_bars, self.accelerated_confirmation_min_bars),
        )


_DEFAULT_BREAKOUT_CONFIG = MarketProfileBreakoutConfig()


def resolve_breakout_config(context: Mapping[str, Any]) -> MarketProfileBreakoutConfig:
    explicit_confirmation = "market_profile_breakout_confirmation_bars" in context
    confirmation = context.get(
        "market_profile_breakout_confirmation_bars",
        _DEFAULT_BREAKOUT_CONFIG.confirmation_bars,
    )
    try:
        confirmation = int(confirmation)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        confirmation = _DEFAULT_BREAKOUT_CONFIG.confirmation_bars
    if confirmation < 1:
        confirmation = _DEFAULT_BREAKOUT_CONFIG.confirmation_bars
    mode_str = str(context.get("mode", "backtest")).lower()
    if not explicit_confirmation and mode_str in {"live", "sim"}:
        confirmation = 1

    early_window = context.get(
        "market_profile_breakout_early_window",
        _DEFAULT_BREAKOUT_CONFIG.early_confirmation_window,
    )
    try:
        early_window = int(early_window)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        early_window = _DEFAULT_BREAKOUT_CONFIG.early_confirmation_window
    if early_window < 1:
        early_window = _DEFAULT_BREAKOUT_CONFIG.early_confirmation_window

    early_pct = context.get(
        "market_profile_breakout_early_distance_pct",
        _DEFAULT_BREAKOUT_CONFIG.early_confirmation_distance_pct,
    )
    try:
        early_pct = float(early_pct)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        early_pct = _DEFAULT_BREAKOUT_CONFIG.early_confirmation_distance_pct
    if early_pct < 0:
        early_pct = _DEFAULT_BREAKOUT_CONFIG.early_confirmation_distance_pct

    require_full_candle = context.get("market_profile_breakout_require_full_candle")
    if require_full_candle is None:
        require_full_candle = _DEFAULT_BREAKOUT_CONFIG.require_full_candle_confirmation
    elif isinstance(require_full_candle, str):
        require_full_candle = require_full_candle.strip().lower() in {"1", "true", "yes", "on"}
    else:
        require_full_candle = bool(require_full_candle)

    accel_min = context.get(
        "market_profile_breakout_acceleration_min_bars",
        _DEFAULT_BREAKOUT_CONFIG.accelerated_confirmation_min_bars,
    )
    try:
        accel_min = int(accel_min)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        accel_min = _DEFAULT_BREAKOUT_CONFIG.accelerated_confirmation_min_bars
    if accel_min < 1:
        accel_min = _DEFAULT_BREAKOUT_CONFIG.accelerated_confirmation_min_bars
    accel_min = min(accel_min, confirmation)

    resolved = MarketProfileBreakoutConfig(
        confirmation_bars=confirmation,
        early_confirmation_window=early_window,
        early_confirmation_distance_pct=early_pct,
        require_full_candle_confirmation=bool(require_full_candle),
        accelerated_confirmation_min_bars=accel_min,
    )
    log.debug(
        (
            "mp_brk | config_resolved | confirmation_bars=%d | early_window=%d "
            "| early_pct=%.5f | require_full_candle=%s | accel_min=%d"
        ),
        resolved.confirmation_bars,
        resolved.early_confirmation_window,
        resolved.early_confirmation_distance_pct,
        resolved.require_full_candle_confirmation,
        resolved.accelerated_confirmation_min_bars,
    )
    return resolved


__all__ = ["MarketProfileBreakoutConfig", "resolve_breakout_config", "_DEFAULT_BREAKOUT_CONFIG"]
