"""Market Profile signal configuration shared by runtime consumers."""

from __future__ import annotations

from dataclasses import dataclass


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


BREAKOUT_CACHE_KEY = "market_profile_breakouts"
BREAKOUT_CACHE_INITIALIZED_FLAG = "_market_profile_breakouts_initialised"
BREAKOUT_READY_FLAG = "_market_profile_breakouts_ready"


__all__ = [
    "MarketProfileBreakoutConfig",
    "BREAKOUT_CACHE_KEY",
    "BREAKOUT_CACHE_INITIALIZED_FLAG",
    "BREAKOUT_READY_FLAG",
]
