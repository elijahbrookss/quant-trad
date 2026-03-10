"""Shared risk math utilities for R/ATR driven risk management.

This module is a compatibility wrapper that re-exports functionality from src.risk.
All core risk calculation logic now lives in the src/ library.
"""

from __future__ import annotations

# Re-export from src.risk
from risk import (
    clamp_stop,
    direction_sign,
    price_from_r,
    r_value_from_atr,
    ticks_for_r,
    ticks_from_entry,
    trailing_stop_price,
)

__all__ = [
    "clamp_stop",
    "direction_sign",
    "price_from_r",
    "r_value_from_atr",
    "ticks_for_r",
    "ticks_from_entry",
    "trailing_stop_price",
]
