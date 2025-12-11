"""Risk management utilities for trading systems."""

from .math import (
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
