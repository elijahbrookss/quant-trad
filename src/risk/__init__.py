"""Risk management utilities for trading systems."""

from .config import DEFAULT_RISK_CONFIG, normalise_risk_config
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
    "DEFAULT_RISK_CONFIG",
    "clamp_stop",
    "direction_sign",
    "normalise_risk_config",
    "price_from_r",
    "r_value_from_atr",
    "ticks_for_r",
    "ticks_from_entry",
    "trailing_stop_price",
]
