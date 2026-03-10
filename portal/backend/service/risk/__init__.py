"""Risk utilities and ATM template helpers."""

from .atm import merge_templates, normalise_template, template_metrics, DEFAULT_ATM_TEMPLATE
from .risk_math import (
    clamp_stop,
    direction_sign,
    price_from_r,
    r_value_from_atr,
    ticks_for_r,
    ticks_from_entry,
    trailing_stop_price,
)

__all__ = [
    "DEFAULT_ATM_TEMPLATE",
    "normalise_template",
    "merge_templates",
    "template_metrics",
    "clamp_stop",
    "direction_sign",
    "price_from_r",
    "r_value_from_atr",
    "ticks_for_r",
    "ticks_from_entry",
    "trailing_stop_price",
]
