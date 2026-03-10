"""Shared risk math utilities for R/ATR driven risk management."""

from __future__ import annotations

from typing import Optional


def direction_sign(direction: str) -> int:
    """Return +1 for longs and -1 for shorts."""

    return 1 if str(direction).lower() == "long" else -1


def r_value_from_atr(atr: Optional[float], atr_multiple: Optional[float]) -> Optional[float]:
    """Calculate the monetary value of 1R from ATR and configured multiple."""

    if atr in (None, 0) or atr_multiple in (None, 0):
        return None
    return float(atr) * float(atr_multiple)


def ticks_for_r(r_value: Optional[float], tick_size: Optional[float]) -> Optional[float]:
    """Convert an R value into ticks using *tick_size* if possible."""

    if r_value in (None, 0) or tick_size in (None, 0):
        return None
    return float(r_value) / float(tick_size)


def price_from_r(entry_price: float, direction: str, r_value: float, r_multiple: float) -> float:
    """Translate an R multiple into an absolute price."""

    sign = direction_sign(direction)
    return entry_price + sign * r_value * float(r_multiple)


def ticks_from_entry(entry_price: float, price: float, direction: str, tick_size: Optional[float]) -> float:
    """Return signed ticks from entry to *price* respecting *direction*."""

    if tick_size in (None, 0):
        return 0.0
    sign = direction_sign(direction)
    return ((price - entry_price) / float(tick_size)) * sign


def clamp_stop(current_stop: float, candidate: float, direction: str) -> float:
    """Tighten stop *current_stop* toward *candidate* without loosening."""

    if direction_sign(direction) > 0:
        return max(current_stop, candidate)
    return min(current_stop, candidate)


def trailing_stop_price(
    best_price: float, direction: str, atr_value: Optional[float], atr_multiple: Optional[float]
) -> Optional[float]:
    """Return a trailing stop price based on the best favorable price and ATR multiple."""

    if atr_value in (None, 0) or atr_multiple in (None, 0):
        return None
    distance = float(atr_value) * float(atr_multiple)
    if direction_sign(direction) > 0:
        return best_price - distance
    return best_price + distance
