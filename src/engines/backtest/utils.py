"""Utility functions for backtest engine."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional

TIMEFRAME_MULTIPLIERS = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 60 * 60 * 24,
    "w": 60 * 60 * 24 * 7,
}


def isoformat(value: Optional[datetime]) -> Optional[str]:
    """Return a UTC ISO8601 string with Z suffix for *value*."""

    if value is None:
        return None
    target = value
    if target.tzinfo is None:
        return target.replace(tzinfo=None).isoformat() + "Z"
    return target.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    """Safely convert value to float with fallback."""
    try:
        if value is None:
            return default
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric


def instrument_key(datasource: Optional[str], exchange: Optional[str], symbol: Optional[str]) -> str:
    """Generate a unique instrument identifier key."""
    return "::".join(
        [
            (datasource or "").strip().lower(),
            (exchange or "").strip().lower(),
            (symbol or "").strip().upper(),
        ]
    )


def timeframe_to_seconds(label: Optional[str]) -> Optional[int]:
    """Convert timeframe strings like '15m' or '4h' into seconds."""

    if not label:
        return None
    value = str(label).strip().lower()
    if not value:
        return None
    match = re.fullmatch(r"(\d+)([a-z]+)", value)
    if not match:
        return None
    amount = int(match.group(1))
    suffix = match.group(2)
    key = suffix[0]
    multiplier = TIMEFRAME_MULTIPLIERS.get(key)
    if not multiplier:
        return None
    return amount * multiplier


def timeframe_duration(label: Optional[str]) -> Optional[timedelta]:
    """Convert timeframe string to timedelta."""
    seconds = timeframe_to_seconds(label)
    if not seconds:
        return None
    return timedelta(seconds=seconds)
