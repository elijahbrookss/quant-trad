"""Time and numeric helpers for bot runtime domain."""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

_TIMEFRAME_MULTIPLIERS = {
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
    """Attempt to cast *value* to float and fall back to *default* on failure."""

    try:
        if value is None:
            return default
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric


def coalesce_numeric(*values: Optional[float], default: float = 0.0, allow_zero: bool = False) -> float:
    """Return the first non-None, non-zero value, or default.

    Args:
        *values: Values to check in order of precedence
        default: Value to return if all inputs are None or zero
        allow_zero: If True, treat 0 as a valid value (don't skip it)

    Returns:
        First valid value or default
    """
    for value in values:
        if value is None:
            continue
        if not allow_zero and value == 0:
            continue
        return float(value)
    return default


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
    multiplier = _TIMEFRAME_MULTIPLIERS.get(key)
    if not multiplier:
        return None
    return amount * multiplier


def timeframe_duration(label: Optional[str]) -> Optional[timedelta]:
    seconds = timeframe_to_seconds(label)
    if not seconds:
        return None
    return timedelta(seconds=seconds)


def normalize_epoch(value: Any) -> Optional[int]:
    """Convert various timestamp formats to Unix epoch (seconds since 1970-01-01 UTC).

    Handles:
    - None or empty string -> None
    - int/float -> int (already epoch)
    - numeric string -> int
    - ISO 8601 string -> epoch via parsing

    Args:
        value: Timestamp in various formats

    Returns:
        Unix epoch timestamp in seconds, or None if invalid
    """
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        # Treat 13-digit unix values as milliseconds.
        if abs(numeric) > 2e10:
            numeric = numeric / 1000.0
        return int(numeric)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        numeric = float(text)
        if abs(numeric) > 2e10:
            numeric = numeric / 1000.0
        return int(numeric)
    try:
        numeric = float(text)
        if not math.isfinite(numeric):
            return None
        if abs(numeric) > 2e10:
            numeric = numeric / 1000.0
        return int(numeric)
    except (TypeError, ValueError):
        pass
    try:
        if text.endswith("Z"):
            text = text[:-1]
        parsed = datetime.fromisoformat(text)
        return int(parsed.timestamp())
    except ValueError:
        return None


__all__ = [
    "coalesce_numeric",
    "coerce_float",
    "isoformat",
    "normalize_epoch",
    "timeframe_duration",
    "timeframe_to_seconds",
]
