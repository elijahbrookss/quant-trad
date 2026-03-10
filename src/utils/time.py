"""Time and timestamp utilities for consistent datetime handling across the codebase."""

from __future__ import annotations

from datetime import timezone
from typing import Any

import pandas as pd


def ts_to_iso(ts: Any) -> str:
    """Convert timestamp to ISO 8601 string in UTC.

    Args:
        ts: Timestamp (pandas.Timestamp, datetime, or timestamp-like)

    Returns:
        ISO 8601 formatted string with 'Z' suffix

    Example:
        >>> ts_to_iso("2024-01-15 10:30:00")
        '2024-01-15T10:30:00Z'
    """
    stamp = pd.Timestamp(ts)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.isoformat().replace("+00:00", "Z")


def ts_to_business_day(ts: Any) -> str:
    """Convert timestamp to business day string (YYYY-MM-DD).

    Args:
        ts: Timestamp (pandas.Timestamp, datetime, or timestamp-like)

    Returns:
        ISO date string (YYYY-MM-DD)

    Example:
        >>> ts_to_business_day("2024-01-15 10:30:00")
        '2024-01-15'
    """
    return pd.Timestamp(ts).tz_convert("UTC").date().isoformat()


def ts_to_unix(ts: Any) -> int:
    """Convert timestamp to Unix epoch seconds.

    Args:
        ts: Timestamp (pandas.Timestamp, datetime, or timestamp-like)

    Returns:
        Unix timestamp in seconds

    Example:
        >>> ts_to_unix("2024-01-15 00:00:00")
        1705276800
    """
    return int(pd.Timestamp(ts).tz_convert("UTC").timestamp())


def normalize_timestamp(value: Any) -> pd.Timestamp:
    """Normalize any timestamp-like value to UTC pandas.Timestamp.

    Args:
        value: Timestamp (string, datetime, pandas.Timestamp, etc.)

    Returns:
        UTC-aware pandas.Timestamp

    Example:
        >>> normalize_timestamp("2024-01-15")
        Timestamp('2024-01-15 00:00:00+0000', tz='UTC')
    """
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp


__all__ = [
    "ts_to_iso",
    "ts_to_business_day",
    "ts_to_unix",
    "normalize_timestamp",
]
