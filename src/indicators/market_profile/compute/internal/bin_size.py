"""
Bin size inference and selection logic.

Pure functions for determining appropriate price bucket sizes.
"""

import math
from typing import Optional, Tuple

import numpy as np
import pandas as pd


def normalize_step(value: float) -> float:
    """
    Normalize a step size to a nice round number.

    Rounds to 1, 2, 5, or 10 times a power of 10.

    Args:
        value: Input step value

    Returns:
        Normalized step value
    """
    if not math.isfinite(value) or value <= 0:
        return 0.1

    exponent = math.floor(math.log10(value))
    mantissa = value / (10 ** exponent)

    if mantissa < 1.5:
        mantissa = 1
    elif mantissa < 3:
        mantissa = 2
    elif mantissa < 7:
        mantissa = 5
    else:
        mantissa = 10

    return mantissa * (10 ** exponent)


def infer_bin_size(df: pd.DataFrame) -> float:
    """
    Infer appropriate bin size from OHLC data.

    Analyzes price range and typical spreads to determine a suitable bucket size.

    Args:
        df: DataFrame with OHLC data

    Returns:
        Inferred bin size
    """
    highs = pd.to_numeric(df.get("high"), errors="coerce")
    lows = pd.to_numeric(df.get("low"), errors="coerce")
    closes = pd.to_numeric(df.get("close"), errors="coerce")

    highs = highs.dropna()
    lows = lows.dropna()
    closes = closes.dropna()

    if highs.empty or lows.empty:
        return 0.1

    span = float(highs.max() - lows.min())
    if not math.isfinite(span) or span <= 0:
        base_price = float(closes.median()) if not closes.empty else 1.0
        span = max(abs(base_price) * 0.05, 1e-6)

    spreads = (highs - lows).abs()
    spreads = spreads.replace(0, np.nan).dropna()
    characteristic = float(spreads.median()) if not spreads.empty else span / max(len(df), 1)
    characteristic = max(characteristic, span / 50, 1e-8)

    step = max(normalize_step(characteristic), 1e-8)

    # Prevent excessive bins
    max_bins = 2000
    if span / step > max_bins:
        step = max(normalize_step(span / max_bins), 1e-8)

    return step


def select_bin_size(
    df: pd.DataFrame,
    provided: Optional[float]
) -> Tuple[float, bool]:
    """
    Select bin size, using provided value or inferring from data.

    Args:
        df: DataFrame with OHLC data
        provided: User-provided bin size (optional)

    Returns:
        Tuple of (bin_size, is_locked) where is_locked indicates user provided the value
    """
    candidate = provided

    # Coerce string inputs
    if isinstance(candidate, str):
        candidate = candidate.strip()
        if not candidate:
            candidate = None

    # Try to use provided value
    if candidate is not None:
        try:
            numeric = float(candidate)
        except (TypeError, ValueError):
            numeric = None
        if numeric is not None and numeric > 0:
            return numeric, True  # Locked to user value

    # Fallback to inference
    return infer_bin_size(df), False


def infer_precision_from_step(step: float) -> int:
    """
    Infer decimal precision from step size.

    Args:
        step: Step size value

    Returns:
        Number of decimal places for formatting
    """
    if not math.isfinite(step) or step <= 0:
        return 4

    exponent = math.floor(math.log10(step))
    if exponent >= 0:
        return 2

    return min(8, abs(exponent) + 2)
