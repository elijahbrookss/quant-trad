"""
Core TPO computation logic for Market Profile.

Pure functions with no dependencies on indicator class or UI concerns.
"""

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

from ..models import ValueArea

logger = logging.getLogger(__name__)


def build_tpo_histogram(
    data: pd.DataFrame,
    bin_size: float,
    bin_precision: int
) -> Dict[float, int]:
    """
    Count how many bars visit each price bucket defined by bin_size.

    Args:
        data: DataFrame with 'low' and 'high' columns for one session
        bin_size: Price bucket size
        bin_precision: Decimal precision for rounding buckets

    Returns:
        Dictionary mapping price bucket -> count of TPO occurrences
    """
    tpo_counts: Dict[float, int] = {}
    logger.debug("Building TPO histogram for session with %d bars", len(data))
    if data is None or data.empty or bin_size <= 0:
        logger.debug("Built TPO histogram with %d buckets", len(tpo_counts))
        return tpo_counts

    lows = data["low"].to_numpy(dtype=float, copy=False)
    highs = data["high"].to_numpy(dtype=float, copy=False)
    finite_mask = np.isfinite(lows) & np.isfinite(highs)
    if not finite_mask.any():
        logger.debug("Built TPO histogram with %d buckets", len(tpo_counts))
        return tpo_counts

    lows = lows[finite_mask]
    highs = highs[finite_mask]
    lows, highs = np.minimum(lows, highs), np.maximum(lows, highs)

    spans = np.maximum(highs - lows, 0.0)
    steps = np.floor(spans / bin_size + 1e-9).astype(np.int64)
    run_lengths = steps + 1
    total_bins = int(run_lengths.sum())
    if total_bins <= 0:
        logger.debug("Built TPO histogram with %d buckets", len(tpo_counts))
        return tpo_counts

    start_scaled = np.rint(lows / bin_size).astype(np.int64)
    all_scaled = np.empty(total_bins, dtype=np.int64)
    cursor = 0
    for scaled_start, length in zip(start_scaled.tolist(), run_lengths.tolist()):
        next_cursor = cursor + length
        all_scaled[cursor:next_cursor] = np.arange(
            scaled_start,
            scaled_start + length,
            dtype=np.int64,
        )
        cursor = next_cursor

    unique_scaled, counts = np.unique(all_scaled, return_counts=True)
    tpo_counts = {
        round(float(scaled * bin_size), bin_precision): int(count)
        for scaled, count in zip(unique_scaled.tolist(), counts.tolist())
    }

    logger.debug("Built TPO histogram with %d buckets", len(tpo_counts))
    return tpo_counts


def extract_value_area(
    tpo_hist: Dict[float, int],
    price_precision: int
) -> Optional[ValueArea]:
    """
    Extract value area from TPO histogram.

    Computes:
    - POC: price with highest count
    - VAH: upper bound of 70% cumulative TPO
    - VAL: lower bound of 70% cumulative TPO

    Args:
        tpo_hist: Histogram of price -> TPO count
        price_precision: Decimal precision for rounding prices

    Returns:
        ValueArea object or None if histogram is empty
    """
    total = sum(tpo_hist.values())
    if total == 0:
        logger.warning("TPO histogram is empty, cannot extract value area.")
        return None

    # Sort buckets by descending count
    sorted_buckets = sorted(tpo_hist.items(), key=lambda item: item[1], reverse=True)
    poc_price = sorted_buckets[0][0]

    # Find 70% value area
    cumulative = 0
    va_prices = []
    threshold = 0.7 * total

    for price, count in sorted_buckets:
        cumulative += count
        va_prices.append(price)
        if cumulative >= threshold:
            break

    poc = round(float(poc_price), price_precision)
    vah = round(float(max(va_prices)), price_precision)
    val = round(float(min(va_prices)), price_precision)

    logger.debug(
        "Extracted value area: POC=%.{prec}f, VAH=%.{prec}f, VAL=%.{prec}f, total TPO=%d".replace("{prec}", str(price_precision)),
        poc, vah, val, total
    )

    return ValueArea(vah=vah, val=val, poc=poc)
