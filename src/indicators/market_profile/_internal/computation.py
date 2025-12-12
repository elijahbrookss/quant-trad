"""
Core TPO computation logic for Market Profile.

Pure functions with no dependencies on indicator class or UI concerns.
"""

import logging
import math
from typing import Dict, Optional

import pandas as pd

from ..domain import ValueArea

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
    tpo_counts = {}
    logger.debug("Building TPO histogram for session with %d bars", len(data))

    for _, row in data.iterrows():
        low, high = float(row["low"]), float(row["high"])

        if not math.isfinite(low) or not math.isfinite(high):
            continue
        if high < low:
            low, high = high, low

        if bin_size <= 0:
            continue

        tolerance = abs(bin_size) * 1e-9
        span = max(high - low, 0.0)
        steps = int(math.floor(span / bin_size + 1e-9))

        for idx in range(steps + 1):
            price = low + idx * bin_size
            if price > high + tolerance:
                break

            scaled = round(price / bin_size)
            bucket = round(scaled * bin_size, bin_precision)
            tpo_counts[bucket] = tpo_counts.get(bucket, 0) + 1

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
