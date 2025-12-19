"""Market Profile-specific confirmation helpers."""

from __future__ import annotations

from typing import Optional

import pandas as pd


def enforce_full_bar_confirmation(
    df: pd.DataFrame,
    *,
    start_index: Optional[int],
    boundary_price: float,
    direction: str,
    required_bars: int,
) -> bool:
    """
    Require N consecutive full candles outside the value-area boundary.

    A candle counts as "outside" when:
    - breakout above: low > VAH
    - breakout below: high < VAL
    """
    if df is None or df.empty or start_index is None:
        return False

    if required_bars <= 0:
        return True

    if start_index < 0 or start_index >= len(df):
        return False

    direction = str(direction).lower()
    comparator = None
    if direction == "above":
        comparator = lambda high, low: low > boundary_price
    elif direction == "below":
        comparator = lambda high, low: high < boundary_price
    else:
        return False

    consecutive = 0
    for _, row in df.iloc[start_index:].iterrows():
        high = row.get("high")
        low = row.get("low")
        if high is None or low is None:
            consecutive = 0
            continue

        if comparator(high, low):
            consecutive += 1
            if consecutive >= required_bars:
                return True
        else:
            consecutive = 0

    return False


__all__ = ["enforce_full_bar_confirmation"]
