from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from signals.rules.common.utils import value_area_identifier


def _directional_touch(close: float, boundary: float, direction: str) -> bool:
    if direction == "above":
        return close <= boundary
    return close >= boundary


def _directional_reclaim(close: float, boundary: float, direction: str) -> bool:
    if direction == "above":
        return close >= boundary
    return close <= boundary


def detect_retests_v2(
    context: Mapping[str, Any],
    value_area: Mapping[str, Any],
    breakouts: Sequence[Mapping[str, Any]],
    *,
    window: int = 5,
    reclaim_bars: int = 1,
) -> List[Dict[str, Any]]:
    df: Optional[pd.DataFrame] = context.get("df")  # type: ignore[assignment]
    if df is None or df.empty or "close" not in df.columns:
        return []

    vah = value_area.get("VAH")
    val = value_area.get("VAL")
    if vah is None or val is None:
        return []
    try:
        vah = float(vah)
        val = float(val)
    except (TypeError, ValueError):
        return []

    va_id = value_area_identifier(value_area) or "va"
    results: List[Dict[str, Any]] = []

    for b in breakouts:
        if b.get("value_area_id") != va_id:
            continue
        boundary = b.get("boundary")
        direction = str(b.get("direction", "")).lower()
        break_idx = b.get("bar_index")
        if boundary not in {"VAH", "VAL"} or direction not in {"above", "below"}:
            continue
        if not isinstance(break_idx, int):
            continue

        boundary_price = vah if boundary == "VAH" else val
        touch_idx: Optional[int] = None
        search_end = min(len(df), break_idx + 1 + max(1, window))

        for idx in range(break_idx + 1, search_end):
            close = float(df.iloc[idx]["close"])
            if touch_idx is None:
                if _directional_touch(close, boundary_price, direction):
                    touch_idx = idx
                continue

            # Reclaim/reject after touch
            reclaim_slice = range(idx, min(search_end, idx + reclaim_bars))
            if all(
                _directional_reclaim(float(df.iloc[r]["close"]), boundary_price, direction)
                for r in reclaim_slice
            ):
                retest_idx = max(reclaim_slice)
                retest_time = df.index[retest_idx]
                touch_time = df.index[touch_idx]
                results.append(
                    {
                        "type": "retest_v2",
                        "rule_id": "market_profile_retest_v2",
                        "pattern_id": "retest_v2",
                        "source": "MarketProfile",
                        "boundary": boundary,
                        "level_type": boundary,
                        "breakout_id": b.get("breakout_id"),
                        "breakout_variant": b.get("breakout_variant"),
                        "direction": direction,
                        "va_id": va_id,
                        "value_area_id": va_id,
                        "VAH": vah,
                        "VAL": val,
                        "level_price": boundary_price,
                        "break_time": b.get("break_time"),
                        "touch_time": touch_time.to_pydatetime() if hasattr(touch_time, "to_pydatetime") else touch_time,
                        "retest_time": retest_time.to_pydatetime() if hasattr(retest_time, "to_pydatetime") else retest_time,
                        "time": retest_time.to_pydatetime() if hasattr(retest_time, "to_pydatetime") else retest_time,
                        "bar_index": retest_idx,
                        "touch_bar_index": touch_idx,
                        "confirm_bars": b.get("confirm_bars"),
                        "window": window,
                        "retest_type": "reclaim" if direction == "above" else "reject",
                    }
                )
                break

    return results


__all__ = ["detect_retests_v2"]
