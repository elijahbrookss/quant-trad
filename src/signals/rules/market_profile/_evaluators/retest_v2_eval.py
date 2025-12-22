from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from signals.rules.common.utils import value_area_identifier

log = logging.getLogger("RetestV2Eval")


def _is_valid_retest(
    open_price: float, close: float, level: float, direction: str, tolerance_pct: float = 0.5
) -> bool:
    """
    Check if candle represents a valid retest (pullback to level without going too far past it).

    Uses body (not wicks) for pullback detection.

    For breakout above:
        - Body should come back down near/below the level
        - Close should NOT have gone too far below the level (beyond tolerance)

    For breakout below:
        - Body should come back up near/above the level
        - Close should NOT have gone too far above the level (beyond tolerance)

    tolerance_pct: how close is "near" (0.5% default)
    """
    tolerance = abs(level * tolerance_pct / 100.0)
    body_high = max(open_price, close)
    body_low = min(open_price, close)

    if direction == "above":
        # For upside breakout, retest is when body pulls back to/below level
        # Check if body low touches or crosses the level (within tolerance)
        body_touches_level = body_low <= level + tolerance

        # Close should not have gone too far below the level
        # If close is below (level - tolerance), the retest went too far and doesn't qualify
        close_not_too_far_past = close >= level - tolerance

        return body_touches_level and close_not_too_far_past
    else:
        # For downside breakout, retest is when body pulls back to/above level
        # Check if body high touches or crosses the level (within tolerance)
        body_touches_level = body_high >= level - tolerance

        # Close should not have gone too far above the level
        # If close is above (level + tolerance), the retest went too far and doesn't qualify
        close_not_too_far_past = close <= level + tolerance

        return body_touches_level and close_not_too_far_past


def detect_retests_v2(
    context: Mapping[str, Any],
    value_area: Mapping[str, Any],
    breakouts: Sequence[Mapping[str, Any]],
    *,
    max_lookback: int = 50,
) -> List[Dict[str, Any]]:
    """
    Walk forward from each breakout candle looking for pullbacks to the broken level.

    Logic:
    1. For each breakout in the cache (all breakouts, not just current payload)
    2. Walk forward up to max_lookback candles from the breakout
    3. Check if price comes close/near the level that was just broken
    4. Emit a retest signal when pullback is detected

    Args:
        context: Signal context
        value_area: Current value area payload (used for VAH/VAL prices)
        breakouts: All cached breakout signals to scan
        max_lookback: Maximum candles to scan forward from each breakout (default 50)
    """
    df: Optional[pd.DataFrame] = context.get("df")  # type: ignore[assignment]
    if df is None or df.empty or "close" not in df.columns:
        return []

    # Get tolerance from context (default 0.5%)
    tolerance_pct = float(context.get("market_profile_retest_v2_tolerance_pct", 0.5))

    results: List[Dict[str, Any]] = []
    processed_breakouts: set[str] = set()  # Track to avoid duplicate retests

    log.debug(
        "Retest v2 scan | total_breakouts=%d | max_lookback=%d | tolerance=%.2f%%",
        len(breakouts),
        max_lookback,
        tolerance_pct,
    )

    # Scan ALL breakouts, not just those matching current value_area
    for b_idx, breakout in enumerate(breakouts):
        breakout_id = breakout.get("breakout_id")
        if not breakout_id or breakout_id in processed_breakouts:
            continue

        boundary = breakout.get("boundary")
        direction = str(breakout.get("direction", "")).lower()
        break_idx = breakout.get("bar_index")

        if boundary not in {"VAH", "VAL"} or direction not in {"above", "below"}:
            log.debug(
                "Retest v2 skip | breakout[%d] | reason=invalid_direction_or_boundary | boundary=%s | direction=%s",
                b_idx,
                boundary,
                direction,
            )
            continue

        if not isinstance(break_idx, int) or break_idx < 0:
            log.debug(
                "Retest v2 skip | breakout[%d] | reason=invalid_bar_index | bar_index=%s",
                b_idx,
                break_idx,
            )
            continue

        # Get the level price from the breakout metadata
        level_price = breakout.get("level_price")
        if level_price is None:
            log.debug(
                "Retest v2 skip | breakout[%d] | reason=no_level_price",
                b_idx,
            )
            continue

        level_price = float(level_price)
        vah = breakout.get("VAH")
        val = breakout.get("VAL")
        va_id = breakout.get("value_area_id", "unknown")

        # Get minimum candles required before allowing retest (default 3)
        min_bars_before_retest = int(context.get("market_profile_retest_v2_min_bars", 3))

        # Walk forward from breakout candle looking for pullback
        # Start scanning min_bars_before_retest candles after breakout (not immediately)
        search_start = break_idx + 1 + min_bars_before_retest
        search_end = min(len(df), break_idx + 1 + max_lookback)

        if search_start >= search_end:
            log.debug(
                "Retest v2 skip | breakout[%d] | reason=insufficient_bars_after_breakout | "
                "break_idx=%d | min_bars=%d | search_end=%d",
                b_idx,
                break_idx,
                min_bars_before_retest,
                search_end,
            )
            continue

        log.debug(
            "Retest v2 scan | breakout[%d] | id=%s | boundary=%s | direction=%s | level=%.2f | "
            "scan_range=[%d, %d) | min_bars=%d",
            b_idx,
            breakout_id,
            boundary,
            direction,
            level_price,
            search_start,
            search_end,
            min_bars_before_retest,
        )

        for scan_idx in range(search_start, search_end):
            try:
                row = df.iloc[scan_idx]
                open_price = float(row["open"])
                close = float(row["close"])
            except (IndexError, KeyError, ValueError, TypeError):
                log.warning(
                    "Retest v2 | failed to read OHLC | scan_idx=%d | df_len=%d",
                    scan_idx,
                    len(df),
                )
                continue

            # Check if candle body has pulled back near the broken level
            # AND that close hasn't gone too far past the level
            if _is_valid_retest(open_price, close, level_price, direction, tolerance_pct):
                # Additional filter: Check if any candle between breakout and retest
                # went beyond tolerance threshold (price moved too far away)
                tolerance = abs(level_price * tolerance_pct / 100.0)
                extreme_threshold = tolerance * 1.0

                price_went_too_far = False
                for check_idx in range(break_idx + 1, scan_idx):
                    try:
                        check_row = df.iloc[check_idx]
                        check_open = float(check_row["open"])
                        check_close = float(check_row["close"])
                        check_body_high = max(check_open, check_close)
                        check_body_low = min(check_open, check_close)

                        if direction == "above":
                            # For upside breakout, check if body went too far below
                            # Use body low (not close) to be more strict
                            if check_body_low < level_price - extreme_threshold:
                                price_went_too_far = True
                                log.debug(
                                    "Retest v2 reject | breakout[%d] | reason=body_went_too_far | "
                                    "check_idx=%d | body_low=%.2f | threshold=%.2f",
                                    b_idx,
                                    check_idx,
                                    check_body_low,
                                    level_price - extreme_threshold,
                                )
                                break
                        else:
                            # For downside breakout, check if body went too far above
                            # Use body high (not close) to be more strict
                            if check_body_high > level_price + extreme_threshold:
                                price_went_too_far = True
                                log.debug(
                                    "Retest v2 reject | breakout[%d] | reason=body_went_too_far | "
                                    "check_idx=%d | body_high=%.2f | threshold=%.2f",
                                    b_idx,
                                    check_idx,
                                    check_body_high,
                                    level_price + extreme_threshold,
                                )
                                break
                    except (IndexError, KeyError, ValueError, TypeError):
                        continue

                # Skip this retest if price went too far
                if price_went_too_far:
                    continue

                retest_time = df.index[scan_idx]
                bars_since_breakout = scan_idx - break_idx

                body_high = max(open_price, close)
                body_low = min(open_price, close)

                log.info(
                    "Retest v2 DETECTED | breakout_id=%s | boundary=%s | direction=%s | "
                    "level=%.2f | retest_close=%.2f | body=[%.2f, %.2f] | bars_since=%d",
                    breakout_id,
                    boundary,
                    direction,
                    level_price,
                    close,
                    body_low,
                    body_high,
                    bars_since_breakout,
                )

                results.append(
                    {
                        "type": "retest",
                        "rule_id": "market_profile_retest_v2",
                        "pattern_id": "retest_v2",
                        "source": "MarketProfile",
                        "boundary": boundary,
                        "level_type": boundary,
                        "breakout_id": breakout_id,
                        "breakout_variant": breakout.get("breakout_variant"),
                        "breakout_direction": direction,
                        "direction": direction,
                        "va_id": va_id,
                        "value_area_id": va_id,
                        "VAH": vah,
                        "VAL": val,
                        "level_price": level_price,
                        "break_time": breakout.get("break_time"),
                        "retest_time": retest_time.to_pydatetime() if hasattr(retest_time, "to_pydatetime") else retest_time,
                        "time": retest_time.to_pydatetime() if hasattr(retest_time, "to_pydatetime") else retest_time,
                        "trigger_time": retest_time.to_pydatetime() if hasattr(retest_time, "to_pydatetime") else retest_time,
                        "bar_index": scan_idx,
                        "trigger_index": scan_idx,
                        "breakout_bar_index": break_idx,
                        "bars_since_breakout": bars_since_breakout,
                        "retest_close": close,
                        "retest_open": open_price,
                        "trigger_close": close,
                        "trigger_open": open_price,
                        "trigger_high": body_high,
                        "trigger_low": body_low,
                        "tolerance_pct": tolerance_pct,
                        "retest_role": "support" if direction == "below" else "resistance",
                        "pointer_direction": "up" if direction == "above" else "down",
                    }
                )

                # Mark this breakout as processed to avoid duplicate retests
                processed_breakouts.add(breakout_id)
                # Only emit first retest for each breakout
                break

    log.debug(
        "Retest v2 complete | scanned=%d | emitted=%d",
        len(breakouts),
        len(results),
    )

    return results


__all__ = ["detect_retests_v2"]
