"""
Level Breakout Detector v1

Body-only level breakout detection with confirmation and prior windows.

This module provides a pure function `detect_level_breakouts()` that identifies
bullish and bearish breakouts relative to a price level using:

1. **State Classification:** Each candle is classified as ABOVE, BELOW, or STRADDLE
   based on its body (min/max of open and close) relative to the level.

2. **Confirmation Window:** N consecutive bars must all be in the target state
   (ABOVE for bullish, BELOW for bearish) to confirm a breakout.

3. **Prior Window:** P bars immediately before the confirmation window must show
   the opposite state pattern (NOT ABOVE for bullish, with at least one true BELOW).

4. **Deduplication:** Prevents repeated signals while the state remains consistent
   after an emit. Must leave the state (see a NOT ABOVE bar for bullish) before
   allowing the next emit in that direction.

5. **Debug Mode:** Optionally returns detailed records for every evaluated candle,
   showing state, gate checks, and emit decisions.

Example Usage
-------------
>>> import pandas as pd
>>> from level_breakout_v1 import detect_level_breakouts
>>>
>>> df = pd.DataFrame({
...     'open': [95, 96, 105, 106, 107],
...     'close': [96, 97, 106, 107, 108],
... }, index=pd.date_range('2024-01-01', periods=5, freq='1h'))
>>>
>>> events, _ = detect_level_breakouts(df, level=100.0, confirm_bars=3, prior_bars=2)
>>> print(f"Detected {len(events)} breakout(s)")
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

log = logging.getLogger("LevelBreakoutV1")

# State type
State = str
ABOVE: State = "ABOVE"
BELOW: State = "BELOW"
STRADDLE: State = "STRADDLE"


def classify_state(open_price: float, close_price: float, level: float) -> State:
    """
    Classify candle state relative to level using body-only logic.

    Parameters
    ----------
    open_price : float
        Opening price of the candle
    close_price : float
        Closing price of the candle
    level : float
        Price level to classify against

    Returns
    -------
    State
        ABOVE if body_low > level
        BELOW if body_high < level
        STRADDLE otherwise (body touches or crosses level)
    """
    body_high = max(open_price, close_price)
    body_low = min(open_price, close_price)

    if body_low > level:
        return ABOVE
    if body_high < level:
        return BELOW
    return STRADDLE


def detect_level_breakouts(
    df: pd.DataFrame,
    level: float,
    *,
    level_name: str = "level",
    confirm_bars: int = 3,
    prior_bars: int = 3,
    debug: bool = False,
) -> Tuple[List[Dict[str, Any]], Optional[List[Dict[str, Any]]]]:
    """
    Detect level breakouts using body-only state classification with confirmation and prior windows.

    Parameters
    ----------
    df : pd.DataFrame
        Price data with DatetimeIndex and columns: open, close
    level : float
        Price level to detect breakouts around
    level_name : str, default="level"
        Human-readable name for the level (e.g., "VAH", "VAL")
    confirm_bars : int, default=3
        Number of consecutive bars required in target state for confirmation
    prior_bars : int, default=3
        Number of bars before confirm window that must be in opposite state
    debug : bool, default=False
        If True, return debug records for each evaluated candle

    Returns
    -------
    events : List[Dict[str, Any]]
        List of breakout events (see Event Schema in docstring)
    debug_records : Optional[List[Dict[str, Any]]]
        Debug records if debug=True, else None

    Event Schema
    ------------
    {
        "time": pd.Timestamp,              # confirm_end_time (last bar of confirm)
        "direction": str,                   # "bull" or "bear"
        "level": float,
        "level_name": str,
        "confirm_times": List[pd.Timestamp],
        "confirm_states": List[str],
        "confirm_start_time": pd.Timestamp,
        "confirm_end_time": pd.Timestamp,
        "prior_times": List[pd.Timestamp],
        "prior_states": List[str],
        "prior_start_time": pd.Timestamp,
        "prior_end_time": pd.Timestamp,
        "id": str,  # f"{level_name}:{direction}:{confirm_end_time.isoformat()}"
    }

    Debug Record Schema
    -------------------
    {
        "candle_time": pd.Timestamp,
        "candle_idx": int,
        "state": str,
        "body_high": float,
        "body_low": float,
        "is_candidate_bull": bool,
        "is_candidate_bear": bool,
        "gate_failed": Optional[str],  # "insufficient_history", "confirm_not_strict",
                                        # "prior_not_opposite", "prior_missing_true_opposite", "deduped"
        "emit_direction": Optional[str],  # "bull", "bear", or None
        "confirm_window": Optional[List[str]],
        "prior_window": Optional[List[str]],
    }

    Notes
    -----
    - Requires at least (confirm_bars + prior_bars) bars in df
    - Uses timestamp-based windowing to avoid index confusion
    - State classification:
        * ABOVE: body_low > level
        * BELOW: body_high < level
        * STRADDLE: otherwise (body touches or crosses level)
    - Deduplication prevents repeated signals while in same state after emit
    """
    # Validate input
    if df is None or df.empty:
        log.debug("level_breakout_v1 | Empty or None dataframe")
        return ([], None if not debug else [])

    if "open" not in df.columns or "close" not in df.columns:
        log.warning("level_breakout_v1 | Missing required columns (open, close)")
        return ([], None if not debug else [])

    # Drop NaN rows
    original_len = len(df)
    df = df.dropna(subset=["open", "close"])
    if len(df) < original_len:
        log.warning("level_breakout_v1 | level=%s | Dropped %d rows with NaN values", level_name, original_len - len(df))

    # Check minimum history
    min_required = confirm_bars + prior_bars
    if len(df) < min_required:
        log.debug(
            "level_breakout_v1 | level=%s | Insufficient history: need %d bars, got %d",
            level_name, min_required, len(df)
        )
        return ([], None if not debug else [])

    # Pre-compute all states
    states: List[Tuple[pd.Timestamp, State, float, float]] = []
    for ts in df.index:
        row = df.loc[ts]
        open_price = float(row["open"])
        close_price = float(row["close"])
        state = classify_state(open_price, close_price, level)
        body_high = max(open_price, close_price)
        body_low = min(open_price, close_price)
        states.append((ts, state, body_high, body_low))

    # Initialize results
    events: List[Dict[str, Any]] = []
    debug_records: List[Dict[str, Any]] = [] if debug else []

    # Deduplication state machine
    must_leave_above = False
    must_leave_below = False

    # Sliding window evaluation
    for i in range(min_required - 1, len(states)):
        current_ts, current_state, current_body_high, current_body_low = states[i]

        # Update deduplication flags based on current state
        if current_state != ABOVE:
            must_leave_above = False
        if current_state != BELOW:
            must_leave_below = False

        # Extract windows
        confirm_window = states[i - confirm_bars + 1 : i + 1]
        prior_window = states[i - confirm_bars - prior_bars + 1 : i - confirm_bars + 1]

        confirm_states_list = [s for _, s, _, _ in confirm_window]
        prior_states_list = [s for _, s, _, _ in prior_window]

        # Initialize evaluation tracking
        is_candidate_bull = False
        is_candidate_bear = False
        gate_failed: Optional[str] = None
        emit_direction: Optional[str] = None

        # Evaluate bullish breakout
        if all(s == ABOVE for s in confirm_states_list):
            is_candidate_bull = True

            # Check prior window
            if not all(s != ABOVE for s in prior_states_list):
                gate_failed = "prior_not_opposite"
            elif not any(s == BELOW for s in prior_states_list):
                gate_failed = "prior_missing_true_opposite"
            elif must_leave_above:
                gate_failed = "deduped"
            else:
                # Bullish breakout detected!
                emit_direction = "bull"
                must_leave_above = True

                confirm_times = [ts for ts, _, _, _ in confirm_window]
                prior_times = [ts for ts, _, _, _ in prior_window]

                event = {
                    "time": current_ts,
                    "direction": "bull",
                    "level": level,
                    "level_name": level_name,
                    "confirm_times": confirm_times,
                    "confirm_states": confirm_states_list,
                    "confirm_start_time": confirm_times[0],
                    "confirm_end_time": confirm_times[-1],
                    "prior_times": prior_times,
                    "prior_states": prior_states_list,
                    "prior_start_time": prior_times[0],
                    "prior_end_time": prior_times[-1],
                    "id": f"{level_name}:bull:{current_ts.isoformat()}",
                }
                events.append(event)

                log.debug(
                    "level_breakout_v1 EMIT | level=%s | direction=bull | time=%s | confirm=%s | prior=%s",
                    level_name, current_ts, confirm_states_list, prior_states_list
                )

        # Evaluate bearish breakout (only if not bullish)
        if not is_candidate_bull and all(s == BELOW for s in confirm_states_list):
            is_candidate_bear = True

            # Check prior window
            if not all(s != BELOW for s in prior_states_list):
                gate_failed = "prior_not_opposite"
            elif not any(s == ABOVE for s in prior_states_list):
                gate_failed = "prior_missing_true_opposite"
            elif must_leave_below:
                gate_failed = "deduped"
            else:
                # Bearish breakout detected!
                emit_direction = "bear"
                must_leave_below = True

                confirm_times = [ts for ts, _, _, _ in confirm_window]
                prior_times = [ts for ts, _, _, _ in prior_window]

                event = {
                    "time": current_ts,
                    "direction": "bear",
                    "level": level,
                    "level_name": level_name,
                    "confirm_times": confirm_times,
                    "confirm_states": confirm_states_list,
                    "confirm_start_time": confirm_times[0],
                    "confirm_end_time": confirm_times[-1],
                    "prior_times": prior_times,
                    "prior_states": prior_states_list,
                    "prior_start_time": prior_times[0],
                    "prior_end_time": prior_times[-1],
                    "id": f"{level_name}:bear:{current_ts.isoformat()}",
                }
                events.append(event)

                log.debug(
                    "level_breakout_v1 EMIT | level=%s | direction=bear | time=%s | confirm=%s | prior=%s",
                    level_name, current_ts, confirm_states_list, prior_states_list
                )

        # Collect debug record
        if debug:
            debug_record = {
                "candle_time": current_ts,
                "candle_idx": i,
                "state": current_state,
                "body_high": current_body_high,
                "body_low": current_body_low,
                "is_candidate_bull": is_candidate_bull,
                "is_candidate_bear": is_candidate_bear,
                "gate_failed": gate_failed,
                "emit_direction": emit_direction,
                "confirm_window": confirm_states_list if (is_candidate_bull or is_candidate_bear) else None,
                "prior_window": prior_states_list if (is_candidate_bull or is_candidate_bear) else None,
            }
            debug_records.append(debug_record)

    # Summary logging
    num_bull = sum(1 for e in events if e["direction"] == "bull")
    num_bear = sum(1 for e in events if e["direction"] == "bear")

    log.info(
        "level_breakout_v1 | level=%s (%.2f) | events=%d (bull=%d, bear=%d) | evaluated=%d candles",
        level_name, level, len(events), num_bull, num_bear, len(states) - min_required + 1
    )

    return (events, debug_records if debug else None)


__all__ = [
    "detect_level_breakouts",
    "classify_state",
    "ABOVE",
    "BELOW",
    "STRADDLE",
]
