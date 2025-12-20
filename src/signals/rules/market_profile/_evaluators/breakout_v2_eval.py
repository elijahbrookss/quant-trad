from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

import logging
import pandas as pd

from signals.rules.common.utils import value_area_identifier
from signals.util.level_breakout_v1 import (
    detect_level_breakouts,
    ABOVE as LB_ABOVE,
    BELOW as LB_BELOW,
    STRADDLE as LB_STRADDLE,
)

log = logging.getLogger("MarketProfileBreakoutV2Eval")

Zone = str

INSIDE: Zone = "INSIDE_VA"
ABOVE: Zone = "OUTSIDE_ABOVE"
BELOW: Zone = "OUTSIDE_BELOW"


def _zone(body_high: float, body_low: float, vah: float, val: float) -> Zone:
    if body_low > vah:
        return ABOVE
    if body_high < val:
        return BELOW
    if body_low >= val and body_high <= vah:
        return INSIDE
    return INSIDE  # Straddles count as inside for origin gating to avoid premature confirmation


def _all_zones(zones: Sequence[Zone], start: int, end: int, target: Zone) -> bool:
    return all(z == target for z in zones[start:end])


def _make_breakout_meta(
    *,
    boundary: str,
    variant: str,
    direction: str,
    pre_zone: Zone,
    post_zone: Zone,
    break_idx: int,
    break_time: pd.Timestamp,
    va_id: str,
    vah: float,
    val: float,
    confirm_bars: int,
    lockout_bars: int,
    formed_at: pd.Timestamp,
    session_count: int,
    va_start: Any,
    va_end: Any,
    confirm_indices: Sequence[int],
    confirm_times: Sequence[pd.Timestamp],
    prior_indices: Optional[Sequence[int]] = None,
    prior_times: Optional[Sequence[pd.Timestamp]] = None,
) -> Dict[str, Any]:
    level_price = vah if boundary == "VAH" else val
    breakout_id = f"{va_id}:{boundary}:{break_idx}"
    meta = {
        "type": "breakout_v2",
        "rule_id": "market_profile_breakout_v2",
        "pattern_id": "breakout_v2",
        "source": "MarketProfile",
        "boundary": boundary,
        "level_type": boundary,
        "level_price": level_price,
        "breakout_variant": variant,
        "breakout_type": variant,
        "direction": direction,
        "pre_zone": pre_zone,
        "post_zone": post_zone,
        "break_time": break_time.to_pydatetime() if hasattr(break_time, "to_pydatetime") else break_time,
        "time": break_time.to_pydatetime() if hasattr(break_time, "to_pydatetime") else break_time,
        "bar_index": break_idx,
        "trigger_index": break_idx,
        "trigger_time": break_time.to_pydatetime() if hasattr(break_time, "to_pydatetime") else break_time,
        "confirm_bars": confirm_bars,
        "lockout_bars": lockout_bars,
        "confirm_indices": list(confirm_indices),
        "confirm_times": [
            t.to_pydatetime() if hasattr(t, "to_pydatetime") else t for t in confirm_times
        ],
        "va_id": va_id,
        "value_area_id": va_id,
        "VAH": vah,
        "VAL": val,
        "breakout_id": breakout_id,
        "formed_at": formed_at.to_pydatetime() if hasattr(formed_at, "to_pydatetime") else formed_at,
        "session_count": session_count,
        "va_start": va_start,
        "va_end": va_end,
    }

    # Add prior window data if provided
    if prior_indices is not None:
        meta["prior_indices"] = list(prior_indices)
    if prior_times is not None:
        meta["prior_times"] = [
            t.to_pydatetime() if hasattr(t, "to_pydatetime") else t for t in prior_times
        ]

    return meta


def _convert_to_breakout_v2_meta(
    event: Dict[str, Any],
    boundary: str,
    value_area: Mapping[str, Any],
    df: pd.DataFrame,
    vah: float,
    val: float,
) -> Dict[str, Any]:
    """Convert level_breakout_v1 event to breakout_v2 metadata format."""
    direction = event["direction"]
    confirm_times = event["confirm_times"]
    prior_times = event["prior_times"]

    # Map direction + boundary to variant and breakout_direction
    if boundary == "VAH":
        if direction == "bull":
            variant = "inside_to_outside_above"
            breakout_direction = "above"
            pre_zone = INSIDE
            post_zone = ABOVE
        else:
            variant = "outside_above_to_inside"
            breakout_direction = "below"
            pre_zone = ABOVE
            post_zone = INSIDE
    else:  # VAL
        if direction == "bull":
            variant = "outside_below_to_inside"
            breakout_direction = "above"
            pre_zone = BELOW
            post_zone = INSIDE
        else:
            variant = "inside_to_outside_below"
            breakout_direction = "below"
            pre_zone = INSIDE
            post_zone = BELOW

    # Calculate global indices using full df
    confirm_indices = [df.index.get_loc(ts) for ts in confirm_times]
    prior_indices = [df.index.get_loc(ts) for ts in prior_times]

    break_time = event["confirm_end_time"]
    break_idx = confirm_indices[-1]

    log.debug(
        "Converting event to metadata | boundary=%s | direction=%s | break_time=%s | break_idx=%s | "
        "confirm_times=%s | confirm_indices=%s | prior_times=%s | prior_indices=%s",
        boundary, direction, break_time, break_idx, confirm_times, confirm_indices, prior_times, prior_indices
    )

    va_id = value_area_identifier(value_area) or value_area.get("value_area_id") or "va"
    formed_at_raw = value_area.get("formed_at") or value_area.get("end") or value_area.get("va_end")
    try:
        formed_at = pd.Timestamp(formed_at_raw) if formed_at_raw is not None else break_time
    except Exception:
        formed_at = break_time

    return _make_breakout_meta(
        boundary=boundary,
        variant=variant,
        direction=breakout_direction,
        pre_zone=pre_zone,
        post_zone=post_zone,
        break_idx=break_idx,
        break_time=break_time,
        va_id=va_id,
        vah=vah,
        val=val,
        confirm_bars=len(confirm_times),
        lockout_bars=3,  # Legacy field, no longer used
        formed_at=formed_at,
        session_count=value_area.get("session_count", 1),
        va_start=value_area.get("va_start"),
        va_end=value_area.get("va_end"),
        confirm_indices=confirm_indices,
        confirm_times=confirm_times,
        prior_indices=prior_indices,
        prior_times=prior_times,
    )


def detect_breakouts_v2(
    context: Mapping[str, Any],
    value_area: Mapping[str, Any],
    *,
    confirm_bars: int = 3,
    lockout_bars: int = 3,
) -> List[Dict[str, Any]]:
    """
    Close-only breakout detector with explicit origin/post zones.

    Now powered by level_breakout_v1 engine. The lockout_bars parameter
    is deprecated as deduplication is handled internally by the new engine.
    """
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

    formed_at_raw = value_area.get("formed_at") or value_area.get("end") or value_area.get("va_end")
    try:
        formed_at = pd.Timestamp(formed_at_raw) if formed_at_raw is not None else None
    except Exception:
        formed_at = None

    if formed_at is None:
        return []

    eligible_df = df[df.index >= formed_at]
    if eligible_df.empty:
        return []

    session_count = int(value_area.get("session_count") or 1)
    min_merge_sessions = int(value_area.get("min_merge_sessions") or 1)
    if session_count < min_merge_sessions:
        return []

    # Call level_breakout_v1 for VAH
    vah_events, _ = detect_level_breakouts(
        df=eligible_df,
        level=vah,
        level_name="VAH",
        confirm_bars=confirm_bars,
        prior_bars=3,
        debug=False,
    )

    # Call level_breakout_v1 for VAL
    val_events, _ = detect_level_breakouts(
        df=eligible_df,
        level=val,
        level_name="VAL",
        confirm_bars=confirm_bars,
        prior_bars=3,
        debug=False,
    )

    # Convert events to breakout_v2 metadata format
    results: List[Dict[str, Any]] = []
    for event in vah_events:
        results.append(_convert_to_breakout_v2_meta(event, "VAH", value_area, df, vah, val))
    for event in val_events:
        results.append(_convert_to_breakout_v2_meta(event, "VAL", value_area, df, vah, val))

    return results


__all__ = [
    "detect_breakouts_v2",
    "INSIDE",
    "ABOVE",
    "BELOW",
]
