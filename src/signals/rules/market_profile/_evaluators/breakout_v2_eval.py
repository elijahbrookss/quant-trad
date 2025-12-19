from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

import logging
import pandas as pd

from signals.rules.common.utils import value_area_identifier

log = logging.getLogger("MarketProfileBreakoutV2Eval")

Zone = str

INSIDE: Zone = "INSIDE_VA"
ABOVE: Zone = "OUTSIDE_ABOVE"
BELOW: Zone = "OUTSIDE_BELOW"


def _zone(close: float, vah: float, val: float) -> Zone:
    if close > vah:
        return ABOVE
    if close < val:
        return BELOW
    return INSIDE


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
) -> Dict[str, Any]:
    level_price = vah if boundary == "VAH" else val
    breakout_id = f"{va_id}:{boundary}:{break_idx}"
    return {
        "type": "breakout_v2",
        "rule_id": "market_profile_breakout_v2",
        "pattern_id": "breakout_v2",
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
        "va_id": va_id,
        "value_area_id": va_id,
        "VAH": vah,
        "VAL": val,
        "breakout_id": breakout_id,
    }


def detect_breakouts_v2(
    context: Mapping[str, Any],
    value_area: Mapping[str, Any],
    *,
    confirm_bars: int = 3,
    lockout_bars: int = 3,
) -> List[Dict[str, Any]]:
    """
    Close-only breakout detector with explicit origin/post zones.
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

    va_id = value_area_identifier(value_area) or "va"
    zones = [_zone(float(c), vah, val) for c in df["close"]]

    results: List[Dict[str, Any]] = []
    last_emit = {"VAH": -1, "VAL": -1}

    for idx in range(len(df)):
        start_idx = idx - confirm_bars + 1
        if start_idx < 0:
            continue

        origin_idx = start_idx - 1
        origin_zone = zones[origin_idx] if origin_idx >= 0 else None

        # Type 1: inside -> above (VAH)
        if (
            origin_zone == INSIDE
            and _all_zones(zones, start_idx, idx + 1, ABOVE)
            and last_emit["VAH"] < start_idx
        ):
            if last_emit["VAH"] >= 0 and (idx - last_emit["VAH"]) <= lockout_bars:
                log.debug(
                    "breakout_v2 suppressed | reason=lockout | boundary=VAH | last=%s | idx=%s | lockout=%s",
                    last_emit["VAH"],
                    idx,
                    lockout_bars,
                )
            else:
                last_emit["VAH"] = idx
                results.append(
                    _make_breakout_meta(
                        boundary="VAH",
                        variant="inside_to_outside_above",
                        direction="above",
                        pre_zone=origin_zone,
                        post_zone=ABOVE,
                        break_idx=idx,
                        break_time=df.index[idx],
                        va_id=va_id,
                        vah=vah,
                        val=val,
                        confirm_bars=confirm_bars,
                        lockout_bars=lockout_bars,
                    )
                )
            continue

        # Type 2: outside above -> inside (VAH)
        if (
            origin_zone == ABOVE
            and _all_zones(zones, start_idx, idx + 1, INSIDE)
            and last_emit["VAH"] < start_idx
        ):
            if last_emit["VAH"] >= 0 and (idx - last_emit["VAH"]) <= lockout_bars:
                log.debug(
                    "breakout_v2 suppressed | reason=lockout | boundary=VAH | last=%s | idx=%s | lockout=%s",
                    last_emit["VAH"],
                    idx,
                    lockout_bars,
                )
            else:
                last_emit["VAH"] = idx
                results.append(
                    _make_breakout_meta(
                        boundary="VAH",
                        variant="outside_above_to_inside",
                        direction="below",
                        pre_zone=origin_zone,
                        post_zone=INSIDE,
                        break_idx=idx,
                        break_time=df.index[idx],
                        va_id=va_id,
                        vah=vah,
                        val=val,
                        confirm_bars=confirm_bars,
                        lockout_bars=lockout_bars,
                    )
                )
            continue

        # Type 3: outside below -> inside (VAL)
        if (
            origin_zone == BELOW
            and _all_zones(zones, start_idx, idx + 1, INSIDE)
            and last_emit["VAL"] < start_idx
        ):
            if last_emit["VAL"] >= 0 and (idx - last_emit["VAL"]) <= lockout_bars:
                log.debug(
                    "breakout_v2 suppressed | reason=lockout | boundary=VAL | last=%s | idx=%s | lockout=%s",
                    last_emit["VAL"],
                    idx,
                    lockout_bars,
                )
            else:
                last_emit["VAL"] = idx
                results.append(
                    _make_breakout_meta(
                        boundary="VAL",
                        variant="outside_below_to_inside",
                        direction="above",
                        pre_zone=origin_zone,
                        post_zone=INSIDE,
                        break_idx=idx,
                        break_time=df.index[idx],
                        va_id=va_id,
                        vah=vah,
                        val=val,
                        confirm_bars=confirm_bars,
                        lockout_bars=lockout_bars,
                    )
                )
            continue

        # Type 4: inside -> below (VAL)
        if (
            origin_zone == INSIDE
            and _all_zones(zones, start_idx, idx + 1, BELOW)
            and last_emit["VAL"] < start_idx
        ):
            if last_emit["VAL"] >= 0 and (idx - last_emit["VAL"]) <= lockout_bars:
                log.debug(
                    "breakout_v2 suppressed | reason=lockout | boundary=VAL | last=%s | idx=%s | lockout=%s",
                    last_emit["VAL"],
                    idx,
                    lockout_bars,
                )
            else:
                last_emit["VAL"] = idx
                results.append(
                    _make_breakout_meta(
                        boundary="VAL",
                        variant="inside_to_outside_below",
                        direction="below",
                        pre_zone=origin_zone,
                        post_zone=BELOW,
                        break_idx=idx,
                        break_time=df.index[idx],
                        va_id=va_id,
                        vah=vah,
                        val=val,
                        confirm_bars=confirm_bars,
                        lockout_bars=lockout_bars,
                    )
                )

    return results


__all__ = [
    "detect_breakouts_v2",
    "INSIDE",
    "ABOVE",
    "BELOW",
]
