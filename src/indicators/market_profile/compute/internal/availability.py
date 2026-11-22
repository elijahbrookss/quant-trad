"""Availability of sealed native UTC daily inputs, separate from profile geometry."""
from __future__ import annotations

from typing import Any
from math import ceil

import pandas as pd

SOURCE_STEP = pd.Timedelta(minutes=30)


def daily_source_availability(frame: pd.DataFrame) -> tuple[dict[pd.Timestamp, dict[str, Any]], list[dict[str, Any]]]:
    """Return source watermarks only for complete native days.

    A prefix ending inside a day cannot stand in for that day's final profile.
    Missing native bars remain missing; no fill or resampling is performed.
    Explicit canonical clocks must be valid. Legacy frames without clocks use
    the native interval close, with that inference disclosed in the payload.
    """
    index = pd.to_datetime(frame.index, utc=True, errors="raise")
    if index.hasnans or index.has_duplicates or any(index.asi8 % SOURCE_STEP.value):
        raise ValueError("market_profile_source_clock_invalid: unique aligned native 30m opens required")
    available = {}
    withheld = []
    source = frame.copy()
    source.index = index
    for day, group in source.groupby(index.normalize(), sort=True):
        expected_close = group.index + SOURCE_STEP
        clocks = {}
        for field in ("close_time", "known_at"):
            if field not in group:
                continue
            try:
                values = pd.DatetimeIndex(pd.to_datetime(group[field], utc=True, errors="raise", format="mixed"))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"market_profile_source_clock_invalid: {field} day={day.isoformat()}") from exc
            if values.hasnans:
                raise ValueError(f"market_profile_source_clock_invalid: missing {field} day={day.isoformat()}")
            clocks[field] = values
        if "close_time" in clocks and not clocks["close_time"].equals(expected_close):
            raise ValueError(f"market_profile_source_clock_invalid: native close_time mismatch day={day.isoformat()}")
        known = clocks.get("known_at", expected_close)
        if any(known < expected_close):
            raise ValueError(f"market_profile_source_clock_invalid: known_at before native close day={day.isoformat()}")
        expected = pd.date_range(day, periods=48, freq=SOURCE_STEP)
        if not group.index.sort_values().equals(expected):
            withheld.append({"session": day.isoformat(), "reason": "incomplete_native_day", "source_bars": len(group)})
            continue
        session_close = day + pd.Timedelta(days=1)
        available[day] = {
            "formed_at": int(session_close.timestamp()),
            # The resolver uses integer epoch seconds: round up, never backdate
            # a subsecond publication onto the preceding chart boundary.
            "source_known_at": ceil(max(session_close, known.max()).timestamp()),
            "source_availability_basis": "canonical_known_at" if "known_at" in clocks else "inferred_native_close",
        }
    return available, withheld
