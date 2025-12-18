import datetime as dt
from typing import Iterable, List, Tuple

import pandas as pd


def interval_to_timedelta(interval: str) -> dt.timedelta:
    """Convert a string interval such as ``1h`` into a ``timedelta``."""

    unit = interval.lower()

    if unit.endswith("ms"):
        return dt.timedelta(milliseconds=max(1, int(unit[:-2])))
    if unit.endswith("s"):
        return dt.timedelta(seconds=max(1, int(unit[:-1])))
    if unit.endswith("m"):
        return dt.timedelta(minutes=max(1, int(unit[:-1])))
    if unit.endswith("h"):
        return dt.timedelta(hours=max(1, int(unit[:-1])))
    if unit.endswith("d"):
        return dt.timedelta(days=max(1, int(unit[:-1])))
    if unit.endswith("w"):
        return dt.timedelta(weeks=max(1, int(unit[:-1])))
    if unit.endswith("mo"):
        return dt.timedelta(days=max(1, int(unit[:-2])) * 30)
    if unit.endswith("y"):
        return dt.timedelta(days=max(1, int(unit[:-1])) * 365)

    raise ValueError(f"Unsupported interval string: {interval}")


def compute_tr_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """Annotate a dataframe with true range and Wilder's ATR columns."""

    required_cols = {"high", "low", "close"}
    if df is None or df.empty or not required_cols.issubset(df.columns):
        return df

    hl = df["high"] - df["low"]
    h_cp = (df["high"] - df["close"].shift()).abs()
    l_cp = (df["low"] - df["close"].shift()).abs()

    # For first candle, use high-low when previous close is NaN
    tr = pd.concat([hl, h_cp, l_cp], axis=1).max(axis=1, skipna=True)
    df["tr"] = tr
    df["atr_wilder"] = tr.ewm(alpha=1 / period, adjust=False).mean()
    return df


def collect_missing_ranges(
    timestamps: Iterable[pd.Timestamp],
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
    interval: str,
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Return gaps between cached timestamps and the requested window."""

    ordered = sorted(set(pd.to_datetime(list(timestamps), utc=True)))
    if not ordered:
        return []

    try:
        step = interval_to_timedelta(interval)
    except Exception:
        step = None

    if step is None and len(ordered) >= 2:
        deltas = pd.Series(ordered).diff().dropna()
        if not deltas.empty:
            step = deltas.median()

    if step is None:
        step = pd.Timedelta(0)

    has_step = step > pd.Timedelta(0)
    tolerance = step / 2 if has_step else pd.Timedelta(0)
    missing: List[Tuple[pd.Timestamp, pd.Timestamp]] = []

    first = ordered[0]
    if first - requested_start > tolerance:
        missing.append((requested_start, min(first, requested_end)))

    if has_step:
        for previous, current in zip(ordered, ordered[1:]):
            gap = current - previous
            if gap > step * 1.5 and previous + step < current:
                gap_start = previous + step
                gap_end = current
                missing.append((gap_start, gap_end))

    last = ordered[-1]
    effective_end = requested_end
    trailing_start = max(last, requested_start)

    if has_step:
        effective_end = max(requested_start, requested_end - step)
        trailing_start = last + step

    if effective_end - last > tolerance:
        trailing_start = max(trailing_start, requested_start)
        if trailing_start < requested_end:
            missing.append((trailing_start, requested_end))

    filtered: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    for start, end in missing:
        if end <= start:
            continue
        filtered.append((start, end))

    return filtered


def subtract_ranges(
    ranges: List[Tuple[pd.Timestamp, pd.Timestamp]],
    closures: List[Tuple[pd.Timestamp, pd.Timestamp]],
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Remove known closures from the candidate fetch windows."""

    if not closures:
        return ranges

    result: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    for start, end in ranges:
        segments = [(start, end)]
        for closure_start, closure_end in closures:
            updated: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
            for seg_start, seg_end in segments:
                if closure_end <= seg_start or closure_start >= seg_end:
                    updated.append((seg_start, seg_end))
                    continue

                if seg_start < closure_start:
                    updated.append((seg_start, closure_start))
                if closure_end < seg_end:
                    updated.append((closure_end, seg_end))
            segments = updated
            if not segments:
                break
        result.extend(segments)

    filtered: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    for start, end in result:
        if end <= start:
            continue
        filtered.append((start, end))

    return filtered


def split_history_range(
    start: pd.Timestamp,
    end: pd.Timestamp,
    interval: str,
    *,
    max_points: int,
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Chunk a historical request into segments with bounded candle counts."""

    try:
        step = interval_to_timedelta(interval)
    except Exception:
        step = dt.timedelta(minutes=1)

    if step <= dt.timedelta(0):
        step = dt.timedelta(minutes=1)

    span = step * max(max_points, 1)
    if span <= dt.timedelta(0):
        span = dt.timedelta(minutes=1)

    segments: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    cursor = start

    while cursor < end:
        segment_end = min(cursor + span, end)
        if segment_end <= cursor:
            break
        segments.append((cursor, segment_end))
        cursor = segment_end

    if not segments:
        segments.append((start, end))

    return segments


__all__ = [
    "collect_missing_ranges",
    "compute_tr_atr",
    "interval_to_timedelta",
    "split_history_range",
    "subtract_ranges",
]
