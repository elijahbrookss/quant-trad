"""Shared diagnostics for indicator runtime source data."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import pandas as pd

from core.candle_continuity import (
    PROVIDER_MISSING_DATA,
    expected_interval_seconds,
    summarize_candle_continuity,
)
from data_providers.utils.ohlcv import interval_to_timedelta


def _iso_timestamp(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat().replace("+00:00", "Z")


def _empty_source_payload(
    *,
    timeframe: str,
    requested_start: Any = None,
    requested_end: Any = None,
    message: str = "No indicator source candles were available.",
) -> Dict[str, Any]:
    return {
        "schema_version": "indicator_source_candle_continuity.v1",
        "timeframe": str(timeframe or ""),
        "requested_start": _iso_timestamp(requested_start),
        "requested_end": _iso_timestamp(requested_end),
        "row_count": 0,
        "status": "warning",
        "severity": "warning",
        "acceptability": "investigate",
        "message": message,
        "continuity": {
            "candle_count": 0,
            "final_status": "missing",
        },
    }


def _normalise_frame_index(frame: Any) -> tuple[Any, pd.DatetimeIndex] | None:
    if frame is None or getattr(frame, "empty", False):
        return None
    if not hasattr(frame, "iterrows"):
        return None
    if hasattr(frame, "columns") and "timestamp" in frame.columns:
        index = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    else:
        index = pd.to_datetime(frame.index, utc=True, errors="coerce")
    if not isinstance(index, pd.DatetimeIndex):
        index = pd.DatetimeIndex(index)
    valid_mask = ~index.isna()
    if not bool(valid_mask.any()):
        return None
    if hasattr(frame, "iloc"):
        frame = frame.iloc[valid_mask.to_numpy() if hasattr(valid_mask, "to_numpy") else valid_mask]
    index = index[valid_mask]
    if not index.is_monotonic_increasing:
        order = index.argsort(kind="stable")
        if hasattr(frame, "iloc"):
            frame = frame.iloc[order]
        index = index.take(order)
    return frame, index


def _entry_value(row: Any, key: str) -> Any:
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key)
    try:
        return row[key]
    except Exception:
        return None


def build_source_candle_continuity_payload(
    source_frame: Any,
    *,
    timeframe: str,
    requested_start: Any = None,
    requested_end: Any = None,
) -> Dict[str, Any]:
    """Return a compact, indicator-agnostic candle continuity diagnostic."""

    normalised = _normalise_frame_index(source_frame)
    if normalised is None:
        return _empty_source_payload(
            timeframe=timeframe,
            requested_start=requested_start,
            requested_end=requested_end,
        )
    frame, index = normalised

    entries: list[dict[str, Any]] = []
    for timestamp, (_, row) in zip(index, frame.iterrows()):
        entries.append(
            {
                "time": timestamp.isoformat(),
                "open": _entry_value(row, "open"),
                "high": _entry_value(row, "high"),
                "low": _entry_value(row, "low"),
                "close": _entry_value(row, "close"),
                "volume": _entry_value(row, "volume"),
            }
        )

    gap_classification = (
        getattr(source_frame, "attrs", {}).get("gap_classification")
        if hasattr(source_frame, "attrs")
        else None
    )
    continuity = summarize_candle_continuity(
        entries,
        expected_interval_seconds_value=expected_interval_seconds(timeframe=timeframe),
        gap_classification=gap_classification,
    ).to_dict()
    final_status = str(continuity.get("final_status") or "unknown")
    gap_count_by_type = dict(continuity.get("gap_count_by_type") or {})
    provider_sparse_gaps = int(gap_count_by_type.get(PROVIDER_MISSING_DATA, 0) or 0)
    data_defects = sum(
        int(continuity.get(key) or 0)
        for key in ("duplicate_count", "out_of_order_count", "missing_ohlcv_count")
    )
    defect_gaps = int(continuity.get("defect_gap_count") or 0)
    provider_sparse_only = (
        provider_sparse_gaps > 0
        and defect_gaps == provider_sparse_gaps
        and data_defects == 0
    )

    if final_status == "healthy":
        status = "ok"
        severity = "ok"
        acceptability = "accepted"
        message = "Indicator source candle continuity is healthy."
    elif provider_sparse_only:
        status = "warning"
        severity = "warning"
        acceptability = "acceptable_with_caveat"
        message = "Indicator source candles are provider-sparse; derived geometry may diverge from comparable instruments."
    elif final_status == "expected_sparse":
        status = "info"
        severity = "info"
        acceptability = "acceptable_with_caveat"
        message = "Indicator source candles include expected sparse-session gaps."
    else:
        status = "warning"
        severity = "warning"
        acceptability = "investigate"
        message = "Indicator source candle continuity has defects that may affect derived outputs."

    first_candle = index[0] if len(index) else None
    last_candle = index[-1] if len(index) else None
    coverage_end = None
    if last_candle is not None:
        try:
            coverage_end = last_candle + interval_to_timedelta(timeframe)
        except Exception:
            coverage_end = None

    return {
        "schema_version": "indicator_source_candle_continuity.v1",
        "timeframe": str(timeframe or ""),
        "requested_start": _iso_timestamp(requested_start),
        "requested_end": _iso_timestamp(requested_end),
        "row_count": int(len(index)),
        "first_candle_start": _iso_timestamp(first_candle),
        "last_candle_start": _iso_timestamp(last_candle),
        "available_end": _iso_timestamp(coverage_end),
        "status": status,
        "severity": severity,
        "acceptability": acceptability,
        "message": message,
        "continuity": continuity,
    }


__all__ = ["build_source_candle_continuity_payload"]
