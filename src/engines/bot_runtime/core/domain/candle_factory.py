"""Strict dataframe-to-candle conversion shared by runtime and previews."""

from __future__ import annotations

import math
from typing import Any, List, Optional

from .models import Candle
from .time_utils import timeframe_duration


class CandleFrameValidationError(RuntimeError):
    """Raised when a dataframe cannot be represented as valid candles."""


def _column_name(frame: Any, logical_name: str) -> str:
    for candidate in (logical_name, logical_name.capitalize()):
        if candidate in frame.columns:
            return candidate
    raise CandleFrameValidationError(
        f"candle_frame_invalid: missing required column {logical_name!r}"
    )


def _optional_column_name(frame: Any, candidates: tuple[str, ...]) -> Optional[str]:
    return next((candidate for candidate in candidates if candidate in frame.columns), None)


def _row_error(*, position: int, timestamp: Any, field: str, reason: str) -> CandleFrameValidationError:
    timestamp_text = timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp)
    return CandleFrameValidationError(
        "candle_frame_invalid: "
        f"row={position} timestamp={timestamp_text} field={field} reason={reason}"
    )


def _required_number(value: Any, *, position: int, timestamp: Any, field: str) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise _row_error(
            position=position,
            timestamp=timestamp,
            field=field,
            reason=f"not_numeric value={value!r}",
        ) from exc
    if not math.isfinite(numeric):
        raise _row_error(
            position=position,
            timestamp=timestamp,
            field=field,
            reason=f"not_finite value={value!r}",
        )
    return numeric


def _optional_number(
    value: Any,
    *,
    position: int,
    timestamp: Any,
    field: str,
    allow_negative: bool,
) -> Optional[float]:
    import pandas as pd

    if value is None:
        return None
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        missing = False
    if isinstance(missing, bool) and missing:
        return None
    numeric = _required_number(
        value,
        position=position,
        timestamp=timestamp,
        field=field,
    )
    if not allow_negative and numeric < 0:
        raise _row_error(
            position=position,
            timestamp=timestamp,
            field=field,
            reason=f"negative value={numeric}",
        )
    return numeric


def build_candles_from_dataframe(
    df: Any,
    *,
    timeframe: Optional[str] = None,
) -> List[Candle]:
    """Build ordered candles or fail loudly on malformed timestamps/OHLCV rows."""

    import pandas as pd

    if df is None or getattr(df, "empty", False):
        return []

    frame = df.copy()
    try:
        frame.index = pd.to_datetime(frame.index, utc=True, errors="raise")
    except Exception as exc:
        raise CandleFrameValidationError(
            "candle_frame_invalid: dataframe index contains an invalid timestamp"
        ) from exc
    if frame.index.hasnans:
        raise CandleFrameValidationError(
            "candle_frame_invalid: dataframe index contains an invalid timestamp"
        )
    if frame.index.has_duplicates:
        duplicate = frame.index[frame.index.duplicated(keep=False)][0]
        raise CandleFrameValidationError(
            "candle_frame_invalid: "
            f"duplicate timestamp={duplicate.isoformat()}"
        )
    if not frame.index.is_monotonic_increasing:
        frame = frame.sort_index(kind="stable")

    columns = {
        field: _column_name(frame, field)
        for field in ("open", "high", "low", "close")
    }
    atr_col = _optional_column_name(frame, ("ATR_Wilder", "atr", "atr_wilder"))
    volume_col = _optional_column_name(frame, ("volume", "Volume"))
    normalized_rows = []
    for position, (timestamp, row) in enumerate(frame.iterrows()):
        prices = {
            field: _required_number(
                row[column],
                position=position,
                timestamp=timestamp,
                field=field,
            )
            for field, column in columns.items()
        }
        if prices["high"] < prices["low"]:
            raise _row_error(
                position=position,
                timestamp=timestamp,
                field="high_low",
                reason=f"high={prices['high']} below low={prices['low']}",
            )
        if prices["high"] < max(prices["open"], prices["close"]):
            raise _row_error(
                position=position,
                timestamp=timestamp,
                field="high",
                reason=(
                    f"high={prices['high']} below "
                    f"open_or_close={max(prices['open'], prices['close'])}"
                ),
            )
        if prices["low"] > min(prices["open"], prices["close"]):
            raise _row_error(
                position=position,
                timestamp=timestamp,
                field="low",
                reason=(
                    f"low={prices['low']} above "
                    f"open_or_close={min(prices['open'], prices['close'])}"
                ),
            )
        atr = (
            _optional_number(
                row[atr_col],
                position=position,
                timestamp=timestamp,
                field=atr_col,
                allow_negative=False,
            )
            if atr_col
            else None
        )
        volume = (
            _optional_number(
                row[volume_col],
                position=position,
                timestamp=timestamp,
                field=volume_col,
                allow_negative=False,
            )
            if volume_col
            else None
        )
        normalized_rows.append(
            {
                "timestamp": timestamp,
                **prices,
                "atr": atr,
                "volume": volume,
                "range": prices["high"] - prices["low"],
            }
        )

    normalized = pd.DataFrame(normalized_rows).set_index("timestamp")
    normalized["avg_range_15"] = normalized["range"].rolling(window=15).mean().shift(1)
    if atr_col:
        normalized["avg_atr_15"] = normalized["atr"].rolling(window=15).mean().shift(1)
    if volume_col:
        normalized["avg_volume_15"] = (
            normalized["volume"].rolling(window=15).mean().shift(1)
        )

    duration = timeframe_duration(timeframe)
    candles: List[Candle] = []
    for timestamp, row in normalized.iterrows():
        candles.append(
            Candle(
                time=timestamp.to_pydatetime(),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                end=timestamp.to_pydatetime() + duration if duration else None,
                atr=float(row["atr"]) if atr_col and not pd.isna(row["atr"]) else None,
                volume=(
                    float(row["volume"])
                    if volume_col and not pd.isna(row["volume"])
                    else None
                ),
                range=float(row["range"]),
                lookback_15={
                    "avg_range_15": (
                        float(row["avg_range_15"])
                        if not pd.isna(row["avg_range_15"])
                        else None
                    ),
                    "avg_atr_15": (
                        float(row["avg_atr_15"])
                        if atr_col and not pd.isna(row["avg_atr_15"])
                        else None
                    ),
                    "avg_volume_15": (
                        float(row["avg_volume_15"])
                        if volume_col and not pd.isna(row["avg_volume_15"])
                        else None
                    ),
                },
            )
        )
    return candles
