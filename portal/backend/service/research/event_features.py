"""Baseline feature calculations shared by event-and-fact procedures.

Selection, causal admission and outcomes remain owned by the calling procedure.
"""
from __future__ import annotations
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from statistics import fmean
from typing import Any
import pandas as pd
from data_providers.utils.ohlcv import compute_tr_atr

def _path(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in str(path).split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def prepare_atr(
    candles: Sequence[Mapping[str, Any]],
    baseline_specs: Sequence[Mapping[str, Any]],
) -> dict[int, dict[datetime, float]]:
    atr_by_period: dict[int, dict[datetime, float]] = {}
    if any(str(row.get("operator")) == "atr_fraction" for row in baseline_specs):
        frame = pd.DataFrame(candles)
        frame.index = pd.to_datetime(
            [row.get("open_time") or row.get("time") for row in candles], utc=True
        )
        for period in sorted(
            {int(row["period"]) for row in baseline_specs if row["operator"] == "atr_fraction"}
        ):
            enriched_frame = compute_tr_atr(frame.copy(), period=period)
            atr_by_period[period] = {
                timestamp.to_pydatetime(): float(value)
                for timestamp, value in enriched_frame["atr_wilder"].items()
                if pd.notna(value)
            }

    return atr_by_period

def baseline_features(
    *,
    baseline_specs: Sequence[Mapping[str, Any]],
    output: Mapping[str, Any],
    direction: float,
    event_time: datetime,
    step: timedelta,
    event_close: float,
    event_candle: Mapping[str, Any],
    candle_by_open: Mapping[datetime, Mapping[str, Any]],
    atr_by_period: Mapping[int, Mapping[datetime, float]],
) -> dict[str, float | None]:
    features: dict[str, float | None] = {}
    for spec in baseline_specs:
        name = str(spec["name"])
        operator = str(spec["operator"])
        value: float | None
        if operator == "event_direction":
            value = direction
        elif operator == "event_metadata_number":
            raw_value = _path(dict(output.get("event") or {}), str(spec["path"]))
            value = (
                float(raw_value) * float(spec.get("scale", 1.0))
                if raw_value is not None
                else None
            )
        elif operator in {"direction_signed_return", "lagged_return"}:
            lookback = int(spec["lookback_bars"])
            previous_time = event_time - step * lookback
            previous = candle_by_open.get(previous_time)
            local_times = [
                event_time - step * offset
                for offset in range(1, lookback + 1)
            ]
            raw_return = (
                (event_close / float(previous["close"])) - 1.0
                if previous is not None
                and all(item in candle_by_open for item in local_times)
                else None
            )
            value = (
                raw_return * direction
                if raw_return is not None
                and operator == "direction_signed_return"
                else raw_return
            )
        elif operator == "atr_fraction":
            atr = atr_by_period[int(spec["period"])].get(event_time)
            value = atr / event_close if atr is not None and event_close else None
        elif operator == "volume_ratio":
            lookback = int(spec["lookback_bars"])
            history = [
                candle_by_open.get(event_time - step * offset)
                for offset in range(1, lookback + 1)
            ]
            volumes = [
                float(item.get("volume") or 0.0)
                for item in history
                if item is not None
            ]
            denominator = fmean(volumes) if len(volumes) == lookback else 0.0
            value = (
                float(event_candle.get("volume") or 0.0) / denominator
                if denominator > 0.0
                else None
            )
        else:  # pragma: no cover - normalization makes this unreachable.
            raise AssertionError(operator)
        features[name] = value

    return features
