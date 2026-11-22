"""Forward horizons and invalidation for an admitted event and entry."""
from __future__ import annotations
from collections.abc import Mapping, Sequence
from collections import Counter
from datetime import datetime, timedelta
from typing import Any
from research_science.price_paths import ratio_path_observation
from .event_time import _utc
from .event_features import _path
from .event_configuration import _INDICATOR_EVENT_DETECTOR, _FACT_SNAPSHOT_DETECTOR


def resolve_event_outcomes(
    *, row: dict[str, Any], outcomes: Mapping[str, Any], detector_type: str,
    entry_time: datetime, entry_close_time: datetime, entry_close: float,
    direction: float, step: timedelta, evaluation_end: datetime,
    candles: Sequence[Mapping[str, Any]], candle_by_open: Mapping[datetime, Mapping[str, Any]],
    event_payload: Mapping[str, Any], horizon_resolution: Mapping[int, Counter],
    invalidation_resolution: Counter,
) -> None:
    for horizon in outcomes["horizons"]:
        horizon_value = int(horizon)
        if outcomes["horizon_kind"] == "bars":
            target_open = entry_time + step * horizon_value
            required_times = [
                entry_time + step * offset
                for offset in range(1, horizon_value + 1)
            ]
            missing_times = [
                value for value in required_times if value not in candle_by_open
            ]
            target = candle_by_open.get(target_open)
            if target is None:
                reason = (
                    "outside_materialized_range"
                    if target_open >= evaluation_end + step * max(outcomes["horizons"])
                    else "target_bar_missing"
                )
            elif missing_times:
                reason = "gap_in_horizon"
            else:
                reason = None
        else:
            target_close = entry_close_time + timedelta(seconds=horizon_value)
            matches = [
                candidate
                for candidate in candles
                if _utc(
                    candidate.get("close_time") or candidate.get("known_at"),
                    field="candle.close_time",
                )
                == target_close
            ]
            target = matches[0] if len(matches) == 1 else None
            if target is None:
                reason = "target_time_unavailable"
                required_times = []
            else:
                target_open = _utc(
                    target.get("open_time") or target.get("time"),
                    field="target.open_time",
                )
                required_times = []
                cursor = entry_time + step
                while cursor <= target_open:
                    required_times.append(cursor)
                    cursor += step
                missing_times = [
                    value
                    for value in required_times
                    if value not in candle_by_open
                ]
                reason = "gap_in_horizon" if missing_times else None
        if reason is not None or target is None:
            row["outcomes"][str(horizon_value)] = {
                "status": "unresolved",
                "reason": reason,
                "horizon_kind": outcomes["horizon_kind"],
            }
            horizon_resolution[horizon_value][f"unresolved:{reason}"] += 1
            continue
        path_rows = (
            [candle_by_open[value] for value in required_times]
            if required_times
            else [target]
        )
        observation = ratio_path_observation(entry_price=entry_close, endpoint_price=float(target["close"]),
            high=max(float(value["high"]) for value in path_rows),
            low=min(float(value["low"]) for value in path_rows),
            direction=1.0 if detector_type == _FACT_SNAPSHOT_DETECTOR else direction)
        forward_return = observation.endpoint_return
        signed_return = (observation.direction_signed_return
            if detector_type == _INDICATOR_EVENT_DETECTOR else forward_return)
        favorable, adverse = observation.maximum_favorable_excursion, observation.maximum_adverse_excursion
        row["outcomes"][str(horizon_value)] = {
            "status": "resolved",
            "reason": None,
            "horizon_kind": outcomes["horizon_kind"],
            "target_time": str(target.get("open_time") or target.get("time")),
            "forward_return": forward_return,
            "direction_signed_forward_return": signed_return,
            "maximum_favorable_excursion": favorable,
            "maximum_adverse_excursion": adverse,
            "positive": (
                signed_return > 0.0
                if detector_type == _INDICATOR_EVENT_DETECTOR
                else forward_return > 0.0
            ),
        }
        horizon_resolution[horizon_value]["resolved"] += 1

    invalidation = dict(outcomes.get("invalidation") or {})
    if invalidation:
        reference = _path(event_payload, str(invalidation["reference_path"]))
        if reference is None:
            row["invalidation"] = {
                "status": "unresolved",
                "reason": "event_reference_missing",
                "invalidated": None,
                "time_to_invalidation_bars": None,
            }
            invalidation_resolution["unresolved:event_reference_missing"] += 1
        else:
            reference_value = float(reference)
            max_bars = int(invalidation["max_bars"])
            path = [
                candle_by_open.get(entry_time + step * offset)
                for offset in range(1, max_bars + 1)
            ]
            if any(candidate is None for candidate in path):
                row["invalidation"] = {
                    "status": "unresolved",
                    "reason": "gap_in_invalidation_window",
                    "invalidated": None,
                    "time_to_invalidation_bars": None,
                    "reference_price": reference_value,
                }
                invalidation_resolution[
                    "unresolved:gap_in_invalidation_window"
                ] += 1
            else:
                first = next(
                    (
                        offset
                        for offset, candidate in enumerate(path, start=1)
                        if (
                            float(candidate["close"]) <= reference_value
                            if direction > 0
                            else float(candidate["close"]) >= reference_value
                        )
                    ),
                    None,
                )
                row["invalidation"] = {
                    "status": "resolved",
                    "reason": None,
                    "invalidated": first is not None,
                    "time_to_invalidation_bars": first,
                    "reference_price": reference_value,
                    "max_bars": max_bars,
                }
                invalidation_resolution[
                    "resolved:invalidated" if first is not None else "resolved:not_invalidated"
                ] += 1

