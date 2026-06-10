"""Lightweight analytical research checks over source market facts."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Any, Mapping, Sequence

import pandas as pd


SUPPORTED_CHECK_FAMILY = "candle_event_forward_outcome"
CHECK_RESULT_SCHEMA_VERSION = "research_check_result.v1"


@dataclass(frozen=True)
class EventOutcome:
    event_index: int
    event_time: str
    close: float
    outcomes: dict[int, dict[str, float]]


def evaluate_candle_event_check(
    candles: pd.DataFrame,
    *,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
    data_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate a known-at candle detector and forward analytical outcomes."""

    frame = _normalize_candles(candles)
    outcome_spec = dict(outcomes or {})
    forward_bars = _forward_bars(outcome_spec)
    direction = str(outcome_spec.get("direction") or "long").strip().lower()
    if direction not in {"long", "short"}:
        raise ValueError("outcomes.direction must be 'long' or 'short'")
    min_sample_count = int(outcome_spec.get("min_sample_count") or 20)
    min_edge_pct = float(outcome_spec.get("min_edge_pct") or 0.0)
    max_events = int(outcome_spec.get("max_events") or 250)

    event_indexes = [
        idx
        for idx in range(len(frame))
        if _evaluate_detector(dict(detector or {}), frame, idx)
    ]
    event_outcomes = [
        _event_outcome(frame, idx, forward_bars=forward_bars, direction=direction)
        for idx in event_indexes
    ]
    baseline_outcomes = [
        _event_outcome(frame, idx, forward_bars=forward_bars, direction=direction)
        for idx in range(len(frame))
    ]

    summary = _summarize_outcomes(
        event_outcomes,
        baseline_outcomes,
        forward_bars=forward_bars,
        min_sample_count=min_sample_count,
        min_edge_pct=min_edge_pct,
    )
    examples = [
        {
            "event_time": outcome.event_time,
            "event_index": outcome.event_index,
            "close": outcome.close,
            "outcomes": {str(k): v for k, v in outcome.outcomes.items()},
        }
        for outcome in event_outcomes[: max(0, max_events)]
    ]
    return {
        "schema_version": CHECK_RESULT_SCHEMA_VERSION,
        "check_family": SUPPORTED_CHECK_FAMILY,
        "status": "completed",
        "sample_count": len(event_indexes),
        "eligible_bars": len(frame),
        "detector": dict(detector or {}),
        "outcomes": {
            "direction": direction,
            "forward_bars": forward_bars,
            "min_sample_count": min_sample_count,
            "min_edge_pct": min_edge_pct,
            "summary": summary["by_window"],
        },
        "data_quality": dict(data_quality or {}),
        "recommendation": summary["recommendation"],
        "caveats": summary["caveats"],
        "events": examples,
        "event_count_truncated": max(0, len(event_outcomes) - len(examples)),
    }


def blocked_check_result(
    *,
    reason: str,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None,
    data_quality: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": CHECK_RESULT_SCHEMA_VERSION,
        "check_family": SUPPORTED_CHECK_FAMILY,
        "status": "blocked",
        "sample_count": 0,
        "eligible_bars": 0,
        "detector": dict(detector or {}),
        "outcomes": dict(outcomes or {}),
        "data_quality": dict(data_quality or {}),
        "recommendation": "blocked",
        "caveats": [reason],
        "events": [],
        "event_count_truncated": 0,
    }


def _normalize_candles(candles: pd.DataFrame) -> pd.DataFrame:
    if candles is None or candles.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    frame = candles.copy()
    if "time" in frame.columns:
        times = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    elif "timestamp" in frame.columns:
        times = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    else:
        times = pd.to_datetime(frame.index, utc=True, errors="coerce")
    frame = frame.assign(time=times)
    frame = frame.dropna(subset=["time"])
    required = ["open", "high", "low", "close"]
    missing = [field for field in required if field not in frame.columns]
    if missing:
        raise ValueError(f"candles are missing required fields: {', '.join(missing)}")
    if "volume" not in frame.columns:
        frame["volume"] = 0.0
    for field in ["open", "high", "low", "close", "volume"]:
        frame[field] = pd.to_numeric(frame[field], errors="coerce")
    frame = frame.dropna(subset=required).sort_values("time").reset_index(drop=True)
    return frame


def _forward_bars(outcome_spec: Mapping[str, Any]) -> list[int]:
    raw = outcome_spec.get("forward_bars") or outcome_spec.get("bars") or [1, 3, 5, 10]
    if isinstance(raw, int):
        values = [raw]
    elif isinstance(raw, str):
        values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    elif isinstance(raw, Sequence):
        values = [int(item) for item in raw]
    else:
        raise ValueError("outcomes.forward_bars must be an int or list of ints")
    bars = sorted({value for value in values if value > 0})
    if not bars:
        raise ValueError("outcomes.forward_bars must contain at least one positive value")
    return bars


def _evaluate_detector(detector: Mapping[str, Any], frame: pd.DataFrame, idx: int) -> bool:
    if "all" in detector:
        conditions = detector.get("all")
        if not isinstance(conditions, Sequence) or isinstance(conditions, (str, bytes)):
            raise ValueError("detector.all must be a list")
        return all(_evaluate_detector(_mapping(item, "detector.all item"), frame, idx) for item in conditions)
    if "any" in detector:
        conditions = detector.get("any")
        if not isinstance(conditions, Sequence) or isinstance(conditions, (str, bytes)):
            raise ValueError("detector.any must be a list")
        return any(_evaluate_detector(_mapping(item, "detector.any item"), frame, idx) for item in conditions)
    if "not" in detector:
        return not _evaluate_detector(_mapping(detector.get("not"), "detector.not"), frame, idx)

    detector_type = str(detector.get("type") or "candle_condition").strip()
    if detector_type != "candle_condition":
        raise ValueError(f"unsupported research check detector type: {detector_type}")
    field = str(detector.get("field") or "").strip()
    if not field:
        raise ValueError("detector.field is required")
    left = _field_value(frame, idx, field)
    right = _right_value(detector, frame, idx)
    return _compare(left, str(detector.get("operator") or "eq"), right)


def _right_value(detector: Mapping[str, Any], frame: pd.DataFrame, idx: int) -> Any:
    if detector.get("value_field") is not None:
        return _field_value(frame, idx, str(detector.get("value_field")))
    if detector.get("value") is not None:
        return detector.get("value")
    if str(detector.get("operator") or "").lower() in {"is_true", "true"}:
        return True
    raise ValueError("detector.value or detector.value_field is required")


def _field_value(frame: pd.DataFrame, idx: int, field: str) -> float:
    normalized = str(field or "").strip().lower()
    row = frame.iloc[idx]
    if normalized in {"open", "high", "low", "close", "volume"}:
        return float(row[normalized])
    close = float(row["close"])
    open_ = float(row["open"])
    high = float(row["high"])
    low = float(row["low"])
    previous_close = float(frame.iloc[idx - 1]["close"]) if idx > 0 else close
    previous_volume = float(frame.iloc[idx - 1]["volume"]) if idx > 0 else float(row["volume"])
    if normalized == "previous_close":
        return previous_close
    if normalized == "range":
        return high - low
    if normalized == "range_pct":
        return _safe_ratio(high - low, close)
    if normalized == "body":
        return abs(close - open_)
    if normalized == "body_pct":
        return _safe_ratio(abs(close - open_), open_)
    if normalized == "return_pct":
        return _safe_ratio(close - previous_close, previous_close)
    if normalized == "volume_change_pct":
        return _safe_ratio(float(row["volume"]) - previous_volume, previous_volume)
    if normalized == "upper_wick_pct":
        return _safe_ratio(high - max(open_, close), close)
    if normalized == "lower_wick_pct":
        return _safe_ratio(min(open_, close) - low, close)
    if normalized == "close_position":
        return _safe_ratio(close - low, high - low)
    raise ValueError(f"unsupported candle detector field: {field}")


def _compare(left: Any, operator: str, right: Any) -> bool:
    op = str(operator or "").strip().lower()
    if op in {"eq", "equals", "=="}:
        return left == right
    if op in {"ne", "!=", "not_equals"}:
        return left != right
    if op in {"gt", ">"}:
        return float(left) > float(right)
    if op in {"gte", ">=", "ge"}:
        return float(left) >= float(right)
    if op in {"lt", "<"}:
        return float(left) < float(right)
    if op in {"lte", "<=", "le"}:
        return float(left) <= float(right)
    if op == "between":
        if not isinstance(right, Sequence) or isinstance(right, (str, bytes)) or len(right) != 2:
            raise ValueError("between operator requires value=[min,max]")
        return float(right[0]) <= float(left) <= float(right[1])
    if op == "in":
        if not isinstance(right, Sequence) or isinstance(right, (str, bytes)):
            raise ValueError("in operator requires a list value")
        return left in right
    if op in {"is_true", "true"}:
        return bool(left) is True
    raise ValueError(f"unsupported detector operator: {operator}")


def _event_outcome(
    frame: pd.DataFrame,
    idx: int,
    *,
    forward_bars: Sequence[int],
    direction: str,
) -> EventOutcome:
    close = float(frame.iloc[idx]["close"])
    sign = -1.0 if direction == "short" else 1.0
    outcomes: dict[int, dict[str, float]] = {}
    for bars in forward_bars:
        end_idx = idx + int(bars)
        if end_idx >= len(frame):
            continue
        future = frame.iloc[idx + 1 : end_idx + 1]
        future_close = float(frame.iloc[end_idx]["close"])
        high = float(future["high"].max())
        low = float(future["low"].min())
        if direction == "short":
            mfe = _safe_ratio(close - low, close)
            mae = _safe_ratio(close - high, close)
        else:
            mfe = _safe_ratio(high - close, close)
            mae = _safe_ratio(low - close, close)
        outcomes[int(bars)] = {
            "forward_return_pct": _safe_ratio(future_close - close, close) * sign,
            "max_favorable_excursion_pct": mfe,
            "max_adverse_excursion_pct": mae,
        }
    return EventOutcome(
        event_index=idx,
        event_time=pd.Timestamp(frame.iloc[idx]["time"]).isoformat().replace("+00:00", "Z"),
        close=close,
        outcomes=outcomes,
    )


def _summarize_outcomes(
    events: Sequence[EventOutcome],
    baseline: Sequence[EventOutcome],
    *,
    forward_bars: Sequence[int],
    min_sample_count: int,
    min_edge_pct: float,
) -> dict[str, Any]:
    by_window: dict[str, Any] = {}
    caveats: list[str] = []
    best_edge: float | None = None
    any_positive = False
    for bars in forward_bars:
        values = [event.outcomes[bars]["forward_return_pct"] for event in events if bars in event.outcomes]
        baseline_values = [
            event.outcomes[bars]["forward_return_pct"]
            for event in baseline
            if bars in event.outcomes
        ]
        baseline_mean = _mean(baseline_values)
        mean_value = _mean(values)
        edge = None if mean_value is None or baseline_mean is None else mean_value - baseline_mean
        if edge is not None:
            best_edge = edge if best_edge is None else max(best_edge, edge)
        if mean_value is not None and mean_value > 0:
            any_positive = True
        if len(values) < min_sample_count:
            caveats.append(f"forward_bars={bars} has sample_count={len(values)} below min_sample_count={min_sample_count}")
        by_window[str(bars)] = {
            "sample_count": len(values),
            "baseline_count": len(baseline_values),
            "mean_forward_return_pct": mean_value,
            "median_forward_return_pct": _median(values),
            "positive_rate": _positive_rate(values),
            "min_forward_return_pct": min(values) if values else None,
            "max_forward_return_pct": max(values) if values else None,
            "baseline_mean_forward_return_pct": baseline_mean,
            "edge_vs_baseline_pct": edge,
            "mean_mfe_pct": _mean(
                [event.outcomes[bars]["max_favorable_excursion_pct"] for event in events if bars in event.outcomes]
            ),
            "mean_mae_pct": _mean(
                [event.outcomes[bars]["max_adverse_excursion_pct"] for event in events if bars in event.outcomes]
            ),
        }
    max_sample_count = max((row["sample_count"] for row in by_window.values()), default=0)
    if not events:
        recommendation = "discard"
        caveats.append("detector matched no events")
    elif max_sample_count < min_sample_count:
        recommendation = "refine"
    elif best_edge is not None and best_edge > min_edge_pct:
        recommendation = "promote_to_hypothesis"
    elif not any_positive:
        recommendation = "discard"
    else:
        recommendation = "refine"
    return {"by_window": by_window, "recommendation": recommendation, "caveats": sorted(set(caveats))}


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def _mean(values: Sequence[float]) -> float | None:
    return float(sum(values) / len(values)) if values else None


def _median(values: Sequence[float]) -> float | None:
    return float(median(values)) if values else None


def _positive_rate(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(sum(1 for value in values if value > 0) / len(values))


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value
