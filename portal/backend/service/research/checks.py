"""Lightweight analytical research checks over source market facts."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from statistics import median
from typing import Any, Mapping, Sequence

import pandas as pd


RAW_FORWARD_OUTCOME = "raw_forward_outcome"
INDICATOR_FORWARD_OUTCOME = "indicator_forward_outcome"
RUN_SIGNAL_SUMMARY = "run_signal_summary"
RUN_DECISION_TRADE_COMPARISON = "run_decision_trade_comparison"
SUPPORTED_CHECK_FAMILY = RAW_FORWARD_OUTCOME
SUPPORTED_CHECK_FAMILIES = {
    RAW_FORWARD_OUTCOME,
    INDICATOR_FORWARD_OUTCOME,
    RUN_SIGNAL_SUMMARY,
    RUN_DECISION_TRADE_COMPARISON,
}
CHECK_RESULT_SCHEMA_VERSION = "research_check_result.v1"
_RAW_DETECTOR_FIELDS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "previous_open",
    "previous_high",
    "previous_low",
    "previous_close",
    "previous_volume",
}
_DETECTOR_OPERATORS = {
    "eq",
    "equals",
    "==",
    "ne",
    "!=",
    "not_equals",
    "gt",
    ">",
    "gte",
    ">=",
    "ge",
    "lt",
    "<",
    "lte",
    "<=",
    "le",
    "between",
    "in",
    "is_true",
    "true",
}
_RUN_DETECTOR_TYPES_BY_FAMILY = {
    RUN_SIGNAL_SUMMARY: {"run_signal_match", "record_match"},
    RUN_DECISION_TRADE_COMPARISON: {"run_decision_match", "record_match"},
}
_INDICATOR_DETECTOR_TYPES = {"indicator_output_match", "indicator_event_match", "record_match"}


@dataclass(frozen=True)
class EventOutcome:
    event_index: int
    event_time: str
    close: float
    outcomes: dict[int, dict[str, float]]


def validate_check_detector(*, check_family: str, detector: Mapping[str, Any]) -> None:
    if check_family == RAW_FORWARD_OUTCOME:
        _validate_raw_detector(detector)
        return
    if check_family == INDICATOR_FORWARD_OUTCOME:
        _validate_indicator_detector(detector)
        return
    if check_family in {RUN_SIGNAL_SUMMARY, RUN_DECISION_TRADE_COMPARISON}:
        _validate_run_detector(detector, check_family=check_family)
        return
    raise ValueError(f"unsupported research check family: {check_family}")


def evaluate_raw_event_check(
    candles: pd.DataFrame,
    *,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
    data_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate a known-at raw OHLCV detector and forward analytical outcomes."""

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
        "check_family": RAW_FORWARD_OUTCOME,
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
    check_family: str = RAW_FORWARD_OUTCOME,
) -> dict[str, Any]:
    return {
        "schema_version": CHECK_RESULT_SCHEMA_VERSION,
        "check_family": check_family,
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


def evaluate_run_signal_summary(
    dataset: Mapping[str, Any],
    *,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
    data_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_run_detector(detector, check_family=RUN_SIGNAL_SUMMARY)
    signals = normalize_run_signal_records(dataset.get("signals"))
    decisions = _records(dataset.get("decisions"))
    trades = _records(dataset.get("trades"))
    filtered = [signal for signal in signals if _record_matches(signal, detector)]
    outcome_spec = dict(outcomes or {})
    bucket_fields = _bucket_fields(outcome_spec, default=("symbol", "output_name", "event_key"))
    max_examples = int(outcome_spec.get("max_examples") or 100)
    min_sample_count = int(outcome_spec.get("min_sample_count") or 5)
    decision_by_signal = _index_by_any(decisions, ("signal_id", "root_signal_id", "parent_signal_id"))
    trades_by_decision = _group_by_any(trades, ("decision_id", "entry_decision_id", "open_decision_id"))

    buckets: Counter[str] = Counter()
    decision_states: Counter[str] = Counter()
    trade_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for signal in filtered:
        buckets[_bucket_key(signal, bucket_fields)] += 1
        decision = decision_by_signal.get(str(signal.get("signal_id") or ""))
        decision_id = str((decision or {}).get("decision_id") or signal.get("decision_id") or "").strip()
        decision_state = _decision_state(decision or signal)
        if decision_state:
            decision_states[decision_state] += 1
        linked_trades = trades_by_decision.get(decision_id, []) if decision_id else []
        if linked_trades:
            trade_counts["with_trade"] += len(linked_trades)
        else:
            trade_counts["without_trade"] += 1
        if len(examples) < max_examples:
            examples.append(
                {
                    "signal_id": signal.get("signal_id"),
                    "symbol": signal.get("symbol"),
                    "output_name": signal.get("output_name"),
                    "event_key": signal.get("event_key"),
                    "time": signal.get("time") or signal.get("bar_time") or signal.get("known_at"),
                    "decision_id": decision_id or None,
                    "decision_state": decision_state,
                    "linked_trade_count": len(linked_trades),
                }
            )

    return {
        "schema_version": CHECK_RESULT_SCHEMA_VERSION,
        "check_family": RUN_SIGNAL_SUMMARY,
        "status": "completed",
        "sample_count": len(filtered),
        "eligible_events": len(signals),
        "detector": dict(detector or {}),
        "outcomes": {
            "bucket_by": bucket_fields,
            "buckets": dict(sorted(buckets.items())),
            "decision_states": dict(sorted(decision_states.items())),
            "trade_counts": dict(sorted(trade_counts.items())),
        },
        "data_quality": dict(data_quality or {}),
        "recommendation": "promote_to_hypothesis" if len(filtered) >= min_sample_count else "needs_more_samples",
        "caveats": [] if filtered else ["No matching run signals found."],
        "events": examples,
        "event_count_truncated": max(0, len(filtered) - len(examples)),
    }


def evaluate_run_decision_trade_comparison(
    dataset: Mapping[str, Any],
    *,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
    data_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_run_detector(detector, check_family=RUN_DECISION_TRADE_COMPARISON)
    decisions = [decision for decision in _records(dataset.get("decisions")) if _record_matches(decision, detector)]
    trades = _records(dataset.get("trades"))
    outcome_spec = dict(outcomes or {})
    min_sample_count = int(outcome_spec.get("min_sample_count") or 5)
    max_examples = int(outcome_spec.get("max_examples") or 100)
    trades_by_decision = _group_by_any(trades, ("decision_id", "entry_decision_id", "open_decision_id"))
    by_state: dict[str, dict[str, Any]] = defaultdict(lambda: {"decision_count": 0, "trade_count": 0, "net_pnl": 0.0})
    examples: list[dict[str, Any]] = []

    for decision in decisions:
        state = _decision_state(decision) or "unknown"
        decision_id = str(decision.get("decision_id") or decision.get("id") or "").strip()
        linked_trades = trades_by_decision.get(decision_id, []) if decision_id else []
        bucket = by_state[state]
        bucket["decision_count"] += 1
        bucket["trade_count"] += len(linked_trades)
        bucket["net_pnl"] += sum(_float_or_zero(trade.get("net_pnl")) for trade in linked_trades)
        if len(examples) < max_examples:
            examples.append(
                {
                    "decision_id": decision_id or None,
                    "decision_state": state,
                    "symbol": decision.get("symbol"),
                    "reason_code": decision.get("reason_code") or decision.get("reason"),
                    "linked_trade_count": len(linked_trades),
                    "linked_trade_net_pnl": sum(_float_or_zero(trade.get("net_pnl")) for trade in linked_trades),
                }
            )

    summary = {
        state: {
            **payload,
            "avg_net_pnl_per_trade": (
                payload["net_pnl"] / payload["trade_count"]
                if payload["trade_count"]
                else None
            ),
        }
        for state, payload in sorted(by_state.items())
    }
    return {
        "schema_version": CHECK_RESULT_SCHEMA_VERSION,
        "check_family": RUN_DECISION_TRADE_COMPARISON,
        "status": "completed",
        "sample_count": len(decisions),
        "eligible_decisions": len(_records(dataset.get("decisions"))),
        "detector": dict(detector or {}),
        "outcomes": {"by_decision_state": summary},
        "data_quality": dict(data_quality or {}),
        "recommendation": "promote_to_hypothesis" if len(decisions) >= min_sample_count else "needs_more_samples",
        "caveats": [] if decisions else ["No matching run decisions found."],
        "events": examples,
        "event_count_truncated": max(0, len(decisions) - len(examples)),
    }


def evaluate_indicator_forward_outcome(
    evidence: Mapping[str, Any],
    *,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
    data_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_indicator_detector(detector)
    frame = _normalize_candles(pd.DataFrame(list(evidence.get("candles") or [])))
    rows = [row for row in _records(evidence.get("outputs")) if _indicator_row_matches(row, detector)]
    outcome_spec = dict(outcomes or {})
    forward_bars = _forward_bars(outcome_spec)
    direction = str(outcome_spec.get("direction") or "long").strip().lower()
    if direction not in {"long", "short"}:
        raise ValueError("outcomes.direction must be 'long' or 'short'")
    min_sample_count = int(outcome_spec.get("min_sample_count") or 20)
    min_edge_pct = float(outcome_spec.get("min_edge_pct") or 0.0)
    max_examples = int(outcome_spec.get("max_examples") or outcome_spec.get("max_events") or 100)
    bucket_fields = _bucket_fields(outcome_spec, default=("indicator_id", "output_name", "event_key"))

    event_outcomes: list[EventOutcome] = []
    examples: list[dict[str, Any]] = []
    buckets: Counter[str] = Counter()
    for row in rows:
        bar_index = int(row.get("bar_index") or 0)
        if bar_index < 0 or bar_index >= len(frame):
            continue
        outcome = _event_outcome(frame, bar_index, forward_bars=forward_bars, direction=direction)
        event_outcomes.append(outcome)
        buckets[_bucket_key(row, bucket_fields)] += 1
        if len(examples) < max_examples:
            examples.append(
                {
                    "event_time": outcome.event_time,
                    "event_index": outcome.event_index,
                    "indicator_id": row.get("indicator_id"),
                    "indicator_type": row.get("indicator_type"),
                    "output_name": row.get("output_name"),
                    "output_type": row.get("output_type"),
                    "event_key": row.get("event_key"),
                    "field": detector.get("field"),
                    "field_value": _indicator_field_value(row, str(detector.get("field") or ""))
                    if detector.get("field")
                    else None,
                    "close": outcome.close,
                    "outcomes": {str(k): v for k, v in outcome.outcomes.items()},
                }
            )

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
    return {
        "schema_version": CHECK_RESULT_SCHEMA_VERSION,
        "check_family": INDICATOR_FORWARD_OUTCOME,
        "status": "completed",
        "sample_count": len(event_outcomes),
        "eligible_events": len(_records(evidence.get("outputs"))),
        "eligible_bars": len(frame),
        "detector": dict(detector or {}),
        "outcomes": {
            "direction": direction,
            "forward_bars": forward_bars,
            "min_sample_count": min_sample_count,
            "min_edge_pct": min_edge_pct,
            "bucket_by": bucket_fields,
            "buckets": dict(sorted(buckets.items())),
            "summary": summary["by_window"],
        },
        "data_quality": {
            **dict(data_quality or {}),
            "indicator": dict(evidence.get("indicator") or {}),
            "runtime_path": evidence.get("runtime_path"),
            "ready_counts": dict(evidence.get("ready_counts") or {}),
            "not_ready_counts": dict(evidence.get("not_ready_counts") or {}),
        },
        "recommendation": summary["recommendation"],
        "caveats": summary["caveats"] if event_outcomes else ["No matching indicator output events found."],
        "events": examples,
        "event_count_truncated": max(0, len(event_outcomes) - len(examples)),
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

    detector_type = str(detector.get("type") or "raw_condition").strip()
    if detector_type != "raw_condition":
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
    previous = frame.iloc[idx - 1] if idx > 0 else row
    if normalized in {"previous_open", "previous_high", "previous_low", "previous_close", "previous_volume"}:
        return float(previous[normalized.removeprefix("previous_")])
    raise ValueError(f"unsupported raw detector field: {field}")


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


def _validate_raw_detector(detector: Mapping[str, Any]) -> None:
    if "all" in detector:
        conditions = detector.get("all")
        if not isinstance(conditions, Sequence) or isinstance(conditions, (str, bytes)):
            raise ValueError("detector.all must be a list")
        for item in conditions:
            _validate_raw_detector(_mapping(item, "detector.all item"))
        return
    if "any" in detector:
        conditions = detector.get("any")
        if not isinstance(conditions, Sequence) or isinstance(conditions, (str, bytes)):
            raise ValueError("detector.any must be a list")
        for item in conditions:
            _validate_raw_detector(_mapping(item, "detector.any item"))
        return
    if "not" in detector:
        _validate_raw_detector(_mapping(detector.get("not"), "detector.not"))
        return

    detector_type = str(detector.get("type") or "raw_condition").strip()
    if detector_type != "raw_condition":
        raise ValueError(f"unsupported research check detector type: {detector_type}")
    field = str(detector.get("field") or "").strip().lower()
    if not field:
        raise ValueError("detector.field is required")
    if field not in _RAW_DETECTOR_FIELDS:
        raise ValueError(f"unsupported raw detector field: {field}")
    operator = str(detector.get("operator") or "eq").strip().lower()
    if operator not in _DETECTOR_OPERATORS:
        raise ValueError(f"unsupported detector operator: {operator}")
    if detector.get("value_field") is not None:
        value_field = str(detector.get("value_field") or "").strip().lower()
        if value_field not in _RAW_DETECTOR_FIELDS:
            raise ValueError(f"unsupported raw detector field: {value_field}")
    elif detector.get("value") is None and operator not in {"is_true", "true"}:
        raise ValueError("detector.value or detector.value_field is required")
    if operator == "between":
        value = detector.get("value")
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != 2:
            raise ValueError("between operator requires value=[min,max]")
    if operator == "in":
        value = detector.get("value")
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise ValueError("in operator requires a list value")


def _validate_indicator_detector(detector: Mapping[str, Any]) -> None:
    detector_type = str(detector.get("type") or "").strip()
    if detector_type not in _INDICATOR_DETECTOR_TYPES:
        raise ValueError(f"unsupported indicator check detector type: {detector_type or '<empty>'}")
    output_name = str(detector.get("output_name") or "").strip()
    if not output_name and detector_type != "record_match":
        raise ValueError("detector.output_name is required")
    if detector_type == "indicator_output_match":
        field = str(detector.get("field") or "").strip()
        if not field:
            raise ValueError("detector.field is required")
        operator = str(detector.get("operator") or "eq").strip().lower()
        if operator not in _DETECTOR_OPERATORS:
            raise ValueError(f"unsupported detector operator: {operator}")
        if detector.get("value") is None and detector.get("value_field") is None and operator not in {"is_true", "true"}:
            raise ValueError("detector.value or detector.value_field is required")
    elif detector_type == "indicator_event_match":
        operator = str(detector.get("operator") or "eq").strip().lower()
        if operator not in _DETECTOR_OPERATORS:
            raise ValueError(f"unsupported detector operator: {operator}")


def _validate_run_detector(detector: Mapping[str, Any], *, check_family: str) -> None:
    detector_type = str(detector.get("type") or "").strip()
    allowed_types = _RUN_DETECTOR_TYPES_BY_FAMILY.get(check_family)
    if allowed_types is None:
        raise ValueError(f"unsupported research check family: {check_family}")
    if detector_type and detector_type not in allowed_types:
        raise ValueError(
            f"unsupported run check detector type for {check_family}: {detector_type}"
        )


def _indicator_row_matches(row: Mapping[str, Any], detector: Mapping[str, Any]) -> bool:
    detector_type = str(detector.get("type") or "").strip()
    if detector_type not in _INDICATOR_DETECTOR_TYPES:
        raise ValueError(f"unsupported indicator check detector type: {detector_type or '<empty>'}")

    for key, expected in detector.items():
        if key in {"type", "bucket_by", "field", "operator", "value", "value_field"}:
            continue
        candidates = _match_values(row, str(key))
        if not candidates:
            return False
        if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes, Mapping)):
            expected_values = _sequence_values(expected)
            if not any(_values_equal(candidate, item) for candidate in candidates for item in expected_values):
                return False
            continue
        if not any(_values_equal(candidate, expected) for candidate in candidates):
            return False

    if detector_type == "record_match":
        return True
    if detector_type == "indicator_event_match":
        return str(row.get("output_type") or "") == "signal"

    field = str(detector.get("field") or "").strip()
    found_left, left = _indicator_field_lookup(row, field)
    if not found_left:
        return False
    if detector.get("value_field") is not None:
        found_right, right = _indicator_field_lookup(row, str(detector.get("value_field")))
        if not found_right:
            return False
    elif detector.get("value") is not None:
        right = detector.get("value")
    elif str(detector.get("operator") or "").lower() in {"is_true", "true"}:
        right = True
    else:
        raise ValueError("detector.value or detector.value_field is required")
    return _compare(left, str(detector.get("operator") or "eq"), right)


def _indicator_field_value(row: Mapping[str, Any], field: str) -> Any:
    found, value = _indicator_field_lookup(row, field)
    return value if found else None


def _indicator_field_lookup(row: Mapping[str, Any], field: str) -> tuple[bool, Any]:
    normalized = str(field or "").strip()
    if not normalized:
        return False, None
    found, value = _nested_lookup(row, normalized)
    if found:
        return True, value
    for root_key in ("value", "event"):
        root = row.get(root_key)
        if isinstance(root, Mapping):
            found, value = _nested_lookup(root, normalized)
            if found:
                return True, value
    event = row.get("event")
    metadata = event.get("metadata") if isinstance(event, Mapping) and isinstance(event.get("metadata"), Mapping) else None
    if metadata:
        found, value = _nested_lookup(metadata, normalized)
        if found:
            return True, value
    value_root = row.get("value")
    fields = value_root.get("fields") if isinstance(value_root, Mapping) and isinstance(value_root.get("fields"), Mapping) else None
    if fields:
        found, value = _nested_lookup(fields, normalized)
        if found:
            return True, value
    return False, None


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def normalize_run_signal_records(value: Any) -> list[dict[str, Any]]:
    return [_normalize_run_signal_record(record) for record in _records(value)]


def _normalize_run_signal_record(record: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    context = _mapping(normalized.get("context"), "signal.context") if isinstance(normalized.get("context"), Mapping) else {}
    artifact = context.get("decision_artifact") if isinstance(context.get("decision_artifact"), Mapping) else {}
    artifact_context = artifact.get("decision_context") if isinstance(artifact.get("decision_context"), Mapping) else {}
    trigger = artifact.get("trigger") if isinstance(artifact.get("trigger"), Mapping) else {}

    output_refs = _unique_text(
        [
            normalized.get("output_ref"),
            normalized.get("source_id") if str(normalized.get("source_type") or "") == "indicator_output" else None,
            context.get("trigger_output_ref"),
            artifact_context.get("trigger_output_ref"),
            trigger.get("output_ref"),
        ]
    )
    output_names = _unique_text(
        [
            normalized.get("output_name"),
            context.get("output_name"),
            *[_output_name_from_ref(ref) for ref in output_refs],
            *_output_names_from_signal_output_maps(normalized, context),
        ]
    )
    event_keys = _unique_text(
        [
            normalized.get("event_key"),
            context.get("event_key"),
            artifact_context.get("event_key"),
            trigger.get("event_key"),
            *_event_keys_from_signal_output_maps(normalized, context),
        ]
    )
    if output_refs:
        normalized["output_refs"] = output_refs
    if output_names:
        normalized["output_names"] = output_names
        if normalized.get("output_name") in (None, ""):
            normalized["output_name"] = output_names[0]
    if event_keys:
        normalized["event_keys"] = event_keys
        if normalized.get("event_key") in (None, ""):
            normalized["event_key"] = event_keys[0]
    return normalized


def _signal_output_maps(record: Mapping[str, Any], context: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    maps: list[Mapping[str, Any]] = []
    indicator_context = record.get("indicator_context") if isinstance(record.get("indicator_context"), Mapping) else {}
    outputs = indicator_context.get("outputs") if isinstance(indicator_context.get("outputs"), Mapping) else {}
    if outputs:
        maps.append(outputs)

    containers: list[Mapping[str, Any]] = [context]
    artifact = context.get("decision_artifact") if isinstance(context.get("decision_artifact"), Mapping) else {}
    if artifact:
        containers.append(artifact)
        artifact_context = artifact.get("decision_context") if isinstance(artifact.get("decision_context"), Mapping) else {}
        if artifact_context:
            containers.append(artifact_context)

    for container in containers:
        for key in (
            "referenced_outputs",
            "observed_outputs",
            "resolved_indicator_values",
            "indicator_outputs",
            "context_values",
        ):
            value = container.get(key)
            if isinstance(value, Mapping):
                maps.append(value)
    return maps


def _output_names_from_signal_output_maps(record: Mapping[str, Any], context: Mapping[str, Any]) -> list[str]:
    values: list[Any] = []
    for output_map in _signal_output_maps(record, context):
        for output_ref, raw in output_map.items():
            if isinstance(raw, Mapping):
                values.append(raw.get("output_name"))
            values.append(_output_name_from_ref(output_ref))
    return _unique_text(values)


def _event_keys_from_signal_output_maps(record: Mapping[str, Any], context: Mapping[str, Any]) -> list[str]:
    values: list[Any] = []
    for output_map in _signal_output_maps(record, context):
        for raw in output_map.values():
            values.extend(_event_keys_from_output_payload(raw))
    return _unique_text(values)


def _event_keys_from_output_payload(raw: Any) -> list[Any]:
    if not isinstance(raw, Mapping):
        return []
    values: list[Any] = [raw.get("event_key")]
    values.extend(_sequence_values(raw.get("event_keys")))
    events = raw.get("events")
    if isinstance(events, Sequence) and not isinstance(events, (str, bytes, Mapping)):
        for event in events:
            if isinstance(event, Mapping):
                values.append(event.get("event_key") or event.get("key"))
            else:
                values.append(event)
    value = raw.get("value")
    if isinstance(value, Mapping):
        values.extend(_event_keys_from_output_payload(value))
    return values


def _output_name_from_ref(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "." not in text:
        return text
    return text.rsplit(".", 1)[-1].strip() or None


def _unique_text(values: Sequence[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _record_matches(record: Mapping[str, Any], detector: Mapping[str, Any]) -> bool:
    detector_type = str(detector.get("type") or "").strip()
    if detector_type and detector_type not in {"run_signal_match", "run_decision_match", "record_match"}:
        raise ValueError(f"unsupported run check detector type: {detector_type}")
    for key, expected in detector.items():
        if key in {"type", "bucket_by"}:
            continue
        candidates = _match_values(record, str(key))
        if not candidates:
            return False
        if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes, Mapping)):
            expected_values = _sequence_values(expected)
            if not any(_values_equal(candidate, item) for candidate in candidates for item in expected_values):
                return False
            continue
        if not any(_values_equal(candidate, expected) for candidate in candidates):
            return False
    return True


def _match_values(record: Mapping[str, Any], path: str) -> list[Any]:
    found, value = _nested_lookup(record, path)
    values = _sequence_values(value) if found else []
    if path == "output_name":
        found_names, names = _nested_lookup(record, "output_names")
        if found_names:
            values.extend(_sequence_values(names))
    elif path == "event_key":
        found_keys, keys = _nested_lookup(record, "event_keys")
        if found_keys:
            values.extend(_sequence_values(keys))
    return values


def _sequence_values(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, Mapping)):
        return list(value)
    return [value]


def _values_equal(actual: Any, expected: Any) -> bool:
    if actual is None or expected is None:
        return actual is None and expected is None
    return _match_text(actual) == _match_text(expected)


def _match_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _nested_value(record: Mapping[str, Any], path: str) -> Any:
    found, value = _nested_lookup(record, path)
    if found and not (path in {"output_name", "event_key"} and value in (None, "")):
        return value
    if path == "output_name":
        found_names, names = _nested_lookup(record, "output_names")
        values = _sequence_values(names) if found_names else []
        return next((value for value in values if value not in (None, "")), None)
    if path == "event_key":
        found_keys, keys = _nested_lookup(record, "event_keys")
        values = _sequence_values(keys) if found_keys else []
        return next((value for value in values if value not in (None, "")), None)
    return None


def _nested_lookup(record: Mapping[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = record
    for part in [item for item in str(path or "").split(".") if item]:
        if not isinstance(current, Mapping):
            return False, None
        if part not in current:
            return False, None
        current = current.get(part)
    return True, current


def _bucket_fields(outcome_spec: Mapping[str, Any], *, default: Sequence[str]) -> list[str]:
    raw = outcome_spec.get("bucket_by") or outcome_spec.get("buckets") or list(default)
    if isinstance(raw, str):
        return [item.strip() for item in raw.split(",") if item.strip()]
    if isinstance(raw, Sequence):
        return [str(item).strip() for item in raw if str(item).strip()]
    return list(default)


def _bucket_key(record: Mapping[str, Any], fields: Sequence[str]) -> str:
    parts = []
    for field in fields:
        value = _nested_value(record, field)
        parts.append(f"{field}={value if value not in (None, '') else 'unknown'}")
    return "|".join(parts)


def _index_by_any(records: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for record in records:
        for key in keys:
            value = str(record.get(key) or "").strip()
            if value:
                indexed.setdefault(value, dict(record))
    return indexed


def _group_by_any(records: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        for key in keys:
            value = str(record.get(key) or "").strip()
            if value:
                grouped[value].append(dict(record))
                break
    return grouped


def _decision_state(record: Mapping[str, Any]) -> str | None:
    for key in ("decision_state", "state", "status", "decision"):
        value = str(record.get(key) or "").strip().lower()
        if value:
            return value
    return None


def _float_or_zero(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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
