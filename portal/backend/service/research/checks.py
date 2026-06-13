"""Lightweight analytical research checks over source market facts."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from statistics import median
from typing import Any, Mapping, Sequence

import pandas as pd


RAW_FORWARD_OUTCOME = "raw_forward_outcome"
INDICATOR_FORWARD_OUTCOME = "indicator_forward_outcome"
SIGNAL_AUDIT = "signal_audit"
CANDIDATE_LIFECYCLE = "candidate_lifecycle"
RUN_SIGNAL_SUMMARY = "run_signal_summary"
RUN_DECISION_TRADE_COMPARISON = "run_decision_trade_comparison"
SUPPORTED_CHECK_FAMILY = RAW_FORWARD_OUTCOME
SUPPORTED_CHECK_FAMILIES = {
    RAW_FORWARD_OUTCOME,
    INDICATOR_FORWARD_OUTCOME,
    SIGNAL_AUDIT,
    CANDIDATE_LIFECYCLE,
    RUN_SIGNAL_SUMMARY,
    RUN_DECISION_TRADE_COMPARISON,
}
CHECK_RESULT_SCHEMA_VERSION = "research_check_result.v1"
SIGNAL_AUDIT_SCHEMA_VERSION = "signal_audit_result.v1"
CANDIDATE_LIFECYCLE_SCHEMA_VERSION = "candidate_lifecycle_result.v1"
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
_SIGNAL_AUDIT_DETECTOR_TYPES = {"signal_audit"}
_CANDIDATE_LIFECYCLE_DETECTOR_TYPES = {"candidate_lifecycle"}
_CANDIDATE_LIFECYCLE_FILTER_FIELDS = {
    "output_name",
    "candidate_id",
    "family",
    "side",
    "stage",
    "status",
    "group_key",
    "source_event_id",
    "source_output",
    "source_event_key",
    "signal_output",
    "signal_event_key",
    "known_at",
    "reason",
}
_CANDIDATE_LIFECYCLE_SIGNAL_LINK_FIELDS = {"signal_output", "signal_event_key"}
_CANDIDATE_LIFECYCLE_MATCHER_CONTROL_FIELDS = {
    "type",
    "funnel_stages",
    "terminal_stages",
    "signal_stages",
    *_CANDIDATE_LIFECYCLE_SIGNAL_LINK_FIELDS,
}


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
    if check_family == SIGNAL_AUDIT:
        _validate_signal_audit_detector(detector)
        return
    if check_family == CANDIDATE_LIFECYCLE:
        _validate_candidate_lifecycle_detector(detector)
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
    max_examples = int(outcome_spec.get("max_examples") or 250)

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
        for outcome in event_outcomes[: max(0, max_examples)]
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
    max_examples = int(outcome_spec.get("max_examples") or 100)
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


def evaluate_signal_audit(
    evidence: Mapping[str, Any],
    *,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
    data_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_signal_audit_detector(detector)
    indicator = evidence.get("indicator") if isinstance(evidence.get("indicator"), Mapping) else {}
    outcome_spec = dict(outcomes or {})
    max_examples = int(outcome_spec.get("max_examples") or 100)
    expectations = _signal_audit_expectations(detector)
    output_rows = _records(evidence.get("outputs"))
    audit = _audit_signal_expectations(
        output_rows,
        expectations=expectations,
    )
    issue_examples = audit["issues"][: max(0, max_examples)]
    summary = audit["summary"]
    missing_count = int(summary["missing_expected_count"])
    invalid_count = int(summary["invalid_emitted_count"])
    excluded_count = int(summary["excluded_candidate_count"])
    if missing_count or invalid_count:
        recommendation = "repair_signal"
        caveats = ["Signal emissions diverged from the declared public-output expectation."]
    elif excluded_count:
        recommendation = "review_contract"
        caveats = ["Some candidate transitions were excluded by the declared expectation policy."]
    else:
        recommendation = "contract_holds"
        caveats = []

    return {
        "schema_version": SIGNAL_AUDIT_SCHEMA_VERSION,
        "check_family": SIGNAL_AUDIT,
        "status": "completed",
        "sample_count": int(summary["expected_count"]),
        "eligible_bars": len(
            {
                _required_int(row.get("bar_index"), "output.bar_index")
                for row in output_rows
                if row.get("bar_index") is not None
            }
        ),
        "eligible_events": int(summary["emitted_count"]),
        "detector": dict(detector or {}),
        "outcomes": {
            "audit_kind": "semantic_signal_reconciliation",
            "expectations": expectations,
            "summary": summary,
            "by_expectation": audit["by_expectation"],
        },
        "data_quality": {
            **dict(data_quality or {}),
            "indicator": dict(indicator),
            "runtime_path": evidence.get("runtime_path"),
            "ready_counts": dict(evidence.get("ready_counts") or {}),
            "not_ready_counts": dict(evidence.get("not_ready_counts") or {}),
        },
        "recommendation": recommendation,
        "caveats": caveats,
        "events": issue_examples,
        "event_count_truncated": max(0, len(audit["issues"]) - len(issue_examples)),
    }


def evaluate_candidate_lifecycle(
    evidence: Mapping[str, Any],
    *,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any] | None = None,
    data_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_candidate_lifecycle_detector(detector)
    indicator = evidence.get("indicator") if isinstance(evidence.get("indicator"), Mapping) else {}
    outcome_spec = dict(outcomes or {})
    max_examples = int(outcome_spec.get("max_examples") or 100)
    funnel_stages = _string_list(
        outcome_spec.get("funnel_stages")
        or detector.get("funnel_stages")
        or ["formed", "eligible", "touched", "confirmed"]
    )
    terminal_stages = set(
        _string_list(
            outcome_spec.get("terminal_stages")
            or detector.get("terminal_stages")
            or ["confirmed", "invalidated", "expired"]
        )
    )
    signal_stages = set(
        _string_list(
            outcome_spec.get("signal_stages")
            or detector.get("signal_stages")
            or ["confirmed"]
        )
    )
    output_rows = _records(evidence.get("outputs"))
    lifecycle_rows = [
        row
        for row in (_normalize_lifecycle_row(row) for row in output_rows if str(row.get("output_type") or "") == "lifecycle")
        if _candidate_lifecycle_row_matches(row, detector)
    ]
    signal_rows = [
        row
        for row in output_rows
        if str(row.get("output_type") or "") == "signal"
    ]
    audit = _audit_candidate_lifecycle(
        lifecycle_rows,
        signal_rows,
        detector=detector,
        funnel_stages=funnel_stages,
        terminal_stages=terminal_stages,
        signal_stages=signal_stages,
    )
    issue_examples = audit["issues"][: max(0, max_examples)]
    summary = audit["summary"]
    if summary["missing_signal_count"] or summary["invalid_signal_count"] or summary["lifecycle_issue_count"]:
        recommendation = "repair_lifecycle"
        caveats = ["Candidate lifecycle evidence diverged from emitted signals or valid stage ordering."]
    elif not lifecycle_rows:
        recommendation = "needs_lifecycle_evidence"
        caveats = ["No matching lifecycle events found."]
    else:
        recommendation = "contract_holds"
        caveats = []

    return {
        "schema_version": CANDIDATE_LIFECYCLE_SCHEMA_VERSION,
        "check_family": CANDIDATE_LIFECYCLE,
        "status": "completed",
        "sample_count": int(summary["candidate_count"]),
        "eligible_events": len(lifecycle_rows),
        "eligible_bars": len(
            {
                _required_int(row.get("bar_index"), "output.bar_index")
                for row in lifecycle_rows
                if row.get("bar_index") is not None
            }
        ),
        "detector": dict(detector or {}),
        "outcomes": {
            "audit_kind": "candidate_lifecycle_funnel",
            "funnel_stages": funnel_stages,
            "terminal_stages": sorted(terminal_stages),
            "signal_stages": sorted(signal_stages),
            "summary": summary,
            "funnel": audit["funnel"],
            "stage_counts": audit["stage_counts"],
            "status_counts": audit["status_counts"],
            "terminal_counts": audit["terminal_counts"],
            "reason_counts": audit["reason_counts"],
            "by_family_side": audit["by_family_side"],
        },
        "data_quality": {
            **dict(data_quality or {}),
            "indicator": dict(indicator),
            "runtime_path": evidence.get("runtime_path"),
            "ready_counts": dict(evidence.get("ready_counts") or {}),
            "not_ready_counts": dict(evidence.get("not_ready_counts") or {}),
        },
        "recommendation": recommendation,
        "caveats": caveats,
        "events": issue_examples,
        "event_count_truncated": max(0, len(audit["issues"]) - len(issue_examples)),
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


def _validate_signal_audit_detector(detector: Mapping[str, Any]) -> None:
    detector_type = str(detector.get("type") or "").strip()
    if detector_type not in _SIGNAL_AUDIT_DETECTOR_TYPES:
        raise ValueError(f"unsupported signal audit detector type: {detector_type or '<empty>'}")
    _signal_audit_expectations(detector)


def _validate_candidate_lifecycle_detector(detector: Mapping[str, Any]) -> None:
    detector_type = str(detector.get("type") or "").strip()
    if detector_type not in _CANDIDATE_LIFECYCLE_DETECTOR_TYPES:
        raise ValueError(f"unsupported candidate lifecycle detector type: {detector_type or '<empty>'}")
    allowed = {
        "type",
        "funnel_stages",
        "terminal_stages",
        "signal_stages",
        *_CANDIDATE_LIFECYCLE_FILTER_FIELDS,
    }
    unsupported = sorted(str(key) for key in detector.keys() if str(key) not in allowed)
    if unsupported:
        raise ValueError(f"unsupported candidate lifecycle detector fields: {', '.join(unsupported)}")


def _signal_audit_expectations(detector: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_expectations = detector.get("expectations")
    if raw_expectations is None:
        raw_expectations = [
            {
                key: value
                for key, value in detector.items()
                if key not in {"type", "expectations"}
            }
        ]
    if not isinstance(raw_expectations, Sequence) or isinstance(raw_expectations, (str, bytes, Mapping)):
        raise ValueError("detector.expectations must be a list")
    expectations = [
        _normalize_signal_audit_expectation(_mapping(raw, "detector.expectations item"), index=index)
        for index, raw in enumerate(raw_expectations)
    ]
    if not expectations:
        raise ValueError("signal audit requires at least one expectation")
    return expectations


def _normalize_signal_audit_expectation(raw: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    expectation_type = str(
        raw.get("expectation_type")
        or raw.get("mode")
        or raw.get("kind")
        or raw.get("audit_type")
        or ("transition" if "transition" in raw or "from" in raw or "to" in raw else "condition")
    ).strip()
    if expectation_type not in {"transition", "condition"}:
        raise ValueError(f"unsupported signal audit expectation type: {expectation_type}")
    source_output = _required_text(
        raw.get("source_output") or raw.get("source_output_name"),
        "expectation.source_output",
    )
    source_field = _required_text(
        raw.get("source_field") or raw.get("field"),
        "expectation.source_field",
    )
    signal_output = _required_text(
        raw.get("signal_output") or raw.get("signal_output_name") or raw.get("output_name"),
        "expectation.signal_output",
    )
    event_key = _required_text(raw.get("event_key"), "expectation.event_key")
    expectation = {
        "name": str(raw.get("name") or f"expectation_{index + 1}").strip(),
        "expectation_type": expectation_type,
        "source_output": source_output,
        "source_field": source_field,
        "signal_output": signal_output,
        "event_key": event_key,
        "same_group_by": _string_list(raw.get("same_group_by") or raw.get("same_group_fields") or []),
        "record_excluded_candidates": bool(raw.get("record_excluded_candidates", True)),
        "require_contiguous_source_rows": bool(raw.get("require_contiguous_source_rows", True)),
    }
    if expectation_type == "transition":
        transition = raw.get("transition") if isinstance(raw.get("transition"), Mapping) else {}
        if _has_mapping_key(transition, "from"):
            from_value = transition.get("from")
        elif "from" in raw:
            from_value = raw.get("from")
        else:
            raise ValueError("transition expectation requires from")
        if _has_mapping_key(transition, "to"):
            to_value = transition.get("to")
        elif "to" in raw:
            to_value = raw.get("to")
        else:
            raise ValueError("transition expectation requires to")
        expectation["from"] = from_value
        expectation["to"] = to_value
    else:
        operator = str(raw.get("operator") or "eq").strip().lower()
        if operator not in _DETECTOR_OPERATORS:
            raise ValueError(f"unsupported signal audit condition operator: {operator}")
        expectation["operator"] = operator
        if raw.get("value_field") is not None:
            expectation["value_field"] = str(raw.get("value_field") or "").strip()
            if not expectation["value_field"]:
                raise ValueError("condition expectation value_field must not be empty")
        elif _has_mapping_key(raw, "value"):
            expectation["value"] = raw.get("value")
        elif operator not in {"is_true", "true"}:
            raise ValueError("condition expectation requires value or value_field")
    return expectation


def _audit_signal_expectations(
    output_rows: Sequence[Mapping[str, Any]],
    *,
    expectations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows_by_output = _rows_by_output(output_rows)
    expected: list[dict[str, Any]] = []
    emitted: list[dict[str, Any]] = []
    excluded_candidates: list[dict[str, Any]] = []
    by_expectation: dict[str, dict[str, int]] = {}
    for expectation in expectations:
        expectation_name = str(expectation["name"])
        expectation_expected, expectation_excluded = _expected_signal_events_for_expectation(
            rows_by_output,
            expectation=expectation,
        )
        expectation_emitted = _emitted_signal_events_for_expectation(
            rows_by_output,
            expectation=expectation,
        )
        expectation_expected_by_key = _audit_events_by_key(expectation_expected)
        expectation_emitted_by_key = _audit_events_by_key(expectation_emitted)
        expectation_expected_keys = set(expectation_expected_by_key)
        expectation_emitted_keys = set(expectation_emitted_by_key)
        by_expectation[expectation_name] = {
            "expected_count": len(expectation_expected),
            "emitted_count": len(expectation_emitted),
            "matched_count": len(expectation_expected_keys & expectation_emitted_keys),
            "missing_expected_count": len(expectation_expected_keys - expectation_emitted_keys),
            "invalid_emitted_count": len(expectation_emitted_keys - expectation_expected_keys),
            "excluded_candidate_count": len(expectation_excluded),
        }
        expected.extend(expectation_expected)
        emitted.extend(expectation_emitted)
        excluded_candidates.extend(expectation_excluded)

    expected_by_key = _audit_events_by_key(expected)
    emitted_by_key = _audit_events_by_key(emitted)
    expected_keys = set(expected_by_key)
    emitted_keys = set(emitted_by_key)
    issues: list[dict[str, Any]] = []
    for key in sorted(expected_keys - emitted_keys, key=_audit_key_sort):
        for event in expected_by_key[key]:
            issues.append({"classification": "missing_expected", **event})
    for key in sorted(emitted_keys - expected_keys, key=_audit_key_sort):
        for event in emitted_by_key[key]:
            issues.append({"classification": "invalid_emitted", **_compact_emitted_event(event)})
    for event in excluded_candidates:
        issues.append({"classification": "excluded_candidate", **event})

    summary = {
        "expected_count": len(expected),
        "emitted_count": len(emitted),
        "matched_count": len(expected_keys & emitted_keys),
        "missing_expected_count": len(expected_keys - emitted_keys),
        "invalid_emitted_count": len(emitted_keys - expected_keys),
        "excluded_candidate_count": len(excluded_candidates),
    }
    return {"summary": summary, "by_expectation": by_expectation, "issues": issues}


def _expected_signal_events_for_expectation(
    rows_by_output: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    expectation: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    source_rows = list(rows_by_output.get(str(expectation["source_output"])) or [])
    previous_row: Mapping[str, Any] | None = None
    expected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in source_rows:
        matches, reason = _source_row_matches_expectation(
            previous_row=previous_row,
            row=row,
            expectation=expectation,
        )
        if matches:
            event = _audit_expected_event(row, expectation=expectation, reason=reason)
            if _expectation_group_changed(previous_row=previous_row, row=row, expectation=expectation):
                if bool(expectation.get("record_excluded_candidates", True)):
                    excluded.append({**event, "reason": "group_changed_excluded"})
            else:
                expected.append(event)
        previous_row = row
    return expected, excluded


def _emitted_signal_events_for_expectation(
    rows_by_output: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    expectation: Mapping[str, Any],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for row in rows_by_output.get(str(expectation["signal_output"])) or []:
        if str(row.get("output_type") or "") != "signal":
            continue
        if not _values_equal(row.get("event_key"), expectation["event_key"]):
            continue
        events.append(
            {
                "expectation": expectation["name"],
                "bar_index": _required_int(row.get("bar_index"), "output.bar_index"),
                "time": row.get("time"),
                "output_name": row.get("output_name"),
                "event_key": row.get("event_key"),
                "source_output": expectation["source_output"],
                "source_field": expectation["source_field"],
            }
        )
    return events


def _source_row_matches_expectation(
    *,
    previous_row: Mapping[str, Any] | None,
    row: Mapping[str, Any],
    expectation: Mapping[str, Any],
) -> tuple[bool, str]:
    expectation_type = str(expectation.get("expectation_type") or "")
    if expectation_type == "transition":
        if previous_row is None:
            return False, "missing_previous_source_row"
        if bool(expectation.get("require_contiguous_source_rows", True)) and (
            _required_int(row.get("bar_index"), "output.bar_index")
            != _required_int(previous_row.get("bar_index"), "output.bar_index") + 1
        ):
            return False, "non_contiguous_source_row"
        found_previous, previous_value = _indicator_field_lookup(previous_row, str(expectation["source_field"]))
        found_current, current_value = _indicator_field_lookup(row, str(expectation["source_field"]))
        return (
            found_previous
            and found_current
            and _values_equal(previous_value, expectation["from"])
            and _values_equal(current_value, expectation["to"]),
            "transition_matched",
        )
    found_left, left = _indicator_field_lookup(row, str(expectation["source_field"]))
    if not found_left:
        return False, "field_missing"
    if "value_field" in expectation and expectation.get("value_field") is not None:
        found_right, right = _indicator_field_lookup(row, str(expectation["value_field"]))
        if not found_right:
            return False, "value_field_missing"
    elif "value" in expectation:
        right = expectation.get("value")
    elif str(expectation.get("operator") or "").lower() in {"is_true", "true"}:
        right = True
    else:
        raise ValueError("condition expectation requires value or value_field")
    return _compare(left, str(expectation.get("operator") or "eq"), right), "condition_matched"


def _expectation_group_changed(
    *,
    previous_row: Mapping[str, Any] | None,
    row: Mapping[str, Any],
    expectation: Mapping[str, Any],
) -> bool:
    fields = list(expectation.get("same_group_by") or [])
    if not fields:
        return False
    if previous_row is None:
        return True
    for field in fields:
        found_previous, previous_value = _indicator_field_lookup(previous_row, str(field))
        found_current, current_value = _indicator_field_lookup(row, str(field))
        if not found_previous or not found_current or not _values_equal(previous_value, current_value):
            return True
    return False


def _audit_expected_event(
    row: Mapping[str, Any],
    *,
    expectation: Mapping[str, Any],
    reason: str,
) -> dict[str, Any]:
    found_value, source_value = _indicator_field_lookup(row, str(expectation["source_field"]))
    group_values: dict[str, Any] = {}
    for field in expectation.get("same_group_by") or []:
        found, value = _indicator_field_lookup(row, str(field))
        group_values[str(field)] = value if found else None
    return {
        "expectation": expectation["name"],
        "bar_index": int(row["bar_index"]),
        "time": row.get("time"),
        "output_name": expectation["signal_output"],
        "event_key": expectation["event_key"],
        "reason": reason,
        "source_output": expectation["source_output"],
        "source_field": expectation["source_field"],
        "source_value": source_value if found_value else None,
        "group_values": group_values,
    }


def _compact_emitted_event(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "expectation": event.get("expectation"),
        "bar_index": int(event["bar_index"]),
        "time": event.get("time"),
        "output_name": event.get("output_name"),
        "event_key": event.get("event_key"),
        "source_output": event.get("source_output"),
        "source_field": event.get("source_field"),
    }


def _audit_events_by_key(events: Sequence[Mapping[str, Any]]) -> dict[tuple[str, int, str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, int, str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        key = (
            str(event.get("expectation") or ""),
            int(event["bar_index"]),
            str(event["output_name"]),
            str(event["event_key"]),
        )
        grouped[key].append(dict(event))
    return grouped


def _audit_key_sort(key: tuple[str, int, str, str]) -> tuple[int, str, str, str]:
    return (key[1], key[0], key[2], key[3])


def _rows_by_output(output_rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in output_rows:
        output_name = str(row.get("output_name") or "").strip()
        if not output_name:
            continue
        _required_int(row.get("bar_index"), "output.bar_index")
        rows[output_name].append(dict(row))
    for values in rows.values():
        values.sort(key=lambda row: (_required_int(row.get("bar_index"), "output.bar_index"), str(row.get("event_key") or "")))
    return rows


def _normalize_lifecycle_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for field in _CANDIDATE_LIFECYCLE_FILTER_FIELDS:
        found, value = _indicator_field_lookup(row, field)
        if found:
            normalized[field] = value
    candidate_id = _required_text(normalized.get("candidate_id"), "lifecycle.candidate_id")
    family = _required_text(normalized.get("family"), "lifecycle.family")
    side = _required_text(normalized.get("side"), "lifecycle.side")
    stage = _required_text(normalized.get("stage"), "lifecycle.stage")
    status = _required_text(normalized.get("status"), "lifecycle.status")
    known_at = normalized.get("known_at")
    if known_at is None:
        raise ValueError("lifecycle.known_at is required")
    normalized["candidate_id"] = candidate_id
    normalized["family"] = family
    normalized["side"] = side
    normalized["stage"] = stage
    normalized["status"] = status
    normalized["known_at"] = known_at
    return normalized


def _candidate_lifecycle_row_matches(row: Mapping[str, Any], detector: Mapping[str, Any]) -> bool:
    for key, expected in detector.items():
        if key in _CANDIDATE_LIFECYCLE_MATCHER_CONTROL_FIELDS:
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
    return True


def _audit_candidate_lifecycle(
    lifecycle_rows: Sequence[Mapping[str, Any]],
    signal_rows: Sequence[Mapping[str, Any]],
    *,
    detector: Mapping[str, Any],
    funnel_stages: Sequence[str],
    terminal_stages: set[str],
    signal_stages: set[str],
) -> dict[str, Any]:
    rows_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    stage_event_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    by_family_side: Counter[str] = Counter()
    for row in lifecycle_rows:
        candidate_id = str(row["candidate_id"])
        row_copy = dict(row)
        rows_by_candidate[candidate_id].append(row_copy)
        stage_event_counts[str(row_copy["stage"])] += 1
        status_counts[str(row_copy["status"])] += 1
        reason = str(row_copy.get("reason") or "").strip()
        if reason:
            reason_counts[reason] += 1
        by_family_side[_bucket_key(row_copy, ("family", "side"))] += 1

    for rows in rows_by_candidate.values():
        rows.sort(key=_lifecycle_row_sort_key)

    stage_candidate_counts = {
        stage: sum(
            1
            for rows in rows_by_candidate.values()
            if any(str(row.get("stage") or "") == stage for row in rows)
        )
        for stage in sorted(stage_event_counts)
    }
    funnel: dict[str, dict[str, Any]] = {}
    previous_count: int | None = None
    for stage in funnel_stages:
        count = sum(
            1
            for rows in rows_by_candidate.values()
            if any(str(row.get("stage") or "") == stage for row in rows)
        )
        funnel[stage] = {
            "candidate_count": count,
            "conversion_from_previous": (
                (count / previous_count) if previous_count else None
            ),
        }
        previous_count = count

    terminal_counts: Counter[str] = Counter()
    open_candidate_count = 0
    lifecycle_issues: list[dict[str, Any]] = []
    for candidate_id, rows in rows_by_candidate.items():
        terminal_rows = [row for row in rows if str(row.get("stage") or "") in terminal_stages]
        if not terminal_rows:
            open_candidate_count += 1
        else:
            terminal_counts[str(terminal_rows[-1].get("stage") or "unknown")] += 1
        if len(terminal_rows) > 1:
            lifecycle_issues.append(
                {
                    "classification": "lifecycle_issue",
                    "issue": "multiple_terminal_stages",
                    "candidate_id": candidate_id,
                    "stages": [row.get("stage") for row in terminal_rows],
                }
            )
        if terminal_rows and rows[-1] is not terminal_rows[-1]:
            lifecycle_issues.append(
                {
                    "classification": "lifecycle_issue",
                    "issue": "terminal_stage_followed_by_lifecycle",
                    "candidate_id": candidate_id,
                    "terminal_stage": terminal_rows[-1].get("stage"),
                    "last_stage": rows[-1].get("stage"),
                }
            )
        previous_known_at: float | None = None
        for row in rows:
            known_at = _known_at_number(row.get("known_at"))
            if known_at is not None and previous_known_at is not None and known_at < previous_known_at:
                lifecycle_issues.append(
                    {
                        "classification": "lifecycle_issue",
                        "issue": "known_at_decreased",
                        "candidate_id": candidate_id,
                        "stage": row.get("stage"),
                        "known_at": row.get("known_at"),
                    }
                )
            if known_at is not None:
                previous_known_at = known_at

    signal_audit = _audit_lifecycle_signal_links(
        lifecycle_rows,
        signal_rows,
        detector=detector,
        signal_stages=signal_stages,
    )
    issues = [*lifecycle_issues, *signal_audit["issues"]]
    summary = {
        "candidate_count": len(rows_by_candidate),
        "lifecycle_event_count": len(lifecycle_rows),
        "open_candidate_count": open_candidate_count,
        "terminal_candidate_count": sum(terminal_counts.values()),
        "missing_signal_count": signal_audit["missing_signal_count"],
        "invalid_signal_count": signal_audit["invalid_signal_count"],
        "matched_signal_count": signal_audit["matched_signal_count"],
        "expected_signal_count": signal_audit["expected_signal_count"],
        "lifecycle_issue_count": len(lifecycle_issues),
    }
    return {
        "summary": summary,
        "funnel": funnel,
        "stage_counts": {
            "events": dict(sorted(stage_event_counts.items())),
            "candidates": dict(sorted(stage_candidate_counts.items())),
        },
        "status_counts": dict(sorted(status_counts.items())),
        "terminal_counts": dict(sorted(terminal_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "by_family_side": dict(sorted(by_family_side.items())),
        "issues": issues,
    }


def _audit_lifecycle_signal_links(
    lifecycle_rows: Sequence[Mapping[str, Any]],
    signal_rows: Sequence[Mapping[str, Any]],
    *,
    detector: Mapping[str, Any],
    signal_stages: set[str],
) -> dict[str, Any]:
    expected: list[dict[str, Any]] = []
    for row in lifecycle_rows:
        if str(row.get("stage") or "") not in signal_stages:
            continue
        signal_output = _lifecycle_signal_field(row, detector, "signal_output")
        signal_event_key = _lifecycle_signal_field(row, detector, "signal_event_key")
        if not signal_output or not signal_event_key:
            continue
        expected.append(
            {
                "candidate_id": str(row["candidate_id"]),
                "bar_index": _required_int(row.get("bar_index"), "output.bar_index"),
                "time": row.get("time"),
                "output_name": signal_output,
                "event_key": signal_event_key,
                "stage": row.get("stage"),
            }
        )
    expected_pairs = {
        (str(item["output_name"]), str(item["event_key"]))
        for item in expected
    }
    if not expected_pairs:
        return {
            "expected_signal_count": 0,
            "matched_signal_count": 0,
            "missing_signal_count": 0,
            "invalid_signal_count": 0,
            "issues": [],
        }
    emitted: list[dict[str, Any]] = []
    for row in signal_rows:
        output_name = str(row.get("output_name") or "").strip()
        event_key = str(row.get("event_key") or "").strip()
        if expected_pairs and (output_name, event_key) not in expected_pairs:
            continue
        candidate_id = _signal_candidate_id(row)
        if not candidate_id:
            continue
        emitted.append(
            {
                "candidate_id": candidate_id,
                "bar_index": _required_int(row.get("bar_index"), "output.bar_index"),
                "time": row.get("time"),
                "output_name": output_name,
                "event_key": event_key,
            }
        )
    expected_by_key = _candidate_signal_events_by_key(expected)
    emitted_by_key = _candidate_signal_events_by_key(emitted)
    expected_keys = set(expected_by_key)
    emitted_keys = set(emitted_by_key)
    issues: list[dict[str, Any]] = []
    for key in sorted(expected_keys - emitted_keys, key=_candidate_signal_key_sort):
        for event in expected_by_key[key]:
            issues.append({"classification": "missing_signal", **event})
    for key in sorted(emitted_keys - expected_keys, key=_candidate_signal_key_sort):
        for event in emitted_by_key[key]:
            issues.append({"classification": "invalid_signal", **event})
    return {
        "expected_signal_count": len(expected),
        "matched_signal_count": len(expected_keys & emitted_keys),
        "missing_signal_count": len(expected_keys - emitted_keys),
        "invalid_signal_count": len(emitted_keys - expected_keys),
        "issues": issues,
    }


def _lifecycle_signal_field(row: Mapping[str, Any], detector: Mapping[str, Any], field: str) -> str | None:
    found, value = _indicator_field_lookup(row, field)
    if found and str(value or "").strip():
        return str(value).strip()
    detector_value = detector.get(field)
    if str(detector_value or "").strip():
        return str(detector_value).strip()
    return None


def _signal_candidate_id(row: Mapping[str, Any]) -> str | None:
    for field in ("candidate_id", "pattern_id", "metadata.candidate_id"):
        found, value = _indicator_field_lookup(row, field)
        if found and str(value or "").strip():
            return str(value).strip()
    return None


def _candidate_signal_events_by_key(
    events: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, int, str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, int, str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        key = (
            str(event["candidate_id"]),
            int(event["bar_index"]),
            str(event["output_name"]),
            str(event["event_key"]),
        )
        grouped[key].append(dict(event))
    return grouped


def _candidate_signal_key_sort(key: tuple[str, int, str, str]) -> tuple[int, str, str, str]:
    return (key[1], key[0], key[2], key[3])


def _lifecycle_row_sort_key(row: Mapping[str, Any]) -> tuple[int, int, float]:
    known_at = _known_at_number(row.get("known_at"))
    return (
        _required_int(row.get("bar_index"), "output.bar_index"),
        _required_int(row.get("event_index") or 0, "output.event_index"),
        float(known_at if known_at is not None else 0.0),
    )


def _known_at_number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _string_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, Mapping)):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ValueError("expected a string or list of strings")


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _has_mapping_key(value: Mapping[str, Any], key: str) -> bool:
    return key in value


def _required_int(value: Any, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is required") from exc


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
