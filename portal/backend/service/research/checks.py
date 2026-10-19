"""Lightweight analytical research checks over source market facts."""

from __future__ import annotations

from research_science.price_paths import safe_ratio as _safe_ratio, difference_path_observation

from core.execution_control import execution_checkpoint

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
    event_close: float
    entry_index: int
    entry_time: str
    entry_close: float
    outcomes: dict[int, dict[str, float]]


def validate_check_detector(*, check_family: str, detector: Mapping[str, Any]) -> None:
    if check_family != RAW_FORWARD_OUTCOME:
        raise ValueError(f"check_definition_retired: unsupported raw detector family: {check_family}")
    _validate_raw_detector(detector)
























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






def _sequence_values(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, Mapping)):
        return list(value)
    return [value]
























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
        execution_checkpoint()
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
