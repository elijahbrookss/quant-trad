"""Deterministic strategy evaluation over canonical indicator outputs."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Deque, Dict, Iterable, Literal, Mapping, Optional

from engines.indicator_engine.contracts import OutputType, RuntimeOutput

from .contracts import (
    CompiledStrategySpec,
    ContextMatchSpec,
    DecisionRuleSpec,
    GuardSpec,
    HoldsForBarsSpec,
    LeafGuardSpec,
    MetricMatchSpec,
    SignalMatchSpec,
    SignalWindowSpec,
)


_RISK_REJECTION_CODES = {
    "CAN_SHORT_DISABLED",
    "MARGIN_CALCULATION_FAILED",
    "MIN_NOTIONAL_NOT_MET",
    "MIN_QTY_NOT_MET",
    "QTY_CAPPED_TO_ZERO",
    "QTY_CONSTRAINT_FAILED",
    "QTY_ROUNDS_TO_ZERO",
    "TP_LEGS_EMPTY",
}

_EXECUTION_REJECTION_CODES = {
    "ENTRY_CANDLE_MISSING",
    "ENTRY_FILL_EMPTY",
    "ENTRY_NOT_FILLED",
    "ENTRY_REQUEST_INVALID",
    "ENTRY_SETTLEMENT_FAILED",
    "ENTRY_UNFILLED",
}

_REJECTION_CONTEXT_ALLOWLIST: dict[str, tuple[str, ...]] = {
    "position_policy": ("active_trade_id", "blocking_trade_id", "blocked_instrument_id"),
    "risk_policy": (
        "available_collateral",
        "cost_per_contract",
        "direction",
        "fee_per_contract",
        "margin_method",
        "margin_per_contract",
        "margin_rate",
        "max_qty",
        "max_qty_by_margin",
        "min_notional",
        "min_qty",
        "notional",
        "qty_raw",
        "qty_step",
        "qty_final",
        "requested_qty",
        "risk_qty",
        "rounded_qty",
        "symbol",
    ),
    "execution_policy": (
        "fallback",
        "limit_price",
        "order_id",
        "order_intent_id",
        "requested_qty",
        "status",
        "trade_id",
    ),
}

DecisionDetailMode = Literal["full", "minimal"]

DETAIL_DECISION_CRITICAL = "A"
DETAIL_AUDIT_LINEAGE = "B"
DETAIL_DEBUG_PROJECTION_ONLY = "C"
DETAIL_UNUSED_DEAD = "D"

DECISION_DETAIL_FIELD_CLASSIFICATION: dict[str, str] = {
    "decision_id": DETAIL_DECISION_CRITICAL,
    "strategy_id": DETAIL_DECISION_CRITICAL,
    "strategy_hash": DETAIL_AUDIT_LINEAGE,
    "instrument_id": DETAIL_DECISION_CRITICAL,
    "symbol": DETAIL_DECISION_CRITICAL,
    "timeframe": DETAIL_DECISION_CRITICAL,
    "bar_epoch": DETAIL_DECISION_CRITICAL,
    "bar_time": DETAIL_DECISION_CRITICAL,
    "decision_time": DETAIL_AUDIT_LINEAGE,
    "rule_id": DETAIL_DECISION_CRITICAL,
    "rule_name": DETAIL_AUDIT_LINEAGE,
    "priority": DETAIL_DECISION_CRITICAL,
    "enabled": DETAIL_DECISION_CRITICAL,
    "evaluation_result": DETAIL_DECISION_CRITICAL,
    "emitted_intent": DETAIL_DECISION_CRITICAL,
    "suppression_reason": DETAIL_DECISION_CRITICAL,
    "decision_context": DETAIL_DECISION_CRITICAL,
    "artifact_summary": DETAIL_AUDIT_LINEAGE,
    "observed_outputs": DETAIL_AUDIT_LINEAGE,
    "referenced_outputs": DETAIL_AUDIT_LINEAGE,
    "trigger": DETAIL_DEBUG_PROJECTION_ONLY,
    "guard_results": DETAIL_DEBUG_PROJECTION_ONLY,
    "matched": DETAIL_UNUSED_DEAD,
    "intent": DETAIL_UNUSED_DEAD,
}

MINIMAL_DECISION_ARTIFACT_FIELDS = frozenset(
    field
    for field, classification in DECISION_DETAIL_FIELD_CLASSIFICATION.items()
    if classification in {DETAIL_DECISION_CRITICAL, DETAIL_AUDIT_LINEAGE}
)

BULKY_DECISION_DETAIL_FIELDS = frozenset(
    field
    for field, classification in DECISION_DETAIL_FIELD_CLASSIFICATION.items()
    if classification == DETAIL_DEBUG_PROJECTION_ONLY
)

_COMPACT_OMITTED = object()
_HISTORY_VALUE_EXCLUDE_KEYS = frozenset({"debug", "details", "overlays", "overlay", "projection", "trigger"})


@dataclass(frozen=True, slots=True)
class StrategyOutputHistoryRecord:
    """Compact immutable output snapshot used only by temporal/window guards."""

    output_key: str
    indicator_id: str
    output_name: str
    output_type: OutputType
    bar_time: datetime
    ready: bool
    value: Mapping[str, Any]


StrategyOutputView = RuntimeOutput | StrategyOutputHistoryRecord


@dataclass
class DecisionEvaluationState:
    """Bounded per-series strategy evaluation state."""

    output_history: Dict[str, Deque[StrategyOutputHistoryRecord]] = field(default_factory=dict)


@dataclass(frozen=True)
class DecisionFrameResult:
    """Result for one strategy evaluation over one bar."""

    artifacts: list[dict[str, Any]]
    selected_artifact: dict[str, Any] | None


def evaluate_strategy_bar(
    *,
    compiled_strategy: CompiledStrategySpec,
    state: DecisionEvaluationState,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    instrument_id: str,
    symbol: str,
    timeframe: str,
    bar_time: datetime,
    minimal_decision_details: bool = False,
) -> DecisionFrameResult:
    epoch = int(bar_time.timestamp())
    artifacts: list[dict[str, Any]] = []
    matched_indexes: list[int] = []
    selected_index: int | None = None

    for index, rule in enumerate(compiled_strategy.rules):
        artifact = _evaluate_rule(
            rule=rule,
            strategy_id=compiled_strategy.strategy_id,
            strategy_hash=compiled_strategy.strategy_hash,
            instrument_id=instrument_id,
            symbol=symbol,
            timeframe=timeframe,
            bar_time=bar_time,
            epoch=epoch,
            state=state,
            outputs=outputs,
            output_types=output_types,
            detail_mode="minimal" if minimal_decision_details else "full",
        )
        if artifact["matched"]:
            matched_indexes.append(index)
        artifacts.append(artifact)

    if matched_indexes:
        selected_index = sorted(
            matched_indexes,
            key=lambda idx: (-int(artifacts[idx].get("priority") or 0), str(artifacts[idx]["rule_id"])),
        )[0]

    for index, artifact in enumerate(artifacts):
        if not artifact["matched"]:
            artifact["evaluation_result"] = "not_matched"
            artifact["emitted_intent"] = None
            artifact["suppression_reason"] = None
            artifact.pop("matched", None)
            if minimal_decision_details:
                artifact.pop("intent", None)
            continue
        artifact["emitted_intent"] = artifact["intent"]
        if selected_index == index:
            artifact["evaluation_result"] = "matched_selected"
            artifact["suppression_reason"] = None
        else:
            artifact["evaluation_result"] = "matched_suppressed"
            selected_priority = int(artifacts[selected_index].get("priority") or 0)
            current_priority = int(artifact.get("priority") or 0)
            artifact["suppression_reason"] = (
                "higher_priority_rule_selected"
                if selected_priority > current_priority
                else "rule_id_tiebreaker_selected"
            )
        artifact.pop("matched", None)
        artifact.pop("intent", None)

    selected_artifact = artifacts[selected_index] if selected_index is not None else None
    if selected_artifact is not None and minimal_decision_details:
        selected_artifact["observed_outputs"] = _capture_observed_outputs(
            outputs=outputs,
            output_types=output_types,
        )
    advance_decision_state(
        state,
        outputs=outputs,
        output_types=output_types,
        max_history_bars=compiled_strategy.max_history_bars,
    )
    return DecisionFrameResult(artifacts=artifacts, selected_artifact=selected_artifact)


def advance_decision_state(
    state: DecisionEvaluationState,
    *,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType] | None = None,
    max_history_bars: int,
) -> None:
    if max_history_bars <= 0:
        state.output_history.clear()
        return
    for output_key, runtime_output in outputs.items():
        history = state.output_history.setdefault(output_key, deque(maxlen=max_history_bars))
        output_type = (output_types or {}).get(output_key) or _infer_output_type(runtime_output)
        history.append(compact_history_record(output_key=output_key, output_type=output_type, output=runtime_output))


def compact_history_record(
    *,
    output_key: str,
    output_type: OutputType,
    output: RuntimeOutput,
) -> StrategyOutputHistoryRecord:
    indicator_id, output_name = _split_output_key(output_key)
    return StrategyOutputHistoryRecord(
        output_key=output_key,
        indicator_id=indicator_id,
        output_name=output_name,
        output_type=output_type,
        bar_time=output.bar_time,
        ready=bool(output.ready),
        value=_compact_output_value(output_type=output_type, output=output),
    )


def classify_rejection_stage(reason_code: str | None) -> str:
    code = str(reason_code or "").strip().upper()
    if code in _RISK_REJECTION_CODES:
        return "risk_policy"
    if code in _EXECUTION_REJECTION_CODES or code.startswith("ENTRY_"):
        return "execution_policy"
    return "risk_policy"


def build_rejection_artifact(
    *,
    decision_artifact: Mapping[str, Any],
    rejection_stage: str,
    rejection_code: str,
    rejection_reason: str,
    context: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    normalized_context = normalize_rejection_context(rejection_stage, context)
    decision_id = str(decision_artifact.get("decision_id") or "").strip()
    return {
        "rejection_id": f"{decision_id}:{rejection_stage}",
        "decision_id": decision_id,
        "strategy_id": decision_artifact.get("strategy_id"),
        "strategy_hash": decision_artifact.get("strategy_hash"),
        "instrument_id": decision_artifact.get("instrument_id"),
        "symbol": decision_artifact.get("symbol"),
        "bar_epoch": decision_artifact.get("bar_epoch"),
        "bar_time": decision_artifact.get("bar_time"),
        "intent": decision_artifact.get("emitted_intent"),
        "rejection_stage": rejection_stage,
        "rejection_code": str(rejection_code or "DECISION_REJECTED").strip().upper(),
        "rejection_reason": str(rejection_reason or "Decision rejected").strip(),
        "context": normalized_context,
    }


def normalize_rejection_context(
    stage: str,
    context: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    if not isinstance(context, Mapping):
        return {}
    allowlist = _REJECTION_CONTEXT_ALLOWLIST.get(str(stage or "").strip(), ())
    normalized: dict[str, Any] = {}
    for key in allowlist:
        value = context.get(key)
        if _is_scalar(value):
            normalized[key] = value
    return normalized


def _evaluate_rule(
    *,
    rule: DecisionRuleSpec,
    strategy_id: str,
    strategy_hash: str,
    instrument_id: str,
    symbol: str,
    timeframe: str,
    bar_time: datetime,
    epoch: int,
    state: DecisionEvaluationState,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    detail_mode: DecisionDetailMode,
) -> dict[str, Any]:
    trigger_result = _evaluate_signal_match(
        trigger=rule.trigger,
        outputs=outputs,
        output_types=output_types,
        detail_mode=detail_mode,
    )
    guard_results = [
        _evaluate_guard(
            guard=guard,
            state=state,
            outputs=outputs,
            output_types=output_types,
            detail_mode=detail_mode,
        )
        for guard in rule.guards
    ]
    matched = bool(trigger_result["matched"]) and all(bool(result["matched"]) for result in guard_results)
    artifact = {
        "decision_id": f"{strategy_id}:{instrument_id}:{epoch}:{rule.id}",
        "strategy_id": strategy_id,
        "strategy_hash": strategy_hash,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "bar_epoch": epoch,
        "bar_time": _isoformat(bar_time),
        "decision_time": _isoformat(bar_time),
        "rule_id": rule.id,
        "rule_name": rule.name,
        "priority": int(rule.priority),
        "enabled": bool(rule.enabled),
        "evaluation_result": "not_matched",
        "emitted_intent": None,
        "suppression_reason": None,
        "intent": rule.intent,
        "matched": bool(rule.enabled) and matched,
    }
    if detail_mode == "minimal":
        artifact["decision_context"] = _build_decision_context(rule=rule, trigger_result=trigger_result)
        artifact["referenced_outputs"] = _capture_referenced_outputs(
            rule=rule,
            outputs=outputs,
            output_types=output_types,
        )
        artifact["artifact_summary"] = _build_artifact_summary(
            trigger_result=trigger_result,
            guard_results=guard_results,
            matched=bool(rule.enabled) and matched,
        )
    else:
        artifact["trigger"] = trigger_result
        artifact["guard_results"] = guard_results
    return artifact


def _capture_observed_outputs(
    *,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
) -> dict[str, dict[str, Any]]:
    captured: dict[str, dict[str, Any]] = {}
    for output_key in sorted(outputs.keys(), key=str):
        runtime_output = outputs.get(output_key)
        output_type = output_types.get(output_key)
        if runtime_output is None or output_type not in {"signal", "context", "metric"}:
            continue
        snapshot = _compact_runtime_output_snapshot(
            output_key=output_key,
            output_type=output_type,
            output=runtime_output,
        )
        if snapshot:
            captured[output_key] = snapshot
    return captured


def _evaluate_guard(
    *,
    guard: GuardSpec,
    state: DecisionEvaluationState,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    detail_mode: DecisionDetailMode,
) -> dict[str, Any]:
    if isinstance(guard, ContextMatchSpec):
        return _evaluate_context_match(guard=guard, outputs=outputs, output_types=output_types, detail_mode=detail_mode)
    if isinstance(guard, MetricMatchSpec):
        return _evaluate_metric_match(guard=guard, outputs=outputs, output_types=output_types, detail_mode=detail_mode)
    if isinstance(guard, HoldsForBarsSpec):
        return _evaluate_holds_for_bars(
            guard=guard,
            state=state,
            outputs=outputs,
            output_types=output_types,
            detail_mode=detail_mode,
        )
    if isinstance(guard, SignalWindowSpec):
        return _evaluate_signal_window(
            guard=guard,
            state=state,
            outputs=outputs,
            output_types=output_types,
            detail_mode=detail_mode,
        )
    raise RuntimeError(f"strategy_guard_invalid: unsupported type={type(guard)!r}")


def _evaluate_signal_match(
    *,
    trigger: SignalMatchSpec,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    detail_mode: DecisionDetailMode,
) -> dict[str, Any]:
    _ = detail_mode
    runtime_output = _require_output(
        output_key=trigger.output_key,
        expected_type="signal",
        outputs=outputs,
        output_types=output_types,
    )
    if not runtime_output.ready:
        return {
            "type": "signal_match",
            "output_ref": trigger.output_key,
            "event_key": trigger.event_key,
            "ready": False,
            "matched": False,
        }
    matched = _signal_event_present(runtime_output, output_key=trigger.output_key, event_key=trigger.event_key)
    return {
        "type": "signal_match",
        "output_ref": trigger.output_key,
        "event_key": trigger.event_key,
        "ready": True,
        "matched": matched,
    }


def _evaluate_context_match(
    *,
    guard: ContextMatchSpec,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    runtime_output: StrategyOutputView | None = None,
    detail_mode: DecisionDetailMode = "full",
) -> dict[str, Any]:
    current_output = runtime_output or _require_output(
        output_key=guard.output_key,
        expected_type="context",
        outputs=outputs,
        output_types=output_types,
    )
    if not current_output.ready:
        result = {
            "type": "context_match",
            "output_ref": guard.output_key,
            "ready": False,
            "matched": False,
        }
        if detail_mode == "full":
            result.update({"field": guard.field, "expected": list(guard.value), "actual": None})
        return result
    actual = _read_context_value(current_output, field=guard.field, output_key=guard.output_key)
    result = {
        "type": "context_match",
        "output_ref": guard.output_key,
        "ready": True,
        "matched": str(actual) in guard.value,
    }
    if detail_mode == "full":
        result.update({"field": guard.field, "expected": list(guard.value), "actual": actual})
    return result


def _evaluate_metric_match(
    *,
    guard: MetricMatchSpec,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    runtime_output: StrategyOutputView | None = None,
    detail_mode: DecisionDetailMode = "full",
) -> dict[str, Any]:
    current_output = runtime_output or _require_output(
        output_key=guard.output_key,
        expected_type="metric",
        outputs=outputs,
        output_types=output_types,
    )
    if not current_output.ready:
        result = {
            "type": "metric_match",
            "output_ref": guard.output_key,
            "ready": False,
            "matched": False,
        }
        if detail_mode == "full":
            result.update(
                {
                    "field": guard.field,
                    "operator": guard.operator,
                    "expected": guard.value,
                    "actual": None,
                }
            )
        return result
    actual = _read_metric_value(current_output, field=guard.field, output_key=guard.output_key)
    if isinstance(actual, bool) or not isinstance(actual, (int, float)):
        raise RuntimeError(
            f"strategy_metric_invalid: output={guard.output_key} field={guard.field} value must be numeric"
        )
    actual_value = float(actual)
    matched = _compare_metric(actual_value=actual_value, operator=guard.operator, expected_value=guard.value)
    result = {
        "type": "metric_match",
        "output_ref": guard.output_key,
        "ready": True,
        "matched": matched,
    }
    if detail_mode == "full":
        result.update(
            {
                "field": guard.field,
                "operator": guard.operator,
                "expected": guard.value,
                "actual": actual_value,
            }
        )
    return result


def _evaluate_holds_for_bars(
    *,
    guard: HoldsForBarsSpec,
    state: DecisionEvaluationState,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    detail_mode: DecisionDetailMode,
) -> dict[str, Any]:
    output_key = guard.guard.output_key
    window = _history_window(state, output_key=output_key, current_output=outputs.get(output_key), bars=guard.bars)
    if detail_mode == "minimal":
        ready = len(window) == int(guard.bars)
        matched = ready
        for _, runtime_output in window:
            entry = _evaluate_leaf_guard_on_runtime_output(
                guard.guard,
                outputs=outputs,
                output_types=output_types,
                runtime_output=runtime_output,
                detail_mode="minimal",
            )
            ready = ready and bool(entry["ready"])
            matched = matched and bool(entry["matched"])
        return {
            "type": "holds_for_bars",
            "bars": int(guard.bars),
            "ready": ready,
            "matched": matched,
        }
    window_results: list[dict[str, Any]] = []
    for offset, runtime_output in window:
        entry = _evaluate_leaf_guard_on_runtime_output(
            guard.guard,
            outputs=outputs,
            output_types=output_types,
            runtime_output=runtime_output,
            detail_mode=detail_mode,
        )
        window_results.append(
            {
                "offset": offset,
                "bar_time": _isoformat(_output_bar_time(runtime_output)),
                "ready": entry["ready"],
                "matched": entry["matched"],
                "actual": entry.get("actual"),
            }
        )
    sufficient_history = len(window_results) == int(guard.bars)
    matched = sufficient_history and all(bool(entry["matched"]) for entry in window_results)
    return {
        "type": "holds_for_bars",
        "bars": int(guard.bars),
        "guard": {
            "type": guard.guard.type,
            "output_ref": guard.guard.output_key,
            **({"field": guard.guard.field} if hasattr(guard.guard, "field") else {}),
            **({"operator": guard.guard.operator} if isinstance(guard.guard, MetricMatchSpec) else {}),
            **({"expected": list(guard.guard.value)} if isinstance(guard.guard, ContextMatchSpec) else {}),
            **({"expected": guard.guard.value} if isinstance(guard.guard, MetricMatchSpec) else {}),
        },
        "window_results": window_results,
        "insufficient_history": not sufficient_history,
        "matched": matched,
    }


def _evaluate_signal_window(
    *,
    guard: SignalWindowSpec,
    state: DecisionEvaluationState,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    detail_mode: DecisionDetailMode,
) -> dict[str, Any]:
    window = _history_window(
        state,
        output_key=guard.output_key,
        current_output=_require_output(
            output_key=guard.output_key,
            expected_type="signal",
            outputs=outputs,
            output_types=output_types,
        ),
        bars=guard.lookback_bars,
    )
    window_results: list[dict[str, Any]] = []
    ready = True
    seen_match = False
    for offset, runtime_output in window:
        if not runtime_output.ready:
            ready = False
            if detail_mode == "minimal":
                continue
            window_results.append(
                {
                    "offset": offset,
                    "bar_time": _isoformat(_output_bar_time(runtime_output)),
                    "ready": False,
                    "event_present": False,
                }
            )
            continue
        event_present = _signal_event_present(runtime_output, output_key=guard.output_key, event_key=guard.event_key)
        seen_match = seen_match or event_present
        if detail_mode == "minimal":
            continue
        window_results.append(
            {
                "offset": offset,
                "bar_time": _isoformat(_output_bar_time(runtime_output)),
                "ready": True,
                "event_present": event_present,
            }
        )
    matched = seen_match if guard.type == "signal_seen_within_bars" else not seen_match
    if detail_mode == "minimal":
        return {
            "type": guard.type,
            "output_ref": guard.output_key,
            "event_key": guard.event_key,
            "lookback_bars": int(guard.lookback_bars),
            "ready": ready,
            "matched": matched,
        }
    return {
        "type": guard.type,
        "output_ref": guard.output_key,
        "event_key": guard.event_key,
        "lookback_bars": int(guard.lookback_bars),
        "window_results": window_results,
        "matched": matched,
    }


def _evaluate_leaf_guard_on_runtime_output(
    guard: LeafGuardSpec,
    *,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    runtime_output: StrategyOutputView,
    detail_mode: DecisionDetailMode,
) -> dict[str, Any]:
    if isinstance(guard, ContextMatchSpec):
        return _evaluate_context_match(
            guard=guard,
            outputs=outputs,
            output_types=output_types,
            runtime_output=runtime_output,
            detail_mode=detail_mode,
        )
    return _evaluate_metric_match(
        guard=guard,
        outputs=outputs,
        output_types=output_types,
        runtime_output=runtime_output,
        detail_mode=detail_mode,
    )


def _build_decision_context(
    *,
    rule: DecisionRuleSpec,
    trigger_result: Mapping[str, Any],
) -> dict[str, Any]:
    intent = str(rule.intent or "").strip()
    direction = "long" if intent == "enter_long" else ("short" if intent == "enter_short" else None)
    return {
        "trigger_output_ref": trigger_result.get("output_ref"),
        "event_key": trigger_result.get("event_key"),
        "intent": intent,
        "direction": direction,
    }


def _capture_referenced_outputs(
    *,
    rule: DecisionRuleSpec,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
) -> dict[str, dict[str, Any]]:
    captured: dict[str, dict[str, Any]] = {}
    for output_key in _rule_referenced_output_keys(rule):
        runtime_output = outputs.get(output_key)
        output_type = output_types.get(output_key)
        if runtime_output is None or output_type not in {"signal", "context", "metric"}:
            continue
        snapshot = _compact_runtime_output_snapshot(
            output_key=output_key,
            output_type=output_type,
            output=runtime_output,
        )
        captured[output_key] = snapshot
    return captured


def _compact_runtime_output_snapshot(
    *,
    output_key: str,
    output_type: OutputType,
    output: RuntimeOutput,
) -> dict[str, Any]:
    indicator_id, output_name = _split_output_key(output_key)
    snapshot: dict[str, Any] = {
        "output_ref": output_key,
        "indicator_id": indicator_id,
        "output_name": output_name,
        "type": output_type,
        "output_type": output_type,
        "ready": bool(output.ready),
        "bar_time": _isoformat(_output_bar_time(output)),
        "indicator_commit_seq": int(getattr(output, "indicator_commit_seq", 0) or 0),
        "indicator_commit_seq_status": str(
            getattr(output, "indicator_commit_seq_status", "unassigned") or "unassigned"
        ),
    }
    if output.ready:
        snapshot.update(_compact_output_snapshot(output_type=output_type, output=output))
    return snapshot


def _rule_referenced_output_keys(rule: DecisionRuleSpec) -> tuple[str, ...]:
    keys: list[str] = []

    def add(output_key: Any) -> None:
        normalized = str(output_key or "").strip()
        if normalized and normalized not in keys:
            keys.append(normalized)

    add(rule.trigger.output_key)
    for guard in rule.guards:
        for output_key in _guard_referenced_output_keys(guard):
            add(output_key)
    return tuple(keys)


def _guard_referenced_output_keys(guard: GuardSpec) -> tuple[str, ...]:
    if isinstance(guard, (ContextMatchSpec, MetricMatchSpec, SignalWindowSpec)):
        return (guard.output_key,)
    if isinstance(guard, HoldsForBarsSpec):
        return _guard_referenced_output_keys(guard.guard)
    return ()


def _compact_output_snapshot(*, output_type: OutputType, output: RuntimeOutput) -> dict[str, Any]:
    compact = _plain_compact_value(_compact_output_value(output_type=output_type, output=output))
    if not isinstance(compact, Mapping):
        return {}
    if output_type == "signal":
        event_keys = compact.get("event_keys")
        return {"event_keys": list(event_keys) if isinstance(event_keys, list) else []}
    if output_type == "context":
        fields = compact.get("fields") if isinstance(compact.get("fields"), Mapping) else {}
        return {
            "state_key": compact.get("state_key"),
            "fields": dict(fields),
        }
    return {"fields": dict(compact)}


def _plain_compact_value(value: Any) -> Any:
    if value is _COMPACT_OMITTED:
        return None
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            plain = _plain_compact_value(item)
            if plain is not None:
                result[str(key)] = plain
        return result
    if isinstance(value, (tuple, list)):
        items: list[Any] = []
        for item in value:
            plain = _plain_compact_value(item)
            if plain is not None:
                items.append(plain)
        return items
    return value if _is_scalar(value) else None


def _build_artifact_summary(
    *,
    trigger_result: Mapping[str, Any],
    guard_results: Iterable[Mapping[str, Any]],
    matched: bool,
) -> dict[str, Any]:
    guard_list = list(guard_results)
    return {
        "trigger_ready": bool(trigger_result.get("ready")),
        "trigger_matched": bool(trigger_result.get("matched")),
        "guard_count": len(guard_list),
        "guards_ready": sum(1 for result in guard_list if _guard_ready(result)),
        "guards_matched": sum(1 for result in guard_list if bool(result.get("matched"))),
        "matched": bool(matched),
    }


def _guard_ready(result: Mapping[str, Any]) -> bool:
    if "ready" in result:
        return bool(result.get("ready"))
    if "insufficient_history" in result:
        return not bool(result.get("insufficient_history"))
    return True


def _compact_output_value(*, output_type: OutputType, output: RuntimeOutput) -> Mapping[str, Any]:
    if not output.ready:
        return MappingProxyType({})
    value = output.value if isinstance(output.value, Mapping) else {}
    if output_type == "signal":
        events = value.get("events")
        if not isinstance(events, list):
            return MappingProxyType({"event_keys": _COMPACT_OMITTED})
        event_keys = tuple(
            str(event.get("key") or "").strip()
            for event in events
            if isinstance(event, Mapping) and str(event.get("key") or "").strip()
        )
        return MappingProxyType({"event_keys": event_keys})
    if output_type == "context":
        fields = value.get("fields")
        compact_fields = _compact_scalar_mapping(fields) if isinstance(fields, Mapping) else MappingProxyType({})
        return MappingProxyType(
            {
                "state_key": _compact_scalar(value.get("state_key")),
                "fields": compact_fields,
            }
        )
    return _compact_scalar_mapping(value)


def _compact_scalar_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        return MappingProxyType({})
    compact: dict[str, Any] = {}
    for key, item in value.items():
        normalized_key = str(key)
        if normalized_key in _HISTORY_VALUE_EXCLUDE_KEYS or not _is_scalar(item):
            continue
        compact[normalized_key] = item
    return MappingProxyType(compact)


def _compact_scalar(value: Any) -> Any:
    return value if _is_scalar(value) else _COMPACT_OMITTED


def _infer_output_type(output: RuntimeOutput) -> OutputType:
    value = output.value if isinstance(output.value, Mapping) else {}
    if isinstance(value.get("events"), list):
        return "signal"
    if "state_key" in value or isinstance(value.get("fields"), Mapping):
        return "context"
    return "metric"


def _split_output_key(output_key: str) -> tuple[str, str]:
    indicator_id, separator, output_name = str(output_key or "").partition(".")
    if not separator:
        return str(output_key or ""), ""
    return indicator_id, output_name


def _output_bar_time(output: StrategyOutputView) -> datetime:
    return output.bar_time


def _signal_event_present(output: StrategyOutputView, *, output_key: str, event_key: str) -> bool:
    if isinstance(output, StrategyOutputHistoryRecord):
        event_keys = output.value.get("event_keys")
        if event_keys is _COMPACT_OMITTED:
            raise RuntimeError(f"strategy_output_invalid: signal events missing output={output_key}")
        return str(event_key or "").strip() in set(event_keys or ())
    events = output.value.get("events")
    if not isinstance(events, list):
        raise RuntimeError(f"strategy_output_invalid: signal events missing output={output_key}")
    return any(
        isinstance(event, Mapping) and str(event.get("key") or "").strip() == event_key
        for event in events
    )


def _read_context_value(output: StrategyOutputView, *, field: str, output_key: str) -> str:
    value = output.value
    if field == "state":
        actual = value.get("state_key")
    else:
        fields = value.get("fields")
        if not isinstance(fields, Mapping):
            raise RuntimeError(f"strategy_context_field_missing: output={output_key} field={field}")
        actual = fields.get(field)
    if actual is _COMPACT_OMITTED or not isinstance(actual, str):
        raise RuntimeError(f"strategy_context_invalid: output={output_key} field={field} value must be string-like")
    return actual


def _read_metric_value(output: StrategyOutputView, *, field: str, output_key: str) -> Any:
    if field not in output.value:
        raise RuntimeError(f"strategy_metric_field_missing: output={output_key} field={field}")
    return output.value[field]


def _history_window(
    state: DecisionEvaluationState,
    *,
    output_key: str,
    current_output: RuntimeOutput | None,
    bars: int,
) -> list[tuple[int, StrategyOutputView]]:
    if current_output is None:
        raise RuntimeError(f"strategy_output_missing: output={output_key}")
    window: list[tuple[int, StrategyOutputView]] = [(0, current_output)]
    history = list(state.output_history.get(output_key) or [])
    if bars <= 1:
        return window
    for offset, runtime_output in enumerate(reversed(history[-max(bars - 1, 0):]), start=1):
        window.append((offset, runtime_output))
    return window


def _require_output(
    *,
    output_key: str,
    expected_type: OutputType,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
) -> RuntimeOutput:
    runtime_output = outputs.get(output_key)
    if runtime_output is None:
        raise RuntimeError(f"strategy_output_missing: output={output_key}")
    actual_type = output_types.get(output_key)
    if actual_type is None:
        raise RuntimeError(f"strategy_output_type_missing: output={output_key}")
    if actual_type != expected_type:
        raise RuntimeError(
            f"strategy_output_type_invalid: output={output_key} expected={expected_type} actual={actual_type}"
        )
    return runtime_output


def _compare_metric(*, actual_value: float, operator: str, expected_value: float) -> bool:
    if operator == ">":
        return actual_value > expected_value
    if operator == ">=":
        return actual_value >= expected_value
    if operator == "<":
        return actual_value < expected_value
    if operator == "<=":
        return actual_value <= expected_value
    if operator == "==":
        return actual_value == expected_value
    if operator == "!=":
        return actual_value != expected_value
    raise RuntimeError(f"strategy_metric_invalid: operator={operator}")


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat().replace("+00:00", "Z")


__all__ = [
    "BULKY_DECISION_DETAIL_FIELDS",
    "DECISION_DETAIL_FIELD_CLASSIFICATION",
    "DecisionEvaluationState",
    "DecisionFrameResult",
    "MINIMAL_DECISION_ARTIFACT_FIELDS",
    "StrategyOutputHistoryRecord",
    "advance_decision_state",
    "build_rejection_artifact",
    "classify_rejection_stage",
    "compact_history_record",
    "evaluate_strategy_bar",
    "normalize_rejection_context",
]
