"""Deterministic strategy evaluation over canonical indicator outputs."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, Iterable, Mapping, Optional

from engines.indicator_engine.contracts import OutputType, RuntimeOutput

from .compiler import intent_to_direction, normalize_rule_intent
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


@dataclass
class DecisionEvaluationState:
    """Bounded per-series strategy evaluation state."""

    output_history: Dict[str, Deque[RuntimeOutput]] = field(default_factory=dict)


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
) -> DecisionFrameResult:
    epoch = int(bar_time.timestamp())
    artifacts: list[dict[str, Any]] = []
    matched_indexes: list[int] = []
    selected_index: int | None = None

    for index, rule in enumerate(compiled_strategy.rules):
        artifact = _evaluate_rule(
            rule=rule,
            strategy_id=compiled_strategy.strategy_id,
            instrument_id=instrument_id,
            symbol=symbol,
            timeframe=timeframe,
            bar_time=bar_time,
            epoch=epoch,
            state=state,
            outputs=outputs,
            output_types=output_types,
        )
        if artifact["matched"]:
            matched_indexes.append(index)
        artifacts.append(artifact)

    if matched_indexes:
        selected_index = sorted(matched_indexes, key=lambda idx: str(artifacts[idx]["rule_id"]))[0]

    for index, artifact in enumerate(artifacts):
        if not artifact["matched"]:
            artifact["evaluation_result"] = "not_matched"
            artifact["emitted_intent"] = None
            artifact["suppression_reason"] = None
            artifact.pop("matched", None)
            continue
        artifact["emitted_intent"] = artifact["intent"]
        if selected_index == index:
            artifact["evaluation_result"] = "matched_selected"
            artifact["suppression_reason"] = None
        else:
            artifact["evaluation_result"] = "matched_suppressed"
            artifact["suppression_reason"] = "another_rule_selected"
        artifact.pop("matched", None)
        artifact.pop("intent", None)

    selected_artifact = artifacts[selected_index] if selected_index is not None else None
    advance_decision_state(state, outputs=outputs, max_history_bars=compiled_strategy.max_history_bars)
    return DecisionFrameResult(artifacts=artifacts, selected_artifact=selected_artifact)


def advance_decision_state(
    state: DecisionEvaluationState,
    *,
    outputs: Mapping[str, RuntimeOutput],
    max_history_bars: int,
) -> None:
    if max_history_bars <= 0:
        state.output_history.clear()
        return
    for output_key, runtime_output in outputs.items():
        history = state.output_history.setdefault(output_key, deque(maxlen=max_history_bars))
        history.append(runtime_output.copy())


def build_signal_candidate(
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    intent = normalize_rule_intent(artifact.get("emitted_intent") or artifact.get("intent"))
    return {
        "epoch": int(artifact.get("bar_epoch") or 0),
        "direction": intent_to_direction(intent),
        "decision_id": artifact.get("decision_id"),
        "rule_id": artifact.get("rule_id"),
        "intent": intent,
        "event_key": ((artifact.get("trigger") or {}).get("event_key") if isinstance(artifact.get("trigger"), Mapping) else None),
    }


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
    instrument_id: str,
    symbol: str,
    timeframe: str,
    bar_time: datetime,
    epoch: int,
    state: DecisionEvaluationState,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
) -> dict[str, Any]:
    trigger_result = _evaluate_signal_match(
        trigger=rule.trigger,
        outputs=outputs,
        output_types=output_types,
    )
    guard_results = [
        _evaluate_guard(
            guard=guard,
            state=state,
            outputs=outputs,
            output_types=output_types,
        )
        for guard in rule.guards
    ]
    matched = bool(trigger_result["matched"]) and all(bool(result["matched"]) for result in guard_results)
    return {
        "decision_id": f"{strategy_id}:{instrument_id}:{epoch}:{rule.id}",
        "strategy_id": strategy_id,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "bar_epoch": epoch,
        "bar_time": _isoformat(bar_time),
        "decision_time": _isoformat(bar_time),
        "rule_id": rule.id,
        "rule_name": rule.name,
        "trigger": trigger_result,
        "guard_results": guard_results,
        "evaluation_result": "not_matched",
        "emitted_intent": None,
        "suppression_reason": None,
        "intent": rule.intent,
        "matched": matched,
    }


def _evaluate_guard(
    *,
    guard: GuardSpec,
    state: DecisionEvaluationState,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
) -> dict[str, Any]:
    if isinstance(guard, ContextMatchSpec):
        return _evaluate_context_match(guard=guard, outputs=outputs, output_types=output_types)
    if isinstance(guard, MetricMatchSpec):
        return _evaluate_metric_match(guard=guard, outputs=outputs, output_types=output_types)
    if isinstance(guard, HoldsForBarsSpec):
        return _evaluate_holds_for_bars(guard=guard, state=state, outputs=outputs, output_types=output_types)
    if isinstance(guard, SignalWindowSpec):
        return _evaluate_signal_window(guard=guard, state=state, outputs=outputs, output_types=output_types)
    raise RuntimeError(f"strategy_guard_invalid: unsupported type={type(guard)!r}")


def _evaluate_signal_match(
    *,
    trigger: SignalMatchSpec,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
) -> dict[str, Any]:
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
    events = runtime_output.value.get("events")
    if not isinstance(events, list):
        raise RuntimeError(f"strategy_output_invalid: signal events missing output={trigger.output_key}")
    matched = any(
        isinstance(event, Mapping) and str(event.get("key") or "").strip() == trigger.event_key
        for event in events
    )
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
    runtime_output: RuntimeOutput | None = None,
) -> dict[str, Any]:
    current_output = runtime_output or _require_output(
        output_key=guard.output_key,
        expected_type="context",
        outputs=outputs,
        output_types=output_types,
    )
    if not current_output.ready:
        return {
            "type": "context_match",
            "output_ref": guard.output_key,
            "field": guard.field,
            "ready": False,
            "expected": list(guard.value),
            "actual": None,
            "matched": False,
        }
    actual = _read_context_value(current_output.value, field=guard.field, output_key=guard.output_key)
    return {
        "type": "context_match",
        "output_ref": guard.output_key,
        "field": guard.field,
        "ready": True,
        "expected": list(guard.value),
        "actual": actual,
        "matched": str(actual) in guard.value,
    }


def _evaluate_metric_match(
    *,
    guard: MetricMatchSpec,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    runtime_output: RuntimeOutput | None = None,
) -> dict[str, Any]:
    current_output = runtime_output or _require_output(
        output_key=guard.output_key,
        expected_type="metric",
        outputs=outputs,
        output_types=output_types,
    )
    if not current_output.ready:
        return {
            "type": "metric_match",
            "output_ref": guard.output_key,
            "field": guard.field,
            "operator": guard.operator,
            "ready": False,
            "expected": guard.value,
            "actual": None,
            "matched": False,
        }
    if guard.field not in current_output.value:
        raise RuntimeError(f"strategy_metric_field_missing: output={guard.output_key} field={guard.field}")
    actual = current_output.value[guard.field]
    if isinstance(actual, bool) or not isinstance(actual, (int, float)):
        raise RuntimeError(
            f"strategy_metric_invalid: output={guard.output_key} field={guard.field} value must be numeric"
        )
    actual_value = float(actual)
    matched = _compare_metric(actual_value=actual_value, operator=guard.operator, expected_value=guard.value)
    return {
        "type": "metric_match",
        "output_ref": guard.output_key,
        "field": guard.field,
        "operator": guard.operator,
        "ready": True,
        "expected": guard.value,
        "actual": actual_value,
        "matched": matched,
    }


def _evaluate_holds_for_bars(
    *,
    guard: HoldsForBarsSpec,
    state: DecisionEvaluationState,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
) -> dict[str, Any]:
    output_key = guard.guard.output_key
    window = _history_window(state, output_key=output_key, current_output=outputs.get(output_key), bars=guard.bars)
    window_results: list[dict[str, Any]] = []
    for offset, runtime_output in window:
        entry = _evaluate_leaf_guard_on_runtime_output(
            guard.guard,
            outputs=outputs,
            output_types=output_types,
            runtime_output=runtime_output,
        )
        window_results.append(
            {
                "offset": offset,
                "bar_time": _isoformat(runtime_output.bar_time),
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
    seen_match = False
    for offset, runtime_output in window:
        if not runtime_output.ready:
            window_results.append(
                {
                    "offset": offset,
                    "bar_time": _isoformat(runtime_output.bar_time),
                    "ready": False,
                    "event_present": False,
                }
            )
            continue
        events = runtime_output.value.get("events")
        if not isinstance(events, list):
            raise RuntimeError(f"strategy_output_invalid: signal events missing output={guard.output_key}")
        event_present = any(
            isinstance(event, Mapping) and str(event.get("key") or "").strip() == guard.event_key
            for event in events
        )
        seen_match = seen_match or event_present
        window_results.append(
            {
                "offset": offset,
                "bar_time": _isoformat(runtime_output.bar_time),
                "ready": True,
                "event_present": event_present,
            }
        )
    matched = seen_match if guard.type == "signal_seen_within_bars" else not seen_match
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
    runtime_output: RuntimeOutput,
) -> dict[str, Any]:
    if isinstance(guard, ContextMatchSpec):
        return _evaluate_context_match(
            guard=guard,
            outputs=outputs,
            output_types=output_types,
            runtime_output=runtime_output,
        )
    return _evaluate_metric_match(
        guard=guard,
        outputs=outputs,
        output_types=output_types,
        runtime_output=runtime_output,
    )


def _history_window(
    state: DecisionEvaluationState,
    *,
    output_key: str,
    current_output: RuntimeOutput | None,
    bars: int,
) -> list[tuple[int, RuntimeOutput]]:
    if current_output is None:
        raise RuntimeError(f"strategy_output_missing: output={output_key}")
    window: list[tuple[int, RuntimeOutput]] = [(0, current_output)]
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


def _read_context_value(value: Mapping[str, Any], *, field: str, output_key: str) -> str:
    if field == "state":
        actual = value.get("state_key")
    else:
        fields = value.get("fields")
        if not isinstance(fields, Mapping):
            raise RuntimeError(f"strategy_context_field_missing: output={output_key} field={field}")
        actual = fields.get(field)
    if not isinstance(actual, str):
        raise RuntimeError(f"strategy_context_invalid: output={output_key} field={field} value must be string-like")
    return actual


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
    "DecisionEvaluationState",
    "DecisionFrameResult",
    "advance_decision_state",
    "build_rejection_artifact",
    "build_signal_candidate",
    "classify_rejection_stage",
    "evaluate_strategy_bar",
    "normalize_rejection_context",
]
