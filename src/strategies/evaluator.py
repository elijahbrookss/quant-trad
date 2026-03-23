"""Typed strategy evaluation over canonical indicator outputs."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from engines.indicator_engine.contracts import OutputType, RuntimeOutput


def _normalise_action(value: Any) -> str:
    action = str(value or "").strip().lower()
    if action in {"buy", "long"}:
        return "buy"
    if action in {"sell", "short"}:
        return "sell"
    raise RuntimeError(f"strategy_rule_invalid: unsupported action={value}")


def evaluate_typed_condition(
    node: Mapping[str, Any],
    *,
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
) -> bool:
    node_type = str(node.get("type") or "").strip().lower()
    if not node_type:
        raise RuntimeError("strategy_condition_invalid: type is required")

    if node_type in {"all", "any"}:
        conditions = node.get("conditions")
        if not isinstance(conditions, list) or not conditions:
            raise RuntimeError(
                f"strategy_condition_invalid: {node_type} requires non-empty conditions"
            )
        results: list[bool] = []
        for condition in conditions:
            if not isinstance(condition, Mapping):
                raise RuntimeError(
                    f"strategy_condition_invalid: {node_type} conditions must be objects"
                )
            results.append(
                evaluate_typed_condition(
                    condition,
                    outputs=outputs,
                    output_types=output_types,
                )
            )
        return all(results) if node_type == "all" else any(results)

    if node_type == "not":
        condition = node.get("condition")
        if not isinstance(condition, Mapping):
            raise RuntimeError("strategy_condition_invalid: not requires condition")
        return not evaluate_typed_condition(
            condition,
            outputs=outputs,
            output_types=output_types,
        )

    indicator_id = str(node.get("indicator_id") or "").strip()
    output_name = str(node.get("output_name") or "").strip()
    if not indicator_id or not output_name:
        raise RuntimeError(
            f"strategy_condition_invalid: indicator_id and output_name required type={node_type}"
        )
    output_key = f"{indicator_id}.{output_name}"
    runtime_output = outputs.get(output_key)
    if runtime_output is None:
        raise RuntimeError(f"strategy_output_missing: output={output_key}")
    actual_type = output_types.get(output_key)
    if actual_type is None:
        raise RuntimeError(f"strategy_output_type_missing: output={output_key}")
    if not runtime_output.ready:
        return False

    if node_type == "signal_match":
        if actual_type != "signal":
            raise RuntimeError(
                f"strategy_output_type_invalid: output={output_key} expected=signal actual={actual_type}"
            )
        event_key = str(node.get("event_key") or "").strip()
        if not event_key:
            raise RuntimeError("strategy_condition_invalid: signal_match requires event_key")
        events = runtime_output.value.get("events")
        if not isinstance(events, list):
            raise RuntimeError(f"strategy_output_invalid: signal events missing output={output_key}")
        return any(
            isinstance(event, Mapping) and str(event.get("key") or "").strip() == event_key
            for event in events
        )

    if node_type == "context_match":
        if actual_type != "context":
            raise RuntimeError(
                f"strategy_output_type_invalid: output={output_key} expected=context actual={actual_type}"
            )
        expected_state_key = str(node.get("state_key") or "").strip()
        if not expected_state_key:
            raise RuntimeError("strategy_condition_invalid: context_match requires state_key")
        return str(runtime_output.value.get("state_key") or "") == expected_state_key

    if node_type == "metric_match":
        if actual_type != "metric":
            raise RuntimeError(
                f"strategy_output_type_invalid: output={output_key} expected=metric actual={actual_type}"
            )
        field = str(node.get("field") or "").strip()
        operator = str(node.get("operator") or "").strip()
        if not field or not operator:
            raise RuntimeError(
                "strategy_condition_invalid: metric_match requires field and operator"
            )
        if field not in runtime_output.value:
            raise RuntimeError(f"strategy_metric_field_missing: output={output_key} field={field}")
        actual_value = runtime_output.value[field]
        if isinstance(actual_value, bool) or not isinstance(actual_value, (int, float)):
            raise RuntimeError(
                f"strategy_metric_invalid: output={output_key} field={field} value must be numeric"
            )
        return _compare_metric(
            actual_value=float(actual_value),
            operator=operator,
            expected_value=node.get("value"),
            output_key=output_key,
            field=field,
        )

    raise RuntimeError(f"strategy_condition_invalid: unsupported type={node_type}")


def evaluate_typed_rules(
    *,
    rules: Mapping[str, Any] | Sequence[Any],
    outputs: Mapping[str, RuntimeOutput],
    output_types: Mapping[str, OutputType],
    current_epoch: int,
) -> list[dict[str, Any]]:
    if isinstance(rules, Mapping):
        iterable = list(rules.values())
    elif isinstance(rules, Sequence):
        iterable = list(rules)
    else:
        raise RuntimeError("strategy_rules_invalid: rules must be mapping or sequence")

    matches: list[dict[str, Any]] = []
    for rule in iterable:
        if not isinstance(rule, Mapping):
            raise RuntimeError("strategy_rule_invalid: rule must be object")
        if not bool(rule.get("enabled", True)):
            continue
        when = rule.get("when")
        if not isinstance(when, Mapping):
            raise RuntimeError(
                f"strategy_rule_invalid: rule={rule.get('id')} missing when condition"
            )
        if not evaluate_typed_condition(when, outputs=outputs, output_types=output_types):
            continue
        matches.append(
            {
                "strategy_rule_id": rule.get("id"),
                "rule_name": rule.get("name"),
                "action": _normalise_action(rule.get("action")),
                "epoch": int(current_epoch),
            }
        )
    return matches


def _compare_metric(
    *,
    actual_value: float,
    operator: str,
    expected_value: Any,
    output_key: str,
    field: str,
) -> bool:
    try:
        expected_numeric = float(expected_value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"strategy_metric_invalid: output={output_key} field={field} expected value must be numeric"
        ) from exc
    if operator == ">":
        return actual_value > expected_numeric
    if operator == ">=":
        return actual_value >= expected_numeric
    if operator == "<":
        return actual_value < expected_numeric
    if operator == "<=":
        return actual_value <= expected_numeric
    if operator == "==":
        return actual_value == expected_numeric
    if operator == "!=":
        return actual_value != expected_numeric
    raise RuntimeError(
        f"strategy_metric_invalid: output={output_key} field={field} operator={operator}"
    )


__all__ = [
    "_normalise_action",
    "evaluate_typed_condition",
    "evaluate_typed_rules",
]
