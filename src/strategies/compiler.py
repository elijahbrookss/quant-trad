"""Compile authored strategy rules into deterministic runtime contracts."""

from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Sequence

from .contracts import (
    CompiledStrategySpec,
    ContextMatchSpec,
    DecisionRuleSpec,
    GuardSpec,
    HoldsForBarsSpec,
    Intent,
    LeafGuardSpec,
    MetricMatchSpec,
    SignalMatchSpec,
    SignalWindowSpec,
)


IndicatorMetaGetter = Callable[[str], Mapping[str, Any]]

_ALLOWED_METRIC_OPERATORS = {">", ">=", "<", "<=", "==", "!="}
_ALLOWED_SIGNAL_WINDOW_TYPES = {"signal_seen_within_bars", "signal_absent_within_bars"}


def normalize_rule_intent(value: Any) -> Intent:
    text = str(value or "").strip().lower()
    if text in {"enter_long", "buy", "long"}:
        return "enter_long"
    if text in {"enter_short", "sell", "short"}:
        return "enter_short"
    raise ValueError(f"Unsupported strategy intent: {value}")


def intent_to_direction(intent: Intent) -> str:
    return "long" if intent == "enter_long" else "short"


def compile_strategy(
    *,
    strategy_id: str,
    timeframe: str,
    rules: Mapping[str, Any] | Sequence[Any],
    attached_indicator_ids: Iterable[str],
    indicator_meta_getter: IndicatorMetaGetter,
) -> CompiledStrategySpec:
    attached_ids = {str(identifier).strip() for identifier in attached_indicator_ids if str(identifier).strip()}
    authored_rules = list(rules.values()) if isinstance(rules, Mapping) else list(rules)
    compiled_rules = [
        _compile_rule(
            raw_rule=rule,
            attached_indicator_ids=attached_ids,
            indicator_meta_getter=indicator_meta_getter,
        )
        for rule in authored_rules
    ]
    compiled_rules.sort(key=lambda item: str(item.id))
    _validate_priority_conflicts(compiled_rules)
    max_history_bars = max((_required_history_bars(rule) for rule in compiled_rules), default=0)
    return CompiledStrategySpec(
        strategy_id=str(strategy_id),
        timeframe=str(timeframe or ""),
        rules=tuple(compiled_rules),
        max_history_bars=max_history_bars,
    )


def _compile_rule(
    *,
    raw_rule: Any,
    attached_indicator_ids: set[str],
    indicator_meta_getter: IndicatorMetaGetter,
) -> DecisionRuleSpec:
    if not isinstance(raw_rule, Mapping):
        raise ValueError("Strategy rule must be an object")
    rule_id = str(raw_rule.get("id") or "").strip()
    if not rule_id:
        raise ValueError("Strategy rule id is required")
    name = str(raw_rule.get("name") or rule_id).strip()
    intent = normalize_rule_intent(raw_rule.get("intent") or raw_rule.get("action"))
    priority = int(raw_rule.get("priority") or 0)
    enabled = bool(raw_rule.get("enabled", True))
    description = str(raw_rule.get("description")).strip() if raw_rule.get("description") else None

    trigger_payload, guard_payloads = _extract_authored_rule_parts(raw_rule)
    trigger = _compile_signal_trigger(
        trigger_payload,
        attached_indicator_ids=attached_indicator_ids,
        indicator_meta_getter=indicator_meta_getter,
    )
    guards = tuple(
        _compile_guard(
            guard,
            attached_indicator_ids=attached_indicator_ids,
            indicator_meta_getter=indicator_meta_getter,
        )
        for guard in guard_payloads
    )
    return DecisionRuleSpec(
        id=rule_id,
        name=name,
        intent=intent,
        priority=priority,
        enabled=enabled,
        trigger=trigger,
        guards=guards,
        description=description,
    )


def _extract_authored_rule_parts(raw_rule: Mapping[str, Any]) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    trigger = raw_rule.get("trigger")
    guards = raw_rule.get("guards")
    if isinstance(trigger, Mapping):
        guard_list = [dict(item) for item in guards] if isinstance(guards, list) else []
        return dict(trigger), guard_list

    when = raw_rule.get("when")
    if isinstance(when, Mapping):
        clauses = _extract_when_clauses(when)
        signal_clauses = [dict(clause) for clause in clauses if str(clause.get("type") or "").strip().lower() == "signal_match"]
        guard_clauses = [dict(clause) for clause in clauses if str(clause.get("type") or "").strip().lower() != "signal_match"]
        if len(signal_clauses) != 1:
            raise ValueError(f"Rule {raw_rule.get('id')} requires exactly one signal trigger")
        return signal_clauses[0], guard_clauses

    conditions = raw_rule.get("conditions")
    if isinstance(conditions, Mapping):
        inner_when = conditions.get("when") if isinstance(conditions.get("when"), Mapping) else conditions
        return _extract_authored_rule_parts({"id": raw_rule.get("id"), "when": inner_when})

    raise ValueError(f"Rule {raw_rule.get('id')} is missing trigger/guards contract")


def _extract_when_clauses(when: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    node_type = str(when.get("type") or "").strip().lower()
    if node_type == "all":
        conditions = when.get("conditions")
        if not isinstance(conditions, list) or not conditions:
            raise ValueError("Rule flow requires non-empty conditions")
        return [item for item in conditions if isinstance(item, Mapping)]
    return [when]


def _compile_signal_trigger(
    node: Mapping[str, Any],
    *,
    attached_indicator_ids: set[str],
    indicator_meta_getter: IndicatorMetaGetter,
) -> SignalMatchSpec:
    node_type = str(node.get("type") or "").strip().lower()
    if node_type != "signal_match":
        raise ValueError("Rule trigger must be signal_match")
    indicator_id, output_name, output_meta = _resolve_output_meta(
        node,
        attached_indicator_ids=attached_indicator_ids,
        indicator_meta_getter=indicator_meta_getter,
    )
    output_type = str(output_meta.get("type") or "").strip().lower()
    if output_type != "signal":
        raise ValueError(f"{indicator_id}.{output_name} is not a signal output")
    event_key = str(node.get("event_key") or "").strip()
    if not event_key:
        raise ValueError("signal_match requires event_key")
    event_keys = output_meta.get("event_keys") if isinstance(output_meta.get("event_keys"), list) else []
    if event_keys and event_key not in event_keys:
        raise ValueError(f"Unknown event key for {indicator_id}.{output_name}: {event_key}")
    return SignalMatchSpec(
        type="signal_match",
        indicator_id=indicator_id,
        output_name=output_name,
        output_key=f"{indicator_id}.{output_name}",
        event_key=event_key,
    )


def _compile_guard(
    node: Mapping[str, Any],
    *,
    attached_indicator_ids: set[str],
    indicator_meta_getter: IndicatorMetaGetter,
) -> GuardSpec:
    node_type = str(node.get("type") or "").strip().lower()
    if node_type == "context_match":
        return _compile_context_guard(
            node,
            attached_indicator_ids=attached_indicator_ids,
            indicator_meta_getter=indicator_meta_getter,
        )
    if node_type == "metric_match":
        return _compile_metric_guard(
            node,
            attached_indicator_ids=attached_indicator_ids,
            indicator_meta_getter=indicator_meta_getter,
        )
    if node_type == "holds_for_bars":
        bars = int(node.get("bars") or 0)
        if bars <= 0:
            raise ValueError("holds_for_bars requires positive bars")
        inner = node.get("guard")
        if not isinstance(inner, Mapping):
            raise ValueError("holds_for_bars requires guard")
        inner_guard = _compile_guard(
            inner,
            attached_indicator_ids=attached_indicator_ids,
            indicator_meta_getter=indicator_meta_getter,
        )
        if not isinstance(inner_guard, (ContextMatchSpec, MetricMatchSpec)):
            raise ValueError("holds_for_bars only supports context_match or metric_match in v1")
        return HoldsForBarsSpec(type="holds_for_bars", bars=bars, guard=inner_guard)
    if node_type in _ALLOWED_SIGNAL_WINDOW_TYPES:
        indicator_id, output_name, output_meta = _resolve_output_meta(
            node,
            attached_indicator_ids=attached_indicator_ids,
            indicator_meta_getter=indicator_meta_getter,
        )
        output_type = str(output_meta.get("type") or "").strip().lower()
        if output_type != "signal":
            raise ValueError(f"{indicator_id}.{output_name} is not a signal output")
        event_key = str(node.get("event_key") or "").strip()
        if not event_key:
            raise ValueError(f"{node_type} requires event_key")
        lookback_bars = int(node.get("lookback_bars") or 0)
        if lookback_bars <= 0:
            raise ValueError(f"{node_type} requires positive lookback_bars")
        event_keys = output_meta.get("event_keys") if isinstance(output_meta.get("event_keys"), list) else []
        if event_keys and event_key not in event_keys:
            raise ValueError(f"Unknown event key for {indicator_id}.{output_name}: {event_key}")
        return SignalWindowSpec(
            type=node_type,
            indicator_id=indicator_id,
            output_name=output_name,
            output_key=f"{indicator_id}.{output_name}",
            event_key=event_key,
            lookback_bars=lookback_bars,
        )
    raise ValueError(f"Unsupported guard type: {node_type or 'unknown'}")


def _compile_context_guard(
    node: Mapping[str, Any],
    *,
    attached_indicator_ids: set[str],
    indicator_meta_getter: IndicatorMetaGetter,
) -> ContextMatchSpec:
    indicator_id, output_name, output_meta = _resolve_output_meta(
        node,
        attached_indicator_ids=attached_indicator_ids,
        indicator_meta_getter=indicator_meta_getter,
    )
    output_type = str(output_meta.get("type") or "").strip().lower()
    if output_type != "context":
        raise ValueError(f"{indicator_id}.{output_name} is not a context output")
    field = str(node.get("field") or node.get("state_key") or "").strip()
    if not field:
        raise ValueError("context_match requires field")
    value = str(node.get("value") or node.get("state_key") or "").strip()
    if not value:
        raise ValueError("context_match requires value")
    normalized_field = "state" if field in {"state", "state_key"} else field
    allowed_fields = output_meta.get("fields") if isinstance(output_meta.get("fields"), list) else []
    if normalized_field != "state" and allowed_fields and normalized_field not in allowed_fields:
        raise ValueError(f"Unknown context field for {indicator_id}.{output_name}: {normalized_field}")
    return ContextMatchSpec(
        type="context_match",
        indicator_id=indicator_id,
        output_name=output_name,
        output_key=f"{indicator_id}.{output_name}",
        field=normalized_field,
        value=value,
    )


def _compile_metric_guard(
    node: Mapping[str, Any],
    *,
    attached_indicator_ids: set[str],
    indicator_meta_getter: IndicatorMetaGetter,
) -> MetricMatchSpec:
    indicator_id, output_name, output_meta = _resolve_output_meta(
        node,
        attached_indicator_ids=attached_indicator_ids,
        indicator_meta_getter=indicator_meta_getter,
    )
    output_type = str(output_meta.get("type") or "").strip().lower()
    if output_type != "metric":
        raise ValueError(f"{indicator_id}.{output_name} is not a metric output")
    field = str(node.get("field") or "").strip()
    operator = str(node.get("operator") or "").strip()
    if not field or not operator:
        raise ValueError("metric_match requires field and operator")
    allowed_fields = output_meta.get("fields") if isinstance(output_meta.get("fields"), list) else []
    if allowed_fields and field not in allowed_fields:
        raise ValueError(f"Unknown metric field for {indicator_id}.{output_name}: {field}")
    if operator not in _ALLOWED_METRIC_OPERATORS:
        raise ValueError(f"Unsupported metric operator: {operator}")
    try:
        value = float(node.get("value"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Metric guard value must be numeric") from exc
    return MetricMatchSpec(
        type="metric_match",
        indicator_id=indicator_id,
        output_name=output_name,
        output_key=f"{indicator_id}.{output_name}",
        field=field,
        operator=operator,
        value=value,
    )


def _resolve_output_meta(
    node: Mapping[str, Any],
    *,
    attached_indicator_ids: set[str],
    indicator_meta_getter: IndicatorMetaGetter,
) -> tuple[str, str, Mapping[str, Any]]:
    indicator_id = str(node.get("indicator_id") or "").strip()
    output_name = str(node.get("output_name") or "").strip()
    if not indicator_id or not output_name:
        raise ValueError("Rule clause requires indicator_id and output_name")
    if attached_indicator_ids and indicator_id not in attached_indicator_ids:
        raise ValueError(f"Indicator {indicator_id} is not attached to this strategy")
    meta = indicator_meta_getter(indicator_id)
    outputs = meta.get("typed_outputs") if isinstance(meta, Mapping) else None
    if not isinstance(outputs, list):
        raise ValueError(f"Indicator {indicator_id} does not expose typed outputs")
    for output in outputs:
        if not isinstance(output, Mapping):
            continue
        if str(output.get("name") or "").strip() == output_name:
            return indicator_id, output_name, output
    raise ValueError(f"Indicator output not found: {indicator_id}.{output_name}")


def _validate_priority_conflicts(rules: Sequence[DecisionRuleSpec]) -> None:
    by_priority: dict[int, set[str]] = {}
    for rule in rules:
        if not rule.enabled:
            continue
        by_priority.setdefault(int(rule.priority), set()).add(str(rule.intent))
    conflicts = [priority for priority, intents in by_priority.items() if len(intents) > 1]
    if conflicts:
        joined = ",".join(str(priority) for priority in sorted(conflicts))
        raise ValueError(
            "Same-priority rules emitting different intents are forbidden in v1 "
            f"(priorities={joined})"
        )


def _required_history_bars(rule: DecisionRuleSpec) -> int:
    return max((_required_history_for_guard(guard) for guard in rule.guards), default=0)


def _required_history_for_guard(guard: GuardSpec) -> int:
    if isinstance(guard, HoldsForBarsSpec):
        return max(int(guard.bars) - 1, 0)
    if isinstance(guard, SignalWindowSpec):
        return max(int(guard.lookback_bars) - 1, 0)
    return 0


__all__ = [
    "compile_strategy",
    "intent_to_direction",
    "normalize_rule_intent",
]
