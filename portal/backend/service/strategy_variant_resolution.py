"""Shared strategy variant resolution helpers.

This module keeps variant resolution deliberately small:

- a strategy provides base/default decision configuration,
- a strategy variant provides named output filters,
- output filters are resolved once and materialized into rule guards,
- run snapshots can preserve the exact effective strategy config used.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


def _value(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(key, default)
    return getattr(source, key, default)


def _clean_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _json_safe(dict(payload)),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _dict(value: Any) -> Dict[str, Any]:
    return dict(deepcopy(value)) if isinstance(value, Mapping) else {}


def _list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(deepcopy(value))
    if isinstance(value, tuple):
        return list(deepcopy(value))
    if isinstance(value, set):
        return sorted(deepcopy(value))
    return [deepcopy(value)]


def _normalize_output_filters(value: Any) -> List[Dict[str, Any]]:
    filters = _list(value)
    normalized: List[Dict[str, Any]] = []
    for index, item in enumerate(filters):
        if not isinstance(item, Mapping):
            raise ValueError(f"output_filters[{index}] must be an object")
        payload = dict(deepcopy(item))
        indicator_id = _clean_text(payload.get("indicator_id"))
        output_name = _clean_text(payload.get("output_name"))
        field_name = _clean_text(payload.get("field"))
        operator = _clean_text(payload.get("operator")) or "equals"
        if not indicator_id:
            raise ValueError(f"output_filters[{index}].indicator_id is required")
        if not output_name:
            raise ValueError(f"output_filters[{index}].output_name is required")
        if not field_name:
            raise ValueError(f"output_filters[{index}].field is required")
        if "value" not in payload:
            raise ValueError(f"output_filters[{index}].value is required")
        payload["indicator_id"] = indicator_id
        payload["output_name"] = output_name
        payload["field"] = field_name
        payload["operator"] = operator
        if isinstance(payload.get("scope"), Mapping):
            payload["scope"] = dict(payload["scope"])
        elif payload.get("scope") in (None, ""):
            payload["scope"] = {}
        else:
            raise ValueError(f"output_filters[{index}].scope must be an object when provided")
        normalized.append(payload)
    return normalized


def _strategy_base_params(strategy: Any) -> Dict[str, Any]:
    base_params = _dict(_value(strategy, "base_params"))
    param_specs = _value(strategy, "param_specs")
    if param_specs:
        for spec in param_specs:
            key = _value(spec, "key")
            if key:
                base_params[str(key)] = deepcopy(_value(spec, "default"))
    if not base_params:
        base_params = _dict(_value(strategy, "default_params"))
    return base_params


def _variant_payload(variant: Any) -> Optional[Dict[str, Any]]:
    if variant is None:
        return None
    payload = {
        "id": _clean_text(_value(variant, "id")),
        "strategy_id": _clean_text(_value(variant, "strategy_id")),
        "name": _clean_text(_value(variant, "name")) or "default",
        "description": _value(variant, "description"),
        "output_filters": _normalize_output_filters(_value(variant, "output_filters")),
        "is_default": bool(_value(variant, "is_default", False)),
    }
    created_at = _value(variant, "created_at")
    updated_at = _value(variant, "updated_at")
    if created_at is not None:
        payload["created_at"] = created_at
    if updated_at is not None:
        payload["updated_at"] = updated_at
    return payload


@dataclass(frozen=True)
class EffectiveStrategyConfig:
    """Resolved strategy variant filters plus compact provenance."""

    strategy_id: Optional[str]
    strategy_name: Optional[str]
    timeframe: Optional[str]
    selected_variant: Optional[Dict[str, Any]]
    default_variant: Optional[Dict[str, Any]]
    base_params: Dict[str, Any] = field(default_factory=dict)
    effective_params: Dict[str, Any] = field(default_factory=dict)
    param_source_map: Dict[str, str] = field(default_factory=dict)
    output_filters: List[Dict[str, Any]] = field(default_factory=list)
    effective_strategy_config_hash: str = ""

    @property
    def resolved_params(self) -> Dict[str, Any]:
        """Existing runtime/report term for the effective strategy params."""

        return deepcopy(self.effective_params)

    @property
    def selected_variant_id(self) -> Optional[str]:
        return _clean_text((self.selected_variant or {}).get("id"))

    @property
    def selected_variant_name(self) -> Optional[str]:
        return _clean_text((self.selected_variant or {}).get("name"))

    def to_effective_strategy_config(self) -> Dict[str, Any]:
        """Return a serializable effective strategy config payload."""

        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "timeframe": self.timeframe,
            "variant": deepcopy(self.selected_variant),
            "default_variant": deepcopy(self.default_variant),
            "base_params": deepcopy(self.base_params),
            "effective_params": deepcopy(self.effective_params),
            "resolved_params": deepcopy(self.effective_params),
            "param_source_map": deepcopy(self.param_source_map),
            "output_filters": deepcopy(self.output_filters),
            "effective_strategy_config_hash": self.effective_strategy_config_hash,
        }

    def to_run_strategy_snapshot(self) -> Dict[str, Any]:
        """Return the run-start snapshot shape used by bot/report metadata."""

        variant = self.selected_variant or {}
        default_variant = self.default_variant or {}
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "timeframe": self.timeframe,
            "variant_id": _clean_text(variant.get("id")),
            "variant_name": _clean_text(variant.get("name")),
            "is_default_variant": bool(variant.get("is_default", False)),
            "default_variant_id": _clean_text(default_variant.get("id")),
            "base_params": deepcopy(self.base_params),
            "effective_params": deepcopy(self.effective_params),
            "resolved_params": deepcopy(self.effective_params),
            "param_source_map": deepcopy(self.param_source_map),
            "output_filters": deepcopy(self.output_filters),
            "effective_strategy_config_hash": self.effective_strategy_config_hash,
        }


def resolve_strategy_variant(
    strategy: Any,
    variant: Any | None,
    *,
    default_variant: Any | None = None,
    include_source_map: bool = True,
) -> EffectiveStrategyConfig:
    """Resolve one effective strategy config from base/default plus filters."""

    default_payload = _variant_payload(default_variant)
    selected_payload = _variant_payload(variant) or default_payload

    strategy_id = _clean_text(_value(strategy, "id")) or _clean_text(_value(strategy, "strategy_id"))
    strategy_name = _clean_text(_value(strategy, "name"))
    timeframe = _clean_text(_value(strategy, "timeframe"))

    base_params = _strategy_base_params(strategy)
    effective_params = deepcopy(base_params)
    param_source_map: Dict[str, str] = {key: "base_params" for key in effective_params}

    output_filters = _normalize_output_filters((selected_payload or {}).get("output_filters"))

    hash_payload = {
        "strategy_id": strategy_id,
        "timeframe": timeframe,
        "variant_id": _clean_text((selected_payload or {}).get("id")),
        "variant_name": _clean_text((selected_payload or {}).get("name")),
        "default_variant_id": _clean_text((default_payload or {}).get("id")),
        "effective_params": effective_params,
        "output_filters": output_filters,
    }

    return EffectiveStrategyConfig(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        timeframe=timeframe,
        selected_variant=deepcopy(selected_payload),
        default_variant=deepcopy(default_payload),
        base_params=base_params,
        effective_params=effective_params,
        param_source_map=param_source_map if include_source_map else {},
        output_filters=output_filters,
        effective_strategy_config_hash=_stable_hash(hash_payload),
    )


def _normalize_scope_values(value: Any) -> set[str]:
    return {str(item).strip() for item in _list(value) if str(item).strip()}


def _filter_matches_rule(output_filter: Mapping[str, Any], rule: Mapping[str, Any]) -> bool:
    scope = output_filter.get("scope") if isinstance(output_filter.get("scope"), Mapping) else {}
    rule_ids = _normalize_scope_values(scope.get("rule_ids") or scope.get("rule_id"))
    intents = _normalize_scope_values(scope.get("intents") or scope.get("intent"))
    rule_id = str(rule.get("id") or "").strip()
    intent = str(rule.get("intent") or "").strip()
    if rule_ids and rule_id not in rule_ids:
        return False
    if intents and intent not in intents:
        return False
    return True


def _output_filter_to_guard(output_filter: Mapping[str, Any], *, index: int) -> Dict[str, Any]:
    operator = str(output_filter.get("operator") or "equals").strip().lower()
    base = {
        "indicator_id": str(output_filter.get("indicator_id") or "").strip(),
        "output_name": str(output_filter.get("output_name") or "").strip(),
        "field": str(output_filter.get("field") or "").strip(),
        "value": deepcopy(output_filter.get("value")),
        "source": {
            "type": "variant_output_filter",
            "filter_index": int(index),
            "filter_hash": _stable_hash(output_filter),
            "operator": operator,
            "scope": deepcopy(output_filter.get("scope") or {}),
        },
    }
    if operator in {"equals", "=", "in"}:
        return {"type": "context_match", **base}
    if operator in {">", ">=", "<", "<=", "==", "!="}:
        return {"type": "metric_match", "operator": operator, **base}
    raise ValueError(f"Unsupported output filter operator: {operator}")


def _guard_semantic_hash(guard: Mapping[str, Any]) -> str:
    payload = dict(deepcopy(guard))
    payload.pop("source", None)
    return _stable_hash(payload)


def materialize_output_filters(
    rules: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    output_filters: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Apply variant output filters to authored rules as deterministic guards."""

    authored_rules = list(rules.values()) if isinstance(rules, Mapping) else list(rules)
    materialized = [dict(deepcopy(rule)) for rule in authored_rules]
    filters = _normalize_output_filters(list(output_filters or []))
    for index, output_filter in enumerate(filters):
        guard = _output_filter_to_guard(output_filter, index=index)
        guard_hash = _guard_semantic_hash(guard)
        matched = False
        for rule in materialized:
            if not _filter_matches_rule(output_filter, rule):
                continue
            guards = list(rule.get("guards") or [])
            existing_hashes = {_guard_semantic_hash(item) for item in guards if isinstance(item, Mapping)}
            if guard_hash not in existing_hashes:
                guards.append(deepcopy(guard))
            rule["guards"] = guards
            matched = True
        if not matched:
            raise ValueError(f"output_filters[{index}] did not match any strategy rules")
    return materialized
