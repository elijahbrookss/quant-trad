"""Shared strategy variant resolution helpers.

This module keeps variant resolution deliberately small:

- a strategy provides base/default decision configuration,
- a strategy variant provides named overrides,
- effective params are resolved once from base/default plus selected overrides,
- run snapshots can preserve the exact effective strategy config used.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
import json
from typing import Any, Dict, Mapping, Optional


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
        "param_overrides": _dict(_value(variant, "param_overrides")),
        "atm_template_id": _clean_text(_value(variant, "atm_template_id")),
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
    """Resolved strategy/variant params plus compact provenance."""

    strategy_id: Optional[str]
    strategy_name: Optional[str]
    timeframe: Optional[str]
    selected_variant: Optional[Dict[str, Any]]
    default_variant: Optional[Dict[str, Any]]
    base_params: Dict[str, Any] = field(default_factory=dict)
    variant_overrides: Dict[str, Any] = field(default_factory=dict)
    bot_overrides: Dict[str, Any] = field(default_factory=dict)
    effective_params: Dict[str, Any] = field(default_factory=dict)
    param_source_map: Dict[str, str] = field(default_factory=dict)
    effective_atm_template_id: Optional[str] = None
    atm_template_source: Optional[str] = None
    effective_strategy_config_hash: str = ""

    @property
    def resolved_params(self) -> Dict[str, Any]:
        """Backward-compatible alias for callers that still say resolved params."""

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
            "variant_overrides": deepcopy(self.variant_overrides),
            "bot_overrides": deepcopy(self.bot_overrides),
            "effective_params": deepcopy(self.effective_params),
            "resolved_params": deepcopy(self.effective_params),
            "param_source_map": deepcopy(self.param_source_map),
            "atm_template_id": self.effective_atm_template_id,
            "atm_template_source": self.atm_template_source,
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
            "variant_overrides": deepcopy(self.variant_overrides),
            "effective_params": deepcopy(self.effective_params),
            "resolved_params": deepcopy(self.effective_params),
            "param_source_map": deepcopy(self.param_source_map),
            "atm_template_id": self.effective_atm_template_id,
            "atm_template_source": self.atm_template_source,
            "effective_strategy_config_hash": self.effective_strategy_config_hash,
        }


def resolve_strategy_variant(
    strategy: Any,
    variant: Any | None,
    *,
    default_variant: Any | None = None,
    bot_overrides: Mapping[str, Any] | None = None,
    include_source_map: bool = True,
) -> EffectiveStrategyConfig:
    """Resolve one effective strategy config from base/default plus overrides."""

    default_payload = _variant_payload(default_variant)
    selected_payload = _variant_payload(variant) or default_payload

    strategy_id = _clean_text(_value(strategy, "id")) or _clean_text(_value(strategy, "strategy_id"))
    strategy_name = _clean_text(_value(strategy, "name"))
    timeframe = _clean_text(_value(strategy, "timeframe"))
    base_atm_template_id = _clean_text(_value(strategy, "atm_template_id"))

    base_params = _strategy_base_params(strategy)
    effective_params = deepcopy(base_params)
    param_source_map: Dict[str, str] = {key: "base_params" for key in effective_params}

    if default_payload is not None:
        for key, value in _dict(default_payload.get("param_overrides")).items():
            effective_params[key] = value
            param_source_map[key] = "default_variant"
    elif selected_payload is not None and bool(selected_payload.get("is_default", False)):
        for key, value in _dict(selected_payload.get("param_overrides")).items():
            effective_params[key] = value
            param_source_map[key] = "default_variant"

    variant_overrides: Dict[str, Any] = {}
    if selected_payload is not None and not bool(selected_payload.get("is_default", False)):
        variant_overrides = _dict(selected_payload.get("param_overrides"))
        for key, value in variant_overrides.items():
            effective_params[key] = value
            param_source_map[key] = "variant_overrides"

    clean_bot_overrides = _dict(bot_overrides)
    for key, value in clean_bot_overrides.items():
        effective_params[key] = value
        param_source_map[key] = "bot_overrides"

    effective_atm_template_id = base_atm_template_id
    atm_template_source = "strategy" if effective_atm_template_id else None
    if default_payload and default_payload.get("atm_template_id"):
        effective_atm_template_id = _clean_text(default_payload.get("atm_template_id"))
        atm_template_source = "default_variant"
    if selected_payload and selected_payload.get("atm_template_id"):
        effective_atm_template_id = _clean_text(selected_payload.get("atm_template_id"))
        atm_template_source = "variant_overrides" if not selected_payload.get("is_default") else "default_variant"

    hash_payload = {
        "strategy_id": strategy_id,
        "timeframe": timeframe,
        "variant_id": _clean_text((selected_payload or {}).get("id")),
        "variant_name": _clean_text((selected_payload or {}).get("name")),
        "default_variant_id": _clean_text((default_payload or {}).get("id")),
        "effective_params": effective_params,
        "atm_template_id": effective_atm_template_id,
    }

    return EffectiveStrategyConfig(
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        timeframe=timeframe,
        selected_variant=deepcopy(selected_payload),
        default_variant=deepcopy(default_payload),
        base_params=base_params,
        variant_overrides=variant_overrides,
        bot_overrides=clean_bot_overrides,
        effective_params=effective_params,
        param_source_map=param_source_map if include_source_map else {},
        effective_atm_template_id=effective_atm_template_id,
        atm_template_source=atm_template_source,
        effective_strategy_config_hash=_stable_hash(hash_payload),
    )
