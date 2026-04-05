"""Strategy authoring templates and parameter schemas."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping


ParamType = Literal["float", "int", "str"]


def _is_valid_param_key(key: str) -> bool:
    if not key:
        return False
    first = key[0]
    if not (first.isalpha() or first == "_"):
        return False
    return all(char.isalnum() or char == "_" for char in key[1:])


def _coerce_param_value(value: Any, param_type: ParamType, key: str) -> float | int | str:
    try:
        if param_type == "float":
            return float(value)
        if param_type == "int":
            return int(value)
        if param_type == "str":
            return str(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Cannot coerce {value!r} to {param_type} for param {key!r}") from exc
    raise ValueError(f"Unsupported parameter type: {param_type}")


@dataclass(frozen=True)
class ParamSpec:
    key: str
    type: ParamType
    default: float | int | str
    label: str | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        if not _is_valid_param_key(self.key):
            raise ValueError(f"Invalid parameter key: {self.key!r}")
        if self.type not in {"float", "int", "str"}:
            raise ValueError(f"Unsupported parameter type: {self.type}")
        coerced_default = _coerce_param_value(self.default, self.type, self.key)
        object.__setattr__(self, "default", coerced_default)


@dataclass(frozen=True)
class StrategyTemplate:
    template_id: str
    name: str
    timeframe: str
    rules: Mapping[str, Any]
    param_specs: tuple[ParamSpec, ...]
    variants: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.template_id or "").strip():
            raise ValueError("template_id is required")
        if not str(self.name or "").strip():
            raise ValueError("name is required")
        if not str(self.timeframe or "").strip():
            raise ValueError("timeframe is required")
        seen: set[str] = set()
        for spec in self.param_specs:
            key = spec.key
            if key in seen:
                raise ValueError(f"Duplicate param key in template: {key}")
            seen.add(key)

    def instantiate(
        self,
        *,
        overrides: Mapping[str, Any] | None = None,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        if not self.param_specs:
            return dict(self.rules), {}

        applied_overrides = dict(overrides) if overrides else {}
        return dict(self.rules), self._resolve_params(applied_overrides)

    def instantiate_variant(
        self,
        name: str,
        overrides: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if name not in self.variants:
            raise ValueError(f"Unknown strategy variant: {name!r}")
        variant_overrides = self.variants[name]
        if not isinstance(variant_overrides, Mapping):
            raise ValueError(f"Strategy variant {name!r} must define a parameter mapping")
        merged_overrides = dict(variant_overrides)
        if overrides:
            merged_overrides.update(dict(overrides))
        if not self.param_specs and not merged_overrides:
            return dict(self.rules), {}
        return dict(self.rules), self._resolve_params(merged_overrides)

    def _resolve_params(self, overrides: Mapping[str, Any]) -> dict[str, float | int | str]:
        specs_by_key = {spec.key: spec for spec in self.param_specs}
        resolved = {spec.key: spec.default for spec in self.param_specs}
        for key, value in overrides.items():
            spec = specs_by_key.get(key)
            if spec is None:
                declared_keys = sorted(specs_by_key)
                raise ValueError(
                    f"Unknown parameter override: {key!r}. "
                    f"Template '{self.template_id}' declares: {declared_keys}"
                )
            resolved[key] = _coerce_param_value(value, spec.type, key)

        return resolved


__all__ = ["ParamSpec", "ParamType", "StrategyTemplate"]
