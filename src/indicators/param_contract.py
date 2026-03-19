from __future__ import annotations

import inspect
from typing import Any, Dict, Mapping


def indicator_required_params(indicator_cls: Any) -> list[str]:
    declared_required = _declared_required_params(indicator_cls)
    declared_defaults = _declared_default_params(indicator_cls)
    _validate_declared_param_contract(indicator_cls, declared_required, declared_defaults)
    if declared_required or declared_defaults:
        return declared_required
    return _signature_required_params(indicator_cls)


def indicator_default_params(indicator_cls: Any) -> Dict[str, Any]:
    declared_required = _declared_required_params(indicator_cls)
    declared_defaults = _declared_default_params(indicator_cls)
    _validate_declared_param_contract(indicator_cls, declared_required, declared_defaults)
    if declared_required or declared_defaults:
        return declared_defaults
    return _signature_default_params(indicator_cls)


def indicator_field_types(indicator_cls: Any) -> Dict[str, str]:
    sig = inspect.signature(indicator_cls.__init__)
    field_types: Dict[str, str] = {}
    for name in indicator_param_names(indicator_cls):
        param = sig.parameters.get(name)
        if param is None:
            continue
        anno = param.annotation
        if anno is inspect._empty:
            field_types[name] = "Any"
        elif isinstance(anno, type):
            field_types[name] = anno.__name__
        else:
            field_types[name] = str(anno)
    return field_types


def indicator_param_names(indicator_cls: Any) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for name in [*indicator_required_params(indicator_cls), *indicator_default_params(indicator_cls).keys()]:
        if name in seen:
            continue
        ordered.append(name)
        seen.add(name)
    return ordered


def resolve_indicator_params(
    indicator_cls: Any,
    params: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    resolved: Dict[str, Any] = dict(params or {})
    defaults = indicator_default_params(indicator_cls)
    for key, value in defaults.items():
        resolved.setdefault(key, value)

    missing = [key for key in indicator_required_params(indicator_cls) if resolved.get(key) is None]
    if missing:
        indicator_name = getattr(indicator_cls, "NAME", indicator_cls.__name__)
        raise ValueError(
            f"{indicator_name} indicator missing required params: {missing}"
        )
    return resolved


def _declared_required_params(indicator_cls: Any) -> list[str]:
    raw = getattr(indicator_cls, "REQUIRED_PARAMS", ())
    return _normalize_param_name_list(raw)


def _declared_default_params(indicator_cls: Any) -> Dict[str, Any]:
    raw_defaults = getattr(indicator_cls, "DEFAULT_PARAMS", None)
    if isinstance(raw_defaults, Mapping):
        return dict(raw_defaults)
    return {}


def _signature_required_params(indicator_cls: Any) -> list[str]:
    required: list[str] = []
    for name, param in inspect.signature(indicator_cls.__init__).parameters.items():
        if name in ("self", "df"):
            continue
        if param.default is inspect._empty:
            required.append(name)
    return required


def _signature_default_params(indicator_cls: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {}
    for name, param in inspect.signature(indicator_cls.__init__).parameters.items():
        if name in ("self", "df"):
            continue
        if param.default is not inspect._empty:
            defaults[name] = param.default
    return defaults


def _normalize_param_name_list(raw: Any) -> list[str]:
    if not isinstance(raw, (list, tuple, set)):
        return []
    ordered: list[str] = []
    seen: set[str] = set()
    for value in raw:
        name = str(value).strip()
        if not name or name in seen:
            continue
        ordered.append(name)
        seen.add(name)
    return ordered


def _validate_declared_param_contract(
    indicator_cls: Any,
    required_params: list[str],
    default_params: Mapping[str, Any],
) -> None:
    overlap = set(required_params) & set(default_params.keys())
    if not overlap:
        return
    indicator_name = getattr(indicator_cls, "NAME", indicator_cls.__name__)
    raise RuntimeError(
        f"Indicator '{indicator_name}' declares params as both required and defaulted: {sorted(overlap)}"
    )
