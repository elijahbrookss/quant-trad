"""Lazy indicator service exports.

This package intentionally keeps lightweight function proxies so callers can
monkeypatch public API symbols in environments where optional indicator/runtime
dependencies are unavailable.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any, Callable

_API_MODULE = "portal.backend.service.indicators.indicator_service.api"


def _api_module():
    return import_module(_API_MODULE)


def _forward(name: str) -> Callable[..., Any]:
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        target = getattr(_api_module(), name)
        return target(*args, **kwargs)

    _wrapper.__name__ = name
    _wrapper.__qualname__ = name
    _wrapper.__doc__ = f"Proxy for `{_API_MODULE}.{name}`."
    return _wrapper


bulk_delete_instances = _forward("bulk_delete_instances")
bulk_set_enabled = _forward("bulk_set_enabled")
clear_overlay_cache = _forward("clear_overlay_cache")
create_instance = _forward("create_instance")
delete_instance = _forward("delete_instance")
duplicate_instance = _forward("duplicate_instance")
generate_signals_for_instance = _forward("generate_signals_for_instance")
runtime_input_plan_for_instance = _forward("runtime_input_plan_for_instance")
get_instance_meta = _forward("get_instance_meta")
get_type_details = _forward("get_type_details")
list_indicator_strategies = _forward("list_indicator_strategies")
list_instances_meta = _forward("list_instances_meta")
list_types = _forward("list_types")
overlays_for_instance = _forward("overlays_for_instance")
set_instance_enabled = _forward("set_instance_enabled")
update_instance = _forward("update_instance")

_LAZY_OBJECTS = {
    "IndicatorService",
    "IndicatorServiceContext",
    "BreakoutCacheContext",
    "default_service",
}

__all__ = [
    "IndicatorService",
    "IndicatorServiceContext",
    "BreakoutCacheContext",
    "bulk_delete_instances",
    "bulk_set_enabled",
    "clear_overlay_cache",
    "create_instance",
    "default_service",
    "delete_instance",
    "duplicate_instance",
    "generate_signals_for_instance",
    "runtime_input_plan_for_instance",
    "get_instance_meta",
    "get_type_details",
    "list_indicator_strategies",
    "list_instances_meta",
    "list_types",
    "overlays_for_instance",
    "set_instance_enabled",
    "update_instance",
]


def __getattr__(name: str) -> Any:
    if name not in _LAZY_OBJECTS:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
    value = getattr(_api_module(), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(__all__))
