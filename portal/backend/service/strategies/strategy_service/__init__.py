"""Strategy service package exposing a thin facade and helper modules."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_MODULE_EXPORTS = {
    "evaluator": "strategies",
    "markers": "strategies",
    "persistence": "portal.backend.service.strategies.strategy_service.persistence",
}

_FACADE_EXPORTS = {
    "StrategyDefinition",
    "StrategyRegistry",
    "StrategyRule",
    "RuleCondition",
    "create_rule",
    "create_strategy",
    "create_rule_filter",
    "create_strategy_filter",
    "delete_rule",
    "delete_rule_filter",
    "delete_strategy",
    "delete_strategy_filter",
    "delete_symbol_preset_service",
    "generate_strategy_signals",
    "get_strategy",
    "list_atm_templates",
    "list_rule_filters",
    "list_strategies",
    "list_strategy_filters",
    "list_symbol_presets_service",
    "register_indicator",
    "save_atm_template",
    "save_symbol_preset_service",
    "unregister_indicator",
    "update_rule_filter",
    "update_rule",
    "update_strategy_filter",
    "update_strategy",
}


def __getattr__(name: str) -> Any:
    module_name = _MODULE_EXPORTS.get(name)
    if module_name is not None:
        module = import_module(module_name)
        return getattr(module, name)

    if name in _FACADE_EXPORTS:
        module = import_module("portal.backend.service.strategies.strategy_service.facade")
        return getattr(module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "evaluator",
    "markers",
    "persistence",
    *_FACADE_EXPORTS,
]
