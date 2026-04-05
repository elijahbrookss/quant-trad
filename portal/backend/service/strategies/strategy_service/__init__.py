"""Strategy service package exposing the typed-rule facade."""

from strategies import evaluator

from . import persistence
from .facade import (
    StrategyDefinition,
    StrategyRegistry,
    StrategyRule,
    compile_strategy_contract,
    create_rule,
    create_strategy,
    delete_rule,
    delete_strategy,
    delete_symbol_preset_service,
    get_strategy,
    list_atm_templates,
    list_strategies,
    list_symbol_presets_service,
    register_indicator,
    save_atm_template,
    save_symbol_preset_service,
    run_strategy_preview,
    unregister_indicator,
    update_rule,
    update_strategy,
)

__all__ = [
    "StrategyDefinition",
    "StrategyRegistry",
    "StrategyRule",
    "compile_strategy_contract",
    "create_rule",
    "create_strategy",
    "delete_rule",
    "delete_strategy",
    "delete_symbol_preset_service",
    "evaluator",
    "get_strategy",
    "list_atm_templates",
    "list_strategies",
    "list_symbol_presets_service",
    "persistence",
    "register_indicator",
    "save_atm_template",
    "save_symbol_preset_service",
    "run_strategy_preview",
    "unregister_indicator",
    "update_rule",
    "update_strategy",
]
