"""ATM template schema definitions and defaults."""

from __future__ import annotations

from typing import Any, Dict, FrozenSet

ATM_SCHEMA_VERSION = 2

ATM_TEMPLATE_FIELDS: FrozenSet[str] = frozenset(
    {
        "schema_version",
        "name",
        "execution_mode",
        "limit_maker",
        "initial_stop",
        "take_profit_orders",
        "exit_plan",
        "breakeven",
        "trailing",
        "stop_adjustments",
    }
)
ATM_LIMIT_MAKER_FIELDS: FrozenSet[str] = frozenset(
    {"anchor_price", "offset_type", "offset_value", "validity_window", "fallback"}
)
ATM_INITIAL_STOP_FIELDS: FrozenSet[str] = frozenset(
    {"mode", "atr_period", "atr_multiplier"}
)
ATM_TAKE_PROFIT_FIELDS: FrozenSet[str] = frozenset(
    {"id", "label", "ticks", "r_multiple", "price", "size_fraction"}
)
ATM_EXIT_PLAN_FIELDS: FrozenSet[str] = frozenset({"fixed_horizon"})
ATM_FIXED_HORIZON_FIELDS: FrozenSet[str] = frozenset(
    {"enabled", "bars", "price", "order_type"}
)
ATM_BREAKEVEN_FIELDS: FrozenSet[str] = frozenset(
    {"enabled", "activation_type", "ticks", "r_multiple"}
)
ATM_TRAILING_FIELDS: FrozenSet[str] = frozenset(
    {
        "enabled",
        "activation_type",
        "ticks",
        "atr_multiplier",
        "r_multiple",
        "target_index",
        "target_id",
    }
)
ATM_STOP_ADJUSTMENT_FIELDS: FrozenSet[str] = frozenset(
    {
        "id",
        "trigger_type",
        "trigger_value",
        "trigger_ticks",
        "action_type",
        "action_value",
    }
)


DEFAULT_ATM_TEMPLATE: Dict[str, Any] = {
    "schema_version": ATM_SCHEMA_VERSION,
    "name": "New ATM template",
    "execution_mode": "market",
    "limit_maker": {
        "anchor_price": "signal_price",
        "offset_type": "ticks",
        "offset_value": 0.0,
        "validity_window": 1,
        "fallback": "cancel",
    },
    "initial_stop": {
        "mode": "atr",
        "atr_period": 14,
        "atr_multiplier": 1.0,
    },
    "take_profit_orders": [
        {"id": "tp-1", "r_multiple": 1.0, "size_fraction": 0.34},
        {"id": "tp-2", "r_multiple": 2.0, "size_fraction": 0.33},
        {"id": "tp-3", "r_multiple": 3.0, "size_fraction": 0.33},
    ],
    "exit_plan": {
        "fixed_horizon": {
            "enabled": False,
            "bars": None,
            "price": "close",
            "order_type": "market",
        },
    },
    "breakeven": {
        "enabled": False,
        "activation_type": "r_multiple",
        "ticks": 0,
        "r_multiple": 1.0,
    },
    "trailing": {
        "enabled": False,
        "activation_type": "r_multiple",
        "ticks": 0,
        "atr_multiplier": None,
        "r_multiple": 1.0,
        "target_index": None,
        "target_id": None,
    },
    "stop_adjustments": [],
}


__all__ = [
    "ATM_BREAKEVEN_FIELDS",
    "ATM_EXIT_PLAN_FIELDS",
    "ATM_FIXED_HORIZON_FIELDS",
    "ATM_INITIAL_STOP_FIELDS",
    "ATM_LIMIT_MAKER_FIELDS",
    "ATM_SCHEMA_VERSION",
    "ATM_STOP_ADJUSTMENT_FIELDS",
    "ATM_TAKE_PROFIT_FIELDS",
    "ATM_TEMPLATE_FIELDS",
    "ATM_TRAILING_FIELDS",
    "DEFAULT_ATM_TEMPLATE",
]
