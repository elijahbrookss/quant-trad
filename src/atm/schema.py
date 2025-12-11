"""ATM template schema definitions and defaults."""

from __future__ import annotations

from typing import Any, Dict

DEFAULT_ATM_TEMPLATE: Dict[str, Any] = {
    "schema_version": 2,
    "name": "New ATM template",
    "initial_stop": {
        "mode": "atr",
        "atr_period": 14,
        "atr_multiplier": 1.0,
    },
    "risk": {
        "global_risk_multiplier": 1.0,
        "base_risk_per_trade": None,
    },
    "take_profit_orders": [
        {"id": "tp-1", "r_multiple": 1.0, "size_fraction": 0.34},
        {"id": "tp-2", "r_multiple": 2.0, "size_fraction": 0.33},
        {"id": "tp-3", "r_multiple": 3.0, "size_fraction": 0.33},
    ],
    "stop_adjustments": [
        {
            "id": "sa-1",
            "trigger": {"type": "r_multiple_reached", "value": 1.0},
            "action": {"type": "move_to_breakeven"},
        }
    ],
    "_meta": {"instrument_overrides": False},
}


__all__ = ["DEFAULT_ATM_TEMPLATE"]
