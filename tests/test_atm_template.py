from __future__ import annotations

from atm.template import normalise_template


def test_normalise_template_accepts_runtime_exit_policy_fields() -> None:
    config = normalise_template(
        {
            "name": "Runtime policy test",
            "fixed_horizon_bars": "12",
            "breakeven_trigger_ticks": "8",
            "trailing_stop": {
                "enabled": True,
                "activation_type": "r_multiple",
                "r_multiple": "1.5",
                "ticks": "4",
            },
            "stop_adjustments": [],
        }
    )

    fixed_horizon = config["exit_plan"]["fixed_horizon"]
    assert fixed_horizon["enabled"] is True
    assert fixed_horizon["bars"] == 12
    assert fixed_horizon["price"] == "close"
    assert fixed_horizon["order_type"] == "market"

    assert config["breakeven"]["enabled"] is True
    assert config["breakeven"]["ticks"] == 8

    assert config["trailing"]["enabled"] is True
    assert config["trailing"]["activation_type"] == "r_multiple"
    assert config["trailing"]["r_multiple"] == 1.5
    assert config["trailing"]["ticks"] == 4
    assert config["stop_adjustments"] == []


def test_normalise_template_flattens_nested_stop_adjustments() -> None:
    config = normalise_template(
        {
            "name": "Nested stop adjustment test",
            "stop_adjustments": [
                {
                    "id": "sa-1",
                    "trigger": {"type": "r_multiple_reached", "value": 1.0},
                    "action": {"type": "move_to_breakeven"},
                }
            ],
        }
    )

    assert config["stop_adjustments"] == [
        {
            "id": "sa-1",
            "trigger_type": "r_multiple",
            "trigger_value": 1.0,
            "trigger_ticks": None,
            "action_type": "move_to_breakeven",
            "action_value": None,
            "atr_period": None,
            "atr_multiplier": None,
        }
    ]


def test_normalise_template_preserves_flattened_stop_adjustments() -> None:
    config = normalise_template(
        {
            "name": "Flattened stop adjustment test",
            "stop_adjustments": [
                {
                    "id": "sa-flat",
                    "trigger_type": "r_multiple",
                    "trigger_ticks": 12,
                    "action_type": "move_to_r",
                    "action_r": 0.5,
                }
            ],
        }
    )

    assert config["stop_adjustments"] == [
        {
            "id": "sa-flat",
            "trigger_type": "r_multiple",
            "trigger_value": None,
            "trigger_ticks": 12.0,
            "action_type": "move_to_r",
            "action_value": 0.5,
            "atr_period": None,
            "atr_multiplier": None,
        }
    ]
