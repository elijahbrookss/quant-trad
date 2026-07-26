from __future__ import annotations

from copy import deepcopy
import re

import pytest

from atm.template import normalise_template
from engines.bot_runtime.core.domain import LadderRiskEngine
from engines.bot_runtime.core.execution_plan import compile_runtime_execution_plan


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"execution_mode": "iceberg"}, "execution_mode"),
        ({"limit_maker": {"offset_type": "basis_points"}}, "limit_maker.offset_type"),
        ({"limit_maker": {"validity_window": 0}}, "limit_maker.validity_window"),
        ({"initial_stop": {"mode": "ticks"}}, "initial_stop.mode"),
        ({"initial_stop": {"atr_multiplier": 0}}, "initial_stop.atr_multiplier"),
        ({"take_profit_orders": []}, "must contain at least one target"),
        (
            {"take_profit_orders": [{"id": "tp-1", "ticks": 10, "r_multiple": 1.0}]},
            "exactly one positive target",
        ),
        (
            {
                "take_profit_orders": [
                    {"id": "tp-1", "ticks": 10, "size_fraction": 0.5},
                    {"id": "tp-2", "ticks": 20},
                ]
            },
            "take_profit_orders[1].size_fraction is required",
        ),
        ({"stop_ticks": "not-a-number"}, "ATM template contains unsupported fields"),
        ({"targets": [2.5]}, "ATM template contains unsupported fields"),
        (
            {
                "take_profit_orders": [
                    {
                        "id": "tp-1",
                        "ticks": 10,
                        "target_ticks": 20,
                        "size_fraction": 1.0,
                    }
                ]
            },
            "take_profit_orders[0] contains unsupported fields",
        ),
        ({"executionMode": "market"}, "ATM template contains unsupported fields"),
        ({"limit_maker": {"offset": {"type": "ticks"}}}, "limit_maker contains unsupported fields"),
        ({"exit_plan": {"fixedHorizon": {}}}, "exit_plan contains unsupported fields"),
        ({"breakeven": 8}, "breakeven must be a mapping"),
        ({"trailing": True}, "trailing must be a mapping"),
        ({"stop_adjustments": ["not-a-rule"]}, "stop_adjustments[0] must be a mapping"),
        (
            {
                "stop_adjustments": [
                    {
                        "id": "sa-ignore",
                        "trigger_type": "r_multiple",
                        "trigger_value": 1.0,
                        "action_type": "ignore",
                    }
                ]
            },
            "action_type='ignore' is unsupported",
        ),
        (
            {
                "stop_adjustments": [
                    {
                        "id": "sa-nested",
                        "trigger": {"type": "r_multiple_reached", "value": 1.0},
                        "action": {"type": "move_to_breakeven"},
                    }
                ]
            },
            "stop_adjustments[0] contains unsupported fields",
        ),
        (
            {
                "stop_adjustments": [
                    {
                        "id": "duplicate",
                        "trigger_type": "r_multiple",
                        "trigger_value": 1.0,
                        "action_type": "move_to_breakeven",
                    },
                    {
                        "id": "duplicate",
                        "trigger_type": "r_multiple",
                        "trigger_value": 2.0,
                        "action_type": "move_to_breakeven",
                    },
                ]
            },
            "duplicates stop-adjustment id",
        ),
        (
            {"exit_plan": {"fixed_horizon": {"enabled": True, "bars": "many"}}},
            "exit_plan.fixed_horizon.bars",
        ),
        (
            {"trailing": {"enabled": True, "activation_type": "future"}},
            "trailing.activation_type",
        ),
    ],
)
def test_normalise_template_rejects_malformed_execution_configuration(
    payload: dict,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        normalise_template(payload)


def test_normalized_template_is_idempotent_and_compilable() -> None:
    normalized = normalise_template({"stop_adjustments": []})

    renormalized = normalise_template(normalized)

    assert renormalized == normalized
    assert compile_runtime_execution_plan(renormalized).entry.order_type == "market"


def test_compiler_rejects_ambiguous_trailing_distance() -> None:
    config = normalise_template(
        {
            "trailing": {
                "enabled": True,
                "ticks": 4,
                "atr_multiplier": 1.5,
            },
            "stop_adjustments": [],
        }
    )

    with pytest.raises(ValueError, match="at most one distance"):
        compile_runtime_execution_plan(config)


def test_compiler_rejects_invalid_disabled_trailing_target_reference() -> None:
    config = normalise_template(
        {
            "take_profit_orders": [{"id": "tp-1", "ticks": 10, "size_fraction": 1.0}],
            "trailing": {
                "enabled": False,
                "activation_type": "target_hit",
                "target_id": "tp-missing",
                "r_multiple": None,
                "ticks": 0,
            },
            "stop_adjustments": [],
        }
    )

    with pytest.raises(ValueError, match="does not reference a configured target"):
        compile_runtime_execution_plan(config)


def test_compiler_rejects_unknown_stop_adjustment_target() -> None:
    config = normalise_template(
        {
            "take_profit_orders": [{"id": "tp-1", "ticks": 10, "size_fraction": 1.0}],
            "stop_adjustments": [],
        }
    )
    config["stop_adjustments"] = [
        {
            "id": "move-after-missing-target",
            "trigger_type": "target_hit",
            "trigger_value": "tp-missing",
            "trigger_ticks": None,
            "action_type": "move_to_breakeven",
            "action_value": None,
        }
    ]

    with pytest.raises(ValueError, match="does not reference a configured target"):
        compile_runtime_execution_plan(config)


def test_compiler_rejects_malformed_config_even_if_normalizer_is_bypassed() -> None:
    config = normalise_template({"stop_adjustments": []})
    malformed = deepcopy(config)
    malformed["execution_mode"] = "iceberg"

    with pytest.raises(ValueError, match="execution_mode"):
        compile_runtime_execution_plan(malformed)


def test_compiler_rejects_unknown_fields_when_normalizer_is_bypassed() -> None:
    config = normalise_template({"stop_adjustments": []})
    malformed = deepcopy(config)
    malformed["take_profit_orders"][0]["target_ticks"] = 10

    with pytest.raises(
        ValueError,
        match=re.escape("take_profit_orders[0] contains unsupported fields"),
    ):
        compile_runtime_execution_plan(malformed)


def test_engine_projects_every_valid_target_from_compiled_plan() -> None:
    engine = LadderRiskEngine(
        config={
            "initial_stop": {"atr_multiplier": 2.0},
            "take_profit_orders": [
                {"id": "tp-small", "ticks": 10, "size_fraction": 0.1},
                {"id": "tp-large", "ticks": 20, "size_fraction": 0.9},
            ],
            "stop_adjustments": [],
        },
        instrument={
            "symbol": "TEST-SPOT",
            "instrument_type": "spot",
            "tick_size": 1.0,
            "contract_size": 1.0,
            "tick_value": 1.0,
            "min_order_size": 1,
            "base_currency": "BTC",
            "quote_currency": "USD",
            "metadata": {"info": {"base_increment": "1"}},
        },
        risk_config={"base_risk_per_trade": 100.0},
    )

    assert [target.target_id for target in engine.execution_plan.take_profits] == [
        "tp-small",
        "tp-large",
    ]
    assert [order["id"] for order in engine.orders] == ["tp-small", "tp-large"]
