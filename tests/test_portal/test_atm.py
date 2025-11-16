import pytest
import pytest

from portal.backend.service.atm import DEFAULT_ATM_TEMPLATE, merge_templates, normalise_template


@pytest.mark.unit
def test_normalise_template_populates_targets_and_stops():
    template = normalise_template(
        {
            "contracts": 2,
            "take_profit_orders": [
                {"ticks": 15, "contracts": 1, "label": "Scout"},
                {"ticks": 30, "contracts": 1},
            ],
            "stop_ticks": 10,
            "breakeven": {"target_index": 0},
            "trailing": {"enabled": True, "atr_multiplier": 2},
        }
    )

    assert template["contracts"] == 2
    assert template["stop_ticks"] == 10
    assert len(template["take_profit_orders"]) == 2
    assert template["take_profit_orders"][0]["label"] == "Scout"
    assert template["breakeven"]["target_index"] == 0
    assert template["trailing"]["enabled"] is True
    assert template["trailing"]["atr_multiplier"] == pytest.approx(2)


@pytest.mark.unit
def test_merge_templates_layers_inputs_over_defaults():
    first = {"contracts": 1, "take_profit_orders": [{"ticks": 10, "contracts": 1}]}
    second = {"stop_ticks": 25, "breakeven": {"ticks": 5}}

    merged = merge_templates(first, second)

    assert merged["contracts"] == 1
    assert merged["stop_ticks"] == 25
    assert merged["breakeven"]["ticks"] == 5
    assert merged["take_profit_orders"][0]["ticks"] == 10


@pytest.mark.unit
def test_merge_templates_returns_defaults_when_none_provided():
    merged = merge_templates(None)
    assert merged["contracts"] == DEFAULT_ATM_TEMPLATE["contracts"]
    assert merged["take_profit_orders"] == DEFAULT_ATM_TEMPLATE["take_profit_orders"]
