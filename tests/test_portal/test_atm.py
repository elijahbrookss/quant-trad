import pytest
import pytest

from portal.backend.service.risk.atm import (
    DEFAULT_ATM_TEMPLATE,
    merge_templates,
    normalise_template,
    template_metrics,
)


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


@pytest.mark.unit
def test_normalise_template_tracks_tick_overrides():
    template = normalise_template({"tick_size": 0.5})

    assert template["tick_size"] == 0.5
    assert template.get("_meta", {}).get("tick_size_override") is True

    reverted = normalise_template({"tick_size": None, "_meta": {"tick_size_override": False}}, base=template)

    assert reverted.get("tick_size") == template.get("tick_size")
    assert reverted.get("_meta", {}).get("tick_size_override") is False


@pytest.mark.unit
def test_template_metrics_reports_reward_to_risk():
    template = {
        "contracts": 3,
        "stop_ticks": 30,
        "take_profit_orders": [
            {"ticks": 20, "contracts": 1},
            {"ticks": 40, "contracts": 1},
            {"ticks": 60, "contracts": 1},
        ],
    }

    metrics = template_metrics(template)

    assert metrics["average_reward_ticks"] == pytest.approx(40)
    assert metrics["stop_ticks"] == pytest.approx(30)
    assert metrics["reward_to_risk"] == pytest.approx(40 / 30, rel=1e-4)
