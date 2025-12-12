from engines.backtest.configuration import InstrumentConfig, RiskConfig
from engines.backtest.risk_engine import DEFAULT_RISK


def test_instrument_config_normalises_values_and_constraints():
    instrument = InstrumentConfig.from_dict(
        {
            "tick_size": 0.25,
            "contract_size": 5,
            "risk_multiplier": 1.5,
            "min_qty": 2,
            "qty_step": 0.5,
        },
        default_tick_size=0.01,
    )

    assert instrument.tick_size == 0.25
    assert instrument.contract_size == 5
    assert instrument.tick_value == 1.25
    assert instrument.risk_multiplier == 1.5

    constrained = instrument.apply_quantity_constraints(0.3)
    assert constrained == 2.0


def test_risk_config_defaults_and_overrides():
    instrument = InstrumentConfig.from_dict({}, default_tick_size=DEFAULT_RISK["tick_size"])
    template = {
        "tick_size": 0.1,
        "stop_ticks": 15,
        "initial_stop": {"atr_multiplier": 2, "mode": "ticks"},
        "risk": {"base_risk_per_trade": 150, "global_risk_multiplier": 1.25},
        "stop_r_multiple": 1.5,
        "stop_price": 98.5,
        "stop_adjustments": [{"trigger_type": "r_multiple", "action_type": "move_to_r", "trigger_value": 1, "action_value": 0.5}],
        "quote_currency": "eur",
        "maker_fee_rate": 0.0002,
        "taker_fee_rate": 0.0004,
    }

    risk = RiskConfig.from_dict(template, instrument, DEFAULT_RISK)

    assert risk.tick_size == 0.1
    assert risk.stop_ticks == 15
    assert risk.r_multiple == 2
    assert risk.base_risk_per_trade == 150
    assert risk.global_risk_multiplier == 1.25
    assert risk.instrument_risk_multiplier == 1.0
    assert risk.stop_r_multiple == 1.5
    assert risk.stop_price == 98.5
    assert risk.stop_adjustments[0]["action_type"] == "move_to_r"
    assert risk.risk_unit_mode == "ticks"
    assert risk.ticks_stop == 15
    assert risk.quote_currency == "EUR"
    assert risk.maker_fee == 0.0002
    assert risk.taker_fee == 0.0004

