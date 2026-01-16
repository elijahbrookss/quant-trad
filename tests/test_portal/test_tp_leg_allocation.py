from engines.bot_runtime.core.amount_constraints import normalize_qty_with_constraints
from engines.bot_runtime.core.domain import LadderRiskEngine


def _build_engine():
    config = {
        "tick_size": 1.0,
        "contract_size": 1.0,
        "tick_value": 1.0,
        "risk": {"base_risk_per_trade": 100},
        "take_profit_orders": [
            {"id": "tp-1", "ticks": 10},
            {"id": "tp-2", "ticks": 20},
            {"id": "tp-3", "ticks": 30},
        ],
    }
    instrument = {
        "symbol": "TEST-FUTURE",
        "instrument_type": "future",
        "tick_size": 1.0,
        "contract_size": 1.0,
        "tick_value": 1.0,
        "min_order_size": 1,
        "metadata": {
            "info": {"base_increment": "1"},
        },
    }
    return LadderRiskEngine(config=config, instrument=instrument)


def test_tp_allocation_drops_legs_when_qty_below_leg_count():
    engine = _build_engine()
    normalization = normalize_qty_with_constraints(engine.amount_constraints, 1.724)

    assert normalization.qty_final == 1.0

    contracts, dropped = engine._allocate_tp_contracts(qty_final=normalization.qty_final, tp_leg_count=3)
    assert contracts == [1.0, 0.0, 0.0]
    assert dropped == [2, 3]


def test_tp_allocation_distributes_remainder_to_earliest_legs():
    engine = _build_engine()
    contracts, dropped = engine._allocate_tp_contracts(qty_final=5.0, tp_leg_count=3)

    assert contracts == [2.0, 2.0, 1.0]
    assert dropped == []
