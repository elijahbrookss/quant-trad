from datetime import datetime

from engines.backtest.domain.configuration import InstrumentConfig, RiskConfig
from engines.backtest.domain.models import Candle
from engines.backtest.services.orders import OrderTemplateBuilder
from engines.backtest.services.risk_engine import DEFAULT_RISK, LadderRiskEngine
from engines.backtest.strategies import (
    DefaultRiskSizingStrategy,
    DefaultStopTargetStrategy,
    StopComputation,
    StopTargetResult,
)


def test_default_risk_sizing_strategy():
    instrument = InstrumentConfig.from_dict(
        {"tick_size": 0.5, "contract_size": 2, "tick_value": 1.0},
        default_tick_size=0.5,
    )
    risk = RiskConfig.from_dict({"risk": {"base_risk_per_trade": 100}}, instrument, DEFAULT_RISK)

    strategy = DefaultRiskSizingStrategy()
    qty = strategy.contracts_from_effective_risk(1.0, risk, instrument)

    assert qty == 50.0


def test_stop_target_strategy_builds_legs_and_stop():
    instrument = InstrumentConfig.from_dict({}, default_tick_size=DEFAULT_RISK["tick_size"])
    risk = RiskConfig.from_dict({"stop_ticks": 10}, instrument, DEFAULT_RISK)
    candle = Candle(time=datetime.utcnow(), open=100.0, high=101.0, low=99.0, close=100.0, atr=2.0)
    orders = [{"ticks": 10, "contracts": 1}]

    strategy = DefaultStopTargetStrategy()
    stop_info = strategy.compute_stop(candle, "long", risk)
    result = strategy.build_targets(candle, "long", risk, orders, stop_info)

    assert result.stop_price < candle.close
    assert result.legs[0].target_price == 100.0 + (10 * risk.tick_size)
    assert result.stop_adjustments == []


def test_ladder_risk_engine_accepts_custom_strategies():
    class StubRiskSizing:
        def __init__(self):
            self.called = False

        def contracts_from_effective_risk(self, stop_distance_price, risk, instrument):
            self.called = True
            return 3

    class StubStopTargets:
        def __init__(self):
            self.compute_called = False
            self.build_called = False

        def compute_stop(self, candle, direction, risk):
            self.compute_called = True
            return StopComputation(
                stop_price=candle.close - 1,
                r_value=1.0,
                r_ticks=1.0,
                one_r_distance=1.0,
                atr_at_entry=1.0,
            )

        def build_targets(self, candle, direction, risk, orders, stop_info):
            self.build_called = True
            return StopTargetResult(
                stop_price=stop_info.stop_price,
                r_value=stop_info.r_value,
                r_ticks=stop_info.r_ticks,
                one_r_distance=stop_info.one_r_distance,
                atr_at_entry=stop_info.atr_at_entry,
                legs=[],
                stop_adjustments=[],
            )

    stub_risk = StubRiskSizing()
    stub_stop = StubStopTargets()
    builder = OrderTemplateBuilder({}, DEFAULT_RISK)
    engine = LadderRiskEngine(
        config={},
        instrument={},
        risk_sizing_strategy=stub_risk,
        stop_target_strategy=stub_stop,
        order_builder=builder,
    )

    candle = Candle(time=datetime.utcnow(), open=100.0, high=101.0, low=99.0, close=100.0)
    engine.maybe_enter(candle, "long")

    assert stub_risk.called is True
    assert stub_stop.compute_called is True
    assert stub_stop.build_called is True
