from __future__ import annotations

from collections import deque

from engines.bot_runtime.core.execution import FillRejection
from engines.bot_runtime.core.execution_runtime import DeterministicExecutionModel
from engines.bot_runtime.core.fees import FeeSchedule, FeeResolver
from dataclasses import replace

from engines.bot_runtime.runtime.components.signal_consumption import consume_signals
from engines.bot_runtime.core.domain import StrategySignal
from tests.helpers.builders.runtime_scenario_builder import RuntimeScenarioBuilder
from tests.helpers.fakes.coinbase_runtime import FakeCoinbaseExchange, InMemoryRuntimeSink


def _execution_model() -> DeterministicExecutionModel:
    schedule = FeeSchedule(source="coinbase", version="test", maker_rate=0.001, taker_rate=0.002)
    return DeterministicExecutionModel(FeeResolver(schedule=schedule))


def test_coinbase_runtime_happy_path_entry_exit():
    engine = RuntimeScenarioBuilder.spot_engine()
    candle = RuntimeScenarioBuilder.candle(close=100.0)

    position = engine.entry_execution.submit_entry(candle, "long")
    assert position is not None

    exit_candle = RuntimeScenarioBuilder.candle(close=120.0)
    exit_candle.high = 140.0
    events = position.apply_bar(exit_candle)
    assert any(event["type"] == "close" for event in events)


def test_coinbase_runtime_partial_fill_progression():
    engine = RuntimeScenarioBuilder.spot_engine(base_risk_per_trade=8)
    candle = RuntimeScenarioBuilder.candle(close=100.0)
    request = engine.build_entry_request(candle, "long")
    pending = RuntimeScenarioBuilder.pending_for(request)

    first = RuntimeScenarioBuilder.fill_for(request, candle, qty=1.0, price=100.0, fill_time="t1")
    second = RuntimeScenarioBuilder.fill_for(request, candle, qty=1.0, price=101.0, fill_time="t2")

    partial = engine.apply_entry_fill(request=request, pending=pending, fill=first)
    opened = engine.apply_entry_fill(request=request, pending=partial.pending, fill=second)

    assert partial.status == "pending"
    assert opened.status == "opened"


def test_coinbase_runtime_rejects_invalid_qty():
    model = _execution_model()
    exchange = FakeCoinbaseExchange()
    sink = InMemoryRuntimeSink()
    intent = RuntimeScenarioBuilder.spot_engine().build_entry_request(RuntimeScenarioBuilder.candle(close=100.0), "long").intent
    intent = replace(intent, qty=0.0)

    outcome, rejection = model.evaluate(intent, candle_high=101, candle_low=99, candle_close=100, candle_open=100)
    if isinstance(rejection, FillRejection):
        exchange.reject(rejection.reason)
        sink.append({"event": "entry_rejected", "reason": rejection.reason})

    assert outcome.status == "rejected"
    assert exchange.rejections[0]["reason"] == "QTY_ROUNDS_TO_ZERO"


def test_coinbase_runtime_cancels_stale_limit_order():
    limit = {"anchor_price": "signal_price", "offset_type": "ticks", "offset_value": 5, "validity_window": 1, "fallback": "cancel"}
    engine = RuntimeScenarioBuilder.spot_engine(execution_mode="limit_maker", limit_maker=limit)
    candle = RuntimeScenarioBuilder.candle(close=100.0)
    exchange = FakeCoinbaseExchange()

    engine.entry_execution.submit_entry(candle, "long")
    result = engine.entry_execution.process_pending(candle)
    if result is None and engine.last_rejection_reason == "ENTRY_UNFILLED":
        exchange.cancel("stale-order")

    assert result is None
    assert engine.last_rejection_reason == "ENTRY_UNFILLED"
    assert exchange.cancellations == ["stale-order"]


def test_coinbase_runtime_stop_loss_on_adverse_move():
    engine = RuntimeScenarioBuilder.spot_engine()
    candle = RuntimeScenarioBuilder.candle(close=100.0)
    position = engine.entry_execution.submit_entry(candle, "long")

    stop_price = float(position.stop_price)
    adverse = RuntimeScenarioBuilder.candle(close=stop_price - 2.0)
    adverse.low = stop_price - 5.0
    events = position.apply_bar(adverse)

    assert any(event["type"] in {"stop", "close"} for event in events)


def test_coinbase_runtime_duplicate_signal_idempotency():
    signals = deque([
        StrategySignal(epoch=10, direction="long"),
        StrategySignal(epoch=10, direction="long"),
    ])

    first_consumed, chosen, last = consume_signals(signals, epoch=10, last_consumed_epoch=0)
    second_consumed, _, _ = consume_signals(signals, epoch=10, last_consumed_epoch=last)

    assert chosen == "long"
    assert len(first_consumed) == 2
    assert second_consumed == []
