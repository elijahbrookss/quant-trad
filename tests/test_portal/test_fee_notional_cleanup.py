from datetime import datetime, timezone
import threading

import pytest

from engines.bot_runtime.core.domain import Candle, LadderRiskEngine
from engines.bot_runtime.core.execution import DerivativesExecutionConstraints, DerivativesExecutionModel
from engines.bot_runtime.core.fees import executed_fee, executed_notional
from engines.bot_runtime.core.runtime_events import (
    EntryFilledContext,
    ExitFilledContext,
    ExitKind,
    RuntimeEventName,
    WalletDelta,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
)
from engines.bot_runtime.core.wallet import project_wallet_from_events
from engines.bot_runtime.core.wallet_gateway import SharedWalletGateway


def _future_instrument(*, contract_size: float = 2.0) -> dict:
    return {
        "symbol": "TEST-FUT",
        "instrument_type": "future",
        "tick_size": 1.0,
        "contract_size": contract_size,
        "tick_value": contract_size,
        "min_order_size": 1.0,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "can_short": True,
        "short_requires_borrow": False,
        "maker_fee_rate": 0.0005,
        "taker_fee_rate": 0.001,
        "margin_rates": {
            "intraday": {"long_margin_rate": 0.5, "short_margin_rate": 0.5},
            "overnight": {"long_margin_rate": 0.5, "short_margin_rate": 0.5},
        },
        "metadata": {"info": {"base_increment": "1"}},
    }


def _spot_instrument() -> dict:
    return {
        "symbol": "TEST-SPOT",
        "instrument_type": "spot",
        "tick_size": 1.0,
        "contract_size": 1.0,
        "tick_value": 1.0,
        "min_order_size": 1.0,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "maker_fee_rate": 0.0005,
        "taker_fee_rate": 0.001,
        "metadata": {"info": {"base_increment": "1"}},
    }


def _engine(instrument: dict, *, base_risk_per_trade: float = 10.0) -> LadderRiskEngine:
    return LadderRiskEngine(
        config={
            "tick_size": instrument["tick_size"],
            "contract_size": instrument["contract_size"],
            "tick_value": instrument["tick_value"],
            "taker_fee_rate": 0.001,
            "maker_fee_rate": 0.0005,
            "initial_stop": {"atr_multiplier": 1.0},
            "take_profit_orders": [{"id": "tp-1", "ticks": 10, "contracts": 1}],
            "execution_mode": "market",
        },
        instrument=instrument,
        risk_config={"base_risk_per_trade": base_risk_per_trade},
    )


def _candle(*, close: float, high: float | None = None, low: float | None = None) -> Candle:
    return Candle(
        time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        open=close,
        high=close + 1.0 if high is None else high,
        low=close - 1.0 if low is None else low,
        close=close,
        atr=1.0,
    )


class _RejectingWalletGateway:
    def __init__(self) -> None:
        self.can_apply_calls = 0
        self.reject_calls = []

    def can_apply(self, **kwargs):
        self.can_apply_calls += 1
        return False, "WALLET_INSUFFICIENT_MARGIN", dict(kwargs)

    def reject(self, reason, payload, trade_id=None, leg_id=None):
        self.reject_calls.append(
            {
                "reason": reason,
                "payload": dict(payload or {}),
                "trade_id": trade_id,
                "leg_id": leg_id,
            }
        )


def _wallet_initialized_event():
    return new_runtime_event(
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(
            run_id="run-fee-test",
            symbol=None,
            timeframe=None,
            bar_ts=None,
        ),
        context=WalletInitializedContext(
            run_id="run-fee-test",
            bot_id="bot-fee-test",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            balances={"USD": 1000.0},
            source="test",
        ),
    )


def test_executed_notional_uses_contract_size_for_futures_and_one_for_spot():
    assert executed_notional(price=100.0, quantity=3.0, contract_size=5.0) == pytest.approx(1500.0)
    assert executed_notional(price=100.0, quantity=3.0, contract_size=1.0) == pytest.approx(300.0)


def test_entry_fee_includes_contract_size_and_is_applied_once():
    engine = _engine(_future_instrument(contract_size=2.0), base_risk_per_trade=10.0)
    position = engine.entry_execution.submit_entry(_candle(close=100.0), "long")

    assert position is not None
    assert position.entry_order["contract_size"] == pytest.approx(2.0)
    assert position.entry_outcome["fee_paid"] == pytest.approx(1.0)
    assert position.fees_paid == pytest.approx(1.0)


def test_exit_fee_uses_same_notional_function_and_is_applied_once():
    engine = _engine(_future_instrument(contract_size=2.0), base_risk_per_trade=10.0)
    position = engine.entry_execution.submit_entry(_candle(close=100.0), "long")
    assert position is not None

    events = position.apply_bar(_candle(close=110.0, high=111.0, low=109.0))
    target = next(event for event in events if event["type"] == "target")

    assert target["notional"] == pytest.approx(executed_notional(price=110.0, quantity=5.0, contract_size=2.0))
    assert target["fee_paid"] == pytest.approx(executed_fee(price=110.0, quantity=5.0, contract_size=2.0, fee_rate=0.001))
    assert position.fees_paid == pytest.approx(2.1)


def test_round_trip_fees_are_symmetric_entry_plus_exit():
    engine = _engine(_future_instrument(contract_size=2.0), base_risk_per_trade=10.0)
    position = engine.entry_execution.submit_entry(_candle(close=100.0), "long")
    assert position is not None

    position.apply_bar(_candle(close=110.0, high=111.0, low=109.0))

    expected_entry = executed_fee(price=100.0, quantity=5.0, contract_size=2.0, fee_rate=0.001)
    expected_exit = executed_fee(price=110.0, quantity=5.0, contract_size=2.0, fee_rate=0.001)
    assert position.fees_paid == pytest.approx(expected_entry + expected_exit)
    assert position.net_pnl == pytest.approx(position.gross_pnl - position.fees_paid)


def test_backtest_terminal_close_uses_final_bar_price_and_applies_exit_fee_once():
    engine = _engine(_future_instrument(contract_size=2.0), base_risk_per_trade=10.0)
    position = engine.entry_execution.submit_entry(_candle(close=100.0), "long")
    assert position is not None
    entry_fee = position.fees_paid
    terminal_candle = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=104.0,
        high=106.0,
        low=104.0,
        close=105.0,
        atr=1.0,
    )

    events = position.force_close_at_backtest_end(terminal_candle)
    terminal_fill = next(event for event in events if event["type"] == "backtest_end")
    close_event = next(event for event in events if event["type"] == "close")
    expected_exit_fee = executed_fee(price=105.0, quantity=5.0, contract_size=2.0, fee_rate=0.001)

    assert terminal_fill["price"] == pytest.approx(105.0)
    assert terminal_fill["time"] == "2024-01-01T01:00:00Z"
    assert terminal_fill["reason_code"] == "BACKTEST_END"
    assert terminal_fill["settlement"]["exit_kind"] == "CLOSE"
    assert terminal_fill["fee_paid"] == pytest.approx(expected_exit_fee)
    assert close_event["close_reason"] == "BACKTEST_END"
    assert position.fees_paid == pytest.approx(entry_fee + expected_exit_fee)
    assert position.closed_at == terminal_candle.time
    assert not position.is_active()

    fees_after_first_settlement = position.fees_paid
    assert position.force_close_at_backtest_end(terminal_candle) == []
    assert position.fees_paid == pytest.approx(fees_after_first_settlement)


def test_backtest_terminal_close_does_not_run_entry_margin_precheck_for_exit():
    instrument = _future_instrument(contract_size=2.0)
    engine = _engine(instrument, base_risk_per_trade=10.0)
    position = engine.entry_execution.submit_entry(_candle(close=100.0), "long")
    assert position is not None
    wallet = _RejectingWalletGateway()
    position.execution_adapter = DerivativesExecutionModel(
        DerivativesExecutionConstraints(
            tick_size=1.0,
            qty_step=1.0,
            min_qty=1.0,
            min_notional=1.0,
            contract_size=2.0,
        )
    )
    position.wallet_gateway = wallet
    position.instrument = instrument

    events = position.force_close_at_backtest_end(_candle(close=105.0))

    assert [event["type"] for event in events] == ["backtest_end", "close"]
    assert wallet.can_apply_calls == 0
    assert wallet.reject_calls == []


def test_normal_target_exit_does_not_run_entry_margin_precheck_for_exit():
    instrument = _future_instrument(contract_size=2.0)
    engine = _engine(instrument, base_risk_per_trade=10.0)
    position = engine.entry_execution.submit_entry(_candle(close=100.0), "long")
    assert position is not None
    wallet = _RejectingWalletGateway()
    position.execution_adapter = DerivativesExecutionModel(
        DerivativesExecutionConstraints(
            tick_size=1.0,
            qty_step=1.0,
            min_qty=1.0,
            min_notional=1.0,
            contract_size=2.0,
        )
    )
    position.wallet_gateway = wallet
    position.instrument = instrument

    events = position.apply_bar(_candle(close=110.0, high=111.0, low=109.0))

    assert [event["type"] for event in events] == ["target", "close"]
    assert wallet.can_apply_calls == 0
    assert wallet.reject_calls == []


def test_risk_engine_terminal_close_clears_active_trade_and_reports_completed_trade():
    engine = _engine(_future_instrument(contract_size=2.0), base_risk_per_trade=10.0)
    position = engine.entry_execution.submit_entry(_candle(close=100.0), "long")
    assert position is not None
    engine.active_trade = position
    engine.trades.append(position)
    terminal_candle = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=104.0,
        high=106.0,
        low=104.0,
        close=105.0,
        atr=1.0,
    )

    events = engine.force_close_active_trade_at_backtest_end(terminal_candle)
    serialized = engine.serialise_trades()[0]

    assert [event["type"] for event in events] == ["backtest_end", "close"]
    assert engine.active_trade is None
    assert engine.stats()["completed_trades"] == 1
    assert serialized["closed_at"] == "2024-01-01T01:00:00Z"
    assert serialized["reason_code"] == "BACKTEST_END"


def test_fee_calculation_is_deterministic_for_same_inputs():
    instrument = _future_instrument(contract_size=2.0)
    constraints = DerivativesExecutionConstraints(
        tick_size=1.0,
        qty_step=1.0,
        min_qty=1.0,
        min_notional=1.0,
        contract_size=instrument["contract_size"],
    )
    model = DerivativesExecutionModel(constraints)
    kwargs = {
        "side": "sell",
        "requested_qty": 5.0,
        "price": 110.0,
        "fee_rate": 0.001,
    }

    first, first_rejection = model.fill_market(**kwargs)
    second, second_rejection = model.fill_market(**kwargs)

    assert first_rejection is None
    assert second_rejection is None
    assert first is not None and second is not None
    assert first.notional == second.notional == pytest.approx(1100.0)
    assert first.fee == second.fee == pytest.approx(1.1)


def test_wallet_projection_replay_does_not_double_apply_duplicate_fill_events():
    init = _wallet_initialized_event()
    bar_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    entry = new_runtime_event(
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-fee-test",
            symbol="TEST-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=EntryFilledContext(
            run_id="run-fee-test",
            bot_id="bot-fee-test",
            strategy_id="strategy-fee-test",
            symbol="TEST-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="buy",
            direction="long",
            qty=5.0,
            price=100.0,
            notional=1000.0,
            fee_paid=1.0,
            fee_rate=0.001,
            fee_type="taker",
            fee_source="instrument",
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            wallet_delta=WalletDelta(
                collateral_reserved=500.0,
                collateral_released=0.0,
                fee_paid=1.0,
                balance_delta=-1.0,
            ),
            reason_code=None,
        ),
        allow_missing_parent=True,
        event_ts=bar_ts,
    )
    exit_event = new_runtime_event(
        event_name=RuntimeEventName.EXIT_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-fee-test",
            symbol="TEST-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=ExitFilledContext(
            run_id="run-fee-test",
            bot_id="bot-fee-test",
            strategy_id="strategy-fee-test",
            symbol="TEST-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="sell",
            direction="long",
            qty=5.0,
            price=110.0,
            notional=1100.0,
            fee_paid=1.1,
            fee_rate=0.001,
            fee_type="taker",
            fee_source="instrument",
            realized_pnl=100.0,
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            exit_kind=ExitKind.TARGET,
            wallet_delta=WalletDelta(
                collateral_reserved=0.0,
                collateral_released=500.0,
                fee_paid=1.1,
                balance_delta=98.9,
            ),
            reason_code=None,
        ),
        allow_missing_parent=True,
        event_ts=bar_ts,
    )

    state = project_wallet_from_events([init, entry, entry, exit_event, exit_event])

    assert state.balances["USD"] == pytest.approx(1097.9)
    assert state.locked_margin.get("USD", 0.0) == pytest.approx(0.0)
    assert state.free_collateral["USD"] == pytest.approx(1097.9)


def test_margin_reservation_uses_corrected_fee_and_matches_wallet_hold():
    gateway = SharedWalletGateway(
        {
            "runtime_events": [_wallet_initialized_event().serialize()],
            "reservations": {},
            "lock": threading.RLock(),
        }
    )
    notional = executed_notional(price=100.0, quantity=5.0, contract_size=2.0)
    fee = executed_fee(price=100.0, quantity=5.0, contract_size=2.0, fee_rate=0.001)

    allowed, reason, payload = gateway.can_apply(
        side="buy",
        base_currency="BTC",
        quote_currency="USD",
        qty=5.0,
        qty_raw=5.0,
        qty_final=5.0,
        notional=notional,
        fee=fee,
        short_requires_borrow=False,
        instrument=_future_instrument(contract_size=2.0),
        reserve=True,
        correlation_id="trade:trade-1",
        trade_id="trade-1",
    )

    assert allowed is True
    assert reason is None
    assert payload["required_delta"]["fee_estimate"] == pytest.approx(fee)
    assert payload["required_delta"]["estimated_exit_fee"] == pytest.approx(fee)
    assert payload["required_delta"]["collateral_reserved"] == pytest.approx(525.1)
    assert payload["reserved_amount"] == pytest.approx(527.1)
