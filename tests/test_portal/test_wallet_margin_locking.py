from datetime import datetime, timezone
import threading

import pytest

from engines.bot_runtime.core.execution_profile import compile_series_execution_profile
from engines.bot_runtime.core.exit_settlement import ExitSettlementContext, ExitSettlementService
from engines.bot_runtime.core.runtime_events import (
    EntryFilledContext,
    ExitFilledContext,
    ExitKind,
    ReasonCode,
    RuntimeEventName,
    WalletDelta,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
)
from engines.bot_runtime.core.wallet import (
    WalletLedger,
    WalletState,
    project_wallet_from_events,
    wallet_can_apply,
)
from engines.bot_runtime.core.wallet_gateway import BaseWalletGateway, SharedWalletGateway
from engines.bot_runtime.runtime.components.settlement import SettlementApplier


FUTURE_INSTRUMENT = {
    "symbol": "BTC-FUT",
    "instrument_type": "future",
    "tick_size": 1.0,
    "tick_value": 1.0,
    "contract_size": 1.0,
    "base_currency": "BTC",
    "quote_currency": "USD",
    "can_short": True,
    "short_requires_borrow": False,
    "margin_rates": {
        "intraday": {"long_margin_rate": 0.5, "short_margin_rate": 0.5},
        "overnight": {"long_margin_rate": 0.5, "short_margin_rate": 0.5},
    },
}


def test_margin_positions_lock_and_release_collateral_through_trade_lifecycle():
    ledger = WalletLedger()
    ledger.deposit({"USD": 1000})

    ledger.trade_fill(
        event_type="ENTRY_FILL",
        side="buy",
        base_currency="BTC",
        quote_currency="USD",
        qty=2.0,
        price=100.0,
        fee=1.0,
        notional=200.0,
        trade_id="trade-1",
        accounting_mode="margin",
        realized_pnl=0.0,
        margin_locked=100.0,
    )
    state = ledger.project()
    assert abs(state.balances["USD"] - 999.0) <= 1e-9
    assert abs(state.locked_margin["USD"] - 100.0) <= 1e-9
    assert abs(state.free_collateral["USD"] - 899.0) <= 1e-9
    assert abs(state.margin_positions["trade-1"]["open_qty"] - 2.0) <= 1e-9

    ledger.trade_fill(
        event_type="EXIT_FILL",
        side="sell",
        base_currency="BTC",
        quote_currency="USD",
        qty=1.0,
        price=110.0,
        fee=1.0,
        notional=110.0,
        trade_id="trade-1",
        accounting_mode="margin",
        realized_pnl=10.0,
    )
    state = ledger.project()
    assert abs(state.balances["USD"] - 1008.0) <= 1e-9
    assert abs(state.locked_margin["USD"] - 50.0) <= 1e-9
    assert abs(state.free_collateral["USD"] - 958.0) <= 1e-9
    assert abs(state.margin_positions["trade-1"]["open_qty"] - 1.0) <= 1e-9

    ledger.trade_fill(
        event_type="EXIT_FILL",
        side="sell",
        base_currency="BTC",
        quote_currency="USD",
        qty=1.0,
        price=96.0,
        fee=1.0,
        notional=96.0,
        trade_id="trade-1",
        accounting_mode="margin",
        realized_pnl=-4.0,
    )
    state = ledger.project()
    assert abs(state.balances["USD"] - 1003.0) <= 1e-9
    assert abs(state.locked_margin.get("USD", 0.0) - 0.0) <= 1e-9
    assert abs(state.free_collateral["USD"] - 1003.0) <= 1e-9
    assert "trade-1" not in state.margin_positions


def test_wallet_can_apply_uses_free_collateral_not_raw_balance_for_margin_checks():
    constrained_state = WalletState(
        balances={"USD": 1000.0},
        locked_margin={"USD": 700.0},
        free_collateral={"USD": 300.0},
    )

    allowed, reason, _ = wallet_can_apply(
        state=constrained_state,
        side="buy",
        base_currency="BTC",
        quote_currency="USD",
        qty=1.0,
        notional=800.0,
        fee=0.0,
        short_requires_borrow=False,
        instrument=FUTURE_INSTRUMENT,
    )

    assert allowed is False
    assert reason == "WALLET_INSUFFICIENT_MARGIN"

    relaxed_state = WalletState(
        balances={"USD": 1000.0},
        locked_margin={"USD": 300.0},
        free_collateral={"USD": 700.0},
    )

    allowed, reason, _ = wallet_can_apply(
        state=relaxed_state,
        side="buy",
        base_currency="BTC",
        quote_currency="USD",
        qty=1.0,
        notional=800.0,
        fee=0.0,
        short_requires_borrow=False,
        instrument=FUTURE_INSTRUMENT,
    )

    assert allowed is True
    assert reason is None


def test_wallet_validation_is_repeatable_for_identical_input():
    state = WalletState(
        balances={"USD": 1000.0},
        locked_margin={"USD": 100.0},
        free_collateral={"USD": 900.0},
    )
    kwargs = {
        "state": state,
        "side": "buy",
        "base_currency": "BTC",
        "quote_currency": "USD",
        "qty": 1.0,
        "notional": 200.0,
        "fee": 1.0,
        "short_requires_borrow": False,
        "instrument": FUTURE_INSTRUMENT,
    }

    first = wallet_can_apply(**kwargs)
    second = wallet_can_apply(**kwargs)

    assert first == second
    assert state.free_collateral["USD"] == 900.0


def _wallet_initialized_event():
    return new_runtime_event(
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(
            run_id="run-wallet-test",
            symbol=None,
            timeframe=None,
            bar_ts=None,
        ),
        context=WalletInitializedContext(
            run_id="run-wallet-test",
            bot_id="bot-wallet-test",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            balances={"USD": 1000.0},
            source="test",
        ),
    )


def _entry_filled_event(*, qty: float = 2.0):
    bar_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return new_runtime_event(
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=EntryFilledContext(
            run_id="run-wallet-test",
            bot_id="bot-wallet-test",
            strategy_id="strategy-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="buy",
            direction="long",
            qty=qty,
            price=100.0,
            notional=100.0 * qty,
            fee_paid=1.0,
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            wallet_delta=WalletDelta(
                collateral_reserved=100.0,
                collateral_released=0.0,
                fee_paid=1.0,
                balance_delta=-1.0,
            ),
            reason_code=None,
        ),
        allow_missing_parent=True,
        event_ts=bar_ts,
    )


def test_wallet_projection_quantizes_tiny_negative_locked_margin_from_float_release():
    init = _wallet_initialized_event()
    entry = _entry_filled_event(qty=2.0)
    bar_ts = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    exit_event = new_runtime_event(
        event_name=RuntimeEventName.EXIT_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=ExitFilledContext(
            run_id="run-wallet-test",
            bot_id="bot-wallet-test",
            strategy_id="strategy-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="sell",
            direction="long",
            qty=2.0,
            price=100.0,
            notional=200.0,
            fee_paid=0.0,
            realized_pnl=0.0,
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            exit_kind=ExitKind.CLOSE,
            wallet_delta=WalletDelta(
                collateral_reserved=0.0,
                collateral_released=100.00000000000182,
                fee_paid=0.0,
                balance_delta=0.0,
            ),
            reason_code=None,
        ),
        allow_missing_parent=True,
        event_ts=bar_ts,
    )

    state = project_wallet_from_events([init, entry, exit_event])

    assert state.locked_margin.get("USD", 0.0) == pytest.approx(0.0)
    assert state.free_collateral["USD"] == pytest.approx(state.balances["USD"])
    assert "trade-1" not in state.margin_positions


def test_shared_gateway_exit_metadata_releases_locked_margin_once():
    init = _wallet_initialized_event()
    entry = _entry_filled_event()
    proxy = {
        "runtime_events": [init.serialize(), entry.serialize()],
        "reservations": {},
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)

    metadata = gateway.apply_fill(
        event_type="EXIT_FILL",
        side="sell",
        base_currency="BTC",
        quote_currency="USD",
        qty=1.0,
        price=110.0,
        fee=0.5,
        notional=110.0,
        trade_id="trade-1",
        leg_id="tp-1",
        position_direction="long",
        accounting_mode="margin",
        realized_pnl=10.0,
        correlation_id="trade:trade-1",
        exit_kind="TARGET",
    )

    wallet_delta = metadata["wallet_delta"]
    assert wallet_delta["collateral_reserved"] == 0.0
    assert wallet_delta["collateral_released"] == pytest.approx(50.0)
    assert wallet_delta["fee_paid"] == pytest.approx(0.5)
    assert wallet_delta["balance_delta"] == pytest.approx(9.5)

    bar_ts = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    exit_event = new_runtime_event(
        event_name=RuntimeEventName.EXIT_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=ExitFilledContext(
            run_id="run-wallet-test",
            bot_id="bot-wallet-test",
            strategy_id="strategy-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="sell",
            direction="long",
            qty=1.0,
            price=110.0,
            notional=110.0,
            fee_paid=0.5,
            realized_pnl=10.0,
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            exit_kind=ExitKind.TARGET,
            wallet_delta=WalletDelta(**wallet_delta),
            reason_code=None,
        ),
        allow_missing_parent=True,
        event_ts=bar_ts,
    )
    state = project_wallet_from_events([init, entry, exit_event])

    assert state.balances["USD"] == pytest.approx(1008.5)
    assert state.locked_margin["USD"] == pytest.approx(50.0)
    assert state.free_collateral["USD"] == pytest.approx(958.5)
    assert state.margin_positions["trade-1"]["open_qty"] == pytest.approx(1.0)


def test_shared_gateway_commits_fill_state_before_runtime_event_append():
    init = _wallet_initialized_event().serialize()
    init["seq"] = 0
    entry = _entry_filled_event(qty=2.0).serialize()
    entry["seq"] = 1
    initial_state = project_wallet_from_events([init, entry])
    proxy = {
        "runtime_events": [],
        "wallet_events": [init, entry],
        "wallet_state": BaseWalletGateway._wallet_state_snapshot(initial_state),
        "reservations": {},
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)

    first = gateway.apply_fill(
        event_type="EXIT_FILL",
        side="sell",
        base_currency="BTC",
        quote_currency="USD",
        qty=1.0,
        price=110.0,
        fee=0.5,
        notional=110.0,
        trade_id="trade-1",
        leg_id="tp-1",
        position_direction="long",
        accounting_mode="margin",
        realized_pnl=10.0,
        correlation_id="trade:trade-1",
        exit_kind="TARGET",
    )
    second = gateway.apply_fill(
        event_type="EXIT_FILL",
        side="sell",
        base_currency="BTC",
        quote_currency="USD",
        qty=1.0,
        price=110.0,
        fee=0.5,
        notional=110.0,
        trade_id="trade-1",
        leg_id="tp-2",
        position_direction="long",
        accounting_mode="margin",
        realized_pnl=10.0,
        correlation_id="trade:trade-1",
        exit_kind="TARGET",
    )

    assert first["wallet_before"]["balances"]["USD"] == pytest.approx(999.0)
    assert first["wallet_before"]["locked_margin"]["USD"] == pytest.approx(100.0)
    assert second["wallet_before"]["balances"]["USD"] == pytest.approx(1008.5)
    assert second["wallet_before"]["locked_margin"]["USD"] == pytest.approx(50.0)
    assert first["wallet_after"]["balances"]["USD"] == pytest.approx(1008.5)
    assert second["wallet_after"]["balances"]["USD"] == pytest.approx(1018.0)
    assert len(proxy["runtime_events"]) == 0

    state = gateway.project()
    assert state.balances["USD"] == pytest.approx(1018.0)
    assert state.locked_margin.get("USD", 0.0) == pytest.approx(0.0)
    assert "trade-1" not in state.margin_positions


def test_shared_gateway_projects_runtime_events_by_seq_not_append_order():
    init = _wallet_initialized_event().serialize()
    init["seq"] = 0
    entry = _entry_filled_event().serialize()
    entry["seq"] = 2
    bar_ts = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    exit_event = new_runtime_event(
        event_name=RuntimeEventName.EXIT_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=ExitFilledContext(
            run_id="run-wallet-test",
            bot_id="bot-wallet-test",
            strategy_id="strategy-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="sell",
            direction="long",
            qty=1.0,
            price=110.0,
            notional=110.0,
            fee_paid=0.5,
            realized_pnl=10.0,
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            exit_kind=ExitKind.TARGET,
            wallet_delta=WalletDelta(
                collateral_reserved=0.0,
                collateral_released=50.0,
                fee_paid=0.5,
                balance_delta=9.5,
            ),
            reason_code=None,
        ),
        allow_missing_parent=True,
        event_ts=bar_ts,
    ).serialize()
    exit_event["seq"] = 3
    gateway = SharedWalletGateway(
        {
            "runtime_events": [init, exit_event, entry],
            "reservations": {},
            "lock": threading.RLock(),
        }
    )

    state = gateway.project()

    assert state.balances["USD"] == pytest.approx(1008.5)
    assert state.locked_margin["USD"] == pytest.approx(50.0)
    assert state.free_collateral["USD"] == pytest.approx(958.5)


def test_backtest_terminal_settlement_releases_all_margin():
    init = _wallet_initialized_event()
    entry = _entry_filled_event()
    proxy = {
        "runtime_events": [init.serialize(), entry.serialize()],
        "reservations": {},
        "lock": threading.RLock(),
    }
    gateway = SharedWalletGateway(proxy)
    event = {
        "type": "backtest_end",
        "reason_code": "BACKTEST_END",
        "settlement": {
            "event_type": "EXIT_FILL",
            "exit_kind": "CLOSE",
            "side": "sell",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "qty": 2.0,
            "price": 105.0,
            "fee": 0.5,
            "notional": 210.0,
            "trade_id": "trade-1",
            "leg_id": "terminal",
            "position_direction": "long",
            "accounting_mode": "margin",
            "realized_pnl": 10.0,
            "allow_short_borrow": False,
            "instrument": FUTURE_INSTRUMENT,
        },
    }

    SettlementApplier(obs_enabled=False).apply([event], ExitSettlementService(gateway))

    wallet_delta = event["wallet_fill_metadata"]["wallet_delta"]
    assert wallet_delta["collateral_released"] == pytest.approx(100.0)
    assert wallet_delta["fee_paid"] == pytest.approx(0.5)

    bar_ts = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    exit_event = new_runtime_event(
        event_name=RuntimeEventName.EXIT_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=ExitFilledContext(
            run_id="run-wallet-test",
            bot_id="bot-wallet-test",
            strategy_id="strategy-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="sell",
            direction="long",
            qty=2.0,
            price=105.0,
            notional=210.0,
            fee_paid=0.5,
            realized_pnl=10.0,
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            exit_kind=ExitKind.CLOSE,
            wallet_delta=WalletDelta(**wallet_delta),
            event_subtype="backtest_end",
            reason_code=ReasonCode.BACKTEST_END,
        ),
        allow_missing_parent=True,
        event_ts=bar_ts,
    )
    state = project_wallet_from_events([init, entry, exit_event])

    assert state.locked_margin.get("USD", 0.0) == pytest.approx(0.0)
    assert state.free_collateral["USD"] == pytest.approx(state.balances["USD"])
    assert "trade-1" not in state.margin_positions


def test_exit_settlement_validation_uses_execution_profile_when_instrument_payload_is_sparse():
    profile = compile_series_execution_profile(
        FUTURE_INSTRUMENT,
        risk_config={"base_risk_per_trade": 100.0},
    )
    gateway = SharedWalletGateway(
        {
            "runtime_events": [_wallet_initialized_event().serialize()],
            "reservations": {},
            "lock": threading.RLock(),
        }
    )
    settlement = ExitSettlementService(gateway)

    applied, metadata = settlement.apply_exit_fill(
        ExitSettlementContext(
            event_type="EXIT_FILL",
            exit_kind="TARGET",
            side="sell",
            base_currency="BTC",
            quote_currency="USD",
            qty=1.0,
            price=100.0,
            fee=0.0,
            notional=100.0,
            trade_id="trade-1",
            leg_id="tp-1",
            position_direction="long",
            accounting_mode="margin",
            realized_pnl=5.0,
            allow_short_borrow=False,
            instrument={"symbol": "BTC-FUT", "instrument_type": "future"},
            execution_profile=profile,
        ),
        force=False,
    )

    assert applied is True
    assert metadata["wallet_delta"]["balance_delta"] == pytest.approx(5.0)


def test_margin_exit_settlement_validates_open_position_not_free_collateral():
    init = new_runtime_event(
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(
            run_id="run-wallet-test",
            symbol=None,
            timeframe=None,
            bar_ts=None,
        ),
        context=WalletInitializedContext(
            run_id="run-wallet-test",
            bot_id="bot-wallet-test",
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            balances={"USD": 100.0},
            source="test",
        ),
    )
    bar_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    entry = new_runtime_event(
        event_name=RuntimeEventName.ENTRY_FILLED,
        correlation_id=build_correlation_id(
            run_id="run-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
        ),
        context=EntryFilledContext(
            run_id="run-wallet-test",
            bot_id="bot-wallet-test",
            strategy_id="strategy-wallet-test",
            symbol="BTC-FUT",
            timeframe="1m",
            bar_ts=bar_ts,
            trade_id="trade-1",
            wallet_correlation_id="trade:trade-1",
            side="buy",
            direction="long",
            qty=1.0,
            price=100.0,
            notional=100.0,
            fee_paid=0.0,
            base_currency="BTC",
            quote_currency="USD",
            accounting_mode="margin",
            wallet_delta=WalletDelta(
                collateral_reserved=100.0,
                collateral_released=0.0,
                fee_paid=0.0,
                balance_delta=0.0,
            ),
            reason_code=None,
        ),
        allow_missing_parent=True,
        event_ts=bar_ts,
    )
    gateway = SharedWalletGateway(
        {
            "runtime_events": [init.serialize(), entry.serialize()],
            "reservations": {},
            "lock": threading.RLock(),
        }
    )
    settlement = ExitSettlementService(gateway)

    applied, metadata = settlement.apply_exit_fill(
        ExitSettlementContext(
            event_type="EXIT_FILL",
            exit_kind="TARGET",
            side="sell",
            base_currency="BTC",
            quote_currency="USD",
            qty=1.0,
            price=110.0,
            fee=1.0,
            notional=110.0,
            trade_id="trade-1",
            leg_id="tp-1",
            position_direction="long",
            accounting_mode="margin",
            realized_pnl=10.0,
            allow_short_borrow=False,
            instrument=FUTURE_INSTRUMENT,
        ),
        force=False,
    )

    assert applied is True
    assert metadata["wallet_delta"]["collateral_released"] == pytest.approx(100.0)
    assert metadata["wallet_delta"]["balance_delta"] == pytest.approx(9.0)


def test_settlement_applier_is_idempotent_for_already_settled_event():
    class CountingExitSettlement:
        def __init__(self):
            self.calls = 0

        def apply_exit_fill(self, context, *, force):
            self.calls += 1
            return True, {
                "event_name": "EXIT_FILLED",
                "wallet_delta": {
                    "collateral_reserved": 0.0,
                    "collateral_released": 50.0,
                    "fee_paid": 0.5,
                    "balance_delta": 9.5,
                },
            }

    event = {
        "settlement": {
            "event_type": "EXIT_FILL",
            "exit_kind": "TARGET",
            "side": "sell",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "qty": 1.0,
            "price": 110.0,
            "fee": 0.5,
            "notional": 110.0,
            "trade_id": "trade-1",
            "leg_id": "tp-1",
            "position_direction": "long",
            "accounting_mode": "margin",
            "realized_pnl": 10.0,
            "allow_short_borrow": False,
            "instrument": FUTURE_INSTRUMENT,
        }
    }
    settlement = CountingExitSettlement()
    applier = SettlementApplier(obs_enabled=False)

    applier.apply([event], settlement)
    applier.apply([event], settlement)

    assert settlement.calls == 1
    assert event["wallet_fill_metadata"]["wallet_delta"]["collateral_released"] == pytest.approx(50.0)
