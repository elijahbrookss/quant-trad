from engines.bot_runtime.core.wallet import WalletLedger, WalletState, wallet_can_apply


FUTURE_INSTRUMENT = {
    "symbol": "BTC-FUT",
    "instrument_type": "future",
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
