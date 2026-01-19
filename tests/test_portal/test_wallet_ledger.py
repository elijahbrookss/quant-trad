import pytest

from engines.bot_runtime.core.wallet import WalletLedger, project_wallet, wallet_can_apply


def test_wallet_buy_and_sell_projection():
    ledger = WalletLedger()
    ledger.deposit({"USDC": 1000})

    ledger.trade_fill(
        side="buy",
        base_currency="ETH",
        quote_currency="USDC",
        qty=0.5,
        price=2000,
        fee=1.0,
        notional=1000,
    )
    state = project_wallet(ledger.events())
    assert state.balances["ETH"] == pytest.approx(0.5)
    assert state.balances["USDC"] == pytest.approx(-1.0)

    ledger.trade_fill(
        side="sell",
        base_currency="ETH",
        quote_currency="USDC",
        qty=0.2,
        price=2100,
        fee=0.5,
        notional=420,
    )
    state = project_wallet(ledger.events())
    assert state.balances["ETH"] == pytest.approx(0.3)
    assert state.balances["USDC"] == pytest.approx(418.5)


def test_wallet_can_apply_rejections():
    ledger = WalletLedger()
    ledger.deposit({"USDC": 50, "BTC": 0.1})
    state = ledger.project()

    allowed, reason, _ = wallet_can_apply(
        state=state,
        side="buy",
        base_currency="BTC",
        quote_currency="USDC",
        qty=0.01,
        notional=100,
        fee=0.1,
        short_requires_borrow=True,
    )
    assert not allowed
    assert reason == "WALLET_INSUFFICIENT_CASH"

    allowed, reason, _ = wallet_can_apply(
        state=state,
        side="sell",
        base_currency="BTC",
        quote_currency="USDC",
        qty=0.5,
        notional=100,
        fee=0.1,
        short_requires_borrow=True,
    )
    assert not allowed
    assert reason == "WALLET_INSUFFICIENT_QTY"
