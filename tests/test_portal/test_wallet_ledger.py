import pytest

from engines.bot_runtime.core.wallet import (
    WalletLedger,
    canonical_wallet_ledger_events,
    project_wallet,
    validate_wallet_ledger_state,
    wallet_can_apply,
)
from scripts.reporting import check_wallet_determinism, replay_wallet


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
    assert abs(state.balances["ETH"] - 0.5) <= 1e-9
    assert abs(state.balances["USDC"] + 1.0) <= 1e-9

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
    assert abs(state.balances["ETH"] - 0.3) <= 1e-9
    assert abs(state.balances["USDC"] - 418.5) <= 1e-9


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


def test_wallet_reporting_scripts_order_by_wallet_commit_seq_before_run_seq():
    first_wallet_event = {
        "event_id": "wallet-1",
        "run_seq": 20,
        "payload": {
            "event_id": "wallet-1",
            "event_name": "MARGIN_RESERVED",
            "context": {"wallet_commit_seq": 1, "wallet_commit_seq_status": "runtime_assigned"},
        },
    }
    second_wallet_event = {
        "event_id": "wallet-2",
        "run_seq": 10,
        "payload": {
            "event_id": "wallet-2",
            "event_name": "MARGIN_RELEASED",
            "context": {"wallet_commit_seq": 2, "wallet_commit_seq_status": "runtime_assigned"},
        },
    }

    assert sorted([second_wallet_event, first_wallet_event], key=replay_wallet._ordering_key) == [
        first_wallet_event,
        second_wallet_event,
    ]
    assert sorted([second_wallet_event, first_wallet_event], key=check_wallet_determinism._ordering_key) == [
        first_wallet_event,
        second_wallet_event,
    ]


def test_wallet_replay_projects_canonical_ledger_events_without_float_drift():
    events = [
        {
            "event_id": "wallet-init",
            "event_name": "WALLET_INITIALIZED",
            "context": {"balances": {"USD": 1000.0}},
        },
        {
            "event_id": "margin-reserved",
            "event_name": "MARGIN_RESERVED",
            "context": {
                "trade_id": "trade-1",
                "currency": "USD",
                "qty": 2.0,
                "margin_required": 100.0,
            },
        },
        {
            "event_id": "entry-fee",
            "event_name": "FEE_APPLIED",
            "context": {
                "currency": "USD",
                "balance_before": 1000.0,
                "balance_after": 999.0,
                "fee": 1.0,
            },
        },
        {
            "event_id": "margin-released",
            "event_name": "MARGIN_RELEASED",
            "context": {
                "trade_id": "trade-1",
                "currency": "USD",
                "qty": 1.0,
                "margin_required": 50.0,
            },
        },
        {
            "event_id": "exit-fee",
            "event_name": "FEE_APPLIED",
            "context": {
                "currency": "USD",
                "balance_before": 999.0,
                "balance_after": 998.5,
                "fee": 0.5,
            },
        },
        {
            "event_id": "exit-pnl",
            "event_name": "REALIZED_PNL_APPLIED",
            "context": {
                "currency": "USD",
                "balance_before": 998.5,
                "balance_after": 1008.5,
            },
        },
    ]

    state = project_wallet(events)

    assert state.balances["USD"] == pytest.approx(1008.5)
    assert state.locked_margin["USD"] == pytest.approx(50.0)
    assert state.free_collateral["USD"] == pytest.approx(958.5)
    assert state.margin_positions["trade-1"]["open_qty"] == pytest.approx(1.0)


def test_wallet_replay_initializes_from_persisted_wallet_after_shape():
    state = project_wallet(
        [
            {
                "event_id": "wallet-init",
                "event_name": "WALLET_INITIALIZED",
                "context": {
                    "currency": "USD",
                    "balance_before": 0.0,
                    "balance_after": 10000.0,
                    "wallet_after": {"balances": {"USD": 10000.0}},
                },
            },
            {
                "event_id": "fee-1",
                "event_name": "FEE_APPLIED",
                "context": {
                    "currency": "USD",
                    "balance_before": 10000.0,
                    "balance_after": 9999.25,
                    "fee": 0.75,
                },
            },
        ]
    )

    assert state.balances["USD"] == pytest.approx(9999.25)
    assert state.free_collateral["USD"] == pytest.approx(9999.25)


def test_wallet_ledger_state_validation_passes_complete_absolute_events():
    events = [
        {
            "event_id": "wallet-init",
            "event_name": "WALLET_INITIALIZED",
            "context": {
                "currency": "USD",
                "wallet_commit_seq": 0,
                "wallet_event_order": 0,
                "balance_before": 0.0,
                "balance_after": 1000.0,
                "wallet_after": {
                    "balances": {"USD": 1000.0},
                    "locked_margin": {},
                    "free_collateral": {"USD": 1000.0},
                    "margin_positions": {},
                },
            },
        },
        {
            "event_id": "margin-reserved",
            "event_name": "MARGIN_RESERVED",
            "context": {
                "trade_id": "trade-1",
                "currency": "USD",
                "wallet_commit_seq": 1,
                "wallet_event_order": 10,
                "qty": 2.0,
                "margin_required": 100.0,
                "balance_before": 1000.0,
                "balance_after": 1000.0,
                "wallet_before": {
                    "balances": {"USD": 1000.0},
                    "locked_margin": {},
                    "free_collateral": {"USD": 1000.0},
                    "margin_positions": {},
                },
                "wallet_after": {
                    "balances": {"USD": 1000.0},
                    "locked_margin": {"USD": 100.0},
                    "free_collateral": {"USD": 900.0},
                    "margin_positions": {
                        "trade-1": {"currency": "USD", "open_qty": 2.0, "locked_margin": 100.0}
                    },
                },
            },
        },
    ]

    validate_wallet_ledger_state(events)


def test_wallet_ledger_state_validation_rejects_missing_wallet_commit_seq():
    events = [
        {
            "event_id": "wallet-init",
            "event_name": "WALLET_INITIALIZED",
            "context": {
                "currency": "USD",
                "balance_after": 1000.0,
                "wallet_after": {"balances": {"USD": 1000.0}},
            },
        },
    ]

    with pytest.raises(ValueError, match="missing_wallet_commit_seq"):
        validate_wallet_ledger_state(events)


def test_wallet_ledger_state_validation_rejects_placeholder_absolute_state():
    events = [
        {
            "event_id": "wallet-init",
            "event_name": "WALLET_INITIALIZED",
            "context": {
                "currency": "USD",
                "wallet_commit_seq": 0,
                "wallet_event_order": 0,
                "balance_after": 1000.0,
                "wallet_after": {"balances": {"USD": 1000.0}, "free_collateral": {"USD": 1000.0}},
            },
        },
        {
            "event_id": "fee-1",
            "event_name": "FEE_APPLIED",
            "context": {
                "currency": "USD",
                "wallet_commit_seq": 1,
                "wallet_event_order": 20,
                "balance_before": 0.0,
                "balance_after": 0.0,
                "fee": 1.0,
                "wallet_before": {"balances": {"USD": 1000.0}, "free_collateral": {"USD": 1000.0}},
                "wallet_after": {"balances": {"USD": 0.0}, "free_collateral": {"USD": 0.0}},
            },
        },
    ]

    with pytest.raises(ValueError, match="wallet_ledger_state_invalid"):
        validate_wallet_ledger_state(events)


def test_wallet_ledger_validation_uses_wallet_commit_seq_for_causal_order():
    events = [
        {
            "event_id": "botlens:wallet:000000000000:init:00:wallet_initialized",
            "event_name": "WALLET_INITIALIZED",
            "context": {
                "currency": "USD",
                "source_run_seq": 0,
                "wallet_commit_seq": 0,
                "wallet_event_order": 0,
                "balance_before": 0.0,
                "balance_after": 1000.0,
                "wallet_after": {
                    "balances": {"USD": 1000.0},
                    "locked_margin": {},
                    "free_collateral": {"USD": 1000.0},
                    "margin_positions": {},
                },
            },
        },
        {
            "event_id": "botlens:wallet:000000000003:entry-fee:20:fee_applied",
            "event_name": "FEE_APPLIED",
            "context": {
                "currency": "USD",
                "source_run_seq": 3,
                "wallet_commit_seq": 1,
                "wallet_event_order": 20,
                "balance_before": 1000.0,
                "balance_after": 999.0,
                "fee": 1.0,
                "wallet_before": {
                    "balances": {"USD": 1000.0},
                    "locked_margin": {"USD": 100.0},
                    "free_collateral": {"USD": 900.0},
                    "margin_positions": {
                        "trade-1": {"currency": "USD", "open_qty": 1.0, "locked_margin": 100.0}
                    },
                },
                "wallet_after": {
                    "balances": {"USD": 999.0},
                    "locked_margin": {"USD": 100.0},
                    "free_collateral": {"USD": 899.0},
                    "margin_positions": {
                        "trade-1": {"currency": "USD", "open_qty": 1.0, "locked_margin": 100.0}
                    },
                },
            },
        },
        {
            "event_id": "botlens:wallet:000000000002:margin-reserved:10:margin_reserved",
            "event_name": "MARGIN_RESERVED",
            "context": {
                "trade_id": "trade-1",
                "currency": "USD",
                "source_run_seq": 2,
                "wallet_commit_seq": 1,
                "wallet_event_order": 10,
                "qty": 1.0,
                "margin_required": 100.0,
                "balance_before": 1000.0,
                "balance_after": 1000.0,
                "wallet_before": {
                    "balances": {"USD": 1000.0},
                    "locked_margin": {},
                    "free_collateral": {"USD": 1000.0},
                    "margin_positions": {},
                },
                "wallet_after": {
                    "balances": {"USD": 1000.0},
                    "locked_margin": {"USD": 100.0},
                    "free_collateral": {"USD": 900.0},
                    "margin_positions": {
                        "trade-1": {"currency": "USD", "open_qty": 1.0, "locked_margin": 100.0}
                    },
                },
            },
        },
        {
            "event_id": "botlens:wallet:000000000004:margin-released:10:margin_released",
            "event_name": "MARGIN_RELEASED",
            "context": {
                "trade_id": "trade-1",
                "currency": "USD",
                "source_run_seq": 4,
                "wallet_commit_seq": 2,
                "wallet_event_order": 10,
                "qty": 1.0,
                "margin_required": 100.0,
                "margin_released": 100.0,
                "balance_before": 999.0,
                "balance_after": 999.0,
                "wallet_before": {
                    "balances": {"USD": 999.0},
                    "locked_margin": {"USD": 100.0},
                    "free_collateral": {"USD": 899.0},
                    "margin_positions": {
                        "trade-1": {"currency": "USD", "open_qty": 1.0, "locked_margin": 100.0}
                    },
                },
                "wallet_after": {
                    "balances": {"USD": 999.0},
                    "locked_margin": {},
                    "free_collateral": {"USD": 999.0},
                    "margin_positions": {},
                },
            },
        },
    ]

    validate_wallet_ledger_state(events)
    state = project_wallet(canonical_wallet_ledger_events(events))

    assert state.balances["USD"] == pytest.approx(999.0)
    assert state.locked_margin == {}


def test_wallet_replay_rejects_malformed_initialization():
    with pytest.raises(ValueError, match="WALLET_INITIALIZED missing balances"):
        project_wallet(
            [
                {
                    "event_id": "wallet-init",
                    "event_name": "WALLET_INITIALIZED",
                    "context": {"currency": "USD"},
                }
            ]
        )


def test_wallet_replay_rejects_duplicate_initialization_after_activity():
    with pytest.raises(ValueError, match="duplicate WALLET_INITIALIZED after wallet activity"):
        project_wallet(
            [
                {
                    "event_id": "wallet-init-1",
                    "event_name": "WALLET_INITIALIZED",
                    "context": {"balances": {"USD": 10000.0}},
                },
                {
                    "event_id": "fee-1",
                    "event_name": "FEE_APPLIED",
                    "context": {
                        "currency": "USD",
                        "balance_before": 10000.0,
                        "balance_after": 9999.0,
                    },
                },
                {
                    "event_id": "wallet-init-2",
                    "event_name": "WALLET_INITIALIZED",
                    "context": {"balances": {"USD": 9999.0}},
                },
            ]
        )
