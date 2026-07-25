from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from engines.bot_runtime.adapters import BacktestAdapter, PaperAdapter
from engines.bot_runtime.core.domain import Candle, LadderRiskEngine
from engines.bot_runtime.core.runtime_events import (
    RuntimeEventName,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
)
from engines.bot_runtime.core.wallet_gateway import SharedWalletGateway
from engines.bot_runtime.runtime.components.settlement import SettlementApplier


STARTING_CASH = 1_000.0
ENTRY_PRICE = 100.0
ATR = 2.0


def _wallet_proxy() -> dict[str, Any]:
    return {
        "runtime_events": [
            new_runtime_event(
                event_name=RuntimeEventName.WALLET_INITIALIZED,
                correlation_id=build_correlation_id(
                    run_id="reference-run",
                    symbol=None,
                    timeframe=None,
                    bar_ts=None,
                ),
                context=WalletInitializedContext(
                    run_id="reference-run",
                    bot_id="reference-bot",
                    strategy_id="reference-strategy",
                    symbol=None,
                    timeframe=None,
                    bar_ts=None,
                    balances={"USD": STARTING_CASH},
                    source="hand_verified_fixture",
                ),
            ).serialize()
        ],
        "reservations": {},
        "lock": threading.RLock(),
    }


def _adapter(adapter_type: type[BacktestAdapter] | type[PaperAdapter]):
    return adapter_type(
        tick_size=1.0,
        qty_step=1.0,
        min_qty=1.0,
        min_notional=0.0,
        contract_size=1.0,
        short_requires_borrow=False,
        slippage_bps=0.0,
    )


def _engine(adapter_type: type[BacktestAdapter] | type[PaperAdapter]) -> tuple[LadderRiskEngine, SharedWalletGateway]:
    engine = LadderRiskEngine(
        config={
            "initial_stop": {"atr_multiplier": 2.0},
            "take_profit_orders": [
                {"id": "tp-1", "ticks": 10, "size_fraction": 1.0}
            ],
            "execution_mode": "market",
        },
        instrument={
            "symbol": "REFERENCE-PERP",
            "venue": "fixture",
            "instrument_type": "future",
            "tick_size": 1.0,
            "contract_size": 1.0,
            "tick_value": 1.0,
            "min_order_size": 1.0,
            "qty_step": 1.0,
            "base_currency": "REFERENCE",
            "quote_currency": "USD",
            "can_short": True,
            "short_requires_borrow": False,
            "maker_fee_rate": 0.001,
            "taker_fee_rate": 0.002,
            "margin_rates": {
                "intraday": {
                    "long_margin_rate": 0.1,
                    "short_margin_rate": 0.1,
                },
                "overnight": {
                    "long_margin_rate": 0.2,
                    "short_margin_rate": 0.2,
                },
            },
            "metadata": {"info": {"base_increment": "1"}},
        },
        risk_config={"base_risk_per_trade": 4.0},
    )
    gateway = SharedWalletGateway(_wallet_proxy())
    engine.attach_wallet_gateway(gateway)
    engine.attach_execution_adapter(_adapter(adapter_type))
    engine.strategy_id = "reference-strategy"
    engine.last_signal_id = "reference-signal"
    engine.last_decision_id = "reference-decision"
    return engine, gateway


def _candles(direction: str, outcome: str) -> list[Candle]:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    entry = Candle(
        time=start,
        open=ENTRY_PRICE,
        high=101.0,
        low=99.0,
        close=ENTRY_PRICE,
        atr=ATR,
        volume=10.0,
    )
    if direction == "long" and outcome == "same_bar":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=100.0,
            high=110.0,
            low=96.0,
            close=100.0,
            atr=ATR,
            volume=10.0,
        )
    elif direction == "short" and outcome == "same_bar":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=100.0,
            high=104.0,
            low=90.0,
            close=100.0,
            atr=ATR,
            volume=10.0,
        )
    elif direction == "long" and outcome == "target":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=100.0,
            high=110.0,
            low=100.0,
            close=110.0,
            atr=ATR,
            volume=10.0,
        )
    elif direction == "long" and outcome == "gap_stop":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=94.0,
            high=95.0,
            low=90.0,
            close=92.0,
            atr=ATR,
            volume=10.0,
        )
    elif direction == "long" and outcome == "gap_target":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=112.0,
            high=114.0,
            low=95.0,
            close=100.0,
            atr=ATR,
            volume=10.0,
        )
    elif direction == "long":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=100.0,
            high=100.0,
            low=96.0,
            close=96.0,
            atr=ATR,
            volume=10.0,
        )
    elif outcome == "target":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=100.0,
            high=100.0,
            low=90.0,
            close=90.0,
            atr=ATR,
            volume=10.0,
        )
    elif outcome == "gap_stop":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=106.0,
            high=110.0,
            low=105.0,
            close=108.0,
            atr=ATR,
            volume=10.0,
        )
    elif outcome == "gap_target":
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=88.0,
            high=105.0,
            low=86.0,
            close=100.0,
            atr=ATR,
            volume=10.0,
        )
    else:
        exit_bar = Candle(
            time=start + timedelta(minutes=1),
            open=100.0,
            high=104.0,
            low=100.0,
            close=104.0,
            atr=ATR,
            volume=10.0,
        )
    return [entry, exit_bar]


def _dataset_fingerprint(candles: list[Candle]) -> str:
    material = json.dumps(
        [candle.serialize() for candle in candles],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _run_reference(
    *,
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
    direction: str,
    outcome: str,
) -> dict[str, Any]:
    engine, gateway = _engine(adapter_type)
    candles = _candles(direction, outcome)
    exit_type = {
        "same_bar": "stop",
        "gap_stop": "stop",
        "gap_target": "target",
    }.get(outcome, outcome)
    position = engine.maybe_enter(candles[0], direction)
    assert position is not None
    events = engine.step(candles[1])
    SettlementApplier(obs_enabled=False).apply(
        events,
        engine.exit_settlement,
        execution_profile=engine.execution_profile,
    )
    wallet = gateway.project()
    trade = position.serialize()
    exit_event = next(event for event in events if event["type"] == exit_type)
    close_event = next(event for event in events if event["type"] == "close")
    entry_order = dict(position.entry_order or {})
    entry_outcome = dict(position.entry_outcome or {})
    ending_equity = float(wallet.balances["USD"])
    report = {
        "gross_pnl": float(trade["gross_pnl"]),
        "fees": float(trade["fees_paid"]),
        "net_pnl": float(trade["net_pnl"]),
        "equity_end": ending_equity,
    }
    return {
        "input_dataset": {
            "identity": "hand-verifiable-v1",
            "fingerprint": _dataset_fingerprint(candles),
            "requested_range": [
                candles[0].time.isoformat(),
                candles[-1].time.isoformat(),
            ],
            "loaded_range": [
                candles[0].time.isoformat(),
                candles[-1].time.isoformat(),
            ],
            "candles": [candle.serialize() for candle in candles],
        },
        "known_at": {
            "boundary": candles[0].time.isoformat(),
            "available_candle_count": 1,
        },
        "indicator_state": {
            "status": "not_applicable",
            "reason": "execution-domain reference fixture",
        },
        "strategy_decision": {
            "decision_id": "reference-decision",
            "signal_id": "reference-signal",
            "intent": f"enter_{direction}",
            "accepted": True,
            "known_at": candles[0].time.isoformat(),
        },
        "normalized_execution_plan": {
            "entry_order_type": engine.execution_plan.entry.order_type,
            "initial_stop_atr_multiplier": engine.execution_plan.initial_stop.atr_multiplier,
            "target_ticks": [
                order.ticks for order in engine.execution_plan.take_profits
            ],
        },
        "generated_order": {
            "side": entry_order["side"],
            "qty": entry_order["qty"],
            "symbol": entry_order["symbol"],
            "order_type": entry_order["order_type"],
            "requested_price": entry_order["requested_price"],
        },
        "entry_fill": {
            "status": entry_outcome["status"],
            "filled_qty": entry_outcome["filled_qty"],
            "avg_fill_price": entry_outcome["avg_fill_price"],
            "fee_paid": entry_outcome["fee_paid"],
            "fee_role": entry_outcome["fee_role"],
            "fee_rate": entry_outcome["fee_rate"],
        },
        "exit_fill": {
            "type": exit_event["type"],
            "price": exit_event["price"],
            "contracts": exit_event["contracts"],
            "pnl": exit_event["pnl"],
            "fee_paid": exit_event["fee_paid"],
            "fee_type": exit_event["fee_type"],
            "order_type": exit_event["order_type"],
            "price_source": exit_event["price_source"],
            "reason_code": exit_event["reason_code"],
            "stop_trigger_price": exit_event.get("stop_trigger_price"),
            "stop_trigger_ticks": exit_event.get("stop_trigger_ticks"),
            "gap_through": exit_event.get("gap_through"),
        },
        "lifecycle": {
            "transitions": ["ENTRY_OPENED", exit_type.upper(), "CLOSED"],
            "same_bar_policy": {
                "same_bar": "pessimistic_stop",
                "gap_stop": "bar_open_stop_precedence",
                "gap_target": "bar_open_target_precedence",
            }.get(outcome, "not_applicable"),
            "close_reason": trade.get("close_reason"),
            "closed_at": trade.get("closed_at"),
            "position_commit_seq": trade["position_commit_seq"],
        },
        "accounting": {
            "starting_cash": STARTING_CASH,
            "realized_pnl": float(trade["gross_pnl"]),
            "fees": float(trade["fees_paid"]),
            "unrealized_pnl": 0.0,
            "ending_equity": ending_equity,
            "locked_margin": float(wallet.locked_margin.get("USD", 0.0)),
            "free_collateral": float(wallet.free_collateral["USD"]),
        },
        "report": report,
        "quality": {
            "status": "ok",
            "gaps": [],
            "provenance": "hand_verified_fixture",
            "caveats": [],
        },
        "close_event": {
            "gross_pnl": close_event["gross_pnl"],
            "fees_paid": close_event["fees_paid"],
            "net_pnl": close_event["net_pnl"],
        },
    }


EXPECTED = {
    ("long", "target"): {
        "exit_price": 110.0,
        "gross_pnl": 10.0,
        "fees": 0.31,
        "net_pnl": 9.69,
        "ending_equity": 1009.69,
        "exit_fee_type": "maker",
        "exit_order_type": "limit_resting",
    },
    ("short", "target"): {
        "exit_price": 90.0,
        "gross_pnl": 10.0,
        "fees": 0.29,
        "net_pnl": 9.71,
        "ending_equity": 1009.71,
        "exit_fee_type": "maker",
        "exit_order_type": "limit_resting",
    },
    ("long", "stop"): {
        "exit_price": 96.0,
        "gross_pnl": -4.0,
        "fees": 0.392,
        "net_pnl": -4.392,
        "ending_equity": 995.608,
        "exit_fee_type": "taker",
        "exit_order_type": "stop_market",
    },
    ("short", "stop"): {
        "exit_price": 104.0,
        "gross_pnl": -4.0,
        "fees": 0.408,
        "net_pnl": -4.408,
        "ending_equity": 995.592,
        "exit_fee_type": "taker",
        "exit_order_type": "stop_market",
    },
    ("long", "same_bar"): {
        "exit_price": 96.0,
        "gross_pnl": -4.0,
        "fees": 0.392,
        "net_pnl": -4.392,
        "ending_equity": 995.608,
        "exit_fee_type": "taker",
        "exit_order_type": "stop_market",
    },
    ("short", "same_bar"): {
        "exit_price": 104.0,
        "gross_pnl": -4.0,
        "fees": 0.408,
        "net_pnl": -4.408,
        "ending_equity": 995.592,
        "exit_fee_type": "taker",
        "exit_order_type": "stop_market",
    },
    ("long", "gap_stop"): {
        "exit_price": 94.0,
        "gross_pnl": -6.0,
        "fees": 0.388,
        "net_pnl": -6.388,
        "ending_equity": 993.612,
        "exit_fee_type": "taker",
        "exit_order_type": "stop_market",
        "price_source": "bar_open_gap_through_stop",
    },
    ("short", "gap_stop"): {
        "exit_price": 106.0,
        "gross_pnl": -6.0,
        "fees": 0.412,
        "net_pnl": -6.412,
        "ending_equity": 993.588,
        "exit_fee_type": "taker",
        "exit_order_type": "stop_market",
        "price_source": "bar_open_gap_through_stop",
    },
    ("long", "gap_target"): {
        "exit_price": 110.0,
        "gross_pnl": 10.0,
        "fees": 0.31,
        "net_pnl": 9.69,
        "ending_equity": 1009.69,
        "exit_fee_type": "maker",
        "exit_order_type": "limit_resting",
        "price_source": "target_price",
    },
    ("short", "gap_target"): {
        "exit_price": 90.0,
        "gross_pnl": 10.0,
        "fees": 0.29,
        "net_pnl": 9.71,
        "ending_equity": 1009.71,
        "exit_fee_type": "maker",
        "exit_order_type": "limit_resting",
        "price_source": "target_price",
    },
}


@pytest.mark.parametrize("adapter_type", [BacktestAdapter, PaperAdapter])
@pytest.mark.parametrize("direction,outcome", EXPECTED)
def test_hand_verifiable_reference_scenario(
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
    direction: str,
    outcome: str,
) -> None:
    trace = _run_reference(
        adapter_type=adapter_type,
        direction=direction,
        outcome=outcome,
    )
    expected = EXPECTED[(direction, outcome)]
    accounting = trace["accounting"]

    assert trace["generated_order"]["qty"] == 1.0
    assert trace["generated_order"]["side"] == (
        "buy" if direction == "long" else "sell"
    )
    assert trace["entry_fill"] == {
        "status": "filled",
        "filled_qty": 1.0,
        "avg_fill_price": ENTRY_PRICE,
        "fee_paid": 0.2,
        "fee_role": "taker",
        "fee_rate": 0.002,
    }
    assert trace["exit_fill"]["price"] == expected["exit_price"]
    assert trace["exit_fill"]["type"] == {
        "same_bar": "stop",
        "gap_stop": "stop",
        "gap_target": "target",
    }.get(outcome, outcome)
    assert trace["exit_fill"]["fee_type"] == expected["exit_fee_type"]
    assert trace["exit_fill"]["order_type"] == expected["exit_order_type"]
    assert trace["exit_fill"]["price_source"] == expected.get(
        "price_source",
        "stop_price" if trace["exit_fill"]["type"] == "stop" else "target_price",
    )
    if outcome == "gap_stop":
        expected_stop = 96.0 if direction == "long" else 104.0
        expected_trigger_ticks = -4.0
        assert trace["exit_fill"]["stop_trigger_price"] == expected_stop
        assert trace["exit_fill"]["stop_trigger_ticks"] == expected_trigger_ticks
        assert trace["exit_fill"]["gap_through"] is True
    assert accounting["realized_pnl"] == expected["gross_pnl"]
    assert accounting["fees"] == pytest.approx(expected["fees"])
    assert trace["report"]["net_pnl"] == pytest.approx(expected["net_pnl"])
    assert accounting["ending_equity"] == pytest.approx(expected["ending_equity"])
    assert accounting["locked_margin"] == pytest.approx(0.0)
    assert accounting["free_collateral"] == pytest.approx(
        accounting["ending_equity"]
    )
    assert (
        accounting["starting_cash"]
        + accounting["realized_pnl"]
        - accounting["fees"]
        + accounting["unrealized_pnl"]
        == pytest.approx(accounting["ending_equity"])
    )
    assert trace["close_event"] == {
        "gross_pnl": expected["gross_pnl"],
        "fees_paid": pytest.approx(expected["fees"]),
        "net_pnl": pytest.approx(expected["net_pnl"]),
    }


@pytest.mark.parametrize("direction,outcome", EXPECTED)
def test_backtest_and_paper_reference_traces_agree(
    direction: str,
    outcome: str,
) -> None:
    backtest = _run_reference(
        adapter_type=BacktestAdapter,
        direction=direction,
        outcome=outcome,
    )
    paper = _run_reference(
        adapter_type=PaperAdapter,
        direction=direction,
        outcome=outcome,
    )

    assert paper == backtest


def test_reference_trace_is_repeatable_for_identical_inputs() -> None:
    first = _run_reference(
        adapter_type=BacktestAdapter,
        direction="long",
        outcome="target",
    )
    second = _run_reference(
        adapter_type=BacktestAdapter,
        direction="long",
        outcome="target",
    )

    assert second == first
