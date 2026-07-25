from __future__ import annotations

import hashlib
import json
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from engines.bot_runtime.adapters import BacktestAdapter, PaperAdapter
from engines.bot_runtime.core.domain import Candle, LadderRiskEngine, StrategySignal
from engines.bot_runtime.core.runtime_events import (
    RuntimeEventName,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
)
from engines.bot_runtime.core.wallet_gateway import SharedWalletGateway
from engines.bot_runtime.runtime.components.settlement import SettlementApplier
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.candle_stats.definition import CandleStatsIndicator
from indicators.candle_stats.runtime import TypedCandleStatsIndicator
from strategies.compiler import compile_strategy
from strategies.evaluator import DecisionEvaluationState, evaluate_strategy_bar


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


def _engine(
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
    *,
    base_risk_per_trade: float = 4.0,
    take_profit_orders: list[dict[str, Any]] | None = None,
) -> tuple[LadderRiskEngine, SharedWalletGateway]:
    engine = LadderRiskEngine(
        config={
            "initial_stop": {"atr_multiplier": 2.0},
            "take_profit_orders": take_profit_orders
            or [{"id": "tp-1", "ticks": 10, "size_fraction": 1.0}],
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
        risk_config={"base_risk_per_trade": base_risk_per_trade},
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


def _known_at_fixture_candles() -> tuple[list[Candle], list[Candle]]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    price_ranges = [
        (100.0, 0.5),
        (100.5, 0.5),
        (101.0, 5.0),
        (111.0, 5.0),
        (111.5, 0.5),
        (112.0, 0.5),
        (1_000.0, 500.0),
    ]
    candles = [
        Candle(
            time=start + timedelta(minutes=index),
            open=close,
            high=close + half_range,
            low=close - half_range,
            close=close,
            atr=ATR,
            volume=1_000.0,
        )
        for index, (close, half_range) in enumerate(price_ranges)
    ]
    return candles[:4], candles[4:]


def _known_at_indicator() -> TypedCandleStatsIndicator:
    params = CandleStatsIndicator.resolve_config(
        {
            "atr_short_window": 1,
            "atr_long_window": 3,
            "atr_z_window": 3,
            "directional_efficiency_window": 1,
            "slope_window": 1,
            "range_window": 1,
            "expansion_window": 1,
            "volume_window": 1,
            "overlap_window": 1,
            "slope_stability_lookback": 1,
            "warmup_bars": 3,
            "atr_expansion_signal_threshold": 0.5,
        },
        strict_unknown=True,
    )
    return TypedCandleStatsIndicator(
        indicator_id="known-at-stats",
        version="v1",
        params=params,
    )


def _position_semantics(engine: LadderRiskEngine) -> dict[str, Any] | None:
    if not engine.trades:
        return None
    payload = engine.trades[-1].serialize()
    semantics = {
        key: deepcopy(payload.get(key))
        for key in (
            "entry_time",
            "bar_time",
            "strategy_id",
            "signal_id",
            "decision_id",
            "entry_price",
            "direction",
            "stop_price",
            "moved_to_breakeven",
            "closed_at",
            "gross_pnl",
            "fees_paid",
            "net_pnl",
            "atr_at_entry",
            "r_value",
            "r_ticks",
            "mae_ticks",
            "mfe_ticks",
            "bars_held",
            "position_commit_seq",
            "close_reason",
            "reason_code",
            "exit_price",
        )
    }
    semantics["legs"] = [
        {
            key: deepcopy(leg.get(key))
            for key in (
                "name",
                "ticks",
                "target_price",
                "status",
                "exit_price",
                "exit_time",
                "contracts",
                "pnl",
                "id",
            )
        }
        for leg in payload.get("legs") or []
    ]
    return semantics


def _execution_event_semantics(event: dict[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(event.get(key))
        for key in (
            "type",
            "leg",
            "price",
            "contracts",
            "pnl",
            "fee_paid",
            "fee_type",
            "order_type",
            "price_source",
            "reason_code",
            "close_reason",
            "exit_price",
            "gross_pnl",
            "fees_paid",
            "net_pnl",
            "stop_trigger_price",
            "stop_trigger_ticks",
            "gap_through",
        )
        if key in event
    }


def _run_known_at_pipeline(
    candles: list[Candle],
    *,
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
) -> list[dict[str, Any]]:
    indicator_engine = IndicatorExecutionEngine([_known_at_indicator()])
    compiled_strategy = compile_strategy(
        strategy_id="known-at-strategy",
        timeframe="1m",
        rules=[
            {
                "id": "atr-expansion-entry",
                "name": "Enter on known ATR expansion",
                "intent": "enter_long",
                "priority": 10,
                "trigger": {
                    "type": "signal_match",
                    "indicator_id": "known-at-stats",
                    "output_name": "atr_expansion",
                    "event_key": "atr_expansion_long",
                },
                "guards": [],
            }
        ],
        attached_indicator_ids=["known-at-stats"],
        indicator_meta_getter=lambda _indicator_id: {
            "typed_outputs": [
                {
                    "name": "atr_expansion",
                    "type": "signal",
                    "event_keys": ["atr_expansion_long"],
                }
            ]
        },
    )
    decision_state = DecisionEvaluationState()
    risk_engine, gateway = _engine(adapter_type)
    risk_engine.strategy_id = compiled_strategy.strategy_id
    trace: list[dict[str, Any]] = []

    for index, candle in enumerate(candles):
        execution_events = risk_engine.step(candle)
        if execution_events:
            SettlementApplier(obs_enabled=False).apply(
                execution_events,
                risk_engine.exit_settlement,
                execution_profile=risk_engine.execution_profile,
            )

        frame = indicator_engine.step(
            bar=candle,
            bar_time=candle.time,
            include_overlays=True,
            include_details=False,
        )
        decision_result = evaluate_strategy_bar(
            compiled_strategy=compiled_strategy,
            state=decision_state,
            outputs=frame.outputs,
            output_types=indicator_engine.output_types,
            instrument_id="known-at-instrument",
            symbol="KNOWN-AT-PERP",
            timeframe="1m",
            bar_time=candle.time,
            minimal_decision_details=True,
        )

        signal = None
        new_position = None
        if decision_result.selected_artifact is not None:
            signal = StrategySignal.from_decision_artifact(
                decision_result.selected_artifact,
                source_type="simulation",
                source_id="known-at-prefix-fixture",
            )
            risk_engine.last_signal_id = signal.signal_id
            risk_engine.last_decision_id = signal.decision_id
            new_position = risk_engine.maybe_enter(candle, signal.direction)

        wallet = gateway.project()
        trace.append(
            {
                "input_dataset": {
                    "identity": "known-at-prefix-fixture-v1",
                    "consumed_fingerprint": _dataset_fingerprint(candles[: index + 1]),
                    "consumed_candles": index + 1,
                    "current_candle": candle.serialize(),
                },
                "known_at": {
                    "boundary": candle.time.isoformat(),
                    "available_candle_count": index + 1,
                },
                "indicator_outputs": {
                    key: {
                        "bar_time": output.bar_time.isoformat(),
                        "ready": output.ready,
                        "value": deepcopy(output.value),
                    }
                    for key, output in sorted(frame.outputs.items())
                },
                "indicator_overlays": {
                    key: {
                        "bar_time": overlay.bar_time.isoformat(),
                        "ready": overlay.ready,
                        "value": deepcopy(overlay.value),
                    }
                    for key, overlay in sorted(frame.overlays.items())
                },
                "strategy_decisions": deepcopy(decision_result.artifacts),
                "selected_signal": signal.to_dict() if signal is not None else None,
                "generated_order": (
                    {
                        key: deepcopy((new_position.entry_order or {}).get(key))
                        for key in (
                            "side",
                            "qty",
                            "symbol",
                            "order_type",
                            "requested_price",
                        )
                    }
                    if new_position is not None
                    else None
                ),
                "execution_events": [
                    _execution_event_semantics(event)
                    for event in execution_events
                ],
                "lifecycle_and_accounting": {
                    "position": _position_semantics(risk_engine),
                    "wallet": {
                        "balances": dict(wallet.balances),
                        "locked_margin": dict(wallet.locked_margin),
                        "free_collateral": dict(wallet.free_collateral),
                    },
                },
            }
        )

    return trace


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
            "reason_code": close_event["reason_code"],
            "close_reason": close_event["close_reason"],
            "exit_price": close_event["exit_price"],
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
    expected_close_reason = (
        "STOP"
        if outcome in {"stop", "same_bar", "gap_stop"}
        else "TARGET"
    )
    expected_reason_code = f"EXEC_EXIT_{expected_close_reason}"
    assert trace["lifecycle"]["close_reason"] == expected_close_reason
    assert trace["close_event"]["close_reason"] == expected_close_reason
    assert trace["close_event"]["reason_code"] == expected_reason_code
    assert trace["close_event"]["exit_price"] == expected["exit_price"]
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
        "reason_code": expected_reason_code,
        "close_reason": expected_close_reason,
        "exit_price": expected["exit_price"],
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


@pytest.mark.parametrize("adapter_type", [BacktestAdapter, PaperAdapter])
def test_known_at_pipeline_is_invariant_to_future_candle_suffix(
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
) -> None:
    prefix, future_suffix = _known_at_fixture_candles()

    prefix_trace = _run_known_at_pipeline(
        prefix,
        adapter_type=adapter_type,
    )
    extended_trace = _run_known_at_pipeline(
        [*prefix, *future_suffix],
        adapter_type=adapter_type,
    )

    assert extended_trace[: len(prefix_trace)] == prefix_trace
    assert prefix_trace[2]["selected_signal"]["event_key"] == "atr_expansion_long"
    assert prefix_trace[2]["generated_order"] == {
        "side": "buy",
        "qty": 1.0,
        "symbol": "REFERENCE-PERP",
        "order_type": "market",
        "requested_price": 101.0,
    }
    assert [
        event["type"]
        for event in prefix_trace[3]["execution_events"]
    ] == ["target", "close"]
    assert extended_trace[-1]["selected_signal"]["event_key"] == "atr_expansion_long"

    terminal_position = prefix_trace[-1]["lifecycle_and_accounting"]["position"]
    terminal_wallet = prefix_trace[-1]["lifecycle_and_accounting"]["wallet"]
    assert terminal_position["close_reason"] == "TARGET"
    assert terminal_position["reason_code"] == "EXEC_EXIT_TARGET"
    assert terminal_position["exit_price"] == 111.0
    assert terminal_position["moved_to_breakeven"] is False
    assert terminal_wallet["locked_margin"].get("USD", 0.0) == pytest.approx(0.0)
    assert terminal_wallet["balances"]["USD"] == pytest.approx(
        STARTING_CASH
        + terminal_position["gross_pnl"]
        - terminal_position["fees_paid"]
    )


def test_known_at_pipeline_backtest_and_paper_semantics_agree() -> None:
    prefix, future_suffix = _known_at_fixture_candles()
    candles = [*prefix, *future_suffix]

    backtest = _run_known_at_pipeline(
        candles,
        adapter_type=BacktestAdapter,
    )
    paper = _run_known_at_pipeline(
        candles,
        adapter_type=PaperAdapter,
    )

    assert paper == backtest


def _run_multiple_target_reference(
    *,
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
    direction: str,
) -> dict[str, Any]:
    engine, gateway = _engine(
        adapter_type,
        base_risk_per_trade=8.0,
        take_profit_orders=[
            {"id": "tp-1", "ticks": 5, "size_fraction": 0.5},
            {"id": "tp-2", "ticks": 10, "size_fraction": 0.5},
        ],
    )
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    candles = [
        Candle(
            time=start,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.0,
            atr=ATR,
            volume=10.0,
        ),
        Candle(
            time=start + timedelta(minutes=1),
            open=100.0,
            high=105.0 if direction == "long" else 100.0,
            low=100.0 if direction == "long" else 95.0,
            close=105.0 if direction == "long" else 95.0,
            atr=ATR,
            volume=10.0,
        ),
        Candle(
            time=start + timedelta(minutes=2),
            open=105.0 if direction == "long" else 95.0,
            high=110.0 if direction == "long" else 95.0,
            low=105.0 if direction == "long" else 90.0,
            close=110.0 if direction == "long" else 90.0,
            atr=ATR,
            volume=10.0,
        ),
    ]
    position = engine.maybe_enter(candles[0], direction)
    assert position is not None
    event_batches = []
    for candle in candles[1:]:
        events = engine.step(candle)
        SettlementApplier(obs_enabled=False).apply(
            events,
            engine.exit_settlement,
            execution_profile=engine.execution_profile,
        )
        event_batches.append(events)

    target_events = [
        event
        for events in event_batches
        for event in events
        if event["type"] == "target"
    ]
    close_events = [
        event
        for events in event_batches
        for event in events
        if event["type"] == "close"
    ]
    trade = position.serialize()
    wallet = gateway.project()
    ending_equity = float(wallet.balances["USD"])
    return {
        "input_dataset": {
            "identity": f"hand-verifiable-multiple-targets-{direction}-v1",
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
        "strategy_decision": {
            "decision_id": "reference-decision",
            "signal_id": "reference-signal",
            "intent": f"enter_{direction}",
            "known_at": candles[0].time.isoformat(),
        },
        "generated_order": {
            "side": position.entry_order["side"],
            "qty": position.entry_order["qty"],
            "order_type": position.entry_order["order_type"],
        },
        "fills": [
            {
                "type": event["type"],
                "leg_id": event["leg_id"],
                "price": event["price"],
                "contracts": event["contracts"],
                "pnl": event["pnl"],
                "fee_paid": event["fee_paid"],
                "fee_type": event["fee_type"],
                "order_type": event["order_type"],
                "price_source": event["price_source"],
            }
            for event in target_events
        ],
        "lifecycle": {
            "transitions": [
                "ENTRY_OPENED",
                *[event["type"].upper() for event in target_events],
                *[event["type"].upper() for event in close_events],
            ],
            "leg_states": [
                {
                    "leg_id": leg["id"],
                    "status": leg["status"],
                    "contracts": leg["contracts"],
                    "pnl": leg["pnl"],
                }
                for leg in trade["legs"]
            ],
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
        "report": {
            "gross_pnl": float(trade["gross_pnl"]),
            "fees": float(trade["fees_paid"]),
            "net_pnl": float(trade["net_pnl"]),
            "equity_end": ending_equity,
        },
        "quality": {
            "status": "ok",
            "gaps": [],
            "provenance": "hand_verified_fixture",
            "caveats": [],
        },
    }


@pytest.mark.parametrize("adapter_type", [BacktestAdapter, PaperAdapter])
@pytest.mark.parametrize(
    ("direction", "expected_fills", "expected_fees", "expected_equity"),
    [
        (
            "long",
            [
                ("tp-1", 105.0, 1.0, 5.0, 0.105),
                ("tp-2", 110.0, 1.0, 10.0, 0.11),
            ],
            0.615,
            1014.385,
        ),
        (
            "short",
            [
                ("tp-1", 95.0, 1.0, 5.0, 0.095),
                ("tp-2", 90.0, 1.0, 10.0, 0.09),
            ],
            0.585,
            1014.415,
        ),
    ],
)
def test_multiple_target_reference_reconciles(
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
    direction: str,
    expected_fills: list[tuple[str, float, float, float, float]],
    expected_fees: float,
    expected_equity: float,
) -> None:
    trace = _run_multiple_target_reference(
        adapter_type=adapter_type,
        direction=direction,
    )

    assert trace["generated_order"]["qty"] == 2.0
    assert [
        (
            fill["leg_id"],
            fill["price"],
            fill["contracts"],
            fill["pnl"],
            fill["fee_paid"],
        )
        for fill in trace["fills"]
    ] == expected_fills
    assert trace["lifecycle"]["transitions"] == [
        "ENTRY_OPENED",
        "TARGET",
        "TARGET",
        "CLOSE",
    ]
    assert sum(fill["contracts"] for fill in trace["fills"]) == 2.0
    assert trace["accounting"]["realized_pnl"] == 15.0
    assert trace["accounting"]["fees"] == pytest.approx(expected_fees)
    assert trace["report"]["net_pnl"] == pytest.approx(15.0 - expected_fees)
    assert trace["accounting"]["ending_equity"] == pytest.approx(expected_equity)
    assert trace["accounting"]["locked_margin"] == 0.0
    assert (
        trace["accounting"]["starting_cash"]
        + trace["accounting"]["realized_pnl"]
        - trace["accounting"]["fees"]
        + trace["accounting"]["unrealized_pnl"]
        == pytest.approx(trace["accounting"]["ending_equity"])
    )


@pytest.mark.parametrize("direction", ["long", "short"])
def test_multiple_target_reference_has_backtest_paper_parity_and_repeatability(
    direction: str,
) -> None:
    first = _run_multiple_target_reference(
        adapter_type=BacktestAdapter,
        direction=direction,
    )
    second = _run_multiple_target_reference(
        adapter_type=BacktestAdapter,
        direction=direction,
    )
    paper = _run_multiple_target_reference(
        adapter_type=PaperAdapter,
        direction=direction,
    )

    assert second == first
    assert paper == first


@pytest.mark.parametrize("adapter_type", [BacktestAdapter, PaperAdapter])
def test_partial_target_then_stop_has_explicit_mixed_terminal_reason(
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
) -> None:
    engine, gateway = _engine(
        adapter_type,
        base_risk_per_trade=8.0,
        take_profit_orders=[
            {"id": "tp-1", "ticks": 5, "size_fraction": 0.5},
            {"id": "tp-2", "ticks": 10, "size_fraction": 0.5},
        ],
    )
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    entry = Candle(
        time=start,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        atr=ATR,
        volume=10.0,
    )
    target_bar = Candle(
        time=start + timedelta(minutes=1),
        open=100.0,
        high=105.0,
        low=100.0,
        close=105.0,
        atr=ATR,
        volume=10.0,
    )
    stop_bar = Candle(
        time=start + timedelta(minutes=2),
        open=100.0,
        high=105.0,
        low=96.0,
        close=96.0,
        atr=ATR,
        volume=10.0,
    )

    position = engine.maybe_enter(entry, "long")
    assert position is not None
    first_events = engine.step(target_bar)
    SettlementApplier(obs_enabled=False).apply(
        first_events,
        engine.exit_settlement,
        execution_profile=engine.execution_profile,
    )
    terminal_events = engine.step(stop_bar)
    SettlementApplier(obs_enabled=False).apply(
        terminal_events,
        engine.exit_settlement,
        execution_profile=engine.execution_profile,
    )

    close_event = next(event for event in terminal_events if event["type"] == "close")
    trade = position.serialize()
    wallet = gateway.project()
    assert [leg["status"] for leg in trade["legs"]] == ["target", "stop"]
    assert trade["close_reason"] == "MIXED"
    assert trade["reason_code"] == "EXEC_EXIT_CLOSE"
    assert trade["exit_price"] == pytest.approx(100.5)
    assert close_event["close_reason"] == "MIXED"
    assert close_event["reason_code"] == "EXEC_EXIT_CLOSE"
    assert close_event["exit_price"] == pytest.approx(100.5)
    assert wallet.balances["USD"] == pytest.approx(
        STARTING_CASH + trade["gross_pnl"] - trade["fees_paid"]
    )


def _run_terminal_reference(
    *,
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
    direction: str,
) -> dict[str, Any]:
    engine, gateway = _engine(adapter_type)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    entry = Candle(
        time=start,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        atr=ATR,
        volume=10.0,
    )
    terminal = Candle(
        time=start + timedelta(minutes=1),
        open=100.0,
        high=104.0 if direction == "long" else 101.0,
        low=99.0 if direction == "long" else 96.0,
        close=103.0 if direction == "long" else 97.0,
        atr=ATR,
        volume=10.0,
    )
    position = engine.maybe_enter(entry, direction)
    assert position is not None
    events = engine.force_close_active_trade_at_backtest_end(terminal)
    SettlementApplier(obs_enabled=False).apply(
        events,
        engine.exit_settlement,
        execution_profile=engine.execution_profile,
    )
    terminal_fill = next(event for event in events if event["type"] == "backtest_end")
    close_event = next(event for event in events if event["type"] == "close")
    trade = position.serialize()
    wallet = gateway.project()
    ending_equity = float(wallet.balances["USD"])
    candles = [entry, terminal]
    return {
        "input_dataset": {
            "identity": f"hand-verifiable-backtest-end-{direction}-v1",
            "fingerprint": _dataset_fingerprint(candles),
            "requested_range": [entry.time.isoformat(), terminal.time.isoformat()],
            "loaded_range": [entry.time.isoformat(), terminal.time.isoformat()],
            "candles": [candle.serialize() for candle in candles],
        },
        "known_at": {
            "boundary": entry.time.isoformat(),
            "available_candle_count": 1,
        },
        "strategy_decision": {
            "decision_id": "reference-decision",
            "signal_id": "reference-signal",
            "intent": f"enter_{direction}",
            "known_at": entry.time.isoformat(),
        },
        "generated_order": {
            "side": position.entry_order["side"],
            "qty": position.entry_order["qty"],
            "order_type": position.entry_order["order_type"],
        },
        "fill": {
            "type": terminal_fill["type"],
            "price": terminal_fill["price"],
            "contracts": terminal_fill["contracts"],
            "pnl": terminal_fill["pnl"],
            "fee_paid": terminal_fill["fee_paid"],
            "fee_type": terminal_fill["fee_type"],
            "order_type": terminal_fill["order_type"],
            "price_source": terminal_fill["price_source"],
            "reason_code": terminal_fill["reason_code"],
        },
        "lifecycle": {
            "transitions": ["ENTRY_OPENED", "BACKTEST_END", "CLOSE"],
            "close_reason": close_event["close_reason"],
            "closed_at": trade["closed_at"],
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
        "report": {
            "gross_pnl": float(trade["gross_pnl"]),
            "fees": float(trade["fees_paid"]),
            "net_pnl": float(trade["net_pnl"]),
            "equity_end": ending_equity,
        },
        "quality": {
            "status": "ok",
            "gaps": [],
            "provenance": "hand_verified_fixture",
            "caveats": [],
        },
    }


@pytest.mark.parametrize("adapter_type", [BacktestAdapter, PaperAdapter])
@pytest.mark.parametrize(
    ("direction", "exit_price", "expected_fees", "expected_equity"),
    [
        ("long", 103.0, 0.406, 1002.594),
        ("short", 97.0, 0.394, 1002.606),
    ],
)
def test_terminal_reference_reconciles(
    adapter_type: type[BacktestAdapter] | type[PaperAdapter],
    direction: str,
    exit_price: float,
    expected_fees: float,
    expected_equity: float,
) -> None:
    trace = _run_terminal_reference(
        adapter_type=adapter_type,
        direction=direction,
    )

    assert trace["generated_order"]["qty"] == 1.0
    assert trace["fill"] == {
        "type": "backtest_end",
        "price": exit_price,
        "contracts": 1.0,
        "pnl": 3.0,
        "fee_paid": pytest.approx(exit_price * 0.002),
        "fee_type": "taker",
        "order_type": "market",
        "price_source": "bar_close",
        "reason_code": "BACKTEST_END",
    }
    assert trace["lifecycle"]["transitions"] == [
        "ENTRY_OPENED",
        "BACKTEST_END",
        "CLOSE",
    ]
    assert trace["lifecycle"]["close_reason"] == "BACKTEST_END"
    assert trace["accounting"]["realized_pnl"] == 3.0
    assert trace["accounting"]["fees"] == pytest.approx(expected_fees)
    assert trace["report"]["net_pnl"] == pytest.approx(3.0 - expected_fees)
    assert trace["accounting"]["ending_equity"] == pytest.approx(expected_equity)
    assert trace["accounting"]["locked_margin"] == 0.0
    assert (
        trace["accounting"]["starting_cash"]
        + trace["accounting"]["realized_pnl"]
        - trace["accounting"]["fees"]
        + trace["accounting"]["unrealized_pnl"]
        == pytest.approx(trace["accounting"]["ending_equity"])
    )


@pytest.mark.parametrize("direction", ["long", "short"])
def test_terminal_reference_has_backtest_paper_parity_and_repeatability(
    direction: str,
) -> None:
    first = _run_terminal_reference(
        adapter_type=BacktestAdapter,
        direction=direction,
    )
    second = _run_terminal_reference(
        adapter_type=BacktestAdapter,
        direction=direction,
    )
    paper = _run_terminal_reference(
        adapter_type=PaperAdapter,
        direction=direction,
    )

    assert second == first
    assert paper == first
