from datetime import datetime, timezone
import threading
from typing import Optional

from engines.bot_runtime.core import (
    CandleSnapshot,
    CanonicalOrderState,
    EntryFill,
    EntryFillResult,
    FillOrder,
    PendingEntry,
)
from engines.bot_runtime.core.domain import Candle, EntryRequest, EntryValidation, LadderRiskEngine
from engines.bot_runtime.core.execution import FillRejection, FillResult
from engines.bot_runtime.core.execution_intent import ExecutionIntent, ExecutionOutcome
from engines.bot_runtime.core.exit_settlement import ExitSettlementContext
from engines.bot_runtime.core.fees import executed_fee, executed_notional
from engines.bot_runtime.core.runtime_events import (
    RuntimeEventName,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
)
from engines.bot_runtime.core.wallet_gateway import SharedWalletGateway


def _build_spot_engine(
    *,
    execution_mode: str = "market",
    limit_maker: Optional[dict] = None,
    base_risk_per_trade: float = 100,
    take_profit_orders: Optional[list[dict]] = None,
    extra_config: Optional[dict] = None,
    maker_fee_rate: float = 0.0,
    taker_fee_rate: float = 0.0,
) -> LadderRiskEngine:
    config = {
        "initial_stop": {"atr_multiplier": 2.0},
        "take_profit_orders": take_profit_orders or [{"id": "tp-1", "ticks": 10, "size_fraction": 1.0}],
        "execution_mode": execution_mode,
    }
    if limit_maker is not None:
        config["limit_maker"] = limit_maker
    if extra_config:
        config.update(extra_config)
    instrument = {
        "symbol": "TEST-SPOT",
        "instrument_type": "spot",
        "tick_size": 1.0,
        "contract_size": 1.0,
        "tick_value": 1.0,
        "min_order_size": 1,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "maker_fee_rate": maker_fee_rate,
        "taker_fee_rate": taker_fee_rate,
        "metadata": {
            "info": {"base_increment": "1"},
        },
    }
    return LadderRiskEngine(
        config=config,
        instrument=instrument,
        risk_config={"base_risk_per_trade": base_risk_per_trade},
    )


def _build_future_engine() -> LadderRiskEngine:
    config = {
        "initial_stop": {"atr_multiplier": 1.0},
        "take_profit_orders": [{"id": "tp-1", "ticks": 10, "size_fraction": 1.0}],
        "execution_mode": "market",
    }
    instrument = {
        "symbol": "TEST-FUTURE",
        "instrument_type": "future",
        "tick_size": 5.0,
        "contract_size": 0.01,
        "tick_value": 0.05,
        "min_order_size": 1,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "can_short": True,
        "short_requires_borrow": False,
        "margin_rates": {
            "intraday": {"long_margin_rate": 0.1, "short_margin_rate": 0.1},
            "overnight": {"long_margin_rate": 0.2, "short_margin_rate": 0.2},
        },
        "metadata": {
            "info": {
                "base_increment": "1",
            },
        },
    }
    return LadderRiskEngine(
        config=config,
        instrument=instrument,
        risk_config={"base_risk_per_trade": 1000},
    )


def _build_candle(*, close: float, atr: float) -> Candle:
    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return Candle(
        time=timestamp,
        open=close,
        high=close + 1,
        low=close - 1,
        close=close,
        atr=atr,
    )


class _FillAdapter:
    def execute_order(self, order: FillOrder):
        notional = executed_notional(price=order.price, quantity=order.requested_qty, contract_size=1.0)
        return (
            FillResult(
                filled_qty=float(order.requested_qty),
                fill_price=float(order.price),
                notional=notional,
                fee=executed_fee(
                    price=order.price,
                    quantity=order.requested_qty,
                    contract_size=1.0,
                    fee_rate=order.fee_rate,
                ),
                fee_rate=float(order.fee_rate or 0.0),
                side=order.side,
                metadata={"source": "test"},
            ),
            None,
        )


class _RejectSellAdapter(_FillAdapter):
    def execute_order(self, order: FillOrder):
        if order.side == "sell":
            return None, FillRejection(
                reason="TEST_EXIT_REJECTED",
                metadata={"order_type": order.order_type},
            )
        return super().execute_order(order)


class _PartialSellAdapter(_FillAdapter):
    def __init__(self, sell_fill_quantities: list[float]) -> None:
        self._sell_fill_quantities = list(sell_fill_quantities)

    def execute_order(self, order: FillOrder):
        if order.side != "sell":
            return super().execute_order(order)
        quantity = min(float(self._sell_fill_quantities.pop(0)), float(order.requested_qty))
        return (
            FillResult(
                filled_qty=quantity,
                fill_price=float(order.price),
                notional=executed_notional(price=order.price, quantity=quantity, contract_size=1.0),
                fee=executed_fee(
                    price=order.price,
                    quantity=quantity,
                    contract_size=1.0,
                    fee_rate=order.fee_rate,
                ),
                fee_rate=float(order.fee_rate or 0.0),
                side=order.side,
                metadata={"source": "partial-test"},
            ),
            None,
        )


def _enable_runtime_execution(engine: LadderRiskEngine) -> None:
    engine.attach_wallet_gateway(SharedWalletGateway(_wallet_proxy({"USD": 1_000_000.0})))
    engine.attach_execution_adapter(_FillAdapter())


def _build_pending(request: EntryRequest, validity_remaining: int = 0) -> PendingEntry:
    intent = request.intent
    assert intent is not None
    return PendingEntry(
        request=request,
        intent=intent,
        direction=request.direction,
        qty_raw=request.qty_raw,
        requested_qty=request.requested_qty,
        r_ticks=float(request.r_ticks),
        r_value=request.r_value,
        atr_at_entry=request.atr_at_entry,
        r_multiple_at_entry=request.r_multiple_at_entry,
        order_intent_id=str(request.order_intent_id),
        trade_id=str(request.trade_id),
        validity_remaining=validity_remaining,
        fallback=request.limit_params.fallback if request.limit_params else "cancel",
        remaining_qty=float(request.requested_qty),
    )


def _snapshot(candle: Candle) -> CandleSnapshot:
    return CandleSnapshot(
        time=candle.time,
        open=float(candle.open),
        high=float(candle.high),
        low=float(candle.low),
        close=float(candle.close),
        atr=candle.atr,
        lookback_15=candle.lookback_15,
    )


def _wallet_proxy(balances: dict[str, float]) -> dict:
    return {
        "runtime_events": [
            new_runtime_event(
                event_name=RuntimeEventName.WALLET_INITIALIZED,
                correlation_id=build_correlation_id(
                    run_id="run-test",
                    symbol=None,
                    timeframe=None,
                    bar_ts=None,
                ),
                context=WalletInitializedContext(
                    run_id="run-test",
                    bot_id="bot-test",
                    strategy_id="__runtime__",
                    symbol=None,
                    timeframe=None,
                    bar_ts=None,
                    balances=balances,
                    source="test",
                ),
            ).serialize()
        ],
        "reservations": {},
        "lock": threading.RLock(),
    }


def test_attach_wallet_gateway_wires_exit_settlement_to_shared_gateway() -> None:
    engine = _build_future_engine()
    gateway = SharedWalletGateway(_wallet_proxy({"USD": 1_000_000.0}))

    engine.attach_wallet_gateway(gateway)

    assert getattr(engine.exit_settlement, "_wallet_gateway", None) is gateway
    applied, metadata = engine.exit_settlement.apply_exit_fill(
        ExitSettlementContext(
            event_type="EXIT_FILL",
            exit_kind="CLOSE",
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
            realized_pnl=1.0,
            allow_short_borrow=False,
            instrument={"symbol": "TEST-FUTURE", "instrument_type": "future"},
            execution_profile=engine.execution_profile,
        ),
        force=True,
    )

    assert applied is True
    assert metadata["wallet_commit_seq"] is not None
    assert metadata["wallet_commit_seq_status"] == "runtime_assigned"
    assert metadata["wallet_eval_seq"] is not None


def test_build_entry_request_matches_expected_values():
    engine = _build_spot_engine()
    candle = _build_candle(close=100.0, atr=2.0)

    request = engine.build_entry_request(candle, "long")

    r_ticks = engine._compute_r_ticks(candle)
    r_value = engine._r_value(candle)
    risk_based_qty = engine._calculate_total_contracts(r_ticks)
    capped_qty, was_margin_capped, margin_info = engine._cap_qty_by_margin(
        risk_qty=risk_based_qty,
        price=candle.close,
        direction="long",
    )
    normalization = engine._normalize_qty(capped_qty)
    expected_qty = float(normalization.qty_final)

    assert request.validation.ok is True
    assert request.r_ticks == float(r_ticks)
    assert request.requested_qty == expected_qty
    assert request.order_type == "market"
    assert request.limit_params is None
    assert request.side == "buy"
    assert request.margin_info == margin_info
    assert request.was_margin_capped == was_margin_capped
    assert request.intent is not None


def test_submit_entry_market_returns_position():
    engine = _build_spot_engine()
    candle = _build_candle(close=100.0, atr=2.0)
    expected = engine.build_entry_request(candle, "long")

    position = engine.entry_execution.submit_entry(candle, "long")

    assert position is not None
    assert expected.trade_id is not None
    assert expected.entry_request_id.startswith("entry_request:")
    assert position.trade_id is not None
    assert position.trade_id != expected.entry_request_id
    assert position.entry_order["qty"] == expected.requested_qty
    assert engine.entry_execution.pending_entry is None


def test_submit_entry_limit_maker_creates_pending_entry():
    limit_maker = {
        "anchor_price": "signal_price",
        "offset_type": "ticks",
        "offset_value": 5,
        "validity_window": 2,
        "fallback": "convert_to_market",
    }
    engine = _build_spot_engine(execution_mode="limit_maker", limit_maker=limit_maker)
    candle = _build_candle(close=100.0, atr=2.0)

    position = engine.entry_execution.submit_entry(candle, "long")

    assert position is None
    pending = engine.entry_execution.pending_entry
    assert pending is not None
    assert pending.validity_remaining == 2
    assert pending.fallback == "convert_to_market"
    assert pending.intent.order_type == "limit_maker"


def test_limit_maker_entry_does_not_fill_from_signal_bar_range():
    limit_maker = {
        "anchor_price": "signal_price",
        "offset_type": "ticks",
        "offset_value": 5,
        "validity_window": 1,
        "fallback": "cancel",
    }
    engine = _build_spot_engine(execution_mode="limit_maker", limit_maker=limit_maker)
    signal_bar = Candle(
        time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=94.0,
        close=100.0,
        atr=2.0,
    )

    position = engine.entry_execution.submit_entry(signal_bar, "long")

    assert position is None
    assert engine.entry_execution.pending_entry is not None

    next_bar = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=100.0,
        low=94.0,
        close=96.0,
        atr=2.0,
    )
    position = engine.entry_execution.process_pending(next_bar)

    assert position is not None
    assert position.entry_price == 95.0


def test_partial_entry_is_retained_but_cannot_be_abandoned_before_phase_3():
    limit_maker = {
        "anchor_price": "signal_price",
        "offset_type": "ticks",
        "offset_value": 5,
        "validity_window": 2,
        "fallback": "cancel",
    }
    engine = _build_spot_engine(
        execution_mode="limit_maker",
        limit_maker=limit_maker,
        base_risk_per_trade=8,
    )
    signal_bar = _build_candle(close=100.0, atr=2.0)

    class SequenceModel:
        def __init__(self) -> None:
            self._statuses = ["open", "partially_filled", "canceled"]

        def evaluate(self, intent, *, candle_high, candle_low, candle_close, candle_open):
            status = self._statuses.pop(0)
            filled_qty = 1.0 if status == "partially_filled" else 0.0
            remaining_qty = max(float(intent.qty) - filled_qty, 0.0)
            return (
                ExecutionOutcome(
                    order_id=intent.order_id,
                    status=status,
                    filled_qty=filled_qty,
                    avg_fill_price=float(candle_close) if filled_qty else None,
                    fee_paid=0.0,
                    fee_role="maker",
                    fee_rate=0.0,
                    fee_source="test",
                    fee_version="test",
                    created_at="now",
                    updated_at="now",
                    filled_at="now" if filled_qty else None,
                    remaining_qty=remaining_qty,
                    limit_price=(
                        float(intent.limit_params.limit_price)
                        if intent.limit_params is not None
                        and intent.limit_params.limit_price is not None
                        else None
                    ),
                ),
                None,
            )

    engine.attach_execution_model(SequenceModel())
    assert engine.entry_execution.submit_entry(signal_bar, "long") is None

    partial_bar = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=95.0,
        high=96.0,
        low=94.0,
        close=95.0,
        atr=2.0,
    )
    assert engine.entry_execution.process_pending(partial_bar) is None
    pending = engine.entry_execution.pending_entry
    assert pending is not None
    assert pending.filled_qty == 1.0
    assert pending.remaining_qty == pending.requested_qty - 1.0
    assert pending.order_lifecycle is not None
    assert pending.order_lifecycle.snapshot().state == CanonicalOrderState.PARTIALLY_FILLED

    canceled_bar = Candle(
        time=datetime(2024, 1, 1, 2, tzinfo=timezone.utc),
        open=96.0,
        high=97.0,
        low=95.0,
        close=96.0,
        atr=2.0,
    )
    try:
        engine.entry_execution.process_pending(canceled_bar)
    except RuntimeError as exc:
        assert "partial entry disposition is not admitted" in str(exc)
    else:
        raise AssertionError("expected partial entry cancellation to fail closed")

    assert engine.entry_execution.pending_entry is pending
    assert pending.order_lifecycle.snapshot().state == CanonicalOrderState.PARTIALLY_FILLED


def test_submit_entry_limit_maker_rejects_marketable_post_only_order():
    limit_maker = {
        "anchor_price": "signal_price",
        "offset_type": "ticks",
        "offset_value": 0,
        "validity_window": 1,
        "fallback": "cancel",
    }
    engine = _build_spot_engine(execution_mode="limit_maker", limit_maker=limit_maker)
    candle = _build_candle(close=100.0, atr=2.0)

    position = engine.entry_execution.submit_entry(candle, "long")

    assert position is None
    assert engine.last_rejection_reason == "POST_ONLY_WOULD_CROSS"
    assert engine.entry_execution.pending_entry is None


def test_limit_maker_rejects_fake_next_bar_anchor():
    limit_maker = {
        "anchor_price": "next_bar_open",
        "offset_type": "ticks",
        "offset_value": 5,
        "validity_window": 1,
        "fallback": "cancel",
    }

    try:
        _build_spot_engine(execution_mode="limit_maker", limit_maker=limit_maker)
    except ValueError as exc:
        assert "Next-bar entry requires an explicit pending signal-entry lifecycle" in str(exc)
    else:
        raise AssertionError("expected next_bar_open to fail loud")


def test_runtime_rejects_unimplemented_stop_adjustment_trail_atr_action():
    try:
        _build_spot_engine(
            extra_config={
                "stop_adjustments": [
                    {
                        "id": "sa-trail",
                        "trigger_type": "r_multiple",
                        "trigger_value": 1.0,
                        "action_type": "trail_atr",
                    }
                ]
            }
        )
    except ValueError as exc:
        assert "action_type='trail_atr' is unsupported" in str(exc)
    else:
        raise AssertionError("expected trail_atr stop adjustment to fail loud")


def test_submit_entry_margin_capped_uses_request_qty():
    engine = _build_future_engine()
    engine.attach_wallet_gateway(SharedWalletGateway(_wallet_proxy({"USD": 500.0})))
    candle = _build_candle(close=110000.0, atr=100.0)

    request = engine.build_entry_request(candle, "long")
    assert request.was_margin_capped is True

    position = engine.entry_execution.submit_entry(candle, "long")

    assert position is not None
    assert position.entry_order["qty"] == request.requested_qty


def test_trade_open_bumps_trade_revision_once():
    engine = _build_spot_engine(base_risk_per_trade=8)
    _enable_runtime_execution(engine)
    candle = _build_candle(close=100.0, atr=2.0)

    position = engine.maybe_enter(candle, "long")

    assert position is not None
    assert engine.trade_revision == 1


def test_active_noop_bar_does_not_bump_trade_revision():
    engine = _build_spot_engine(base_risk_per_trade=8)
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None
    revision_after_open = engine.trade_revision

    noop = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=102.0,
        low=98.0,
        close=101.0,
        atr=2.0,
    )
    events = engine.step(noop)

    assert events == []
    assert engine.active_trade is position
    assert position.bars_held == 1
    assert position.mfe_ticks == 2.0
    assert engine.trade_revision == revision_after_open


def test_trade_close_bumps_trade_revision():
    engine = _build_spot_engine(base_risk_per_trade=8)
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None
    revision_after_open = engine.trade_revision

    stop = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=95.0,
        close=96.0,
        atr=2.0,
    )
    events = engine.step(stop)

    assert any(event["type"] == "close" for event in events)
    assert engine.active_trade is None
    assert engine.trade_revision == revision_after_open + 1


def test_stop_movement_bumps_trade_revision_without_trade_event():
    engine = _build_spot_engine(base_risk_per_trade=8)
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None
    position.trailing_activation_ticks = 5
    position.trailing_distance_ticks = 2
    revision_after_open = engine.trade_revision

    trailing = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=105.0,
        high=106.0,
        low=104.5,
        close=105.5,
        atr=2.0,
    )
    events = engine.step(trailing)

    assert events == []
    assert position.stop_price == 104.0
    assert position.trailing_active is True
    assert engine.trade_revision == revision_after_open + 1


def test_trailing_stop_from_normalized_config_only_tightens():
    engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[{"id": "tp-1", "ticks": 100, "size_fraction": 1.0}],
        extra_config={
            "stop_adjustments": [],
            "trailing": {
                "enabled": True,
                "activation_type": "r_multiple",
                "r_multiple": 1.0,
                "ticks": 2,
            },
        },
    )
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None

    activate = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=104.0,
        high=106.0,
        low=104.5,
        close=105.0,
        atr=2.0,
    )
    engine.step(activate)
    assert position.trailing_active is True
    assert position.stop_price == 104.0

    lower_high = Candle(
        time=datetime(2024, 1, 1, 2, tzinfo=timezone.utc),
        open=104.5,
        high=105.0,
        low=104.25,
        close=104.75,
        atr=2.0,
    )
    engine.step(lower_high)
    assert position.stop_price == 104.0

    new_high = Candle(
        time=datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
        open=107.0,
        high=110.0,
        low=106.0,
        close=109.0,
        atr=2.0,
    )
    engine.step(new_high)
    assert position.stop_price == 108.0


def test_disabled_trailing_config_with_stale_distance_fields_does_not_activate():
    engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[{"id": "tp-1", "ticks": 100, "size_fraction": 1.0}],
        extra_config={
            "stop_adjustments": [],
            "trailing": {
                "enabled": False,
                "activation_type": "r_multiple",
                "r_multiple": 1.0,
                "ticks": 2,
            },
        },
    )
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None
    assert position.trailing_activation_ticks is None
    assert position.trailing_distance_ticks is None

    favorable = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=104.0,
        high=110.0,
        low=103.0,
        close=109.0,
        atr=2.0,
    )
    engine.step(favorable)

    assert position.trailing_active is False
    assert position.stop_price == 96.0


def test_flattened_stop_adjustment_rule_executes_after_normalization():
    engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[{"id": "tp-1", "ticks": 100, "size_fraction": 1.0}],
        extra_config={
            "stop_adjustments": [
                {
                    "id": "sa-flat",
                    "trigger_type": "r_multiple",
                    "trigger_ticks": 5,
                    "action_type": "move_to_r",
                    "action_value": 0.5,
                }
            ],
        },
    )
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None
    assert position.stop_price == 96.0

    favorable = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=104.0,
        high=106.0,
        low=103.0,
        close=105.0,
        atr=2.0,
    )
    engine.step(favorable)

    assert position.stop_price == 102.0


def test_omitted_stop_adjustments_do_not_enable_implicit_breakeven():
    engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[{"id": "tp-1", "ticks": 10, "size_fraction": 1.0}],
    )
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None

    favorable = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=104.0,
        high=106.0,
        low=103.0,
        close=105.0,
        atr=2.0,
    )
    engine.step(favorable)

    assert position.moved_to_breakeven is False
    assert position.stop_price == 96.0


def test_fixed_horizon_exit_closes_after_configured_bars_with_taker_fee():
    engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[{"id": "tp-1", "ticks": 100, "size_fraction": 1.0}],
        maker_fee_rate=0.001,
        taker_fee_rate=0.002,
        extra_config={
            "stop_adjustments": [],
            "exit_plan": {"fixed_horizon": {"enabled": True, "bars": 2}},
        },
    )
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None
    assert position.fixed_horizon_bars == 2

    first = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        atr=2.0,
    )
    assert engine.step(first) == []
    assert position.is_active()

    second = Candle(
        time=datetime(2024, 1, 1, 2, tzinfo=timezone.utc),
        open=101.0,
        high=103.0,
        low=100.5,
        close=102.0,
        atr=2.0,
    )
    events = engine.step(second)

    horizon_events = [event for event in events if event["type"] == "fixed_horizon"]
    assert horizon_events
    assert any(event["type"] == "close" for event in events)
    assert engine.active_trade is None
    assert position.close_reason == "FIXED_HORIZON"
    assert horizon_events[0]["fee_type"] == "taker"
    assert horizon_events[0]["order_type"] == "market"


def test_target_exit_uses_maker_fee_and_stop_exit_uses_taker_fee():
    maker_engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[{"id": "tp-1", "ticks": 5, "size_fraction": 1.0}],
        maker_fee_rate=0.001,
        taker_fee_rate=0.002,
    )
    _enable_runtime_execution(maker_engine)
    entry = _build_candle(close=100.0, atr=2.0)
    target_position = maker_engine.maybe_enter(entry, "long")
    assert target_position is not None
    target_bar = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=106.0,
        low=101.0,
        close=105.0,
        atr=2.0,
    )

    target_events = maker_engine.step(target_bar)
    target = next(event for event in target_events if event["type"] == "target")
    assert target["fee_type"] == "maker"
    assert target["order_type"] == "limit_resting"

    taker_engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[{"id": "tp-1", "ticks": 100, "size_fraction": 1.0}],
        maker_fee_rate=0.001,
        taker_fee_rate=0.002,
        extra_config={"stop_adjustments": []},
    )
    _enable_runtime_execution(taker_engine)
    stop_position = taker_engine.maybe_enter(entry, "long")
    assert stop_position is not None
    stop_bar = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=95.0,
        close=96.0,
        atr=2.0,
    )

    stop_events = taker_engine.step(stop_bar)
    stop = next(event for event in stop_events if event["type"] == "stop")
    assert stop["fee_type"] == "taker"
    assert stop["order_type"] == "stop_market"


def test_rejected_stop_fill_keeps_position_open_without_terminal_metadata():
    engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[{"id": "tp-1", "ticks": 100, "size_fraction": 1.0}],
        extra_config={"stop_adjustments": []},
    )
    engine.attach_wallet_gateway(
        SharedWalletGateway(_wallet_proxy({"USD": 1_000_000.0}))
    )
    engine.attach_execution_adapter(_RejectSellAdapter())
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None
    stop_bar = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=101.0,
        low=95.0,
        close=96.0,
        atr=2.0,
    )

    events = engine.step(stop_bar)

    assert [event["type"] for event in events] == ["execution_rejected"]
    assert engine.active_trade is position
    assert position.is_active()
    assert position.close_reason is None
    assert position.reason_code is None
    assert position.serialize()["closed_at"] is None


def test_target_fill_status_and_quantity_change_bumps_trade_revision():
    engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[
            {"id": "tp-1", "ticks": 5, "size_fraction": 0.5},
            {"id": "tp-2", "ticks": 10, "size_fraction": 0.5},
        ],
    )
    _enable_runtime_execution(engine)
    entry = _build_candle(close=100.0, atr=2.0)
    position = engine.maybe_enter(entry, "long")
    assert position is not None
    revision_after_open = engine.trade_revision

    first_target = Candle(
        time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        open=100.0,
        high=106.0,
        low=101.0,
        close=105.0,
        atr=2.0,
    )
    events = engine.step(first_target)

    assert any(event["type"] == "target" for event in events)
    assert position.is_active()
    assert [leg.status for leg in position.legs] == ["target", "open"]
    assert engine.trade_revision == revision_after_open + 1


def test_partial_exit_preserves_residual_and_reuses_one_canonical_order() -> None:
    engine = _build_spot_engine(base_risk_per_trade=8)
    _enable_runtime_execution(engine)
    position = engine.maybe_enter(_build_candle(close=100.0, atr=2.0), "long")
    assert position is not None
    engine.drain_order_lifecycle_events()
    partial_adapter = _PartialSellAdapter([1.0, 1.0])
    engine.attach_execution_adapter(partial_adapter)
    position.execution_adapter = partial_adapter

    first_events = engine.step(
        Candle(
            time=datetime(2024, 1, 2, tzinfo=timezone.utc),
            open=110.0,
            high=111.0,
            low=109.0,
            close=110.0,
            atr=2.0,
        )
    )
    first_lifecycle = engine.drain_order_lifecycle_events()

    assert [event["contracts"] for event in first_events if event["type"] == "target"] == [1.0]
    assert position.legs[0].status == "open"
    assert position.legs[0].contracts == 1.0
    assert first_lifecycle[-1].state == CanonicalOrderState.PARTIALLY_FILLED
    assert first_lifecycle[-1].order_remaining_qty == 1.0
    request_id = first_lifecycle[-1].request_id

    second_events = engine.step(
        Candle(
            time=datetime(2024, 1, 3, tzinfo=timezone.utc),
            open=110.0,
            high=111.0,
            low=109.0,
            close=110.0,
            atr=2.0,
        )
    )
    second_lifecycle = engine.drain_order_lifecycle_events()

    assert [event["contracts"] for event in second_events if event["type"] == "target"] == [1.0]
    assert sum(event["contracts"] for event in first_events + second_events if event["type"] == "target") == 2.0
    assert second_lifecycle[-1].request_id == request_id
    assert second_lifecycle[-1].state == CanonicalOrderState.FILLED
    assert second_lifecycle[-1].order_cumulative_filled_qty == 2.0
    assert second_lifecycle[-1].order_remaining_qty == 0.0
    assert engine.active_trade is None


def test_take_profit_size_fractions_drive_integer_contract_allocation():
    engine = _build_spot_engine(
        base_risk_per_trade=40,
        take_profit_orders=[
            {"id": "tp-small", "ticks": 5, "size_fraction": 0.1},
            {"id": "tp-large", "ticks": 10, "size_fraction": 0.9},
        ],
    )
    _enable_runtime_execution(engine)
    position = engine.maybe_enter(_build_candle(close=100.0, atr=2.0), "long")

    assert position is not None
    expected = [("tp-small", 1.0), ("tp-large", 9.0)]
    assert [(leg.leg_id, leg.contracts) for leg in position.legs] == expected
    assert sum(leg.contracts for leg in position.legs) == 10.0


def test_pre_order_insufficient_margin_rejection_has_entry_request_identity():
    engine = _build_future_engine()
    engine.attach_wallet_gateway(SharedWalletGateway(_wallet_proxy({"USD": 0.0})))
    engine.run_id = "run-1"
    engine.last_signal_id = "signal-1"
    engine.last_decision_id = "decision-1"
    engine.strategy_id = "strategy-1"
    candle = _build_candle(close=110000.0, atr=100.0)

    request = engine.build_entry_request(candle, "long")

    assert request.validation.ok is False
    assert request.trade_id is None
    assert request.order_intent_id is None
    assert request.entry_request_id.startswith("entry_request:")
    assert request.validation.rejection_reason == "WALLET_INSUFFICIENT_MARGIN"
    assert request.validation.rejection_detail is not None
    assert request.validation.rejection_detail["entry_request_id"] == request.entry_request_id
    assert request.validation.rejection_detail["attempt_id"] == request.entry_request_id
    assert "wallet_commit_seq" not in request.validation.rejection_detail

    position = engine.entry_execution.submit_entry(candle, "long")

    assert position is None
    assert engine.last_rejection_reason == "WALLET_INSUFFICIENT_MARGIN"
    assert engine.last_rejection_detail is not None
    assert engine.last_rejection_detail["entry_request_id"] == request.entry_request_id
    assert engine.last_rejection_detail["attempt_id"] == request.entry_request_id
    assert engine.last_rejection_detail["wallet_commit_seq"] == 2
    assert engine.last_rejection_detail["wallet_commit_seq_status"] == "runtime_assigned"
    assert engine.last_rejection_detail["wallet_eval_seq"] == 1
    assert engine.last_rejection_detail["wallet_before"]["balances"]["USD"] == 0.0
    assert engine.last_rejection_detail["wallet_after"]["balances"]["USD"] == 0.0
    assert engine.last_rejection_detail["selected_quantity"] > 0.0
    assert engine.last_rejection_detail["required_margin"] > 0.0
    assert "trade_id" not in engine.last_rejection_detail


def test_entry_request_id_is_stable_for_same_decision_context_and_varies_by_decision():
    engine = _build_future_engine()
    engine.attach_wallet_gateway(SharedWalletGateway(_wallet_proxy({"USD": 0.0})))
    engine.run_id = "run-1"
    engine.last_signal_id = "signal-1"
    engine.last_decision_id = "decision-1"
    engine.strategy_id = "strategy-1"
    candle = _build_candle(close=110000.0, atr=100.0)

    first = engine.build_entry_request(candle, "long")
    second = engine.build_entry_request(candle, "long")

    engine.last_decision_id = "decision-2"
    third = engine.build_entry_request(candle, "long")
    engine.last_decision_id = "decision-1"
    engine.run_id = "run-2"
    fourth = engine.build_entry_request(candle, "long")

    assert first.entry_request_id == second.entry_request_id
    assert first.entry_request_id != third.entry_request_id
    assert first.entry_request_id != fourth.entry_request_id
    assert len({first.entry_request_id, third.entry_request_id, fourth.entry_request_id}) == 3


def test_apply_entry_fill_accumulates_partial_fills():
    engine = _build_spot_engine(base_risk_per_trade=8)
    candle = _build_candle(close=100.0, atr=2.0)
    request = engine.build_entry_request(candle, "long")
    pending = _build_pending(request)

    fill_one = EntryFill(
        order_intent_id=str(request.order_intent_id),
        trade_id=str(request.trade_id),
        candle=_snapshot(candle),
        filled_qty=1.0,
        fill_price=100.0,
        fee_paid=0.05,
        liquidity_role="taker",
        fill_time="t1",
        raw={"outcome": {}},
    )
    result_one = engine.apply_entry_fill(request=request, pending=pending, fill=fill_one)

    assert result_one.status == "pending"
    assert result_one.pending is not None
    assert result_one.pending.filled_qty == 1.0
    assert result_one.pending.filled_notional == 100.0

    fill_two = EntryFill(
        order_intent_id=str(request.order_intent_id),
        trade_id=str(request.trade_id),
        candle=_snapshot(candle),
        filled_qty=1.0,
        fill_price=110.0,
        fee_paid=0.05,
        liquidity_role="taker",
        fill_time="t2",
        raw={"outcome": {}},
    )
    result_two = engine.apply_entry_fill(request=request, pending=result_one.pending, fill=fill_two)

    assert result_two.status == "opened"
    assert result_two.position is not None
    assert result_two.position.entry_price == 105.0
    assert result_two.position.fees_paid == 0.1


def test_apply_entry_fill_opens_position_with_expected_stop():
    engine = _build_spot_engine(base_risk_per_trade=8)
    engine.last_signal_id = "signal-1"
    engine.last_decision_id = "decision-1"
    engine.strategy_id = "strategy-1"
    candle = _build_candle(close=100.0, atr=2.0)
    request = engine.build_entry_request(candle, "long")
    pending = _build_pending(request)
    fill = EntryFill(
        order_intent_id=str(request.order_intent_id),
        trade_id=str(request.trade_id),
        candle=_snapshot(candle),
        filled_qty=request.requested_qty,
        fill_price=100.0,
        fee_paid=0.2,
        liquidity_role="taker",
        fill_time="t1",
        raw={"outcome": {}},
    )

    result = engine.apply_entry_fill(request=request, pending=pending, fill=fill)

    assert result.status == "opened"
    assert result.position is not None
    assert result.position.entry_price == 100.0
    assert result.position.fees_paid == 0.2
    assert result.position.stop_price == engine._calculate_stop_price(100.0, "long", request.r_ticks)
    assert result.position.bar_time == candle.time
    assert result.position.signal_id == "signal-1"
    assert result.position.decision_id == "decision-1"
    serialized = result.position.serialize()
    assert serialized["bar_time"] == "2024-01-01T00:00:00Z"
    assert serialized["strategy_id"] == "strategy-1"
    assert serialized["signal_id"] == "signal-1"
    assert serialized["decision_id"] == "decision-1"


def test_entry_settlement_reservation_released_when_position_build_rejects(monkeypatch):
    proxy = _wallet_proxy({"USD": 1_000_000.0})
    engine = _build_spot_engine(base_risk_per_trade=8)
    engine.attach_wallet_gateway(SharedWalletGateway(proxy))
    engine.attach_execution_adapter(_FillAdapter())
    candle = _build_candle(close=100.0, atr=2.0)
    request = engine.build_entry_request(candle, "long")
    pending = _build_pending(request)
    fill = EntryFill(
        order_intent_id=str(request.order_intent_id),
        trade_id=str(request.trade_id),
        candle=_snapshot(candle),
        filled_qty=request.requested_qty,
        fill_price=100.0,
        fee_paid=0.2,
        liquidity_role="taker",
        fill_time="t1",
        raw={"outcome": {}},
    )
    monkeypatch.setattr(engine, "_build_legs", lambda *args, **kwargs: [])

    result = engine.apply_entry_fill(request=request, pending=pending, fill=fill)

    assert result.status == "rejected"
    assert result.rejection_reason == "TP_LEGS_EMPTY"
    reservations = dict(proxy["reservations"])
    assert reservations
    assert {payload["status"] for payload in reservations.values()} == {"RELEASED"}
    assert engine._wallet_fill_metadata_by_trade == {}


def test_submit_entry_uses_facade_only(monkeypatch):
    engine = _build_spot_engine()
    candle = _build_candle(close=100.0, atr=2.0)
    intent = ExecutionIntent(
        order_id="order-1",
        side="buy",
        qty=1.0,
        symbol="TEST-SPOT",
        order_type="market",
        requested_price=float(candle.close),
        limit_params=None,
        metadata={"direction": "long", "symbol": "TEST-SPOT"},
    )
    request = EntryRequest(
        trade_id="trade-1",
        order_intent_id="order-1",
        entry_request_id="entry_request:test",
        direction="long",
        requested_qty=1.0,
        qty_raw=1.0,
        r_ticks=4.0,
        r_value=None,
        atr_at_entry=candle.atr,
        r_multiple_at_entry=engine.r_multiple,
        order_type="market",
        limit_params=None,
        side="buy",
        requested_price=float(candle.close),
        intent=intent,
        validation=EntryValidation(ok=True),
        margin_info=None,
        was_margin_capped=False,
    )
    monkeypatch.setattr(engine, "build_entry_request", lambda *_args, **_kwargs: request)

    def _fail(*_args, **_kwargs):
        raise AssertionError("unexpected sizing call")

    monkeypatch.setattr(engine, "_compute_r_ticks", _fail)
    monkeypatch.setattr(engine, "_calculate_total_contracts", _fail)
    monkeypatch.setattr(engine, "_cap_qty_by_margin", _fail)
    monkeypatch.setattr(engine, "_build_limit_params", _fail)

    class DummyModel:
        def evaluate(self, _intent, *, candle_high, candle_low, candle_close, candle_open):
            outcome = ExecutionOutcome(
                order_id=_intent.order_id,
                status="filled",
                filled_qty=float(_intent.qty),
                avg_fill_price=float(candle_close),
                fee_paid=0.0,
                fee_role="taker",
                fee_rate=0.0,
                fee_source="test",
                fee_version="test",
                created_at="now",
                updated_at="now",
                filled_at="now",
                remaining_qty=0.0,
                fallback_applied=False,
                fallback_reason=None,
                limit_price=None,
                validity_window=None,
                metadata=dict(_intent.metadata),
            )
            return outcome, None

    def _fail(*_args, **_kwargs):
        raise AssertionError("unexpected leg construction")

    monkeypatch.setattr(engine, "_build_legs", _fail)

    called = {"apply": False}

    def _apply(*, request, pending, fill):
        called["apply"] = True
        return EntryFillResult(
            status="opened",
            pending=None,
            position=None,
            events=[],
            settlement_payloads=[],
        )

    monkeypatch.setattr(engine, "apply_entry_fill", _apply)

    engine.attach_execution_model(DummyModel())
    engine.entry_execution.submit_entry(candle, "long")
    assert called["apply"] is True
