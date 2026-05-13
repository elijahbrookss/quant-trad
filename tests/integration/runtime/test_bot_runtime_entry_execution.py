from datetime import datetime, timezone
import threading
from typing import Optional

from engines.bot_runtime.core import CandleSnapshot, EntryFill, EntryFillResult, PendingEntry
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
) -> LadderRiskEngine:
    config = {
        "tick_size": 1.0,
        "contract_size": 1.0,
        "tick_value": 1.0,
        "initial_stop": {"atr_multiplier": 2.0},
        "take_profit_orders": take_profit_orders or [{"id": "tp-1", "ticks": 10}],
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
        "tick_size": 5.0,
        "contract_size": 0.01,
        "tick_value": 0.05,
        "initial_stop": {"atr_multiplier": 1.0},
        "take_profit_orders": [{"id": "tp-1", "ticks": 10}],
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
    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool,
    ):
        _ = enforce_price_tick
        notional = executed_notional(price=price, quantity=requested_qty, contract_size=1.0)
        return (
            FillResult(
                filled_qty=float(requested_qty),
                fill_price=float(price),
                notional=notional,
                fee=executed_fee(price=price, quantity=requested_qty, contract_size=1.0, fee_rate=fee_rate),
                fee_rate=float(fee_rate or 0.0),
                side=side,
                metadata={"source": "test"},
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
    assert pending.validity_remaining == 1
    assert pending.fallback == "convert_to_market"
    assert pending.intent.order_type == "limit_maker"


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


def test_target_fill_status_and_quantity_change_bumps_trade_revision():
    engine = _build_spot_engine(
        base_risk_per_trade=8,
        take_profit_orders=[
            {"id": "tp-1", "ticks": 5},
            {"id": "tp-2", "ticks": 10},
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
