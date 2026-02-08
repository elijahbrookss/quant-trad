from datetime import datetime, timezone
from typing import Optional

from engines.bot_runtime.core import EntryFill, EntryFillResult, PendingEntry
from engines.bot_runtime.core.domain import Candle, EntryRequest, EntryValidation, LadderRiskEngine
from engines.bot_runtime.core.execution import FillRejection
from engines.bot_runtime.core.execution_intent import ExecutionIntent, ExecutionOutcome
from engines.bot_runtime.core.wallet import WalletLedger


def _build_spot_engine(
    *,
    execution_mode: str = "market",
    limit_maker: Optional[dict] = None,
    base_risk_per_trade: float = 100,
) -> LadderRiskEngine:
    config = {
        "tick_size": 1.0,
        "contract_size": 1.0,
        "tick_value": 1.0,
        "initial_stop": {"atr_multiplier": 2.0},
        "risk": {"base_risk_per_trade": base_risk_per_trade},
        "take_profit_orders": [{"id": "tp-1", "ticks": 10}],
        "execution_mode": execution_mode,
    }
    if limit_maker is not None:
        config["limit_maker"] = limit_maker
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
    return LadderRiskEngine(config=config, instrument=instrument)


def _build_future_engine() -> LadderRiskEngine:
    config = {
        "tick_size": 5.0,
        "contract_size": 0.01,
        "tick_value": 0.05,
        "initial_stop": {"atr_multiplier": 1.0},
        "risk": {"base_risk_per_trade": 1000},
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
    return LadderRiskEngine(config=config, instrument=instrument)


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
    ledger = WalletLedger()
    ledger.deposit({"USD": 500})
    engine.attach_wallet(ledger)
    candle = _build_candle(close=110000.0, atr=100.0)

    request = engine.build_entry_request(candle, "long")
    assert request.was_margin_capped is True

    position = engine.entry_execution.submit_entry(candle, "long")

    assert position is not None
    assert position.entry_order["qty"] == request.requested_qty


def test_apply_entry_fill_accumulates_partial_fills():
    engine = _build_spot_engine(base_risk_per_trade=8)
    candle = _build_candle(close=100.0, atr=2.0)
    request = engine.build_entry_request(candle, "long")
    pending = _build_pending(request)

    fill_one = EntryFill(
        order_intent_id=str(request.order_intent_id),
        trade_id=str(request.trade_id),
        candle_time=candle.time,
        candle_open=candle.open,
        candle_high=candle.high,
        candle_low=candle.low,
        candle_close=candle.close,
        candle_atr=candle.atr,
        candle_lookback_15=candle.lookback_15,
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
        candle_time=candle.time,
        candle_open=candle.open,
        candle_high=candle.high,
        candle_low=candle.low,
        candle_close=candle.close,
        candle_atr=candle.atr,
        candle_lookback_15=candle.lookback_15,
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
    candle = _build_candle(close=100.0, atr=2.0)
    request = engine.build_entry_request(candle, "long")
    pending = _build_pending(request)
    fill = EntryFill(
        order_intent_id=str(request.order_intent_id),
        trade_id=str(request.trade_id),
        candle_time=candle.time,
        candle_open=candle.open,
        candle_high=candle.high,
        candle_low=candle.low,
        candle_close=candle.close,
        candle_atr=candle.atr,
        candle_lookback_15=candle.lookback_15,
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
