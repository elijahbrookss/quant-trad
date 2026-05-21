from __future__ import annotations

from datetime import datetime, timezone

from engines.bot_runtime.core import CandleSnapshot, EntryFill, PendingEntry
from engines.bot_runtime.core.domain import Candle, LadderRiskEngine


class RuntimeScenarioBuilder:
    @staticmethod
    def spot_engine(*, execution_mode: str = "market", limit_maker: dict | None = None, base_risk_per_trade: float = 10.0) -> LadderRiskEngine:
        config = {
            "tick_size": 1.0,
            "contract_size": 1.0,
            "tick_value": 1.0,
            "initial_stop": {"atr_multiplier": 2.0},
            "take_profit_orders": [{"id": "tp-1", "ticks": 10}],
            "execution_mode": execution_mode,
        }
        if limit_maker:
            config["limit_maker"] = limit_maker
        instrument = {
            "symbol": "BTC-USD",
            "venue": "coinbase",
            "instrument_type": "spot",
            "tick_size": 1.0,
            "contract_size": 1.0,
            "tick_value": 1.0,
            "min_order_size": 1,
            "base_currency": "BTC",
            "quote_currency": "USD",
            "metadata": {"info": {"base_increment": "1"}},
        }
        return LadderRiskEngine(
            config=config,
            instrument=instrument,
            risk_config={"base_risk_per_trade": base_risk_per_trade},
        )

    @staticmethod
    def candle(*, close: float, atr: float = 2.0) -> Candle:
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        return Candle(time=ts, open=close, high=close + 1.0, low=close - 1.0, close=close, atr=atr)

    @staticmethod
    def snapshot(candle: Candle) -> CandleSnapshot:
        return CandleSnapshot(time=candle.time, open=candle.open, high=candle.high, low=candle.low, close=candle.close, atr=candle.atr)

    @staticmethod
    def pending_for(request, *, validity_remaining: int = 1) -> PendingEntry:
        return PendingEntry(
            request=request,
            intent=request.intent,
            direction=request.direction,
            qty_raw=request.qty_raw,
            requested_qty=request.requested_qty,
            r_ticks=request.r_ticks,
            r_value=request.r_value,
            atr_at_entry=request.atr_at_entry,
            r_multiple_at_entry=request.r_multiple_at_entry,
            order_intent_id=str(request.order_intent_id),
            trade_id=str(request.trade_id),
            validity_remaining=validity_remaining,
            fallback=request.limit_params.fallback if request.limit_params else "cancel",
            remaining_qty=float(request.requested_qty),
        )

    @staticmethod
    def fill_for(request, candle: Candle, *, qty: float, price: float, fill_time: str) -> EntryFill:
        return EntryFill(
            order_intent_id=str(request.order_intent_id),
            trade_id=str(request.trade_id),
            candle=RuntimeScenarioBuilder.snapshot(candle),
            filled_qty=qty,
            fill_price=price,
            fee_paid=0.01,
            liquidity_role="taker",
            fill_time=fill_time,
            raw={"provider": "coinbase"},
        )
