"""Entry execution coordinator for bot runtime."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Optional, TYPE_CHECKING

from utils.log_context import build_log_context, merge_log_context, with_log_context
from .execution_intent import ExecutionIntent, ExecutionOutcome
from .entry_settlement import EntrySettlementContext
from .execution_intent import LimitParams
if TYPE_CHECKING:
    from .domain import Candle, LadderPosition, LadderRiskEngine

logger = logging.getLogger(__name__)

@dataclass
class PendingEntry:
    """Track pending entry intents awaiting fills."""

    intent: ExecutionIntent
    direction: str
    qty_raw: float
    requested_qty: float
    r_ticks: float
    r_value: Optional[float]
    atr_at_entry: Optional[float]
    r_multiple_at_entry: Optional[float]
    order_intent_id: str
    trade_id: str
    validity_remaining: int
    fallback: str


class EntryExecutionCoordinator:
    """Coordinate entry execution lifecycle for ladder risk engine."""

    def __init__(self, engine: "LadderRiskEngine") -> None:
        self._engine = engine
        self.pending_entry: Optional[PendingEntry] = None

    @property
    def has_pending(self) -> bool:
        return self.pending_entry is not None

    def submit_entry(self, candle: Candle, direction: str) -> Optional[LadderPosition]:
        engine = self._engine
        atr_at_entry = candle.atr if engine._has_valid_atr(candle.atr) else None
        r_ticks = engine._compute_r_ticks(candle)

        r_value = engine._r_value(candle)
        if engine.stop_r_multiple not in (None, 0) and r_value not in (None, 0):
            r_value = float(engine.stop_r_multiple) * float(r_value)

        risk_based_qty = engine._calculate_total_contracts(r_ticks)
        capped_qty, was_margin_capped, margin_info = engine._cap_qty_by_margin(
            risk_qty=risk_based_qty,
            price=candle.close,
            direction=direction,
        )

        if margin_info and margin_info.get("reason") == "margin_calculation_failed":
            engine.last_rejection_reason = "MARGIN_CALCULATION_FAILED"
            engine.last_rejection_detail = margin_info
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason="MARGIN_CALCULATION_FAILED",
                error=margin_info.get("error"),
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        if capped_qty <= 0:
            engine.last_rejection_reason = "QTY_CAPPED_TO_ZERO"
            engine.last_rejection_detail = margin_info or {"risk_qty": risk_based_qty}
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason="QTY_CAPPED_TO_ZERO",
                risk_qty=risk_based_qty,
                capped_qty=capped_qty,
                was_margin_capped=was_margin_capped,
                price=candle.close,
                direction=direction,
                margin_reason=margin_info.get("reason") if margin_info else None,
                margin_error=margin_info.get("error") if margin_info else None,
                available_collateral=margin_info.get("available_collateral") if margin_info else None,
                max_qty_by_margin=margin_info.get("max_qty_by_margin") if margin_info else None,
                cost_per_contract=margin_info.get("cost_per_contract") if margin_info else None,
                margin_per_contract=margin_info.get("margin_per_contract") if margin_info else None,
                fee_per_contract=margin_info.get("fee_per_contract") if margin_info else None,
                margin_rate=margin_info.get("margin_rate") if margin_info else None,
                margin_method=margin_info.get("calculation_method") if margin_info else None,
                balance_trace=margin_info.get("balance_trace") if margin_info else None,
                qty_step=engine.qty_step,
                min_qty=engine.min_qty,
                max_qty=engine.max_qty,
                min_notional=engine.min_notional,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        order_intent_id = engine._new_order_intent_id()
        trade_id = engine._new_trade_id()
        qty_raw = capped_qty
        requested_qty = capped_qty
        normalization = engine._normalize_qty(requested_qty)
        if not normalization.ok:
            engine.last_rejection_reason = normalization.rejected_reason or "QTY_CONSTRAINT_FAILED"
            engine.last_rejection_detail = normalization.to_log_dict()
            context = merge_log_context(
                build_log_context(
                    symbol=engine.instrument.get("symbol"),
                    reason=engine.last_rejection_reason,
                ),
                build_log_context(**normalization.to_log_dict()),
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None
        requested_qty = float(normalization.qty_final)

        order_type = "market" if engine.execution_mode != "limit_maker" else "limit_maker"
        limit_params: Optional[LimitParams] = None
        if order_type == "limit_maker":
            limit_params = engine._build_limit_params(candle, direction=direction, r_value=r_value)

        side = "buy" if direction == "long" else "sell"
        intent = ExecutionIntent(
            order_id=order_intent_id,
            side=side,
            qty=requested_qty,
            symbol=str(engine.instrument.get("symbol") or ""),
            order_type=order_type,
            requested_price=float(candle.close),
            limit_params=limit_params,
            metadata={
                "direction": direction,
                "symbol": engine.instrument.get("symbol"),
            },
        )

        execution_model = engine._resolve_execution_model()
        outcome, rejection = execution_model.evaluate(
            intent,
            candle_high=candle.high,
            candle_low=candle.low,
            candle_close=candle.close,
            candle_open=candle.open,
        )
        if rejection:
            engine.last_rejection_reason = rejection.reason
            engine.last_rejection_detail = {"requested_qty": requested_qty, **(rejection.metadata or {})}
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason=rejection.reason,
                requested_qty=requested_qty,
                order_type=order_type,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        if outcome.status == "open":
            validity_remaining = limit_params.validity_window if limit_params else 1
            pending = PendingEntry(
                intent=intent,
                direction=direction,
                qty_raw=qty_raw,
                requested_qty=requested_qty,
                r_ticks=float(r_ticks),
                r_value=r_value,
                atr_at_entry=atr_at_entry,
                r_multiple_at_entry=engine.r_multiple,
                order_intent_id=order_intent_id,
                trade_id=trade_id,
                validity_remaining=max(int(validity_remaining) - 1, 0),
                fallback=limit_params.fallback if limit_params else "cancel",
            )
            if pending.validity_remaining <= 0:
                return self._apply_entry_fallback(candle, pending, outcome)
            self.pending_entry = pending
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                order_id=order_intent_id,
                order_type=order_type,
                limit_price=outcome.limit_price,
                validity_remaining=pending.validity_remaining,
                fallback=pending.fallback,
            )
            logger.info(with_log_context("entry_order_pending", context))
            return None

        if outcome.status != "filled":
            engine.last_rejection_reason = "ENTRY_NOT_FILLED"
            engine.last_rejection_detail = {"status": outcome.status}
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                order_id=order_intent_id,
                status=outcome.status,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        pending = PendingEntry(
            intent=intent,
            direction=direction,
            qty_raw=qty_raw,
            requested_qty=requested_qty,
            r_ticks=float(r_ticks),
            r_value=r_value,
            atr_at_entry=atr_at_entry,
            r_multiple_at_entry=engine.r_multiple,
            order_intent_id=order_intent_id,
            trade_id=trade_id,
            validity_remaining=0,
            fallback=limit_params.fallback if limit_params else "cancel",
        )
        return self._finalize_entry(candle, pending, outcome)

    def process_pending(self, candle: Candle) -> Optional[LadderPosition]:
        if not self.pending_entry:
            return None
        pending = self.pending_entry
        engine = self._engine
        execution_model = engine._resolve_execution_model()
        outcome, rejection = execution_model.evaluate(
            pending.intent,
            candle_high=candle.high,
            candle_low=candle.low,
            candle_close=candle.close,
            candle_open=candle.open,
        )
        if rejection:
            engine.last_rejection_reason = rejection.reason
            engine.last_rejection_detail = rejection.metadata
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason=rejection.reason,
                order_id=pending.order_intent_id,
            )
            logger.warning(with_log_context("entry_rejected", context))
            self.pending_entry = None
            return None
        if outcome.status == "filled":
            self.pending_entry = None
            return self._finalize_entry(candle, pending, outcome)
        if outcome.status == "open":
            pending.validity_remaining -= 1
            if pending.validity_remaining <= 0:
                self.pending_entry = None
                return self._apply_entry_fallback(candle, pending, outcome)
            self.pending_entry = pending
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                order_id=pending.order_intent_id,
                remaining=pending.validity_remaining,
                status=outcome.status,
            )
            logger.info(with_log_context("entry_order_pending", context))
            return None
        self.pending_entry = None
        engine.last_rejection_reason = "ENTRY_NOT_FILLED"
        engine.last_rejection_detail = {"status": outcome.status}
        context = build_log_context(
            symbol=engine.instrument.get("symbol"),
            order_id=pending.order_intent_id,
            status=outcome.status,
        )
        logger.warning(with_log_context("entry_rejected", context))
        return None

    def _finalize_entry(
        self,
        candle: Candle,
        pending: PendingEntry,
        outcome: ExecutionOutcome,
    ) -> Optional[LadderPosition]:
        engine = self._engine
        entry_price = float(outcome.avg_fill_price or candle.close)
        filled_qty = float(outcome.filled_qty or 0.0)
        notional = abs(entry_price * engine.contract_size * filled_qty)
        if engine.min_notional not in (None, 0) and notional < float(engine.min_notional):
            engine.last_rejection_reason = "MIN_NOTIONAL_NOT_MET"
            engine.last_rejection_detail = {
                "notional": notional,
                "min_notional": engine.min_notional,
            }
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason="MIN_NOTIONAL_NOT_MET",
                notional=round(notional, 4),
                min_notional=engine.min_notional,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        base_currency, quote_currency = engine._resolve_base_quote()
        side = pending.intent.side
        use_wallet_execution = bool(engine.execution_adapter and engine._wallet_gateway)
        if use_wallet_execution:
            settled = engine.entry_settlement.apply_entry_fill(
                EntrySettlementContext(
                    side=side,
                    filled_qty=filled_qty,
                    entry_price=entry_price,
                    notional=notional,
                    fee_paid=outcome.fee_paid,
                    trade_id=pending.trade_id,
                    direction=pending.direction,
                    qty_raw=pending.qty_raw,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                )
            )
            if not settled:
                return None

        stop_price = engine._calculate_stop_price(entry_price, pending.direction, pending.r_ticks)
        legs = engine._build_legs(
            candle,
            pending.direction,
            pending.r_ticks,
            filled_qty,
            entry_price=entry_price,
            qty_raw=pending.qty_raw,
            qty_final=filled_qty,
            order_intent_id=pending.order_intent_id,
            side=side,
        )
        if not legs:
            rounded_qty = (
                engine._floor_to_step(pending.requested_qty, engine.qty_step)
                if engine.qty_step not in (None, 0)
                else pending.requested_qty
            )
            rejection_reason = "QTY_ROUNDS_TO_ZERO" if rounded_qty <= 0 else "TP_LEGS_EMPTY"
            engine.last_rejection_reason = rejection_reason
            engine.last_rejection_detail = {
                "requested_qty": pending.requested_qty,
                "rounded_qty": rounded_qty,
                "symbol": engine.instrument.get("symbol"),
                "qty_step": engine.qty_step,
                "min_qty": engine.min_qty,
                "min_notional": engine.min_notional,
                "tp_leg_count": len(engine.orders),
                "tp_allocation": engine._last_tp_allocation,
            }
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason=rejection_reason,
                requested_qty=pending.requested_qty,
                rounded_qty=rounded_qty,
                qty_step=engine.qty_step,
                min_qty=engine.min_qty,
                min_notional=engine.min_notional,
                tp_leg_count=len(engine.orders),
                tp_allocation=engine._last_tp_allocation,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        runtime_stop_adjustments = engine._build_stop_adjustments(legs, pending.r_ticks)
        breakeven_ticks = 0.0 if runtime_stop_adjustments else engine._breakeven_threshold(legs, pending.r_ticks)
        trailing_activation_ticks = engine._trailing_activation_ticks(legs, pending.r_ticks)
        trailing_distance_ticks = engine._trailing_distance_ticks(pending.atr_at_entry)
        engine.trailing_config = (
            engine.template.get("trailing_stop") if isinstance(engine.template.get("trailing_stop"), dict) else {}
        )
        engine._last_tp_allocation = None

        from .domain import LadderPosition

        position = LadderPosition(
            entry_time=candle.time,
            entry_price=entry_price,
            entry_order=asdict(pending.intent),
            entry_outcome=asdict(outcome),
            direction=pending.direction,
            stop_price=stop_price,
            tick_size=engine.tick_size,
            execution_model=engine.execution_model if use_wallet_execution else None,
            execution_adapter=engine.execution_adapter if use_wallet_execution else None,
            wallet_gateway=engine._wallet_gateway if use_wallet_execution else None,
            exit_settlement=engine.exit_settlement,
            base_currency=base_currency,
            quote_currency_code=quote_currency,
            legs=legs,
            breakeven_trigger_ticks=breakeven_ticks,
            tick_value=engine.tick_value,
            contract_size=engine.contract_size,
            maker_fee_rate=engine.maker_fee,
            taker_fee_rate=engine.taker_fee,
            quote_currency=engine.quote_currency,
            short_requires_borrow=bool(engine.short_requires_borrow),
            instrument=engine.instrument if use_wallet_execution else None,
            atr_at_entry=pending.atr_at_entry,
            r_multiple_at_entry=pending.r_multiple_at_entry,
            r_value=pending.r_value,
            r_ticks=pending.r_ticks,
            trailing_activation_ticks=trailing_activation_ticks,
            trailing_distance_ticks=trailing_distance_ticks,
            trailing_atr_multiple=float(engine.trailing_config.get("atr_multiplier") or 0.0),
            pre_entry_context=getattr(candle, "lookback_15", None),
            stop_adjustments=runtime_stop_adjustments,
            trade_id=pending.trade_id,
        )
        position.apply_entry_fee(outcome.fee_paid)
        return position

    def _apply_entry_fallback(
        self,
        candle: Candle,
        pending: PendingEntry,
        outcome: ExecutionOutcome,
    ) -> Optional[LadderPosition]:
        engine = self._engine
        fallback = pending.fallback
        if fallback == "convert_to_market":
            market_intent = ExecutionIntent(
                order_id=engine._new_order_intent_id(),
                side=pending.intent.side,
                qty=pending.intent.qty,
                symbol=pending.intent.symbol,
                order_type="market",
                requested_price=float(candle.close),
                limit_params=None,
                metadata=dict(pending.intent.metadata),
            )
            execution_model = engine._resolve_execution_model()
            market_outcome, rejection = execution_model.evaluate(
                market_intent,
                candle_high=candle.high,
                candle_low=candle.low,
                candle_close=candle.close,
                candle_open=candle.open,
            )
            if rejection:
                engine.last_rejection_reason = rejection.reason
                engine.last_rejection_detail = rejection.metadata
                context = build_log_context(
                    symbol=engine.instrument.get("symbol"),
                    reason=rejection.reason,
                    order_id=market_intent.order_id,
                    fallback="convert_to_market",
                )
                logger.warning(with_log_context("entry_fallback_rejected", context))
                return None
            market_outcome = ExecutionOutcome(
                **{
                    **asdict(market_outcome),
                    "fallback_applied": True,
                    "fallback_reason": "convert_to_market",
                }
            )
            return self._finalize_entry(candle, pending, market_outcome)

        outcome_payload = ExecutionOutcome(
            **{
                **asdict(outcome),
                "status": "expired",
                "fallback_applied": True,
                "fallback_reason": fallback,
            }
        )
        engine.last_rejection_reason = "ENTRY_UNFILLED"
        engine.last_rejection_detail = asdict(outcome_payload)
        context = build_log_context(
            symbol=engine.instrument.get("symbol"),
            order_id=pending.order_intent_id,
            status="expired",
            fallback=fallback,
        )
        logger.warning(with_log_context("entry_order_expired", context))
        return None


__all__ = ["EntryExecutionCoordinator", "PendingEntry"]
