"""Entry execution coordinator for bot runtime."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Optional, TYPE_CHECKING

from utils.log_context import build_log_context, with_log_context
from .execution_intent import ExecutionIntent, ExecutionOutcome
if TYPE_CHECKING:
    from .domain import Candle, EntryFill, EntryFillResult, EntryRequest, LadderPosition, LadderRiskEngine

logger = logging.getLogger(__name__)

@dataclass
class PendingEntry:
    """Track pending entry intents awaiting fills."""

    request: "EntryRequest"
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
    filled_qty: float = 0.0
    filled_notional: float = 0.0
    fees_paid: float = 0.0
    remaining_qty: float = 0.0


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
        request = engine.build_entry_request(candle, direction)
        if not request.validation.ok:
            engine.last_rejection_reason = request.validation.rejection_reason
            engine.last_rejection_detail = self._entry_rejection_detail(
                request,
                request.validation.rejection_detail,
            )
            return None

        intent = request.intent
        if intent is None:
            engine.last_rejection_reason = "ENTRY_REQUEST_INVALID"
            engine.last_rejection_detail = self._entry_rejection_detail(request, {"reason": "intent_missing"})
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason="ENTRY_REQUEST_INVALID",
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

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
            engine.last_rejection_detail = self._entry_rejection_detail(
                request,
                {"requested_qty": request.requested_qty, **(rejection.metadata or {})},
            )
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason=rejection.reason,
                requested_qty=request.requested_qty,
                order_type=request.order_type,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        def build_pending(validity_remaining: int) -> PendingEntry:
            remaining_qty = float(request.requested_qty)
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
                remaining_qty=remaining_qty,
            )

        if outcome.status == "open":
            validity_remaining = request.limit_params.validity_window if request.limit_params else 1
            pending = build_pending(max(int(validity_remaining) - 1, 0))
            if pending.validity_remaining <= 0:
                return self._apply_entry_fallback(candle, pending, outcome)
            self.pending_entry = pending
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                order_id=request.order_intent_id,
                order_type=request.order_type,
                limit_price=outcome.limit_price,
                validity_remaining=pending.validity_remaining,
                fallback=pending.fallback,
            )
            logger.info(with_log_context("entry_order_pending", context))
            return None

        if outcome.status != "filled":
            engine.last_rejection_reason = "ENTRY_NOT_FILLED"
            engine.last_rejection_detail = self._entry_rejection_detail(request, {"status": outcome.status})
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                order_id=request.order_intent_id,
                status=outcome.status,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        pending = build_pending(0)
        fill = self._build_entry_fill(
            pending=pending,
            outcome=outcome,
            candle=candle,
        )
        result = engine.apply_entry_fill(request=request, pending=pending, fill=fill)
        return self._apply_fill_result(result)

    def process_pending(self, candle: Candle) -> Optional[LadderPosition]:
        if not self.pending_entry:
            return None
        pending = self.pending_entry
        request = pending.request
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
            engine.last_rejection_detail = self._entry_rejection_detail(request, rejection.metadata)
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason=rejection.reason,
                order_id=pending.order_intent_id,
            )
            logger.warning(with_log_context("entry_rejected", context))
            self.pending_entry = None
            return None
        if outcome.status == "filled":
            fill = self._build_entry_fill(pending=pending, outcome=outcome, candle=candle)
            result = engine.apply_entry_fill(request=request, pending=pending, fill=fill)
            return self._apply_fill_result(result)
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
        engine.last_rejection_detail = self._entry_rejection_detail(request, {"status": outcome.status})
        context = build_log_context(
            symbol=engine.instrument.get("symbol"),
            order_id=pending.order_intent_id,
            status=outcome.status,
        )
        logger.warning(with_log_context("entry_rejected", context))
        return None

    def _build_entry_fill(
        self,
        *,
        pending: PendingEntry,
        outcome: ExecutionOutcome,
        candle: Candle,
    ) -> "EntryFill":
        engine = self._engine
        return engine.build_entry_fill(
            pending=pending,
            outcome=outcome,
            candle=candle,
        )

    def _apply_fill_result(self, result: "EntryFillResult") -> Optional[LadderPosition]:
        engine = self._engine
        if result.rejection_reason:
            engine.last_rejection_reason = result.rejection_reason
            engine.last_rejection_detail = result.rejection_detail
        if result.status == "pending":
            self.pending_entry = result.pending
            return None
        if result.status == "opened":
            self.pending_entry = None
            return result.position
        self.pending_entry = None
        return None

    @staticmethod
    def _entry_rejection_detail(request: "EntryRequest", metadata: Optional[dict]) -> dict:
        detail = dict(metadata or {})
        detail.setdefault("entry_request_id", request.entry_request_id)
        detail.setdefault("attempt_id", request.entry_request_id)
        if request.order_intent_id:
            detail.setdefault("order_request_id", str(request.order_intent_id))
        return detail

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
                engine.last_rejection_detail = self._entry_rejection_detail(pending.request, rejection.metadata)
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
            fill = engine.build_entry_fill(
                pending=pending,
                outcome=market_outcome,
                candle=candle,
            )
            result = engine.apply_entry_fill(request=pending.request, pending=pending, fill=fill)
            return self._apply_fill_result(result)

        outcome_payload = ExecutionOutcome(
            **{
                **asdict(outcome),
                "status": "expired",
                "fallback_applied": True,
                "fallback_reason": fallback,
            }
        )
        engine.last_rejection_reason = "ENTRY_UNFILLED"
        engine.last_rejection_detail = self._entry_rejection_detail(pending.request, asdict(outcome_payload))
        context = build_log_context(
            symbol=engine.instrument.get("symbol"),
            order_id=pending.order_intent_id,
            status="expired",
            fallback=fallback,
        )
        logger.warning(with_log_context("entry_order_expired", context))
        return None


__all__ = ["EntryExecutionCoordinator", "PendingEntry"]
