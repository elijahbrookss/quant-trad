"""Entry settlement service for wallet accounting."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Protocol, TYPE_CHECKING

from utils.log_context import build_log_context, merge_log_context, with_log_context
from .wallet import wallet_required_reservation

if TYPE_CHECKING:
    from .domain import LadderRiskEngine

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntrySettlementContext:
    """Context payload for entry settlement."""

    side: str
    filled_qty: float
    entry_price: float
    notional: float
    fee_paid: float
    trade_id: str
    direction: str
    qty_raw: float
    base_currency: str
    quote_currency: str


class EntrySettlement(Protocol):
    """Protocol for entry settlement implementations."""

    def apply_entry_fill(self, context: EntrySettlementContext) -> bool:
        ...


class EntrySettlementService:
    """Apply wallet settlement for entry fills."""

    def __init__(self, engine: "LadderRiskEngine") -> None:
        self._engine = engine

    def apply_entry_fill(self, context: EntrySettlementContext) -> bool:
        engine = self._engine
        if not (engine.execution_adapter and engine._wallet_gateway):
            return True
        correlation_id = f"trade:{context.trade_id}"
        allowed, reason, payload = engine._wallet_gateway.can_apply(
            side=context.side,
            base_currency=context.base_currency,
            quote_currency=context.quote_currency,
            qty=context.filled_qty,
            qty_raw=context.qty_raw,
            qty_final=context.filled_qty,
            notional=context.notional,
            fee=context.fee_paid,
            short_requires_borrow=bool(engine.short_requires_borrow),
            instrument=engine.instrument,
            execution_profile=engine.execution_profile,
            reserve=True,
            correlation_id=correlation_id,
            trade_id=context.trade_id,
        )
        if not allowed:
            engine._wallet_gateway.reject(reason, payload)
            engine.last_rejection_reason = reason
            engine.last_rejection_detail = payload
            context = merge_log_context(
                engine.runtime_log_context(
                    reason=reason,
                    qty=context.filled_qty,
                    price=round(context.entry_price, 4),
                ),
                build_log_context(
                    available=payload.get("available"),
                    required=payload.get("required"),
                    required_used=payload.get("required_used"),
                    required_full_notional=payload.get("required_full_notional"),
                    available_collateral=payload.get("available_collateral"),
                    currency=payload.get("currency"),
                    notional=payload.get("notional"),
                    fee=payload.get("fee"),
                    qty_raw=payload.get("qty_raw"),
                    qty_final=payload.get("qty_final"),
                    instrument_type=payload.get("instrument_type"),
                    margin_rate=payload.get("margin_rate"),
                    required_margin=payload.get("required_margin"),
                    fee_buffer=payload.get("fee_buffer"),
                    safety_buffer=payload.get("safety_buffer"),
                    margin_method=payload.get("margin_method"),
                    margin_session=payload.get("margin_session"),
                    margin_leg=payload.get("margin_leg"),
                    margin_rate_source_path=payload.get("margin_rate_source_path"),
                    margin_total_required=payload.get("margin_total_required"),
                    margin_calc_type=payload.get("margin_calc_type"),
                    shortfall=payload.get("shortfall"),
                    margin_error=payload.get("margin_error"),
                ),
            )
            logger.warning(with_log_context("wallet_entry_rejected", context))
            return False
        accounting_mode = engine.execution_profile.accounting_mode if engine.execution_profile is not None else None
        margin_locked = None
        if accounting_mode == "margin":
            accounting_mode = "margin"
            margin_locked = payload.get("reserved_amount") if isinstance(payload, dict) else None
            if margin_locked in (None, 0, 0.0):
                _, computed_margin_locked = wallet_required_reservation(
                    side=context.side,
                    base_currency=context.base_currency,
                    quote_currency=context.quote_currency,
                    qty=context.filled_qty,
                    notional=context.notional,
                    fee=context.fee_paid,
                    short_requires_borrow=bool(engine.short_requires_borrow),
                    instrument=engine.instrument,
                    execution_profile=engine.execution_profile,
                )
                margin_locked = computed_margin_locked
        reservation_id = payload.get("reservation_id") if isinstance(payload, dict) else None
        fill_metadata = engine._wallet_gateway.apply_fill(
            event_type="ENTRY_FILL",
            side=context.side,
            base_currency=context.base_currency,
            quote_currency=context.quote_currency,
            qty=context.filled_qty,
            price=context.entry_price,
            fee=context.fee_paid,
            notional=context.notional,
            symbol=engine.instrument.get("symbol"),
            trade_id=context.trade_id,
            position_direction=context.direction,
            accounting_mode=accounting_mode,
            realized_pnl=0.0,
            reservation_id=str(reservation_id) if reservation_id else None,
            margin_locked=float(margin_locked or 0.0) if accounting_mode == "margin" else None,
            correlation_id=correlation_id,
        )
        if isinstance(fill_metadata, dict):
            engine.remember_wallet_fill_metadata(context.trade_id, fill_metadata)
        return True


__all__ = ["EntrySettlement", "EntrySettlementContext", "EntrySettlementService"]
