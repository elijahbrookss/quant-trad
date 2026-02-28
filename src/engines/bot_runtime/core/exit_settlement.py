"""Exit settlement service for wallet accounting."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Protocol, Tuple

from utils.log_context import build_log_context, merge_log_context, with_log_context

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExitSettlementContext:
    """Context payload for exit settlement."""

    event_type: str
    exit_kind: str | None
    side: str
    base_currency: str
    quote_currency: str
    qty: float
    price: float
    fee: float
    notional: float
    trade_id: str
    leg_id: str
    position_direction: str
    accounting_mode: str | None
    realized_pnl: float
    allow_short_borrow: bool
    instrument: dict


class ExitSettlement(Protocol):
    """Protocol for exit settlement implementations."""

    def apply_exit_fill(self, context: ExitSettlementContext, *, force: bool) -> Tuple[bool, Dict[str, Any]]:
        ...


class ExitSettlementService:
    """Apply wallet settlement for exit fills."""

    def __init__(self, wallet_gateway) -> None:
        self._wallet_gateway = wallet_gateway

    def apply_exit_fill(self, context: ExitSettlementContext, *, force: bool) -> Tuple[bool, Dict[str, Any]]:
        if not self._wallet_gateway:
            return True, {}
        correlation_id = f"trade:{context.trade_id}"
        allowed, reason, payload = self._wallet_gateway.can_apply(
            side=context.side,
            base_currency=context.base_currency,
            quote_currency=context.quote_currency,
            qty=context.qty,
            qty_raw=context.qty,
            qty_final=context.qty,
            notional=context.notional,
            fee=context.fee,
            short_requires_borrow=context.allow_short_borrow,
            instrument=context.instrument,
            reserve=False,
            correlation_id=correlation_id,
            trade_id=context.trade_id,
        )
        if not allowed:
            self._wallet_gateway.reject(reason, payload, trade_id=context.trade_id, leg_id=context.leg_id)
            context_log = merge_log_context(
                build_log_context(
                    trade_id=context.trade_id,
                    leg_id=context.leg_id,
                    reason=reason,
                    price=round(context.price, 4),
                    direction=context.position_direction,
                ),
                build_log_context(
                    available=payload.get("available"),
                    required=payload.get("required"),
                    required_used=payload.get("required_used"),
                    required_full_notional=payload.get("required_full_notional"),
                    available_collateral=payload.get("available_collateral"),
                    currency=payload.get("currency"),
                    qty=payload.get("qty"),
                    qty_raw=payload.get("qty_raw"),
                    qty_final=payload.get("qty_final"),
                    notional=payload.get("notional"),
                    fee=payload.get("fee"),
                    margin_total_required=payload.get("margin_total_required"),
                    margin_calc_type=payload.get("margin_calc_type"),
                    margin_method=payload.get("margin_method"),
                    margin_session=payload.get("margin_session"),
                    margin_leg=payload.get("margin_leg"),
                    margin_rate_source_path=payload.get("margin_rate_source_path"),
                    shortfall=payload.get("shortfall"),
                ),
            )
            if force:
                logger.warning(with_log_context("wallet_exit_forced_despite_insufficient_balance", context_log))
            else:
                logger.warning(with_log_context("wallet_exit_rejected", context_log))
                return False, {}
        reservation_id = payload.get("reservation_id") if isinstance(payload, dict) else None
        fill_metadata = self._wallet_gateway.apply_fill(
            event_type=context.event_type,
            side=context.side,
            base_currency=context.base_currency,
            quote_currency=context.quote_currency,
            qty=context.qty,
            price=context.price,
            fee=context.fee,
            notional=context.notional,
            symbol=None,
            trade_id=context.trade_id,
            leg_id=context.leg_id,
            position_direction=context.position_direction,
            accounting_mode=context.accounting_mode,
            realized_pnl=context.realized_pnl,
            reservation_id=str(reservation_id) if reservation_id else None,
            correlation_id=correlation_id,
            exit_kind=context.exit_kind,
        )
        return True, dict(fill_metadata or {})


__all__ = ["ExitSettlement", "ExitSettlementContext", "ExitSettlementService"]
