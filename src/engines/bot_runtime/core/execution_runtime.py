"""Deterministic execution model implementation for bot runtime."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional, Tuple

from .execution import FillRejection
from .execution_intent import ExecutionIntent, ExecutionOutcome
from .execution_model import ExecutionModel
from .fees import FeeResolver


class DeterministicExecutionModel(ExecutionModel):
    """Deterministic execution model for market and limit-maker intents."""

    def __init__(self, fee_resolver: FeeResolver) -> None:
        self._fee_resolver = fee_resolver

    def submit(self, intent: ExecutionIntent) -> ExecutionOutcome:
        timestamp = _utc_now()
        limit_price = intent.limit_params.limit_price if intent.limit_params else None
        validity_window = intent.limit_params.validity_window if intent.limit_params else None
        return ExecutionOutcome(
            order_id=intent.order_id,
            status="submitted",
            filled_qty=0.0,
            avg_fill_price=None,
            fee_paid=0.0,
            fee_role="unknown",
            fee_rate=0.0,
            fee_source=self._fee_resolver.schedule.source,
            fee_version=self._fee_resolver.schedule.version,
            created_at=timestamp,
            updated_at=timestamp,
            filled_at=None,
            remaining_qty=float(intent.qty),
            fallback_applied=False,
            fallback_reason=None,
            limit_price=limit_price,
            validity_window=validity_window,
            metadata=dict(intent.metadata),
        )

    def evaluate(
        self,
        intent: ExecutionIntent,
        *,
        candle_high: float,
        candle_low: float,
        candle_close: float,
        candle_open: float,
    ) -> Tuple[ExecutionOutcome, Optional[FillRejection]]:
        if intent.qty <= 0:
            rejection = FillRejection(
                reason="QTY_ROUNDS_TO_ZERO",
                metadata={"requested_qty": intent.qty},
            )
            outcome = self.submit(intent)
            return replace(outcome, status="rejected", updated_at=_utc_now()), rejection

        order_type = intent.order_type
        if order_type == "market":
            fill_price = float(intent.requested_price or candle_close)
            notional = fill_price * float(intent.qty)
            fee_detail = self._fee_resolver.resolve(role="taker", notional=notional)
            timestamp = _utc_now()
            return (
                ExecutionOutcome(
                    order_id=intent.order_id,
                    status="filled",
                    filled_qty=float(intent.qty),
                    avg_fill_price=float(fill_price),
                    fee_paid=fee_detail.fee_paid,
                    fee_role=fee_detail.role,
                    fee_rate=fee_detail.fee_rate,
                    fee_source=fee_detail.source,
                    fee_version=fee_detail.version,
                    created_at=timestamp,
                    updated_at=timestamp,
                    filled_at=timestamp,
                    remaining_qty=0.0,
                    fallback_applied=False,
                    fallback_reason=None,
                    limit_price=None,
                    validity_window=None,
                    metadata=dict(intent.metadata),
                ),
                None,
            )

        if order_type != "limit_maker" or not intent.limit_params:
            rejection = FillRejection(
                reason="UNSUPPORTED_ORDER_TYPE",
                metadata={"order_type": order_type},
            )
            outcome = self.submit(intent)
            return replace(outcome, status="rejected", updated_at=_utc_now()), rejection

        limit_price = float(intent.limit_params.limit_price or intent.requested_price)
        side = str(intent.side).lower()
        if side in {"buy", "long"}:
            filled = candle_low <= limit_price
        else:
            filled = candle_high >= limit_price

        if not filled:
            outcome = self.submit(intent)
            return replace(
                outcome,
                status="open",
                updated_at=_utc_now(),
                remaining_qty=float(intent.qty),
                limit_price=limit_price,
                validity_window=intent.limit_params.validity_window,
            ), None

        notional = limit_price * float(intent.qty)
        fee_detail = self._fee_resolver.resolve(role="maker", notional=notional)
        timestamp = _utc_now()
        return (
            ExecutionOutcome(
                order_id=intent.order_id,
                status="filled",
                filled_qty=float(intent.qty),
                avg_fill_price=float(limit_price),
                fee_paid=fee_detail.fee_paid,
                fee_role=fee_detail.role,
                fee_rate=fee_detail.fee_rate,
                fee_source=fee_detail.source,
                fee_version=fee_detail.version,
                created_at=timestamp,
                updated_at=timestamp,
                filled_at=timestamp,
                remaining_qty=0.0,
                fallback_applied=False,
                fallback_reason=None,
                limit_price=limit_price,
                validity_window=intent.limit_params.validity_window,
                metadata=dict(intent.metadata),
            ),
            None,
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = ["DeterministicExecutionModel"]
