"""Deterministic execution model implementation for bot runtime."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Tuple

from .execution import FillRejection
from .execution_assumptions import (
    ResolvedExecutionAssumptions,
    apply_adverse_slippage,
    legacy_execution_assumptions,
)
from .execution_intent import ExecutionIntent, ExecutionOutcome
from .execution_model import ExecutionModel
from .fees import FeeResolver

if TYPE_CHECKING:
    from .execution_context import ResolvedExecutionContext


class DeterministicExecutionModel(ExecutionModel):
    """Deterministic execution model for market and limit-maker intents."""

    def __init__(
        self,
        fee_resolver: FeeResolver,
        assumptions: ResolvedExecutionAssumptions | None = None,
        execution_context: "ResolvedExecutionContext | None" = None,
    ) -> None:
        self._fee_resolver = fee_resolver
        self.assumptions = assumptions or legacy_execution_assumptions()
        self.execution_context = execution_context
        if execution_context is not None:
            if execution_context.model.assumption_manifest_hash != self.assumptions.manifest_hash:
                raise ValueError("execution_context_assumption_manifest_mismatch")
            if execution_context.fee_schedule.schedule_hash != fee_resolver.schedule.schedule_hash:
                raise ValueError("execution_context_fee_schedule_mismatch")

    def _metadata(
        self,
        intent: ExecutionIntent,
        *,
        requested_price: float | None = None,
        fill_price: float | None = None,
        slippage_bps: float | None = None,
    ) -> dict:
        metadata = {
            **dict(intent.metadata),
            "execution_assumptions_schema_version": self.assumptions.schema_version,
            "execution_model_version": self.assumptions.model_version,
            "execution_assumption_manifest_hash": self.assumptions.manifest_hash,
            "passive_fill_policy": self.assumptions.passive_fill_policy,
            "execution_quality_ceiling": self.assumptions.execution_quality_ceiling,
            "economic_claim_intent": self.assumptions.economic_claim_intent,
            "fee_policy": self.assumptions.fee_policy,
            "full_fill_assumption": self.assumptions.full_fill_assumption,
            "market_slippage_bps": self.assumptions.market_slippage_bps,
            "stop_slippage_bps": self.assumptions.stop_slippage_bps,
            "requested_price": requested_price,
            "fill_price": fill_price,
            "slippage_bps": slippage_bps,
            "time_in_force": intent.time_in_force,
            "post_only": intent.post_only,
            **(
                self.execution_context.evidence_metadata()
                if self.execution_context is not None
                else {}
            ),
        }
        if requested_price is not None and fill_price is not None:
            metadata["slippage_price"] = float(fill_price) - float(requested_price)
        return metadata

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
            metadata=self._metadata(intent),
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
        original_requested_price = float(intent.requested_price or candle_close)
        if self.execution_context is not None:
            try:
                role = self.execution_context.venue.liquidity_role(intent.order_type)
            except ValueError:
                role = None
            conformance = self.execution_context.validate_order(
                order_type=intent.order_type,
                time_in_force=intent.time_in_force,
                post_only=intent.post_only,
                side=intent.side,
                quantity=intent.qty,
                price=intent.limit_params.limit_price
                if intent.limit_params and intent.limit_params.limit_price is not None
                else intent.requested_price,
                liquidity_role=role,
            )
            if not conformance.accepted:
                rejection = FillRejection(
                    reason=str(conformance.reason or "ORDER_CONFORMANCE_FAILED"),
                    metadata=dict(conformance.metadata),
                )
                outcome = self.submit(intent)
                return replace(outcome, status="rejected", updated_at=_utc_now()), rejection
            normalized_limit = intent.limit_params
            if normalized_limit is not None and conformance.normalized_price is not None:
                normalized_limit = replace(
                    normalized_limit,
                    limit_price=float(conformance.normalized_price),
                )
            intent = replace(
                intent,
                qty=float(conformance.normalized_qty or intent.qty),
                limit_params=normalized_limit,
            )
        if intent.qty <= 0:
            rejection = FillRejection(
                reason="QTY_ROUNDS_TO_ZERO",
                metadata={"requested_qty": intent.qty},
            )
            outcome = self.submit(intent)
            return replace(outcome, status="rejected", updated_at=_utc_now()), rejection

        order_type = intent.order_type
        if order_type == "market":
            requested_price = float(intent.requested_price or candle_close)
            slippage_bps = self.assumptions.market_slippage_bps
            fill_price = apply_adverse_slippage(requested_price, intent.side, slippage_bps)
            if self.execution_context is not None:
                protection = self.execution_context.validate_fill_protections(
                    order_type=order_type,
                    side=intent.side,
                    requested_price=original_requested_price,
                    fill_price=fill_price,
                    filled_qty=intent.qty,
                )
                if not protection.accepted:
                    rejection = FillRejection(
                        reason=str(protection.reason or "MARKET_PROTECTION_FAILED"),
                        metadata=dict(protection.metadata),
                    )
                    outcome = self.submit(intent)
                    return (
                        replace(
                            outcome,
                            status="rejected",
                            updated_at=_utc_now(),
                            metadata={**dict(outcome.metadata), **dict(protection.metadata)},
                        ),
                        rejection,
                    )
            fee_detail = self._fee_resolver.resolve(
                role=(
                    self.execution_context.venue.liquidity_role(order_type)
                    if self.execution_context is not None
                    else "taker"
                ),
                price=fill_price,
                quantity=float(intent.qty),
                contract_size=float(intent.contract_size),
            )
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
                    metadata=self._metadata(
                        intent,
                        requested_price=requested_price,
                        fill_price=fill_price,
                        slippage_bps=slippage_bps,
                    ),
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
        pending_evaluation = bool(intent.metadata.get("pending_evaluation"))
        if not pending_evaluation and _post_only_would_cross(
            side=side,
            limit_price=limit_price,
            reference_price=float(intent.requested_price or candle_open or candle_close),
        ):
            rejection = FillRejection(
                reason="POST_ONLY_WOULD_CROSS",
                metadata={
                    "order_type": order_type,
                    "side": side,
                    "limit_price": limit_price,
                    "requested_price": float(intent.requested_price or candle_close),
                },
            )
            outcome = self.submit(intent)
            rejected_status = (
                "canceled"
                if self.execution_context is not None
                and self.execution_context.venue.post_only_behavior == "cancel_would_cross"
                else "rejected"
            )
            return replace(
                outcome,
                status=rejected_status,
                updated_at=_utc_now(),
                limit_price=limit_price,
                validity_window=intent.limit_params.validity_window,
            ), rejection

        strict_penetration = self.assumptions.uses_strict_penetration
        if side in {"buy", "long"}:
            filled = candle_low < limit_price if strict_penetration else candle_low <= limit_price
        else:
            filled = candle_high > limit_price if strict_penetration else candle_high >= limit_price

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

        fee_detail = self._fee_resolver.resolve(
            role=(
                self.execution_context.venue.liquidity_role(order_type)
                if self.execution_context is not None
                else "maker"
            ),
            price=limit_price,
            quantity=float(intent.qty),
            contract_size=float(intent.contract_size),
        )
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
                metadata=self._metadata(
                    intent,
                    requested_price=limit_price,
                    fill_price=limit_price,
                    slippage_bps=0.0,
                ),
            ),
            None,
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _post_only_would_cross(*, side: str, limit_price: float, reference_price: float) -> bool:
    if limit_price <= 0 or reference_price <= 0:
        return False
    if side in {"buy", "long"}:
        return limit_price >= reference_price
    return limit_price <= reference_price


__all__ = ["DeterministicExecutionModel"]
