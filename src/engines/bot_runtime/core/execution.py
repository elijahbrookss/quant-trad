"""Spot execution model for deterministic fills and constraint checks."""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from .amount_constraints import AmountConstraints, normalize_qty_with_constraints
from .execution_assumptions import (
    ResolvedExecutionAssumptions,
    apply_adverse_slippage,
    legacy_execution_assumptions,
)
from .fees import executed_fee, executed_notional, rounded_fee

if TYPE_CHECKING:
    from .execution_context import ResolvedExecutionContext


@dataclass(frozen=True)
class SpotExecutionConstraints:
    """Spot exchange constraints derived from instrument metadata."""

    tick_size: float
    qty_step: Optional[float]
    min_qty: Optional[float]
    min_notional: Optional[float]
    contract_size: float = 1.0
    max_qty: Optional[float] = None
    precision: Optional[int] = None


@dataclass(frozen=True)
class DerivativesExecutionConstraints:
    """Derivatives exchange constraints derived from instrument metadata."""

    tick_size: float
    qty_step: Optional[float]
    min_qty: Optional[float]
    min_notional: Optional[float]
    contract_size: float
    max_qty: Optional[float] = None
    precision: Optional[int] = None


@dataclass(frozen=True)
class FillResult:
    """Canonical fill output for spot entries/exits."""

    filled_qty: float
    fill_price: float
    notional: float
    fee: float
    fee_rate: float
    side: str
    metadata: Dict[str, Any]
    fee_role: str = "taker"
    fee_source: str = "instrument"
    fee_version: Optional[str] = None


@dataclass(frozen=True)
class FillRejection:
    """Structured rejection for spot execution intents."""

    reason: str
    metadata: Dict[str, Any]


class SpotExecutionModel:
    """Deterministic execution model for spot market fills."""

    def __init__(
        self,
        constraints: SpotExecutionConstraints,
        *,
        slippage_bps: float = 0.0,
        assumptions: ResolvedExecutionAssumptions | None = None,
        execution_context: "ResolvedExecutionContext | None" = None,
    ) -> None:
        self.constraints = constraints
        self.amount_constraints = AmountConstraints(
            min_qty=constraints.min_qty,
            max_qty=constraints.max_qty,
            qty_step=constraints.qty_step,
            min_notional=constraints.min_notional,
            precision=constraints.precision,
            step_source="execution_constraints",
            min_qty_source="execution_constraints",
            max_qty_source="execution_constraints",
            precision_source="execution_constraints",
        )
        self.execution_assumptions = assumptions or legacy_execution_assumptions()
        self.execution_context = execution_context
        self._validate_execution_context()
        self.market_slippage_bps = float(
            self.execution_assumptions.market_slippage_bps
            if assumptions is not None and self.execution_assumptions.market_slippage_bps is not None
            else slippage_bps or 0.0
        )
        self.stop_slippage_bps = float(
            self.execution_assumptions.stop_slippage_bps
            if assumptions is not None and self.execution_assumptions.stop_slippage_bps is not None
            else self.market_slippage_bps
        )
        self.slippage_bps = self.market_slippage_bps

    def _validate_execution_context(self) -> None:
        if self.execution_context is None:
            return
        if self.execution_context.model.assumption_manifest_hash != self.execution_assumptions.manifest_hash:
            raise ValueError("execution_context_assumption_manifest_mismatch")
        if not math.isclose(
            float(self.constraints.tick_size),
            float(self.execution_context.instrument.tick_size),
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("execution_context_tick_size_mismatch")

    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool = False,
        slippage_bps: float | None = None,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        if requested_qty <= 0 or price <= 0:
            return None, FillRejection(
                reason="QTY_ROUNDS_TO_ZERO",
                metadata={"requested_qty": requested_qty, "price": price},
            )

        applied_slippage_bps = self.market_slippage_bps if slippage_bps is None else float(slippage_bps)
        fill_price = self._apply_slippage(price, side, applied_slippage_bps)
        if enforce_price_tick:
            fill_price = self._round_price(fill_price)

        normalization = normalize_qty_with_constraints(self.amount_constraints, requested_qty)
        if not normalization.ok:
            return None, FillRejection(
                reason=normalization.rejected_reason or "QTY_CONSTRAINT_FAILED",
                metadata=normalization.to_log_dict(),
            )

        rounded_qty = float(normalization.qty_final)

        notional = executed_notional(
            price=fill_price,
            quantity=rounded_qty,
            contract_size=float(self.constraints.contract_size or 1.0),
        )
        min_notional = self.constraints.min_notional
        if min_notional not in (None, 0) and notional < float(min_notional):
            return None, FillRejection(
                reason="MIN_NOTIONAL_NOT_MET",
                metadata={"rounded_qty": rounded_qty, "notional": notional, "min_notional": min_notional},
            )

        fee = executed_fee(
            price=fill_price,
            quantity=rounded_qty,
            contract_size=float(self.constraints.contract_size or 1.0),
            fee_rate=float(fee_rate or 0.0),
        )
        return (
            FillResult(
                filled_qty=float(rounded_qty),
                fill_price=float(fill_price),
                notional=float(notional),
                fee=float(fee),
                fee_rate=float(fee_rate or 0.0),
                side=side,
                metadata={
                    **normalization.to_log_dict(),
                    "min_notional": min_notional,
                    "tick_size": self.constraints.tick_size,
                    "contract_size": self.constraints.contract_size,
                    "requested_price": float(price),
                    "fill_price": float(fill_price),
                    "slippage_price": float(fill_price) - float(price),
                    "slippage_bps": applied_slippage_bps,
                    "execution_model_version": self.execution_assumptions.model_version,
                    "execution_assumption_manifest_hash": self.execution_assumptions.manifest_hash,
                    "execution_quality_ceiling": self.execution_assumptions.execution_quality_ceiling,
                    "economic_claim_intent": self.execution_assumptions.economic_claim_intent,
                    "fee_policy": self.execution_assumptions.fee_policy,
                    "full_fill_assumption": self.execution_assumptions.full_fill_assumption,
                    "market_slippage_bps": self.market_slippage_bps,
                    "stop_slippage_bps": self.stop_slippage_bps,
                    **(
                        self.execution_context.evidence_metadata()
                        if self.execution_context is not None
                        else {}
                    ),
                },
                fee_role="taker",
                fee_source="instrument",
                fee_version=None,
            ),
            None,
        )

    def execute_order(self, order) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        """Execute a canonical runtime order through the deterministic fill model."""

        requested_order_price = order.price
        context = getattr(order, "execution_context", None) or self.execution_context
        if context is not None:
            conformance = context.validate_order(
                order_type=order.order_type,
                time_in_force=getattr(order, "time_in_force", "gtc"),
                post_only=getattr(order, "post_only", False),
                side=order.side,
                quantity=order.requested_qty,
                price=order.price,
                liquidity_role=order.liquidity_role,
            )
            if not conformance.accepted:
                return None, FillRejection(
                    reason=str(conformance.reason or "ORDER_CONFORMANCE_FAILED"),
                    metadata=dict(conformance.metadata),
                )
            order = replace(
                order,
                requested_qty=float(conformance.normalized_qty or order.requested_qty),
                price=float(conformance.normalized_price or order.price),
            )
        order_type = str(getattr(order, "order_type", "market") or "market").strip().lower()
        applied_slippage_bps = (
            0.0
            if order_type in {"limit_maker", "limit_resting"}
            else self.stop_slippage_bps
            if order_type == "stop_market"
            else self.market_slippage_bps
        )
        fill, rejection = self.fill_market(
            side=order.side,
            requested_qty=order.requested_qty,
            price=order.price,
            fee_rate=order.fee_rate,
            enforce_price_tick=order.enforce_price_tick,
            slippage_bps=applied_slippage_bps,
        )
        fill, protection_rejection = _apply_context_fill_protections(
            context,
            order,
            fill,
            requested_price=requested_order_price,
        )
        if protection_rejection is not None:
            return None, protection_rejection
        return _annotate_execution_order(fill, order), rejection

    def _apply_slippage(self, price: float, side: str, slippage_bps: float | None = None) -> float:
        return apply_adverse_slippage(price, side, slippage_bps)

    def _round_price(self, price: float) -> float:
        tick = self.constraints.tick_size
        if tick in (None, 0):
            return float(price)
        return float(int((price + 1e-12) / tick)) * float(tick)


class DerivativesExecutionModel:
    """Deterministic execution model for derivatives fills."""

    def __init__(
        self,
        constraints: DerivativesExecutionConstraints,
        *,
        slippage_bps: float = 0.0,
        assumptions: ResolvedExecutionAssumptions | None = None,
        execution_context: "ResolvedExecutionContext | None" = None,
    ) -> None:
        self.constraints = constraints
        self.amount_constraints = AmountConstraints(
            min_qty=constraints.min_qty,
            max_qty=constraints.max_qty,
            qty_step=constraints.qty_step,
            min_notional=constraints.min_notional,
            precision=constraints.precision,
            step_source="execution_constraints",
            min_qty_source="execution_constraints",
            max_qty_source="execution_constraints",
            precision_source="execution_constraints",
        )
        self.execution_assumptions = assumptions or legacy_execution_assumptions()
        self.execution_context = execution_context
        self._validate_execution_context()
        self.market_slippage_bps = float(
            self.execution_assumptions.market_slippage_bps
            if assumptions is not None and self.execution_assumptions.market_slippage_bps is not None
            else slippage_bps or 0.0
        )
        self.stop_slippage_bps = float(
            self.execution_assumptions.stop_slippage_bps
            if assumptions is not None and self.execution_assumptions.stop_slippage_bps is not None
            else self.market_slippage_bps
        )
        self.slippage_bps = self.market_slippage_bps

    def _validate_execution_context(self) -> None:
        if self.execution_context is None:
            return
        if self.execution_context.model.assumption_manifest_hash != self.execution_assumptions.manifest_hash:
            raise ValueError("execution_context_assumption_manifest_mismatch")
        if not math.isclose(
            float(self.constraints.tick_size),
            float(self.execution_context.instrument.tick_size),
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("execution_context_tick_size_mismatch")

    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool = False,
        slippage_bps: float | None = None,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        if requested_qty <= 0 or price <= 0:
            return None, FillRejection(
                reason="QTY_ROUNDS_TO_ZERO",
                metadata={"requested_qty": requested_qty, "price": price},
            )

        applied_slippage_bps = self.market_slippage_bps if slippage_bps is None else float(slippage_bps)
        fill_price = self._apply_slippage(price, side, applied_slippage_bps)
        if enforce_price_tick:
            fill_price = self._round_price(fill_price)

        normalization = normalize_qty_with_constraints(self.amount_constraints, requested_qty)
        if not normalization.ok:
            return None, FillRejection(
                reason=normalization.rejected_reason or "QTY_CONSTRAINT_FAILED",
                metadata=normalization.to_log_dict(),
            )

        rounded_qty = float(normalization.qty_final)

        notional = executed_notional(
            price=fill_price,
            quantity=rounded_qty,
            contract_size=float(self.constraints.contract_size),
        )
        min_notional = self.constraints.min_notional
        if min_notional not in (None, 0) and notional < float(min_notional):
            return None, FillRejection(
                reason="MIN_NOTIONAL_NOT_MET",
                metadata={"rounded_qty": rounded_qty, "notional": notional, "min_notional": min_notional},
            )

        fee = executed_fee(
            price=fill_price,
            quantity=rounded_qty,
            contract_size=float(self.constraints.contract_size),
            fee_rate=float(fee_rate or 0.0),
        )
        return (
            FillResult(
                filled_qty=float(rounded_qty),
                fill_price=float(fill_price),
                notional=float(notional),
                fee=float(fee),
                fee_rate=float(fee_rate or 0.0),
                side=side,
                metadata={
                    **normalization.to_log_dict(),
                    "min_notional": min_notional,
                    "tick_size": self.constraints.tick_size,
                    "contract_size": self.constraints.contract_size,
                    "requested_price": float(price),
                    "fill_price": float(fill_price),
                    "slippage_price": float(fill_price) - float(price),
                    "slippage_bps": applied_slippage_bps,
                    "execution_model_version": self.execution_assumptions.model_version,
                    "execution_assumption_manifest_hash": self.execution_assumptions.manifest_hash,
                    "execution_quality_ceiling": self.execution_assumptions.execution_quality_ceiling,
                    "economic_claim_intent": self.execution_assumptions.economic_claim_intent,
                    "fee_policy": self.execution_assumptions.fee_policy,
                    "full_fill_assumption": self.execution_assumptions.full_fill_assumption,
                    "market_slippage_bps": self.market_slippage_bps,
                    "stop_slippage_bps": self.stop_slippage_bps,
                    **(
                        self.execution_context.evidence_metadata()
                        if self.execution_context is not None
                        else {}
                    ),
                },
                fee_role="taker",
                fee_source="instrument",
                fee_version=None,
            ),
            None,
        )

    def execute_order(self, order) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        """Execute a canonical runtime order through the deterministic fill model."""

        requested_order_price = order.price
        context = getattr(order, "execution_context", None) or self.execution_context
        if context is not None:
            conformance = context.validate_order(
                order_type=order.order_type,
                time_in_force=getattr(order, "time_in_force", "gtc"),
                post_only=getattr(order, "post_only", False),
                side=order.side,
                quantity=order.requested_qty,
                price=order.price,
                liquidity_role=order.liquidity_role,
            )
            if not conformance.accepted:
                return None, FillRejection(
                    reason=str(conformance.reason or "ORDER_CONFORMANCE_FAILED"),
                    metadata=dict(conformance.metadata),
                )
            order = replace(
                order,
                requested_qty=float(conformance.normalized_qty or order.requested_qty),
                price=float(conformance.normalized_price or order.price),
            )
        order_type = str(getattr(order, "order_type", "market") or "market").strip().lower()
        applied_slippage_bps = (
            0.0
            if order_type in {"limit_maker", "limit_resting"}
            else self.stop_slippage_bps
            if order_type == "stop_market"
            else self.market_slippage_bps
        )
        fill, rejection = self.fill_market(
            side=order.side,
            requested_qty=order.requested_qty,
            price=order.price,
            fee_rate=order.fee_rate,
            enforce_price_tick=order.enforce_price_tick,
            slippage_bps=applied_slippage_bps,
        )
        fill, protection_rejection = _apply_context_fill_protections(
            context,
            order,
            fill,
            requested_price=requested_order_price,
        )
        if protection_rejection is not None:
            return None, protection_rejection
        return _annotate_execution_order(fill, order), rejection

    def _apply_slippage(self, price: float, side: str, slippage_bps: float | None = None) -> float:
        return apply_adverse_slippage(price, side, slippage_bps)

    def _round_price(self, price: float) -> float:
        tick = self.constraints.tick_size
        if tick in (None, 0):
            return float(price)
        return float(int((price + 1e-12) / tick)) * float(tick)


def _apply_context_fill_protections(
    context,
    order,
    fill: Optional[FillResult],
    *,
    requested_price: float,
) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
    if context is None or fill is None:
        return fill, None
    conformance = context.validate_fill_protections(
        order_type=getattr(order, "order_type", None),
        side=getattr(order, "side", None),
        requested_price=requested_price,
        fill_price=fill.fill_price,
        filled_qty=fill.filled_qty,
    )
    if conformance.accepted:
        return fill, None
    return None, FillRejection(
        reason=str(conformance.reason or "MARKET_PROTECTION_FAILED"),
        metadata=dict(conformance.metadata),
    )


def _annotate_execution_order(fill: Optional[FillResult], order) -> Optional[FillResult]:
    if fill is None:
        return None
    role = str(getattr(order, "liquidity_role", "") or "taker").strip().lower()
    role = "maker" if role == "maker" else "taker"
    metadata = dict(fill.metadata or {})
    order_metadata = getattr(order, "metadata", None)
    if isinstance(order_metadata, dict):
        metadata.update(order_metadata)
    metadata["order_type"] = getattr(order, "order_type", None)
    metadata["liquidity_role"] = role
    metadata["price_source"] = getattr(order, "price_source", None)
    metadata["time_in_force"] = getattr(order, "time_in_force", "gtc")
    metadata["post_only"] = getattr(order, "post_only", False)
    metadata["fee_currency"] = getattr(order, "fee_currency", "quote")
    metadata["fee_rounding_mode"] = getattr(order, "fee_rounding_mode", "unrounded")
    metadata["fee_precision"] = getattr(order, "fee_precision", None)
    metadata["fee_tier"] = getattr(order, "fee_tier", "default")
    metadata["fee_schedule_hash"] = getattr(order, "fee_schedule_hash", None)
    calculation_basis = getattr(order, "fee_calculation_basis", "quote_notional")
    fee_basis = (
        abs(float(fill.filled_qty) * float(metadata.get("contract_size") or 1.0))
        if calculation_basis == "base_quantity"
        else float(fill.notional)
    )
    fee = rounded_fee(
        float(getattr(order, "fee_rate", fill.fee_rate)) * fee_basis,
        mode=str(getattr(order, "fee_rounding_mode", "unrounded")),
        precision=getattr(order, "fee_precision", None),
    )
    return replace(
        fill,
        fee=float(fee),
        fee_role=role,
        fee_rate=float(getattr(order, "fee_rate", fill.fee_rate)),
        fee_source=str(getattr(order, "fee_source", fill.fee_source) or "unresolved"),
        fee_version=getattr(order, "fee_version", fill.fee_version),
        metadata=metadata,
    )


__all__ = [
    "FillResult",
    "FillRejection",
    "SpotExecutionConstraints",
    "SpotExecutionModel",
    "DerivativesExecutionConstraints",
    "DerivativesExecutionModel",
]
