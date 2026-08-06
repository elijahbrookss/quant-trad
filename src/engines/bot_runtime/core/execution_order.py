"""Canonical order execution primitives for bot runtime fills."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol, Tuple

from .execution import FillRejection, FillResult
from .execution_policy import fee_rate_for_role, normalize_liquidity_role

if TYPE_CHECKING:
    from .execution_context import ResolvedExecutionContext


class OrderType(str, Enum):
    """Runtime order styles with explicit liquidity semantics."""

    MARKET = "market"
    LIMIT_MAKER = "limit_maker"
    LIMIT_RESTING = "limit_resting"
    STOP_MARKET = "stop_market"


@dataclass(frozen=True)
class FillOrder:
    """Executable fill request after runtime has resolved order semantics."""

    side: str
    requested_qty: float
    price: float
    order_type: str
    liquidity_role: str
    price_source: str
    fee_rate: float
    fee_source: str = "instrument"
    fee_version: Optional[str] = None
    enforce_price_tick: bool = False
    time_in_force: str = "gtc"
    post_only: bool = False
    fee_currency: str = "quote"
    fee_calculation_basis: str = "quote_notional"
    fee_rounding_mode: str = "unrounded"
    fee_precision: Optional[int] = None
    fee_tier: str = "default"
    fee_schedule_hash: Optional[str] = None
    execution_context: Optional["ResolvedExecutionContext"] = None
    metadata: Dict[str, Any] | None = None


class OrderFillExecutor(Protocol):
    """Execution surface for deterministic/paper/live order fills."""

    def execute_order(
        self,
        order: FillOrder,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        ...


def build_fill_order(
    *,
    side: str,
    requested_qty: float,
    price: float,
    order_type: str,
    liquidity_role: object,
    price_source: str,
    maker_fee_rate: float | None = None,
    taker_fee_rate: float | None = None,
    fee_source: str = "instrument",
    fee_version: Optional[str] = None,
    enforce_price_tick: bool = False,
    time_in_force: str = "gtc",
    post_only: bool | None = None,
    execution_context: Optional["ResolvedExecutionContext"] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> FillOrder:
    """Build a canonical order request with fee role resolved up front."""

    normalized_order_type = str(order_type).strip().lower()
    resolved_post_only = normalized_order_type == OrderType.LIMIT_MAKER.value if post_only is None else post_only
    if not isinstance(resolved_post_only, bool):
        raise ValueError("post_only must be a boolean")
    context_metadata: Dict[str, Any] = {}
    if execution_context is not None:
        expected_role = execution_context.venue.liquidity_role(normalized_order_type)
        supplied_role = normalize_liquidity_role(liquidity_role)
        if supplied_role != expected_role:
            raise ValueError(
                "LIQUIDITY_ROLE_MISMATCH "
                f"order_type={normalized_order_type} expected={expected_role} actual={supplied_role}"
            )
        role = expected_role
        schedule = execution_context.fee_schedule
        maker_fee_rate = schedule.maker_rate
        taker_fee_rate = schedule.taker_rate
        fee_source = schedule.source
        fee_version = schedule.version or schedule.schedule_hash
        context_metadata = execution_context.evidence_metadata()
    else:
        role = normalize_liquidity_role(liquidity_role)
    if maker_fee_rate is None or taker_fee_rate is None:
        raise ValueError("maker_fee_rate and taker_fee_rate must be explicitly resolved")
    schedule = execution_context.fee_schedule if execution_context is not None else None
    return FillOrder(
        side=str(side),
        requested_qty=float(requested_qty),
        price=float(price),
        order_type=normalized_order_type,
        liquidity_role=role,
        price_source=str(price_source),
        fee_rate=fee_rate_for_role(
            role=role,
            maker_fee_rate=float(maker_fee_rate),
            taker_fee_rate=float(taker_fee_rate),
        ),
        fee_source=str(fee_source or "unresolved"),
        fee_version=str(fee_version or "").strip() or None,
        enforce_price_tick=bool(enforce_price_tick),
        time_in_force=str(time_in_force or "gtc").strip().lower(),
        post_only=resolved_post_only,
        fee_currency=schedule.fee_currency if schedule is not None else "quote",
        fee_calculation_basis=schedule.calculation_basis if schedule is not None else "quote_notional",
        fee_rounding_mode=schedule.rounding_mode if schedule is not None else "unrounded",
        fee_precision=schedule.precision if schedule is not None else None,
        fee_tier=schedule.tier if schedule is not None else "default",
        fee_schedule_hash=schedule.schedule_hash if schedule is not None else None,
        execution_context=execution_context,
        metadata={**context_metadata, **dict(metadata or {})},
    )


def execute_fill_order(
    executor: OrderFillExecutor,
    order: FillOrder,
) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
    """Execute an order through the canonical typed order surface."""

    effective_order = order
    if order.execution_context is not None:
        conformance = order.execution_context.validate_order(
            order_type=order.order_type,
            time_in_force=order.time_in_force,
            post_only=order.post_only,
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
        effective_order = replace(
            order,
            requested_qty=float(conformance.normalized_qty or order.requested_qty),
            price=float(conformance.normalized_price or order.price),
            metadata={**dict(order.metadata or {}), **dict(conformance.metadata)},
        )
    execute_order = getattr(executor, "execute_order", None)
    if not callable(execute_order):
        raise RuntimeError(
            "execution adapter does not implement execute_order "
            f"order_type={order.order_type} liquidity_role={order.liquidity_role}"
        )
    fill, rejection = execute_order(effective_order)
    if fill is not None and effective_order.execution_context is not None:
        protection = effective_order.execution_context.validate_fill_protections(
            order_type=effective_order.order_type,
            side=effective_order.side,
            requested_price=order.price,
            fill_price=fill.fill_price,
            filled_qty=fill.filled_qty,
        )
        if not protection.accepted:
            return None, FillRejection(
                reason=str(protection.reason or "MARKET_PROTECTION_FAILED"),
                metadata=dict(protection.metadata),
            )
    return annotate_fill_order(fill, effective_order), rejection


def annotate_fill_order(fill: Optional[FillResult], order: FillOrder) -> Optional[FillResult]:
    """Attach canonical order/liquidity metadata to a fill result."""

    if fill is None:
        return None
    role = normalize_liquidity_role(order.liquidity_role)
    metadata = dict(fill.metadata or {})
    metadata.update(dict(order.metadata or {}))
    metadata["order_type"] = order.order_type
    metadata["liquidity_role"] = role
    metadata["price_source"] = order.price_source
    metadata["time_in_force"] = order.time_in_force
    metadata["post_only"] = order.post_only
    metadata["fee_currency"] = order.fee_currency
    metadata["fee_rounding_mode"] = order.fee_rounding_mode
    metadata["fee_precision"] = order.fee_precision
    metadata["fee_tier"] = order.fee_tier
    metadata["fee_schedule_hash"] = order.fee_schedule_hash
    return replace(
        fill,
        fee_role=role,
        fee_rate=float(order.fee_rate),
        fee_source=order.fee_source,
        fee_version=order.fee_version,
        metadata=metadata,
    )


__all__ = [
    "FillOrder",
    "OrderFillExecutor",
    "OrderType",
    "annotate_fill_order",
    "build_fill_order",
    "execute_fill_order",
]
