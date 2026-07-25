"""Canonical order execution primitives for bot runtime fills."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Dict, Optional, Protocol, Tuple

from .execution import FillRejection, FillResult
from .execution_policy import fee_rate_for_role, normalize_liquidity_role


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
    enforce_price_tick: bool = False
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
    maker_fee_rate: float,
    taker_fee_rate: float,
    enforce_price_tick: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
) -> FillOrder:
    """Build a canonical order request with fee role resolved up front."""

    role = normalize_liquidity_role(liquidity_role)
    return FillOrder(
        side=str(side),
        requested_qty=float(requested_qty),
        price=float(price),
        order_type=str(order_type),
        liquidity_role=role,
        price_source=str(price_source),
        fee_rate=fee_rate_for_role(
            role=role,
            maker_fee_rate=float(maker_fee_rate or 0.0),
            taker_fee_rate=float(taker_fee_rate or 0.0),
        ),
        enforce_price_tick=bool(enforce_price_tick),
        metadata=dict(metadata or {}),
    )


def execute_fill_order(
    executor: OrderFillExecutor,
    order: FillOrder,
) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
    """Execute an order through the canonical typed order surface."""

    execute_order = getattr(executor, "execute_order", None)
    if not callable(execute_order):
        raise RuntimeError(
            "execution adapter does not implement execute_order "
            f"order_type={order.order_type} liquidity_role={order.liquidity_role}"
        )
    fill, rejection = execute_order(order)
    return annotate_fill_order(fill, order), rejection


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
    return replace(
        fill,
        fee_role=role,
        fee_rate=float(order.fee_rate or 0.0),
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
