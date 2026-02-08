"""Execution intent and outcome primitives for bot runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class LimitParams:
    """Limit-maker parameters for execution intents."""

    anchor_price: str
    offset_type: str
    offset_value: float
    validity_window: int
    fallback: str
    limit_price: Optional[float] = None


@dataclass(frozen=True)
class ExecutionIntent:
    """Order intent submitted by strategy/backtest logic."""

    order_id: str
    side: str
    qty: float
    symbol: str
    order_type: str
    requested_price: float
    limit_params: Optional[LimitParams] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionOutcome:
    """Normalized execution outcome for order intents."""

    order_id: str
    status: str
    filled_qty: float
    avg_fill_price: Optional[float]
    fee_paid: float
    fee_role: str
    fee_rate: float
    fee_source: str
    fee_version: Optional[str]
    created_at: str
    updated_at: str
    filled_at: Optional[str]
    remaining_qty: float
    fallback_applied: bool = False
    fallback_reason: Optional[str] = None
    limit_price: Optional[float] = None
    validity_window: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


__all__ = ["LimitParams", "ExecutionIntent", "ExecutionOutcome"]
