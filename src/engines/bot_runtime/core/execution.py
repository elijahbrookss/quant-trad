"""Spot execution model for deterministic fills and constraint checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .amount_constraints import AmountConstraints, normalize_qty_with_constraints


@dataclass(frozen=True)
class SpotExecutionConstraints:
    """Spot exchange constraints derived from instrument metadata."""

    tick_size: float
    qty_step: Optional[float]
    min_qty: Optional[float]
    min_notional: Optional[float]
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


@dataclass(frozen=True)
class FillRejection:
    """Structured rejection for spot execution intents."""

    reason: str
    metadata: Dict[str, Any]


class SpotExecutionModel:
    """Deterministic execution model for spot market fills."""

    def __init__(self, constraints: SpotExecutionConstraints, *, slippage_bps: float = 0.0) -> None:
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
        self.slippage_bps = float(slippage_bps or 0.0)

    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool = False,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        if requested_qty <= 0 or price <= 0:
            return None, FillRejection(
                reason="QTY_ROUNDS_TO_ZERO",
                metadata={"requested_qty": requested_qty, "price": price},
            )

        fill_price = self._apply_slippage(price, side)
        if enforce_price_tick:
            fill_price = self._round_price(fill_price)

        normalization = normalize_qty_with_constraints(self.amount_constraints, requested_qty)
        if not normalization.ok:
            return None, FillRejection(
                reason=normalization.rejected_reason or "QTY_CONSTRAINT_FAILED",
                metadata=normalization.to_log_dict(),
            )

        rounded_qty = float(normalization.qty_final)

        notional = float(fill_price) * float(rounded_qty)
        min_notional = self.constraints.min_notional
        if min_notional not in (None, 0) and notional < float(min_notional):
            return None, FillRejection(
                reason="MIN_NOTIONAL_NOT_MET",
                metadata={"rounded_qty": rounded_qty, "notional": notional, "min_notional": min_notional},
            )

        fee = notional * float(fee_rate or 0.0)
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
                    "slippage_bps": self.slippage_bps,
                },
            ),
            None,
        )

    def _apply_slippage(self, price: float, side: str) -> float:
        if not self.slippage_bps:
            return float(price)
        direction = 1.0 if str(side).lower() in {"buy", "long"} else -1.0
        return float(price) * (1.0 + direction * (self.slippage_bps / 10000.0))

    def _round_price(self, price: float) -> float:
        tick = self.constraints.tick_size
        if tick in (None, 0):
            return float(price)
        return float(int((price + 1e-12) / tick)) * float(tick)


class DerivativesExecutionModel:
    """Deterministic execution model for derivatives fills."""

    def __init__(self, constraints: DerivativesExecutionConstraints, *, slippage_bps: float = 0.0) -> None:
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
        self.slippage_bps = float(slippage_bps or 0.0)

    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool = False,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        if requested_qty <= 0 or price <= 0:
            return None, FillRejection(
                reason="QTY_ROUNDS_TO_ZERO",
                metadata={"requested_qty": requested_qty, "price": price},
            )

        fill_price = self._apply_slippage(price, side)
        if enforce_price_tick:
            fill_price = self._round_price(fill_price)

        normalization = normalize_qty_with_constraints(self.amount_constraints, requested_qty)
        if not normalization.ok:
            return None, FillRejection(
                reason=normalization.rejected_reason or "QTY_CONSTRAINT_FAILED",
                metadata=normalization.to_log_dict(),
            )

        rounded_qty = float(normalization.qty_final)

        notional = float(fill_price) * float(rounded_qty) * float(self.constraints.contract_size)
        min_notional = self.constraints.min_notional
        if min_notional not in (None, 0) and notional < float(min_notional):
            return None, FillRejection(
                reason="MIN_NOTIONAL_NOT_MET",
                metadata={"rounded_qty": rounded_qty, "notional": notional, "min_notional": min_notional},
            )

        fee = notional * float(fee_rate or 0.0)
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
                    "slippage_bps": self.slippage_bps,
                },
            ),
            None,
        )

    def _apply_slippage(self, price: float, side: str) -> float:
        if not self.slippage_bps:
            return float(price)
        direction = 1.0 if str(side).lower() in {"buy", "long"} else -1.0
        return float(price) * (1.0 + direction * (self.slippage_bps / 10000.0))

    def _round_price(self, price: float) -> float:
        tick = self.constraints.tick_size
        if tick in (None, 0):
            return float(price)
        return float(int((price + 1e-12) / tick)) * float(tick)


__all__ = [
    "FillResult",
    "FillRejection",
    "SpotExecutionConstraints",
    "SpotExecutionModel",
    "DerivativesExecutionConstraints",
    "DerivativesExecutionModel",
]
