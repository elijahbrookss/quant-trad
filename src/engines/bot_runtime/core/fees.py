"""Fee and notional primitives for bot runtime execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FeeSchedule:
    """Fee schedule inputs used by execution models."""

    maker_rate: float
    taker_rate: float
    source: str
    version: Optional[str] = None


@dataclass(frozen=True)
class FeeDetail:
    """Resolved fee information from an execution outcome."""

    role: str
    fee_rate: float
    notional: float
    fee_paid: float
    source: str
    version: Optional[str] = None


def executed_notional(*, price: float, quantity: float, contract_size: float) -> float:
    """Canonical executed notional for all fee, fill, and reservation paths."""

    return abs(float(price) * float(quantity) * float(contract_size))


def executed_fee(
    *,
    price: float,
    quantity: float,
    contract_size: float,
    fee_rate: float,
) -> float:
    """Canonical fee calculation for an executed fill."""

    return float(fee_rate or 0.0) * executed_notional(
        price=price,
        quantity=quantity,
        contract_size=contract_size,
    )


class FeeResolver:
    """Centralized fee resolver for maker/taker classification."""

    def __init__(self, schedule: FeeSchedule) -> None:
        self.schedule = schedule

    def resolve(
        self,
        *,
        role: str,
        price: float,
        quantity: float,
        contract_size: float,
    ) -> FeeDetail:
        normalized_role = "maker" if str(role or "").lower() == "maker" else "taker"
        rate = self.schedule.maker_rate if normalized_role == "maker" else self.schedule.taker_rate
        notional = executed_notional(
            price=price,
            quantity=quantity,
            contract_size=contract_size,
        )
        fee_paid = executed_fee(
            price=price,
            quantity=quantity,
            contract_size=contract_size,
            fee_rate=float(rate or 0.0),
        )
        return FeeDetail(
            role=normalized_role,
            fee_rate=float(rate or 0.0),
            notional=float(notional),
            fee_paid=float(fee_paid),
            source=self.schedule.source,
            version=self.schedule.version,
        )


__all__ = ["FeeSchedule", "FeeDetail", "FeeResolver", "executed_fee", "executed_notional"]
