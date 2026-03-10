"""Fee resolution primitives for bot runtime execution."""

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
    fee_paid: float
    source: str
    version: Optional[str] = None


class FeeResolver:
    """Centralized fee resolver for maker/taker classification."""

    def __init__(self, schedule: FeeSchedule) -> None:
        self.schedule = schedule

    def resolve(self, *, role: str, notional: float) -> FeeDetail:
        rate = self.schedule.taker_rate if role == "taker" else self.schedule.maker_rate
        fee_paid = float(notional) * float(rate or 0.0)
        return FeeDetail(
            role=role,
            fee_rate=float(rate or 0.0),
            fee_paid=float(fee_paid),
            source=self.schedule.source,
            version=self.schedule.version,
        )


__all__ = ["FeeSchedule", "FeeDetail", "FeeResolver"]
