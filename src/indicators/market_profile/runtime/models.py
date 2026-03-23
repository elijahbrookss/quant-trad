"""Typed market profile runtime state models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketProfileBarState:
    """Canonical derived state for one walk-forward bar."""

    bar_time: datetime
    active_profile_key: str
    previous_location: str | None
    location: str
    balance_state: str
    close: float
    val: float
    vah: float
    poc: float


__all__ = ["MarketProfileBarState"]
