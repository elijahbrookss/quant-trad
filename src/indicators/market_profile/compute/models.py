"""
Domain types for Market Profile indicator.

These are pure data structures with no dependencies on UI, signals, or plotting libraries.
They represent the core business logic outputs of the indicator.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional
import pandas as pd


@dataclass(frozen=True)
class ValueArea:
    """
    Value area representing the price range where 70% of trading activity occurred.

    Attributes:
        vah: Value Area High - upper boundary
        val: Value Area Low - lower boundary
        poc: Point of Control - price level with most activity
    """
    vah: float
    val: float
    poc: float

    def __post_init__(self):
        """Validate value area invariants."""
        if self.vah < self.val:
            raise ValueError(f"VAH ({self.vah}) must be >= VAL ({self.val})")
        if not (self.val <= self.poc <= self.vah):
            raise ValueError(f"POC ({self.poc}) must be between VAL ({self.val}) and VAH ({self.vah})")

    @property
    def range(self) -> float:
        """Width of the value area."""
        return self.vah - self.val

    @property
    def midpoint(self) -> float:
        """Middle of the value area."""
        return (self.vah + self.val) / 2

    def contains(self, price: float) -> bool:
        """Check if a price is within the value area."""
        return self.val <= price <= self.vah

    def overlap_with(self, other: "ValueArea") -> float:
        """
        Calculate overlap ratio with another value area.

        Returns:
            Fraction of overlap (0.0 to 1.0)
        """
        if self.vah < other.val or other.vah < self.val:
            return 0.0

        overlap_low = max(self.val, other.val)
        overlap_high = min(self.vah, other.vah)
        overlap_range = overlap_high - overlap_low

        min_range = min(self.range, other.range)
        if min_range == 0:
            return 0.0

        return overlap_range / min_range


@dataclass(frozen=True)
class Profile:
    """
    Market profile representing TPO distribution for a trading session or period.

    Attributes:
        start: Session start timestamp
        end: Session end timestamp
        value_area: The value area (VAH, VAL, POC)
        session_count: Number of sessions merged (1 for single session)
        tpo_histogram: Optional histogram of price -> TPO count
        precision: Decimal precision for price formatting
    """
    start: pd.Timestamp
    end: pd.Timestamp
    value_area: ValueArea
    session_count: int = 1
    tpo_histogram: Optional[Dict[float, int]] = field(default=None, repr=False)
    precision: int = 4

    def __post_init__(self):
        """Validate profile invariants."""
        if self.end < self.start:
            raise ValueError(f"End ({self.end}) must be >= start ({self.start})")
        if self.session_count < 1:
            raise ValueError(f"Session count must be >= 1, got {self.session_count}")

    @property
    def vah(self) -> float:
        """Convenience accessor for Value Area High."""
        return self.value_area.vah

    @property
    def val(self) -> float:
        """Convenience accessor for Value Area Low."""
        return self.value_area.val

    @property
    def poc(self) -> float:
        """Convenience accessor for Point of Control."""
        return self.value_area.poc

    @property
    def duration(self) -> pd.Timedelta:
        """Duration of the profile period."""
        return self.end - self.start

    def is_merged(self) -> bool:
        """Check if this profile represents multiple merged sessions."""
        return self.session_count > 1

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "start": self.start,
            "end": self.end,
            "VAH": self.vah,
            "VAL": self.val,
            "POC": self.poc,
            "session_count": self.session_count,
            "precision": self.precision,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Profile":
        """Create Profile from dictionary."""
        value_area = ValueArea(
            vah=data["VAH"],
            val=data["VAL"],
            poc=data["POC"],
        )
        return cls(
            start=pd.Timestamp(data["start"]),
            end=pd.Timestamp(data["end"]),
            value_area=value_area,
            session_count=data.get("session_count", 1),
            precision=data.get("precision", 4),
        )
