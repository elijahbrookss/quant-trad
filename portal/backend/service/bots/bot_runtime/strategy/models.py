"""Domain models for bot runtime strategy loading.

These models provide strong typing and clear contracts for strategy data,
replacing the Dict[str, Any] approach that caused confusion and drift.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class StrategyIndicatorLink:
    """Link between strategy and indicator instance (no snapshot - loads fresh from DB)."""

    id: str
    strategy_id: str
    indicator_id: str
    # REMOVED: indicator_snapshot - indicators loaded fresh from DB

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StrategyIndicatorLink:
        """Create from database dict."""
        return cls(
            id=data["id"],
            strategy_id=data["strategy_id"],
            indicator_id=data["indicator_id"],
            # REMOVED: indicator_snapshot - indicators loaded fresh from DB
        )


@dataclass(frozen=True)
class StrategyInstrumentLink:
    """Link between strategy and instrument."""

    id: str
    strategy_id: str
    instrument_id: str
    instrument_snapshot: Dict[str, Any]

    @property
    def symbol(self) -> Optional[str]:
        """Extract symbol from snapshot."""
        return self.instrument_snapshot.get("symbol")

    @property
    def risk_multiplier(self) -> Optional[float]:
        """Extract risk multiplier from snapshot."""
        return self.instrument_snapshot.get("risk_multiplier")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StrategyInstrumentLink:
        """Create from database dict."""
        return cls(
            id=data["id"],
            strategy_id=data["strategy_id"],
            instrument_id=data["instrument_id"],
            instrument_snapshot=data.get("instrument_snapshot") or {},
        )


@dataclass(frozen=True)
class Strategy:
    """Strategy domain model with relationships loaded from database.

    This replaces the Dict[str, Any] approach with strong typing and clear contracts.
    All data is loaded fresh from the database to avoid drift.
    """

    id: str
    name: str
    timeframe: str
    datasource: str
    exchange: str
    atm_template_id: Optional[str]
    atm_template: Optional[Dict[str, Any]]
    base_risk_per_trade: Optional[float]
    global_risk_multiplier: Optional[float]

    # Relationships
    indicator_links: List[StrategyIndicatorLink]
    instrument_links: List[StrategyInstrumentLink]
    rules: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # rule_id -> rule dict

    @property
    def primary_instrument(self) -> Optional[StrategyInstrumentLink]:
        """Get the first instrument (primary trading instrument)."""
        return self.instrument_links[0] if self.instrument_links else None

    @property
    def symbol(self) -> Optional[str]:
        """Get primary trading symbol."""
        primary = self.primary_instrument
        return primary.symbol if primary else None

    @property
    def indicator_ids(self) -> List[str]:
        """Get list of indicator IDs attached to this strategy."""
        return [link.indicator_id for link in self.indicator_links]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict format for backward compatibility.

        This allows gradual migration from dict-based code.
        """
        return {
            "id": self.id,
            "name": self.name,
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "atm_template_id": self.atm_template_id,
            "atm_template": self.atm_template,
            "base_risk_per_trade": self.base_risk_per_trade,
            "global_risk_multiplier": self.global_risk_multiplier,
            "indicator_links": [
                {
                    "id": link.id,
                    "strategy_id": link.strategy_id,
                    "indicator_id": link.indicator_id,
                    # REMOVED: indicator_snapshot - indicators loaded fresh from DB
                }
                for link in self.indicator_links
            ],
            "instrument_links": [
                {
                    "id": link.id,
                    "strategy_id": link.strategy_id,
                    "instrument_id": link.instrument_id,
                    "instrument_snapshot": link.instrument_snapshot,
                }
                for link in self.instrument_links
            ],
            # Runtime consumes rules from series.meta["rules"].
            "rules": deepcopy(self.rules),
        }
