"""SQLAlchemy models for portal persistence."""

from __future__ import annotations

"""ORM models backing the portal persistence layer."""

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import JSON, Boolean, Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class IndicatorRecord(Base):
    """Database record describing a persisted indicator instance."""

    __tablename__ = "portal_indicators"

    id = Column(String(64), primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(128), nullable=False)
    params = Column(JSON, nullable=False, default=dict)
    color = Column(String(64), nullable=True)
    datasource = Column(String(64), nullable=True)
    exchange = Column(String(64), nullable=True)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the record into a plain dictionary."""

        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "params": self.params or {},
            "color": self.color,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "enabled": bool(self.enabled),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class StrategyRecord(Base):
    """Database record representing a stored strategy."""

    __tablename__ = "portal_strategies"

    id = Column(String(64), primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(String(1024), nullable=True)
    symbols = Column(JSON, nullable=False, default=list)
    timeframe = Column(String(32), nullable=False)
    datasource = Column(String(64), nullable=True)
    exchange = Column(String(64), nullable=True)
    indicator_ids = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise a strategy row for downstream consumers."""

        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "symbols": list(self.symbols or []),
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "indicator_ids": list(self.indicator_ids or []),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class StrategyRuleRecord(Base):
    """Database representation of a strategy rule definition."""

    __tablename__ = "portal_strategy_rules"

    id = Column(String(64), primary_key=True)
    strategy_id = Column(String(64), ForeignKey("portal_strategies.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    action = Column(String(16), nullable=False)
    match = Column(String(16), nullable=False, default="all")
    description = Column(String(1024), nullable=True)
    enabled = Column(Boolean, nullable=False, default=True)
    conditions = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Return a serialisable payload for the stored rule."""

        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "name": self.name,
            "action": self.action,
            "match": self.match,
            "description": self.description,
            "enabled": bool(self.enabled),
            "conditions": list(self.conditions or []),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class StrategyIndicatorLink(Base):
    """Join table linking strategies to indicator instances."""

    __tablename__ = "portal_strategy_indicators"

    id = Column(String(64), primary_key=True)
    strategy_id = Column(String(64), ForeignKey("portal_strategies.id", ondelete="CASCADE"), nullable=False)
    indicator_id = Column(String(64), nullable=False)
    indicator_snapshot = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("strategy_id", "indicator_id", name="uq_strategy_indicator"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain payload describing the relationship."""

        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "indicator_id": self.indicator_id,
            "indicator_snapshot": self.indicator_snapshot or {},
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class SymbolPresetRecord(Base):
    """Persisted combination of datasource, exchange, timeframe, and symbol."""

    __tablename__ = "portal_symbol_presets"

    id = Column(String(64), primary_key=True)
    label = Column(String(255), nullable=False)
    datasource = Column(String(64), nullable=True)
    exchange = Column(String(64), nullable=True)
    timeframe = Column(String(32), nullable=False)
    symbol = Column(String(64), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("label", "datasource", "exchange", "timeframe", "symbol", name="uq_symbol_preset"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Return the preset in API-friendly format."""

        return {
            "id": self.id,
            "label": self.label,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "timeframe": self.timeframe,
            "symbol": self.symbol,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotRecord(Base):
    """Database row describing a persisted bot configuration."""

    __tablename__ = "portal_bots"

    id = Column(String(64), primary_key=True)
    name = Column(String(255), nullable=False)
    strategy_id = Column(String(64), nullable=True)
    datasource = Column(String(64), nullable=True)
    exchange = Column(String(64), nullable=True)
    timeframe = Column(String(32), nullable=False, default="15m")
    mode = Column(String(32), nullable=False, default="instant")
    fetch_seconds = Column(Integer, nullable=False, default=5)
    risk = Column(JSON, nullable=False, default=dict)
    status = Column(String(32), nullable=False, default="idle")
    last_run_at = Column(DateTime, nullable=True)
    last_stats = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Return the bot configuration in API-friendly form."""

        return {
            "id": self.id,
            "name": self.name,
            "strategy_id": self.strategy_id,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "timeframe": self.timeframe,
            "mode": self.mode,
            "fetch_seconds": self.fetch_seconds,
            "risk": dict(self.risk or {}),
            "status": self.status,
            "last_run_at": (self.last_run_at.isoformat() + "Z") if self.last_run_at else None,
            "last_stats": dict(self.last_stats or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }
