"""SQLAlchemy models for portal persistence."""

from __future__ import annotations

"""ORM models backing the portal persistence layer."""

from datetime import datetime
from datetime import datetime
from typing import Any, Dict

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
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
    # datasource and exchange removed; indicators are compute-only definitions
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
    timeframe = Column(String(32), nullable=False)
    datasource = Column(String(64), nullable=False)
    exchange = Column(String(64), nullable=False)
    # indicator_ids removed — attachments are stored in portal_strategy_indicators
    atm_template_id = Column(String(64), nullable=True)
    base_risk_per_trade = Column(Float, nullable=True)
    global_risk_multiplier = Column(Float, nullable=True)
    risk_overrides = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise a strategy row for downstream consumers."""

        def _symbol_names(raw: Any) -> list[str]:
            names: list[str] = []
            for entry in raw or []:
                if isinstance(entry, dict) and entry.get("symbol"):
                    names.append(str(entry.get("symbol")))
                elif entry:
                    names.append(str(entry))
            return names

        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            # legacy `symbols` column removed from DB; registry/service layer will derive slots
            "symbols": [],
            "symbol_names": [],
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "indicator_links": [],
            "atm_template_id": self.atm_template_id,
            "base_risk_per_trade": self.base_risk_per_trade,
            "global_risk_multiplier": self.global_risk_multiplier,
            "risk_overrides": self.risk_overrides or {},
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

    id = Column(String(128), primary_key=True)
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


class StrategyInstrumentLink(Base):
    """Join table linking strategies to persisted instruments."""

    __tablename__ = "portal_strategy_instruments"

    id = Column(String(128), primary_key=True)
    strategy_id = Column(String(64), ForeignKey("portal_strategies.id", ondelete="CASCADE"), nullable=False)
    instrument_id = Column(String(64), nullable=False)
    instrument_snapshot = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("strategy_id", "instrument_id", name="uq_strategy_instrument"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "instrument_id": self.instrument_id,
            "instrument_snapshot": self.instrument_snapshot or {},
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class ATMTemplateRecord(Base):
    """Persisted ATM templates for reuse across strategies."""

    __tablename__ = "portal_atm_templates"

    id = Column(String(64), primary_key=True)
    name = Column(String(255), nullable=False)
    template = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("name", name="uq_atm_template_name"),)

    def to_dict(self) -> Dict[str, Any]:
        """Return the ATM template payload."""

        return {
            "id": self.id,
            "name": self.name,
            "template": self.template or {},
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


class InstrumentRecord(Base):
    """Persisted instrument metadata for tick/fee calculations."""

    __tablename__ = "portal_instruments"

    id = Column(String(64), primary_key=True)
    datasource = Column(String(64), nullable=True)
    exchange = Column(String(64), nullable=True)
    symbol = Column(String(64), nullable=False)
    instrument_type = Column(String(64), nullable=True)
    tick_size = Column(Float, nullable=True)
    tick_value = Column(Float, nullable=True)
    contract_size = Column(Float, nullable=True)
    min_order_size = Column(Float, nullable=True)
    quote_currency = Column(String(16), nullable=True)
    maker_fee_rate = Column(Float, nullable=True)
    taker_fee_rate = Column(Float, nullable=True)
    # ``metadata`` is reserved by SQLAlchemy declarative models, so we expose the
    # JSON payload via an attribute with a different name while keeping the
    # column name stable for existing rows.
    extra_metadata = Column("metadata", JSON, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "datasource", "exchange", "symbol", name="uq_instrument_symbol"
        ),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Return the instrument payload for API consumers."""

        return {
            "id": self.id,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "instrument_type": self.instrument_type,
            "tick_size": self.tick_size,
            "tick_value": self.tick_value,
            "contract_size": self.contract_size,
            "min_order_size": self.min_order_size,
            "quote_currency": self.quote_currency,
            "maker_fee_rate": self.maker_fee_rate,
            "taker_fee_rate": self.taker_fee_rate,
            "metadata": dict(self.extra_metadata or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotRecord(Base):
    """Database row describing a persisted bot configuration."""

    __tablename__ = "portal_bots"

    id = Column(String(64), primary_key=True)
    name = Column(String(255), nullable=False)
    strategy_id = Column(String(64), nullable=True)
    mode = Column(String(32), nullable=False, default="instant")
    run_type = Column(String(32), nullable=False, default="backtest")
    playback_speed = Column("fetch_seconds", Float, nullable=False, default=10.0)
    backtest_start = Column(DateTime, nullable=True)
    backtest_end = Column(DateTime, nullable=True)
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
            
            "mode": self.mode,
            "run_type": self.run_type,
            "playback_speed": float(self.playback_speed if self.playback_speed is not None else 10.0),
            "backtest_start": (self.backtest_start.isoformat() + "Z") if self.backtest_start else None,
            "backtest_end": (self.backtest_end.isoformat() + "Z") if self.backtest_end else None,
            "risk": dict(self.risk or {}),
            "status": self.status,
            "last_run_at": (self.last_run_at.isoformat() + "Z") if self.last_run_at else None,
            "last_stats": dict(self.last_stats or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }

class BotTradeRecord(Base):
    """Database row representing a laddered trade generated by a bot."""

    __tablename__ = "portal_bot_trades"

    id = Column(String(64), primary_key=True)
    bot_id = Column(String(64), ForeignKey("portal_bots.id", ondelete="CASCADE"), nullable=False)
    strategy_id = Column(String(64), ForeignKey("portal_strategies.id", ondelete="SET NULL"), nullable=True)
    symbol = Column(String(64), nullable=True)
    direction = Column(String(16), nullable=False)
    status = Column(String(32), nullable=False, default="open")
    contracts = Column(Integer, nullable=True)
    entry_time = Column(DateTime, nullable=True)
    entry_price = Column(Float, nullable=True)
    stop_price = Column(Float, nullable=True)
    exit_time = Column(DateTime, nullable=True)
    gross_pnl = Column(Float, nullable=True)
    fees_paid = Column(Float, nullable=True)
    net_pnl = Column(Float, nullable=True)
    metrics = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the stored trade entry."""

        return {
            "id": self.id,
            "bot_id": self.bot_id,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "status": self.status,
            "contracts": self.contracts,
            "entry_time": (self.entry_time.isoformat() + "Z") if self.entry_time else None,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "exit_time": (self.exit_time.isoformat() + "Z") if self.exit_time else None,
            "gross_pnl": self.gross_pnl,
            "fees_paid": self.fees_paid,
            "net_pnl": self.net_pnl,
            "metrics": dict(self.metrics or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotTradeEventRecord(Base):
    """Discrete stop/target events generated while a trade is active."""

    __tablename__ = "portal_bot_trade_events"

    id = Column(String(64), primary_key=True)
    trade_id = Column(
        String(64),
        ForeignKey("portal_bot_trades.id", ondelete="CASCADE"),
        nullable=False,
    )
    bot_id = Column(String(64), ForeignKey("portal_bots.id", ondelete="CASCADE"), nullable=False)
    strategy_id = Column(String(64), ForeignKey("portal_strategies.id", ondelete="SET NULL"), nullable=True)
    symbol = Column(String(64), nullable=True)
    event_type = Column(String(32), nullable=False)
    leg = Column(String(64), nullable=True)
    contracts = Column(Integer, nullable=True)
    price = Column(Float, nullable=True)
    pnl = Column(Float, nullable=True)
    event_time = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Return a serialisable payload of the stored event."""

        return {
            "id": self.id,
            "trade_id": self.trade_id,
            "bot_id": self.bot_id,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "event_type": self.event_type,
            "leg": self.leg,
            "contracts": self.contracts,
            "price": self.price,
            "pnl": self.pnl,
            "event_time": (self.event_time.isoformat() + "Z") if self.event_time else None,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }
