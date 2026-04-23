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
    Index,
    Integer,
    String,
    UniqueConstraint,
    and_,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base


Base = declarative_base()


REQUIRED_BOT_RUN_EVENT_INDEXES = frozenset(
    {
        "ix_portal_bot_run_events_bot_run_seq_id",
        "ix_portal_bot_run_events_bot_run_series_seq_id",
        "ix_portal_bot_run_events_candle_series_bar_time_seq_id",
        "ix_portal_bot_run_events_bot_run_event_name_seq_id",
        "ix_portal_bot_run_events_bot_run_correlation_seq_id",
        "ix_portal_bot_run_events_bot_run_root_seq_id",
        "ix_portal_bot_run_events_bot_run_bar_time_seq_id",
    }
)


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
    risk_config = Column(JSON, nullable=False, default=dict)
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
            "risk_config": self.risk_config or {},
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

        raw_conditions = self.conditions if self.conditions is not None else []
        canonical = raw_conditions if isinstance(raw_conditions, dict) else {}
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "name": self.name,
            "action": self.action,
            "intent": canonical.get("intent"),
            "priority": canonical.get("priority", 0),
            "trigger": canonical.get("trigger"),
            "guards": canonical.get("guards") or [],
            "match": self.match,
            "description": self.description,
            "enabled": bool(self.enabled),
            "conditions": raw_conditions,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class StrategyVariantRecord(Base):
    """Database representation of a saved strategy parameter variant."""

    __tablename__ = "portal_strategy_variants"

    id = Column(String(64), primary_key=True)
    strategy_id = Column(String(64), ForeignKey("portal_strategies.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(String(1024), nullable=True)
    param_overrides = Column(JSON, nullable=False, default=dict)
    atm_template_id = Column(String(64), nullable=True)
    is_default = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("strategy_id", "name", name="uq_strategy_variant_name"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Return a serializable payload for the stored strategy variant."""

        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "name": self.name,
            "description": self.description,
            "param_overrides": dict(self.param_overrides or {}),
            "atm_template_id": self.atm_template_id,
            "is_default": bool(self.is_default),
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


class ProviderCredentialRecord(Base):
    """Encrypted provider credential storage."""

    __tablename__ = "portal_provider_credentials"

    provider_id = Column(String(64), primary_key=True)
    venue_id = Column(String(64), primary_key=True, default="")
    secrets_encrypted = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


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
    can_short = Column(Boolean, nullable=False, default=False)
    short_requires_borrow = Column(Boolean, nullable=False, default=False)
    has_funding = Column(Boolean, nullable=False, default=False)
    expiry_ts = Column(DateTime(timezone=True), nullable=True)
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

        metadata = dict(self.extra_metadata or {})
        instrument_fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), dict) else {}
        return {
            "id": self.id,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "instrument_type": self.instrument_type,
            "tick_size": instrument_fields.get("tick_size"),
            "tick_value": instrument_fields.get("tick_value"),
            "contract_size": instrument_fields.get("contract_size"),
            "min_order_size": instrument_fields.get("min_order_size"),
            "qty_step": instrument_fields.get("qty_step"),
            "max_qty": instrument_fields.get("max_qty"),
            "min_notional": instrument_fields.get("min_notional"),
            "base_currency": instrument_fields.get("base_currency"),
            "quote_currency": instrument_fields.get("quote_currency"),
            "maker_fee_rate": instrument_fields.get("maker_fee_rate"),
            "taker_fee_rate": instrument_fields.get("taker_fee_rate"),
            "margin_rates": instrument_fields.get("margin_rates"),
            "can_short": instrument_fields.get("can_short"),
            "short_requires_borrow": instrument_fields.get("short_requires_borrow"),
            "has_funding": instrument_fields.get("has_funding"),
            "expiry_ts": instrument_fields.get("expiry_ts"),
            "metadata": metadata,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotRecord(Base):
    """Database row describing a persisted bot configuration."""

    __tablename__ = "portal_bots"

    id = Column(String(64), primary_key=True)
    name = Column(String(255), nullable=False)
    strategy_id = Column(String(64), nullable=True)
    strategy_variant_id = Column(String(64), nullable=True)
    strategy_variant_name = Column(String(255), nullable=True)
    atm_template_id = Column(String(64), nullable=True)
    resolved_params = Column(JSON, nullable=False, default=dict)
    risk_config = Column(JSON, nullable=False, default=dict)
    mode = Column(String(32), nullable=False, default="instant")
    run_type = Column(String(32), nullable=False, default="backtest")
    playback_speed = Column("fetch_seconds", Float, nullable=False, default=0.0)
    backtest_start = Column(DateTime, nullable=True)
    backtest_end = Column(DateTime, nullable=True)
    risk = Column(JSON, nullable=False, default=dict)
    wallet_config = Column(JSON, nullable=False, default=dict)
    snapshot_interval_ms = Column(Integer, nullable=False, default=250)
    bot_env = Column(JSON, nullable=False, default=dict)
    status = Column(String(32), nullable=False, default="idle")
    last_run_at = Column(DateTime, nullable=True)
    last_stats = Column(JSON, nullable=False, default=dict)
    last_run_artifact = Column(JSON, nullable=True)
    # Heartbeat fields for orphan detection (BotWatchdog)
    runner_id = Column(String(128), nullable=True)
    heartbeat_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Return the bot configuration in API-friendly form."""

        return {
            "id": self.id,
            "name": self.name,
            "strategy_id": self.strategy_id,
            "strategy_variant_id": self.strategy_variant_id,
            "strategy_variant_name": self.strategy_variant_name,
            "atm_template_id": self.atm_template_id,
            "resolved_params": dict(self.resolved_params or {}),
            "risk_config": dict(self.risk_config or {}),
            "mode": self.mode,
            "run_type": self.run_type,
            "playback_speed": float(self.playback_speed if self.playback_speed is not None else 0.0),
            "backtest_start": (self.backtest_start.isoformat() + "Z") if self.backtest_start else None,
            "backtest_end": (self.backtest_end.isoformat() + "Z") if self.backtest_end else None,
            "risk": dict(self.risk or {}),
            "wallet_config": dict(self.wallet_config or {}),
            "snapshot_interval_ms": int(self.snapshot_interval_ms or 0),
            "bot_env": dict(self.bot_env or {}),
            "status": self.status,
            "last_run_at": (self.last_run_at.isoformat() + "Z") if self.last_run_at else None,
            "last_stats": dict(self.last_stats or {}),
            "last_run_artifact": dict(self.last_run_artifact or {}),
            "runner_id": self.runner_id,
            "heartbeat_at": (self.heartbeat_at.isoformat() + "Z") if self.heartbeat_at else None,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }

class BotTradeRecord(Base):
    """Database row representing a laddered trade generated by a bot."""

    __tablename__ = "portal_bot_trades"

    id = Column(String(64), primary_key=True)
    run_id = Column(String(64), nullable=True)
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
            "run_id": self.run_id,
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


class BotRunRecord(Base):
    """Database row representing a completed bot run report snapshot."""

    __tablename__ = "portal_bot_runs"

    run_id = Column(String(64), primary_key=True)
    bot_id = Column(String(64), ForeignKey("portal_bots.id", ondelete="SET NULL"), nullable=True)
    bot_name = Column(String(255), nullable=True)
    strategy_id = Column(String(64), nullable=True)
    strategy_name = Column(String(255), nullable=True)
    run_type = Column(String(32), nullable=False, default="backtest")
    status = Column(String(32), nullable=False, default="completed")
    timeframe = Column(String(32), nullable=True)
    datasource = Column(String(64), nullable=True)
    exchange = Column(String(64), nullable=True)
    symbols = Column(JSON, nullable=True)
    backtest_start = Column(DateTime, nullable=True)
    backtest_end = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)
    summary = Column(JSON, nullable=True)
    config_snapshot = Column(JSON, nullable=True)
    decision_ledger = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the stored run snapshot."""

        return {
            "run_id": self.run_id,
            "bot_id": self.bot_id,
            "bot_name": self.bot_name,
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "run_type": self.run_type,
            "status": self.status,
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "symbols": list(self.symbols or []),
            "backtest_start": (self.backtest_start.isoformat() + "Z") if self.backtest_start else None,
            "backtest_end": (self.backtest_end.isoformat() + "Z") if self.backtest_end else None,
            "started_at": (self.started_at.isoformat() + "Z") if self.started_at else None,
            "ended_at": (self.ended_at.isoformat() + "Z") if self.ended_at else None,
            "summary": dict(self.summary or {}),
            "config_snapshot": dict(self.config_snapshot or {}),
            "decision_ledger": list(self.decision_ledger or []),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotRunLifecycleRecord(Base):
    """Current durable lifecycle state for one bot run."""

    __tablename__ = "portal_bot_run_lifecycle"

    run_id = Column(String(64), ForeignKey("portal_bot_runs.run_id", ondelete="CASCADE"), primary_key=True)
    bot_id = Column(String(64), ForeignKey("portal_bots.id", ondelete="CASCADE"), nullable=False)
    phase = Column(String(64), nullable=False, default="start_requested")
    status = Column(String(32), nullable=False, default="starting")
    owner = Column(String(32), nullable=False, default="backend")
    message = Column(String(1024), nullable=True)
    lifecycle_metadata = Column("metadata", JSONB, nullable=False, default=dict)
    failure = Column(JSONB, nullable=True)
    checkpoint_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "bot_id": self.bot_id,
            "phase": self.phase,
            "status": self.status,
            "owner": self.owner,
            "message": self.message,
            "metadata": dict(self.lifecycle_metadata or {}),
            "failure": dict(self.failure or {}),
            "checkpoint_at": (self.checkpoint_at or datetime.utcnow()).isoformat() + "Z",
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotRunLifecycleEventRecord(Base):
    """Append-only lifecycle checkpoints for one bot run."""

    __tablename__ = "portal_bot_run_lifecycle_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_portal_bot_run_lifecycle_events_event_id"),
        UniqueConstraint("run_id", "seq", name="uq_portal_bot_run_lifecycle_events_run_seq"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(128), nullable=False)
    run_id = Column(String(64), ForeignKey("portal_bot_runs.run_id", ondelete="CASCADE"), nullable=False)
    bot_id = Column(String(64), ForeignKey("portal_bots.id", ondelete="CASCADE"), nullable=False)
    seq = Column(Integer, nullable=False)
    phase = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False)
    owner = Column(String(32), nullable=False)
    message = Column(String(1024), nullable=True)
    lifecycle_metadata = Column("metadata", JSONB, nullable=False, default=dict)
    failure = Column(JSONB, nullable=True)
    checkpoint_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": int(self.id or 0),
            "event_id": self.event_id,
            "run_id": self.run_id,
            "bot_id": self.bot_id,
            "seq": int(self.seq or 0),
            "phase": self.phase,
            "status": self.status,
            "owner": self.owner,
            "message": self.message,
            "metadata": dict(self.lifecycle_metadata or {}),
            "failure": dict(self.failure or {}),
            "checkpoint_at": (self.checkpoint_at or datetime.utcnow()).isoformat() + "Z",
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotRunStepRecord(Base):
    """Timed runtime step trace entry for bot-run profiling."""

    __tablename__ = "portal_bot_run_steps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), nullable=False)
    bot_id = Column(String(64), nullable=True)
    step_name = Column(String(64), nullable=False)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=False)
    duration_ms = Column(Float, nullable=False)
    ok = Column(Boolean, nullable=False, default=True)
    strategy_id = Column(String(64), nullable=True)
    symbol = Column(String(64), nullable=True)
    timeframe = Column(String(32), nullable=True)
    error = Column(String(1024), nullable=True)
    context = Column(JSONB, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "run_id": self.run_id,
            "bot_id": self.bot_id,
            "step_name": self.step_name,
            "started_at": (self.started_at.isoformat() + "Z") if self.started_at else None,
            "ended_at": (self.ended_at.isoformat() + "Z") if self.ended_at else None,
            "duration_ms": self.duration_ms,
            "ok": bool(self.ok),
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "error": self.error,
            "context": dict(self.context or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }

class BotRunEventRecord(Base):
    """Durable runtime event log for BotLens snapshot+stream delivery."""

    __tablename__ = "portal_bot_run_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_portal_bot_run_events_event_id"),
        Index("ix_portal_bot_run_events_bot_run_seq_id", "bot_id", "run_id", "seq", "id"),
        Index("ix_portal_bot_run_events_bot_run_series_seq_id", "bot_id", "run_id", "series_key", "seq", "id"),
        Index("ix_portal_bot_run_events_bot_run_event_name_seq_id", "bot_id", "run_id", "event_name", "seq", "id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(128), nullable=False)
    bot_id = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    seq = Column(Integer, nullable=False)
    event_type = Column(String(64), nullable=False, default="state_delta")
    critical = Column(Boolean, nullable=False, default=False)
    schema_version = Column(Integer, nullable=False, default=1)
    payload = Column(JSONB, nullable=False, default=dict)
    event_name = Column(String(128), nullable=True)
    series_key = Column(String(255), nullable=True)
    correlation_id = Column(String(128), nullable=True)
    root_id = Column(String(128), nullable=True)
    bar_time = Column(DateTime, nullable=True)
    instrument_id = Column(String(128), nullable=True)
    symbol = Column(String(64), nullable=True)
    timeframe = Column(String(32), nullable=True)
    signal_id = Column(String(128), nullable=True)
    decision_id = Column(String(128), nullable=True)
    trade_id = Column(String(128), nullable=True)
    reason_code = Column(String(128), nullable=True)
    event_time = Column(DateTime, nullable=True)
    known_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": int(self.id or 0),
            "event_id": self.event_id,
            "bot_id": self.bot_id,
            "run_id": self.run_id,
            "seq": int(self.seq or 0),
            "event_type": self.event_type,
            "critical": bool(self.critical),
            "schema_version": int(self.schema_version or 1),
            "payload": dict(self.payload or {}),
            "event_name": self.event_name,
            "series_key": self.series_key,
            "correlation_id": self.correlation_id,
            "root_id": self.root_id,
            "bar_time": (self.bar_time.isoformat() + "Z") if self.bar_time else None,
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "signal_id": self.signal_id,
            "decision_id": self.decision_id,
            "trade_id": self.trade_id,
            "reason_code": self.reason_code,
            "event_time": (self.event_time.isoformat() + "Z") if self.event_time else None,
            "known_at": (self.known_at or datetime.utcnow()).isoformat() + "Z",
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


Index(
    "ix_portal_bot_run_events_candle_series_bar_time_seq_id",
    BotRunEventRecord.bot_id,
    BotRunEventRecord.run_id,
    BotRunEventRecord.series_key,
    BotRunEventRecord.bar_time,
    BotRunEventRecord.seq,
    BotRunEventRecord.id,
    postgresql_where=and_(
        BotRunEventRecord.event_name == "CANDLE_OBSERVED",
        BotRunEventRecord.series_key.isnot(None),
        BotRunEventRecord.bar_time.isnot(None),
    ),
)

Index(
    "ix_portal_bot_run_events_bot_run_correlation_seq_id",
    BotRunEventRecord.bot_id,
    BotRunEventRecord.run_id,
    BotRunEventRecord.correlation_id,
    BotRunEventRecord.seq,
    BotRunEventRecord.id,
    postgresql_where=BotRunEventRecord.correlation_id.isnot(None),
)

Index(
    "ix_portal_bot_run_events_bot_run_root_seq_id",
    BotRunEventRecord.bot_id,
    BotRunEventRecord.run_id,
    BotRunEventRecord.root_id,
    BotRunEventRecord.seq,
    BotRunEventRecord.id,
    postgresql_where=BotRunEventRecord.root_id.isnot(None),
)

Index(
    "ix_portal_bot_run_events_bot_run_bar_time_seq_id",
    BotRunEventRecord.bot_id,
    BotRunEventRecord.run_id,
    BotRunEventRecord.bar_time,
    BotRunEventRecord.seq,
    BotRunEventRecord.id,
    postgresql_where=BotRunEventRecord.bar_time.isnot(None),
)


class BotlensBackendEventRecord(Base):
    """Durable backend observability event row for BotLens/Grafana queries."""

    __tablename__ = "botlens_backend_events_v1"
    __table_args__ = (
        Index("ix_botlens_backend_events_v1_observed_at", "observed_at"),
        Index("ix_botlens_backend_events_v1_event_name_observed_at", "event_name", "observed_at"),
        Index("ix_botlens_backend_events_v1_run_id_observed_at", "run_id", "observed_at"),
        {"schema": "observability_events"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    observed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    component = Column(String(128), nullable=False)
    event_name = Column(String(128), nullable=False)
    level = Column(String(32), nullable=False, default="INFO")
    bot_id = Column(String(64), nullable=True)
    run_id = Column(String(64), nullable=True)
    instrument_id = Column(String(128), nullable=True)
    series_key = Column(String(255), nullable=True)
    worker_id = Column(String(128), nullable=True)
    queue_name = Column(String(128), nullable=True)
    pipeline_stage = Column(String(128), nullable=True)
    message_kind = Column(String(128), nullable=True)
    delta_type = Column(String(128), nullable=True)
    storage_target = Column(String(128), nullable=True)
    failure_mode = Column(String(128), nullable=True)
    phase = Column(String(128), nullable=True)
    status = Column(String(128), nullable=True)
    run_seq = Column(Integer, nullable=True)
    bridge_session_id = Column(String(128), nullable=True)
    bridge_seq = Column(Integer, nullable=True)
    message = Column(String(2048), nullable=True)
    details = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": int(self.id or 0),
            "observed_at": (self.observed_at or datetime.utcnow()).isoformat() + "Z",
            "component": self.component,
            "event_name": self.event_name,
            "level": self.level,
            "bot_id": self.bot_id,
            "run_id": self.run_id,
            "instrument_id": self.instrument_id,
            "series_key": self.series_key,
            "worker_id": self.worker_id,
            "queue_name": self.queue_name,
            "pipeline_stage": self.pipeline_stage,
            "message_kind": self.message_kind,
            "delta_type": self.delta_type,
            "storage_target": self.storage_target,
            "failure_mode": self.failure_mode,
            "phase": self.phase,
            "status": self.status,
            "run_seq": int(self.run_seq) if self.run_seq is not None else None,
            "bridge_session_id": self.bridge_session_id,
            "bridge_seq": int(self.bridge_seq) if self.bridge_seq is not None else None,
            "message": self.message,
            "details": dict(self.details or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotlensBackendMetricSampleRecord(Base):
    """Durable backend observability metric sample row for BotLens/Grafana queries."""

    __tablename__ = "botlens_backend_metric_samples_v1"
    __table_args__ = (
        Index("ix_botlens_backend_metric_samples_v1_observed_at", "observed_at"),
        Index(
            "ix_botlens_backend_metric_samples_v1_metric_name_observed_at",
            "metric_name",
            "observed_at",
        ),
        Index("ix_botlens_backend_metric_samples_v1_run_id_observed_at", "run_id", "observed_at"),
        {"schema": "observability_metrics"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    observed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    component = Column(String(128), nullable=False)
    metric_name = Column(String(128), nullable=False)
    metric_kind = Column(String(32), nullable=False)
    value = Column(Float, nullable=False)
    bot_id = Column(String(64), nullable=True)
    run_id = Column(String(64), nullable=True)
    instrument_id = Column(String(128), nullable=True)
    series_key = Column(String(255), nullable=True)
    worker_id = Column(String(128), nullable=True)
    queue_name = Column(String(128), nullable=True)
    pipeline_stage = Column(String(128), nullable=True)
    message_kind = Column(String(128), nullable=True)
    delta_type = Column(String(128), nullable=True)
    storage_target = Column(String(128), nullable=True)
    failure_mode = Column(String(128), nullable=True)
    labels = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": int(self.id or 0),
            "observed_at": (self.observed_at or datetime.utcnow()).isoformat() + "Z",
            "component": self.component,
            "metric_name": self.metric_name,
            "metric_kind": self.metric_kind,
            "value": float(self.value or 0.0),
            "bot_id": self.bot_id,
            "run_id": self.run_id,
            "instrument_id": self.instrument_id,
            "series_key": self.series_key,
            "worker_id": self.worker_id,
            "queue_name": self.queue_name,
            "pipeline_stage": self.pipeline_stage,
            "message_kind": self.message_kind,
            "delta_type": self.delta_type,
            "storage_target": self.storage_target,
            "failure_mode": self.failure_mode,
            "labels": dict(self.labels or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


class AsyncJobRecord(Base):
    """Database-backed async job used by API and worker processes."""

    __tablename__ = "portal_async_jobs"

    id = Column(String(64), primary_key=True)
    job_type = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="queued")
    payload = Column(JSONB, nullable=False, default=dict)
    result = Column(JSONB, nullable=True)
    error = Column(String(2048), nullable=True)
    partition_key = Column(String(255), nullable=True)
    partition_hash = Column(Integer, nullable=False, default=0)
    attempts = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=3)
    lock_owner = Column(String(128), nullable=True)
    locked_at = Column(DateTime, nullable=True)
    available_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "job_type": self.job_type,
            "status": self.status,
            "payload": dict(self.payload or {}),
            "result": dict(self.result or {}) if isinstance(self.result, dict) else self.result,
            "error": self.error,
            "partition_key": self.partition_key,
            "partition_hash": int(self.partition_hash or 0),
            "attempts": int(self.attempts or 0),
            "max_attempts": int(self.max_attempts or 0),
            "lock_owner": self.lock_owner,
            "locked_at": (self.locked_at.isoformat() + "Z") if self.locked_at else None,
            "available_at": (self.available_at.isoformat() + "Z") if self.available_at else None,
            "started_at": (self.started_at.isoformat() + "Z") if self.started_at else None,
            "finished_at": (self.finished_at.isoformat() + "Z") if self.finished_at else None,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }
