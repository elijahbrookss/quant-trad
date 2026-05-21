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
        "ix_portal_bot_run_events_bot_run_run_seq_id",
        "ix_portal_bot_run_events_bot_run_series_seq_id",
        "ix_portal_bot_run_events_candle_series_bar_time_seq_id",
        "ix_portal_bot_run_events_bot_run_event_name_seq_id",
        "ix_portal_bot_run_events_bot_run_correlation_seq_id",
        "ix_portal_bot_run_events_bot_run_root_seq_id",
        "ix_portal_bot_run_events_bot_run_bar_time_seq_id",
        "uq_portal_bot_run_events_run_seq",
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
    """Database representation of saved strategy output filters."""

    __tablename__ = "portal_strategy_variants"

    id = Column(String(64), primary_key=True)
    strategy_id = Column(String(64), ForeignKey("portal_strategies.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(String(1024), nullable=True)
    output_filters = Column(JSON, nullable=False, default=list)
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
            "output_filters": list(self.output_filters or []),
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


class ProviderCredentialRefRecord(Base):
    """Encrypted provider credential reference metadata."""

    __tablename__ = "portal_provider_credential_refs"

    credential_ref = Column(String(128), primary_key=True)
    provider_id = Column(String(64), nullable=False)
    venue_id = Column(String(64), nullable=False, default="")
    environment = Column(String(32), nullable=False, default="paper")
    display_name = Column(String(255), nullable=True)
    status = Column(String(32), nullable=False, default="active")
    secrets_encrypted = Column(String, nullable=False)
    secret_version = Column(Integer, nullable=False, default=1)
    required_secret_keys = Column(JSON, nullable=False, default=list)
    validation = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_validated_at = Column(DateTime, nullable=True)
    last_used_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True)


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
    market_data_stream_policy = Column(JSON, nullable=False, default=dict)
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

        risk_payload = dict(self.risk or {})
        execution_mode = str(risk_payload.get("execution_mode") or "fast").strip().lower()
        if execution_mode not in {"fast", "full"}:
            execution_mode = "fast"
        execution_behavior = str(risk_payload.get("execution_behavior") or "simulated").strip().lower().replace("_", "-")
        if execution_behavior not in {"simulated", "observe-only"}:
            execution_behavior = "simulated"
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
            "execution_mode": execution_mode,
            "execution_behavior": execution_behavior,
            "run_type": self.run_type,
            "playback_speed": float(self.playback_speed if self.playback_speed is not None else 0.0),
            "backtest_start": (self.backtest_start.isoformat() + "Z") if self.backtest_start else None,
            "backtest_end": (self.backtest_end.isoformat() + "Z") if self.backtest_end else None,
            "risk": risk_payload,
            "wallet_config": dict(self.wallet_config or {}),
            "market_data_stream_policy": dict(self.market_data_stream_policy or {}),
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

        config_snapshot = dict(self.config_snapshot or {})
        risk_settings = dict(config_snapshot.get("risk_settings") or {})
        bot_snapshot = dict(config_snapshot.get("bot") or {})
        bot_risk = dict(bot_snapshot.get("risk") or {})
        execution_mode = str(
            config_snapshot.get("execution_mode")
            or bot_snapshot.get("execution_mode")
            or risk_settings.get("execution_mode")
            or bot_risk.get("execution_mode")
            or "fast"
        ).strip().lower()
        if execution_mode not in {"fast", "full"}:
            execution_mode = "fast"
        execution_behavior = str(
            config_snapshot.get("execution_behavior")
            or bot_snapshot.get("execution_behavior")
            or bot_risk.get("execution_behavior")
            or "simulated"
        ).strip().lower().replace("_", "-")
        if execution_behavior not in {"simulated", "observe-only"}:
            execution_behavior = "simulated"
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
            "execution_mode": execution_mode,
            "execution_behavior": execution_behavior,
            "config_snapshot": config_snapshot,
            "decision_ledger": list(self.decision_ledger or []),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class ReportMaterializationRecord(Base):
    """Persisted RunReportDTO artifact and build status for one run."""

    __tablename__ = "portal_report_materializations_v1"

    run_id = Column(String(64), ForeignKey("portal_bot_runs.run_id", ondelete="CASCADE"), primary_key=True)
    contract_version = Column(String(64), nullable=False, default="run_report_v2")
    status = Column(String(32), nullable=False, default="not_started")
    artifact_id = Column(String(160), nullable=True)
    artifact = Column(JSONB, nullable=True)
    cache_key = Column(String(255), nullable=True)
    stale_reason = Column(String(512), nullable=True)
    error = Column(String(2048), nullable=True)
    started_at = Column(DateTime, nullable=True)
    built_at = Column(DateTime, nullable=True)
    duration_ms = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Return a serialisable report materialization status."""

        effective_status = self.status
        if self.status == "ready" and not isinstance(self.artifact, dict):
            effective_status = "stale"
        can_view = effective_status == "ready" and isinstance(self.artifact, dict)
        can_build = effective_status in {"not_started", "failed", "stale"}
        can_retry = effective_status == "failed"
        return {
            "run_id": self.run_id,
            "status": effective_status,
            "contract_version": self.contract_version,
            "artifact_id": self.artifact_id,
            "artifact_path": None,
            "built_at": (self.built_at.isoformat() + "Z") if self.built_at else None,
            "started_at": (self.started_at.isoformat() + "Z") if self.started_at else None,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "stale_reason": self.stale_reason or ("missing_artifact" if effective_status == "stale" else None),
            "cache_key": self.cache_key,
            "can_view": can_view,
            "can_build": can_build,
            "can_retry": can_retry,
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


class BotRunLeaseRecord(Base):
    """Runner-agnostic ownership lease for one bot run."""

    __tablename__ = "portal_bot_run_leases"
    __table_args__ = (
        Index("ix_portal_bot_run_leases_bot_status_expires", "bot_id", "status", "expires_at"),
        Index("ix_portal_bot_run_leases_runner_status", "runner_id", "status"),
    )

    run_id = Column(String(64), ForeignKey("portal_bot_runs.run_id", ondelete="CASCADE"), primary_key=True)
    bot_id = Column(String(64), ForeignKey("portal_bots.id", ondelete="CASCADE"), nullable=False)
    runner_id = Column(String(128), nullable=False)
    lease_token_hash = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="active")
    generation = Column(Integer, nullable=False, default=1)
    acquired_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    renewed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)
    released_at = Column(DateTime, nullable=True)
    lease_metadata = Column("metadata", JSONB, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "bot_id": self.bot_id,
            "runner_id": self.runner_id,
            "lease_token_hash": self.lease_token_hash,
            "status": self.status,
            "generation": int(self.generation or 0),
            "acquired_at": (self.acquired_at or datetime.utcnow()).isoformat() + "Z",
            "renewed_at": (self.renewed_at or datetime.utcnow()).isoformat() + "Z",
            "expires_at": (self.expires_at or datetime.utcnow()).isoformat() + "Z",
            "released_at": (self.released_at.isoformat() + "Z") if self.released_at else None,
            "metadata": dict(self.lease_metadata or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class BotRunStepRollupRecord(Base):
    """Bucketed runtime step profiler metric rollup."""

    __tablename__ = "portal_bot_run_step_rollups_v1"
    __table_args__ = (
        UniqueConstraint(
            "bucket_start",
            "bucket_seconds",
            "run_id",
            "bot_id",
            "step_name",
            "metric_name",
            "strategy_id",
            "symbol",
            "timeframe",
            "status",
            name="uq_portal_bot_run_step_rollups_v1_bucket_identity",
        ),
        Index("ix_portal_bot_run_step_rollups_v1_run_bucket", "run_id", "bucket_start"),
        Index(
            "ix_portal_bot_run_step_rollups_v1_run_step_metric_bucket",
            "run_id",
            "step_name",
            "metric_name",
            "bucket_start",
        ),
        Index("ix_portal_bot_run_step_rollups_v1_bot_bucket", "bot_id", "bucket_start"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    bucket_start = Column(DateTime, nullable=False)
    bucket_seconds = Column(Integer, nullable=False, default=10)
    first_seen = Column(DateTime, nullable=False)
    last_seen = Column(DateTime, nullable=False)
    run_id = Column(String(64), nullable=False)
    bot_id = Column(String(64), nullable=False, default="")
    step_name = Column(String(64), nullable=False)
    metric_name = Column(String(128), nullable=False)
    strategy_id = Column(String(64), nullable=False, default="")
    symbol = Column(String(64), nullable=False, default="")
    timeframe = Column(String(32), nullable=False, default="")
    status = Column(String(32), nullable=False, default="ok")
    sample_count = Column(Integer, nullable=False, default=0)
    value_sum = Column(Float, nullable=False, default=0.0)
    value_min = Column(Float, nullable=False, default=0.0)
    value_max = Column(Float, nullable=False, default=0.0)
    latest_value = Column(Float, nullable=False, default=0.0)
    p95_value = Column(Float, nullable=False, default=0.0)
    p99_value = Column(Float, nullable=False, default=0.0)
    histogram_bounds = Column(JSONB, nullable=False, default=list)
    histogram_counts = Column(JSONB, nullable=False, default=list)
    raw_sample_count = Column(Integer, nullable=False, default=0)
    error_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        sample_count = int(self.sample_count or 0)
        avg_value = (float(self.value_sum or 0.0) / sample_count) if sample_count > 0 else None
        return {
            "id": self.id,
            "bucket_start": (self.bucket_start.isoformat() + "Z") if self.bucket_start else None,
            "bucket_seconds": int(self.bucket_seconds or 0),
            "first_seen": (self.first_seen.isoformat() + "Z") if self.first_seen else None,
            "last_seen": (self.last_seen.isoformat() + "Z") if self.last_seen else None,
            "run_id": self.run_id,
            "bot_id": self.bot_id or None,
            "step_name": self.step_name,
            "metric_name": self.metric_name,
            "strategy_id": self.strategy_id or None,
            "symbol": self.symbol or None,
            "timeframe": self.timeframe or None,
            "status": self.status,
            "sample_count": sample_count,
            "value_sum": float(self.value_sum or 0.0),
            "value_min": float(self.value_min or 0.0),
            "value_max": float(self.value_max or 0.0),
            "latest_value": float(self.latest_value or 0.0),
            "avg_value": avg_value,
            "p95_value": float(self.p95_value or 0.0),
            "p99_value": float(self.p99_value or 0.0),
            "histogram_bounds": list(self.histogram_bounds or []),
            "histogram_counts": list(self.histogram_counts or []),
            "raw_sample_count": int(self.raw_sample_count or 0),
            "error_count": int(self.error_count or 0),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }

class BotRunEventRecord(Base):
    """Durable runtime event log for BotLens snapshot+stream delivery."""

    __tablename__ = "portal_bot_run_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_portal_bot_run_events_event_id"),
        Index("ix_portal_bot_run_events_bot_run_seq_id", "bot_id", "run_id", "seq", "id"),
        Index("ix_portal_bot_run_events_bot_run_run_seq_id", "bot_id", "run_id", "run_seq", "id"),
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
    run_seq = Column(Integer, nullable=True)
    run_seq_status = Column(String(64), nullable=True)
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
            "run_seq": int(self.run_seq) if self.run_seq is not None else None,
            "run_seq_status": self.run_seq_status,
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
    "uq_portal_bot_run_events_run_seq",
    BotRunEventRecord.run_id,
    BotRunEventRecord.run_seq,
    unique=True,
    postgresql_where=BotRunEventRecord.run_seq.isnot(None),
)

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


class BotRunEventSeqAllocatorRecord(Base):
    """Per-run allocator for dense runtime event replay sequence numbers."""

    __tablename__ = "portal_bot_run_event_seq_allocators"

    run_id = Column(String(64), primary_key=True)
    next_run_seq = Column(Integer, nullable=False, default=1)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "next_run_seq": int(self.next_run_seq or 1),
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


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


class BotlensBackendMetricRollupRecord(Base):
    """Bucketed durable backend observability metric rollup row."""

    __tablename__ = "botlens_backend_metric_rollups_v1"
    __table_args__ = (
        UniqueConstraint(
            "bucket_start",
            "bucket_seconds",
            "component",
            "metric_name",
            "metric_kind",
            "bot_id",
            "run_id",
            "instrument_id",
            "series_key",
            "worker_id",
            "queue_name",
            "pipeline_stage",
            "message_kind",
            "delta_type",
            "storage_target",
            "failure_mode",
            "label_hash",
            name="uq_botlens_backend_metric_rollups_v1_bucket_identity",
        ),
        Index("ix_botlens_backend_metric_rollups_v1_bucket_start", "bucket_start"),
        Index(
            "ix_botlens_backend_metric_rollups_v1_metric_bucket",
            "metric_name",
            "bucket_start",
        ),
        Index("ix_botlens_backend_metric_rollups_v1_run_bucket", "run_id", "bucket_start"),
        {"schema": "observability_metrics"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    bucket_start = Column(DateTime, nullable=False)
    bucket_seconds = Column(Integer, nullable=False, default=10)
    first_seen = Column(DateTime, nullable=False)
    last_seen = Column(DateTime, nullable=False)
    component = Column(String(128), nullable=False)
    metric_name = Column(String(128), nullable=False)
    metric_kind = Column(String(32), nullable=False)
    bot_id = Column(String(64), nullable=False, default="")
    run_id = Column(String(64), nullable=False, default="")
    instrument_id = Column(String(128), nullable=False, default="")
    series_key = Column(String(255), nullable=False, default="")
    worker_id = Column(String(128), nullable=False, default="")
    queue_name = Column(String(128), nullable=False, default="")
    pipeline_stage = Column(String(128), nullable=False, default="")
    message_kind = Column(String(128), nullable=False, default="")
    delta_type = Column(String(128), nullable=False, default="")
    storage_target = Column(String(128), nullable=False, default="")
    failure_mode = Column(String(128), nullable=False, default="")
    label_hash = Column(String(64), nullable=False, default="none")
    labels = Column(JSONB, nullable=False, default=dict)
    sample_count = Column(Integer, nullable=False, default=0)
    value_sum = Column(Float, nullable=False, default=0.0)
    value_min = Column(Float, nullable=False, default=0.0)
    value_max = Column(Float, nullable=False, default=0.0)
    latest_value = Column(Float, nullable=False, default=0.0)
    p95_value = Column(Float, nullable=False, default=0.0)
    p99_value = Column(Float, nullable=False, default=0.0)
    raw_sample_count = Column(Integer, nullable=False, default=0)
    source_metric_record_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": int(self.id or 0),
            "bucket_start": (self.bucket_start or datetime.utcnow()).isoformat() + "Z",
            "bucket_seconds": int(self.bucket_seconds or 0),
            "first_seen": (self.first_seen or datetime.utcnow()).isoformat() + "Z",
            "last_seen": (self.last_seen or datetime.utcnow()).isoformat() + "Z",
            "component": self.component,
            "metric_name": self.metric_name,
            "metric_kind": self.metric_kind,
            "bot_id": self.bot_id or None,
            "run_id": self.run_id or None,
            "instrument_id": self.instrument_id or None,
            "series_key": self.series_key or None,
            "worker_id": self.worker_id or None,
            "queue_name": self.queue_name or None,
            "pipeline_stage": self.pipeline_stage or None,
            "message_kind": self.message_kind or None,
            "delta_type": self.delta_type or None,
            "storage_target": self.storage_target or None,
            "failure_mode": self.failure_mode or None,
            "label_hash": self.label_hash,
            "labels": dict(self.labels or {}),
            "sample_count": int(self.sample_count or 0),
            "value_sum": float(self.value_sum or 0.0),
            "value_min": float(self.value_min or 0.0),
            "value_max": float(self.value_max or 0.0),
            "latest_value": float(self.latest_value or 0.0),
            "p95_value": float(self.p95_value or 0.0),
            "p99_value": float(self.p99_value or 0.0),
            "raw_sample_count": int(self.raw_sample_count or 0),
            "source_metric_record_count": int(self.source_metric_record_count or 0),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
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
