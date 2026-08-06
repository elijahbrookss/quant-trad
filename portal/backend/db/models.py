"""SQLAlchemy models for portal persistence."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import (
    JSON,
    Boolean,
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    and_,
    text,
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

REQUIRED_BOT_RUN_LEASE_INDEXES = frozenset(
    {
        "ix_portal_bot_run_leases_bot_status_expires",
        "ix_portal_bot_run_leases_runner_status",
        "ix_portal_bot_run_leases_runner_status_expires",
        "ix_portal_bot_run_leases_status_expires",
    }
)

REQUIRED_BOT_RUN_INDEXES = frozenset(
    {
        "ix_portal_bot_runs_report_list",
        "ix_portal_bot_runs_bot_report_list",
    }
)

REQUIRED_REPORT_MATERIALIZATION_INDEXES = frozenset(
    {
        "ix_portal_report_materializations_input_fingerprint",
    }
)

REQUIRED_RESEARCH_ITEM_INDEXES = frozenset(
    {
        "ix_portal_research_items_kind_status_updated",
        "ix_portal_research_items_symbol_timeframe",
    }
)

REQUIRED_RESEARCH_LINK_INDEXES = frozenset(
    {
        "ix_portal_research_links_source_relation",
        "ix_portal_research_links_target",
    }
)

REQUIRED_RESEARCH_AUTHORITY_INDEXES = {
    "portal_research_protocols": frozenset(
        {"ix_research_protocols_status_created", "ix_research_protocols_hash"}
    ),
    "portal_research_families": frozenset(
        {"ix_research_families_protocol_status", "ix_research_families_hash"}
    ),
    "portal_research_attempts": frozenset(
        {"ix_research_attempts_family_status", "ix_research_attempts_protocol_role"}
    ),
    "portal_research_strategy_graphs": frozenset(
        {"ix_research_strategy_graphs_family_version", "ix_research_strategy_graphs_hash"}
    ),
    "portal_research_candidates": frozenset(
        {"ix_research_candidates_family_created", "ix_research_candidates_hash"}
    ),
    "portal_research_holdout_uses": frozenset(
        {"ix_research_holdout_uses_protocol_status"}
    ),
    "portal_research_certificates": frozenset(
        {"ix_research_certificates_family_quality", "ix_research_certificates_hash"}
    ),
    "portal_research_authority_events": frozenset(
        {"ix_research_authority_events_aggregate", "ix_research_authority_events_request"}
    ),
    "portal_research_governance_cases": frozenset(
        {"ix_research_governance_cases_state_updated", "ix_research_governance_cases_family"}
    ),
    "portal_research_governance_proposals": frozenset(
        {"ix_research_governance_proposals_case_version", "ix_research_governance_proposals_request"}
    ),
    "portal_research_governance_decisions": frozenset(
        {"ix_research_governance_decisions_case_created", "ix_research_governance_decisions_request"}
    ),
}

REQUIRED_PROVIDER_CREDENTIAL_INDEXES = frozenset(
    {
        "ix_provider_credential_refs_provider_venue",
    }
)

REQUIRED_ASYNC_JOB_INDEXES = frozenset(
    {
        "ix_portal_async_jobs_claimable",
        "ix_portal_async_jobs_running_heartbeat",
        "uq_portal_async_jobs_inflight_request",
    }
)

REQUIRED_ASYNC_JOB_CONSTRAINTS = frozenset(
    {
        "ck_portal_async_jobs_claim_generation_nonnegative",
        "ck_portal_async_jobs_claim_state",
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

    __table_args__ = (
        Index(
            "ix_provider_credential_refs_provider_venue",
            "provider_id",
            "venue_id",
            "environment",
            postgresql_where=revoked_at.is_(None),
        ),
    )


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


class ResearchItemRecord(Base):
    """Research-memory item for observations, checks, hypotheses, and studies."""

    __tablename__ = "portal_research_items"
    __table_args__ = (
        Index("ix_portal_research_items_kind_status_updated", "kind", "status", "updated_at"),
        Index("ix_portal_research_items_symbol_timeframe", "symbol", "timeframe"),
    )

    id = Column(String(64), primary_key=True)
    kind = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False, default="draft")
    title = Column(String(255), nullable=False)
    body = Column(String(8192), nullable=True)
    instrument_id = Column(String(64), nullable=True)
    symbol = Column(String(64), nullable=True)
    timeframe = Column(String(32), nullable=True)
    datasource = Column(String(64), nullable=True)
    exchange = Column(String(64), nullable=True)
    window_start = Column(DateTime, nullable=True)
    window_end = Column(DateTime, nullable=True)
    tags = Column(JSONB, nullable=False, default=list)
    payload = Column(JSONB, nullable=False, default=dict)
    source_revision = Column(String(128), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Return the research item as an API payload."""

        return {
            "id": self.id,
            "kind": self.kind,
            "status": self.status,
            "title": self.title,
            "body": self.body,
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "window_start": (self.window_start.isoformat() + "Z") if self.window_start else None,
            "window_end": (self.window_end.isoformat() + "Z") if self.window_end else None,
            "tags": list(self.tags or []),
            "payload": dict(self.payload or {}),
            "source_revision": self.source_revision,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class ResearchLinkRecord(Base):
    """Directed research-memory link to another research item or platform artifact."""

    __tablename__ = "portal_research_links"
    __table_args__ = (
        Index("ix_portal_research_links_source_relation", "source_item_id", "relation"),
        Index("ix_portal_research_links_target", "target_type", "target_id"),
        UniqueConstraint(
            "source_item_id",
            "target_type",
            "target_id",
            "relation",
            name="uq_research_link_identity",
        ),
    )

    id = Column(String(96), primary_key=True)
    source_item_id = Column(
        String(64),
        ForeignKey("portal_research_items.id", ondelete="CASCADE"),
        nullable=False,
    )
    target_type = Column(String(64), nullable=False)
    target_id = Column(String(255), nullable=False)
    relation = Column(String(64), nullable=False)
    link_metadata = Column("metadata", JSONB, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Return the research link as an API payload."""

        return {
            "id": self.id,
            "source_item_id": self.source_item_id,
            "target_type": self.target_type,
            "target_id": self.target_id,
            "relation": self.relation,
            "metadata": dict(self.link_metadata or {}),
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class ResearchProtocolRecord(Base):
    """Immutable private/public scientific protocol manifests."""

    __tablename__ = "portal_research_protocols"
    __table_args__ = (
        UniqueConstraint("protocol_hash", name="uq_research_protocol_hash"),
        Index("ix_research_protocols_status_created", "status", "created_at"),
        Index("ix_research_protocols_hash", "protocol_hash"),
    )

    id = Column(String(64), primary_key=True)
    schema_version = Column(String(64), nullable=False)
    protocol_hash = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="active")
    blindness_class = Column(String(64), nullable=False)
    private_manifest = Column(JSONB, nullable=False)
    public_manifest = Column(JSONB, nullable=False)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)

    def to_dict(self, *, private: bool = False) -> Dict[str, Any]:
        return {
            "id": self.id,
            "schema_version": self.schema_version,
            "protocol_hash": self.protocol_hash,
            "status": self.status,
            "blindness_class": self.blindness_class,
            "manifest": dict(self.private_manifest if private else self.public_manifest or {}),
            "created_by": self.created_by,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "closed_at": (self.closed_at.isoformat() + "Z") if self.closed_at else None,
        }


class ResearchFamilyRecord(Base):
    """Mutable projection for one protocol-bound search family."""

    __tablename__ = "portal_research_families"
    __table_args__ = (
        UniqueConstraint("family_hash", name="uq_research_family_hash"),
        Index("ix_research_families_protocol_status", "protocol_id", "status"),
        Index("ix_research_families_hash", "family_hash"),
    )

    id = Column(String(64), primary_key=True)
    protocol_id = Column(String(64), ForeignKey("portal_research_protocols.id", ondelete="RESTRICT"), nullable=False)
    family_hash = Column(String(64), nullable=False)
    name = Column(String(255), nullable=False)
    status = Column(String(32), nullable=False, default="open")
    current_candidate_id = Column(String(64), nullable=True)
    feedback_released = Column(Boolean, nullable=False, default=False)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "protocol_id": self.protocol_id,
            "family_hash": self.family_hash,
            "name": self.name,
            "status": self.status,
            "current_candidate_id": self.current_candidate_id,
            "feedback_released": bool(self.feedback_released),
            "created_by": self.created_by,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "closed_at": (self.closed_at.isoformat() + "Z") if self.closed_at else None,
        }


class ResearchAttemptRecord(Base):
    """Every admitted trial, including failed and abandoned outcomes."""

    __tablename__ = "portal_research_attempts"
    __table_args__ = (
        UniqueConstraint("family_id", "attempt_ordinal", name="uq_research_attempt_family_ordinal"),
        UniqueConstraint("family_id", "request_id", name="uq_research_attempt_family_request"),
        Index("ix_research_attempts_family_status", "family_id", "status", "attempt_ordinal"),
        Index("ix_research_attempts_protocol_role", "protocol_id", "dataset_role"),
    )

    id = Column(String(64), primary_key=True)
    protocol_id = Column(String(64), ForeignKey("portal_research_protocols.id", ondelete="RESTRICT"), nullable=False)
    family_id = Column(String(64), ForeignKey("portal_research_families.id", ondelete="RESTRICT"), nullable=False)
    attempt_ordinal = Column(Integer, nullable=False)
    request_id = Column(String(128), nullable=False)
    dataset_role = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False, default="registered")
    trial_manifest_hash = Column(String(64), nullable=False)
    trial_manifest = Column(JSONB, nullable=False)
    result_evidence = Column(JSONB, nullable=True)
    error = Column(String(2048), nullable=True)
    estimated_runtime_seconds = Column(Float, nullable=False, default=0.0)
    estimated_compute_units = Column(Float, nullable=False, default=0.0)
    actual_runtime_seconds = Column(Float, nullable=True)
    actual_compute_units = Column(Float, nullable=True)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "protocol_id": self.protocol_id,
            "family_id": self.family_id,
            "attempt_ordinal": int(self.attempt_ordinal),
            "request_id": self.request_id,
            "dataset_role": self.dataset_role,
            "status": self.status,
            "trial_manifest_hash": self.trial_manifest_hash,
            "trial_manifest": dict(self.trial_manifest or {}),
            "result_evidence": dict(self.result_evidence or {}) if self.result_evidence is not None else None,
            "error": self.error,
            "estimated_runtime_seconds": float(self.estimated_runtime_seconds or 0.0),
            "estimated_compute_units": float(self.estimated_compute_units or 0.0),
            "actual_runtime_seconds": float(self.actual_runtime_seconds) if self.actual_runtime_seconds is not None else None,
            "actual_compute_units": float(self.actual_compute_units) if self.actual_compute_units is not None else None,
            "created_by": self.created_by,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "finished_at": (self.finished_at.isoformat() + "Z") if self.finished_at else None,
        }


class ResearchCandidateRecord(Base):
    """Immutable candidate frozen before any final holdout access."""

    __tablename__ = "portal_research_candidates"
    __table_args__ = (
        UniqueConstraint("candidate_hash", name="uq_research_candidate_hash"),
        UniqueConstraint("family_id", "candidate_version", name="uq_research_candidate_family_version"),
        Index("ix_research_candidates_family_created", "family_id", "created_at"),
        Index("ix_research_candidates_hash", "candidate_hash"),
    )

    id = Column(String(64), primary_key=True)
    protocol_id = Column(String(64), ForeignKey("portal_research_protocols.id", ondelete="RESTRICT"), nullable=False)
    family_id = Column(String(64), ForeignKey("portal_research_families.id", ondelete="RESTRICT"), nullable=False)
    source_attempt_id = Column(String(64), ForeignKey("portal_research_attempts.id", ondelete="RESTRICT"), nullable=False)
    candidate_version = Column(Integer, nullable=False)
    candidate_hash = Column(String(64), nullable=False)
    manifest = Column(JSONB, nullable=False)
    frozen_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "protocol_id": self.protocol_id,
            "family_id": self.family_id,
            "source_attempt_id": self.source_attempt_id,
            "candidate_version": int(self.candidate_version),
            "candidate_hash": self.candidate_hash,
            "manifest": dict(self.manifest or {}),
            "frozen_by": self.frozen_by,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


class ResearchStrategyGraphRecord(Base):
    """Immutable typed strategy graph admitted through one budgeted attempt."""

    __tablename__ = "portal_research_strategy_graphs"
    __table_args__ = (
        UniqueConstraint("graph_hash", name="uq_research_strategy_graph_hash"),
        UniqueConstraint("search_attempt_id", name="uq_research_strategy_graph_attempt"),
        UniqueConstraint("family_id", "graph_version", name="uq_research_strategy_graph_family_version"),
        Index("ix_research_strategy_graphs_family_version", "family_id", "graph_version"),
        Index("ix_research_strategy_graphs_hash", "graph_hash"),
    )

    id = Column(String(64), primary_key=True)
    protocol_id = Column(String(64), ForeignKey("portal_research_protocols.id", ondelete="RESTRICT"), nullable=False)
    family_id = Column(String(64), ForeignKey("portal_research_families.id", ondelete="RESTRICT"), nullable=False)
    search_attempt_id = Column(String(64), ForeignKey("portal_research_attempts.id", ondelete="RESTRICT"), nullable=False)
    parent_graph_ids = Column(JSONB, nullable=False)
    graph_version = Column(Integer, nullable=False)
    graph_hash = Column(String(64), nullable=False)
    compiled_hash = Column(String(64), nullable=False)
    manifest = Column(JSONB, nullable=False)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "protocol_id": self.protocol_id,
            "family_id": self.family_id,
            "search_attempt_id": self.search_attempt_id,
            "parent_graph_ids": list(self.parent_graph_ids or []),
            "graph_version": int(self.graph_version),
            "graph_hash": self.graph_hash,
            "compiled_hash": self.compiled_hash,
            "manifest": dict(self.manifest or {}),
            "created_by": self.created_by,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


class ResearchHoldoutUseRecord(Base):
    """Database-unique one-use reservation for a family final holdout."""

    __tablename__ = "portal_research_holdout_uses"
    __table_args__ = (
        UniqueConstraint("family_id", name="uq_research_holdout_family_once"),
        UniqueConstraint("request_id", name="uq_research_holdout_request"),
        Index("ix_research_holdout_uses_protocol_status", "protocol_id", "status"),
    )

    id = Column(String(64), primary_key=True)
    protocol_id = Column(String(64), ForeignKey("portal_research_protocols.id", ondelete="RESTRICT"), nullable=False)
    family_id = Column(String(64), ForeignKey("portal_research_families.id", ondelete="RESTRICT"), nullable=False)
    candidate_id = Column(String(64), ForeignKey("portal_research_candidates.id", ondelete="RESTRICT"), nullable=False)
    request_id = Column(String(128), nullable=False)
    status = Column(String(32), nullable=False, default="reserved")
    blindness_class = Column(String(64), nullable=False)
    reservation_token_hash = Column(String(64), nullable=False)
    reserved_by = Column(String(128), nullable=False)
    executor_actor = Column(String(128), nullable=True)
    result_evidence = Column(JSONB, nullable=True)
    feedback_released = Column(Boolean, nullable=False, default=False)
    reserved_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    def to_dict(self, *, include_result: bool = False) -> Dict[str, Any]:
        payload = {
            "id": self.id,
            "protocol_id": self.protocol_id,
            "family_id": self.family_id,
            "candidate_id": self.candidate_id,
            "request_id": self.request_id,
            "status": self.status,
            "blindness_class": self.blindness_class,
            "reserved_by": self.reserved_by,
            "executor_actor": self.executor_actor,
            "feedback_released": bool(self.feedback_released),
            "reserved_at": (self.reserved_at or datetime.utcnow()).isoformat() + "Z",
            "completed_at": (self.completed_at.isoformat() + "Z") if self.completed_at else None,
        }
        # ``include_result`` is an internal capability decision made by the
        # repository.  Public projections pass it only after feedback release;
        # the sealed certifier needs the evidence before that release.
        payload["result_evidence"] = (
            dict(self.result_evidence or {}) if include_result else None
        )
        return payload


class ResearchCertificateRecord(Base):
    """Append-only scientific quality certificate."""

    __tablename__ = "portal_research_certificates"
    __table_args__ = (
        UniqueConstraint("certificate_hash", name="uq_research_certificate_hash"),
        Index("ix_research_certificates_family_quality", "family_id", "scientific_quality_class", "created_at"),
        Index("ix_research_certificates_hash", "certificate_hash"),
    )

    id = Column(String(64), primary_key=True)
    protocol_id = Column(String(64), ForeignKey("portal_research_protocols.id", ondelete="RESTRICT"), nullable=False)
    family_id = Column(String(64), ForeignKey("portal_research_families.id", ondelete="RESTRICT"), nullable=False)
    candidate_id = Column(String(64), ForeignKey("portal_research_candidates.id", ondelete="RESTRICT"), nullable=False)
    scientific_quality_class = Column(String(8), nullable=False)
    status = Column(String(32), nullable=False)
    evidence = Column(JSONB, nullable=False)
    certificate_hash = Column(String(64), nullable=False)
    created_by = Column(String(128), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "protocol_id": self.protocol_id,
            "family_id": self.family_id,
            "candidate_id": self.candidate_id,
            "scientific_quality_class": self.scientific_quality_class,
            "status": self.status,
            "evidence": dict(self.evidence or {}),
            "certificate_hash": self.certificate_hash,
            "created_by": self.created_by,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


class ResearchAuthorityEventRecord(Base):
    """Append-only audit event for every scientific authority mutation."""

    __tablename__ = "portal_research_authority_events"
    __table_args__ = (
        UniqueConstraint("aggregate_type", "aggregate_id", "event_seq", name="uq_research_authority_aggregate_seq"),
        UniqueConstraint("aggregate_type", "aggregate_id", "idempotency_key", name="uq_research_authority_idempotency"),
        Index("ix_research_authority_events_aggregate", "aggregate_type", "aggregate_id", "event_seq"),
        Index("ix_research_authority_events_request", "request_id", "created_at"),
    )

    id = Column(String(96), primary_key=True)
    aggregate_type = Column(String(32), nullable=False)
    aggregate_id = Column(String(64), nullable=False)
    event_seq = Column(Integer, nullable=False)
    event_type = Column(String(64), nullable=False)
    actor_id = Column(String(128), nullable=False)
    actor_role = Column(String(64), nullable=False)
    request_id = Column(String(128), nullable=False)
    idempotency_key = Column(String(128), nullable=False)
    payload = Column(JSONB, nullable=False)
    evidence_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "event_seq": int(self.event_seq),
            "event_type": self.event_type,
            "actor_id": self.actor_id,
            "actor_role": self.actor_role,
            "request_id": self.request_id,
            "idempotency_key": self.idempotency_key,
            "payload": dict(self.payload or {}),
            "evidence_hash": self.evidence_hash,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


class ResearchGovernanceCaseRecord(Base):
    """Current projection for one append-only offline governance trail."""

    __tablename__ = "portal_research_governance_cases"
    __table_args__ = (
        UniqueConstraint("family_id", name="uq_research_governance_case_family"),
        UniqueConstraint(
            "creation_request_id",
            name="uq_research_governance_case_creation_request",
        ),
        Index("ix_research_governance_cases_state_updated", "current_state", "updated_at"),
        Index("ix_research_governance_cases_family", "family_id"),
    )

    id = Column(String(64), primary_key=True)
    current_state = Column(String(64), nullable=False)
    state_version = Column(Integer, nullable=False, default=0)
    observation_id = Column(String(64), ForeignKey("portal_research_items.id", ondelete="RESTRICT"), nullable=False)
    hypothesis_id = Column(String(64), ForeignKey("portal_research_items.id", ondelete="RESTRICT"), nullable=True)
    protocol_id = Column(String(64), ForeignKey("portal_research_protocols.id", ondelete="RESTRICT"), nullable=True)
    family_id = Column(String(64), ForeignKey("portal_research_families.id", ondelete="RESTRICT"), nullable=True)
    candidate_id = Column(String(64), ForeignKey("portal_research_candidates.id", ondelete="RESTRICT"), nullable=True)
    certificate_id = Column(String(64), ForeignKey("portal_research_certificates.id", ondelete="RESTRICT"), nullable=True)
    created_by = Column(String(128), nullable=False)
    creation_request_id = Column(String(128), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "current_state": self.current_state,
            "state_version": int(self.state_version),
            "observation_id": self.observation_id,
            "hypothesis_id": self.hypothesis_id,
            "protocol_id": self.protocol_id,
            "family_id": self.family_id,
            "candidate_id": self.candidate_id,
            "certificate_id": self.certificate_id,
            "created_by": self.created_by,
            "creation_request_id": self.creation_request_id,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class ResearchGovernanceProposalRecord(Base):
    """Immutable request for exactly one expected state transition."""

    __tablename__ = "portal_research_governance_proposals"
    __table_args__ = (
        UniqueConstraint("case_id", "request_id", name="uq_research_governance_proposal_request"),
        UniqueConstraint("proposal_hash", name="uq_research_governance_proposal_hash"),
        Index("ix_research_governance_proposals_case_version", "case_id", "expected_state_version"),
        Index("ix_research_governance_proposals_request", "request_id", "created_at"),
    )

    id = Column(String(64), primary_key=True)
    case_id = Column(String(64), ForeignKey("portal_research_governance_cases.id", ondelete="RESTRICT"), nullable=False)
    expected_state_version = Column(Integer, nullable=False)
    source_state = Column(String(64), nullable=False)
    target_state = Column(String(64), nullable=False)
    binding_updates = Column(JSONB, nullable=False)
    evidence_hashes = Column(JSONB, nullable=False)
    rationale = Column(String(2048), nullable=False)
    proposed_by = Column(String(128), nullable=False)
    proposed_role = Column(String(64), nullable=False)
    request_id = Column(String(128), nullable=False)
    proposal_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "case_id": self.case_id,
            "expected_state_version": int(self.expected_state_version),
            "source_state": self.source_state,
            "target_state": self.target_state,
            "binding_updates": dict(self.binding_updates or {}),
            "evidence_hashes": list(self.evidence_hashes or []),
            "rationale": self.rationale,
            "proposed_by": self.proposed_by,
            "proposed_role": self.proposed_role,
            "request_id": self.request_id,
            "proposal_hash": self.proposal_hash,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
        }


class ResearchGovernanceDecisionRecord(Base):
    """Immutable authorization or rejection of a transition proposal."""

    __tablename__ = "portal_research_governance_decisions"
    __table_args__ = (
        UniqueConstraint("proposal_id", name="uq_research_governance_decision_proposal"),
        UniqueConstraint("request_id", name="uq_research_governance_decision_request"),
        UniqueConstraint("decision_hash", name="uq_research_governance_decision_hash"),
        Index("ix_research_governance_decisions_case_created", "case_id", "created_at"),
        Index("ix_research_governance_decisions_request", "request_id", "created_at"),
    )

    id = Column(String(64), primary_key=True)
    proposal_id = Column(String(64), ForeignKey("portal_research_governance_proposals.id", ondelete="RESTRICT"), nullable=False)
    case_id = Column(String(64), ForeignKey("portal_research_governance_cases.id", ondelete="RESTRICT"), nullable=False)
    disposition = Column(String(32), nullable=False)
    resulting_state = Column(String(64), nullable=False)
    resulting_state_version = Column(Integer, nullable=False)
    policy_evidence = Column(JSONB, nullable=False)
    authorized_by = Column(String(128), nullable=False)
    authorized_role = Column(String(64), nullable=False)
    request_id = Column(String(128), nullable=False)
    decision_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "proposal_id": self.proposal_id,
            "case_id": self.case_id,
            "disposition": self.disposition,
            "resulting_state": self.resulting_state,
            "resulting_state_version": int(self.resulting_state_version),
            "policy_evidence": dict(self.policy_evidence or {}),
            "authorized_by": self.authorized_by,
            "authorized_role": self.authorized_role,
            "request_id": self.request_id,
            "decision_hash": self.decision_hash,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
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
    """Database row describing a persisted bot definition."""

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
        payload = {
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
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }
        return payload

class BotTradeRecord(Base):
    """Database row representing a laddered trade generated by a bot."""

    __tablename__ = "portal_bot_trades"
    __table_args__ = (
        Index("ix_portal_bot_trades_run_updated_id", "run_id", "updated_at", "id"),
        Index("ix_portal_bot_trades_bot_run_status", "bot_id", "run_id", "status"),
    )

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
    """Database row representing run identity and lifecycle-derived summary state."""

    __tablename__ = "portal_bot_runs"
    __table_args__ = (
        Index("ix_portal_bot_runs_report_list", "run_type", "status", "ended_at", "started_at", "run_id"),
        Index("ix_portal_bot_runs_bot_report_list", "bot_id", "run_type", "status", "ended_at", "started_at", "run_id"),
    )

    run_id = Column(String(64), primary_key=True)
    bot_id = Column(String(64), ForeignKey("portal_bots.id", ondelete="SET NULL"), nullable=True)
    bot_name = Column(String(255), nullable=True)
    strategy_id = Column(String(64), nullable=True)
    strategy_name = Column(String(255), nullable=True)
    run_type = Column(String(32), nullable=False, default="backtest")
    status = Column(String(32), nullable=False, default="idle")
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
    config_hash = Column(String(64), nullable=True)
    material_config_hash = Column(String(64), nullable=True)
    strategy_hash = Column(String(64), nullable=True)
    data_snapshot_hash = Column(String(64), nullable=True)
    runtime_contract_version = Column(String(64), nullable=True)
    runtime_source_revision = Column(String(128), nullable=True)
    runtime_image = Column(String(255), nullable=True)
    storage_schema_version = Column(String(64), nullable=True)
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
            "config_hash": self.config_hash,
            "material_config_hash": self.material_config_hash,
            "strategy_hash": self.strategy_hash,
            "data_snapshot_hash": self.data_snapshot_hash,
            "runtime_contract_version": self.runtime_contract_version,
            "runtime_source_revision": self.runtime_source_revision,
            "runtime_image": self.runtime_image,
            "storage_schema_version": self.storage_schema_version,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }


class ReportMaterializationRecord(Base):
    """Persisted RunReportDTO artifact and build status for one run."""

    __tablename__ = "portal_report_materializations"
    __table_args__ = (
        Index("ix_portal_report_materializations_input_fingerprint", "input_fingerprint"),
    )

    run_id = Column(String(64), ForeignKey("portal_bot_runs.run_id", ondelete="CASCADE"), primary_key=True)
    contract_version = Column(String(64), nullable=False, default="run_report.v2")
    report_schema_version = Column(String(64), nullable=True)
    dataset_schema_version = Column(String(64), nullable=True)
    builder_source_revision = Column(String(128), nullable=True)
    storage_schema_version = Column(String(64), nullable=True)
    status = Column(String(32), nullable=False, default="not_started")
    artifact_id = Column(String(160), nullable=True)
    artifact = Column(JSONB, nullable=True)
    cache_key = Column(String(255), nullable=True)
    input_fingerprint = Column(String(64), nullable=True)
    input_fingerprint_payload = Column(JSONB, nullable=True)
    source_event_count = Column(Integer, nullable=False, default=0)
    source_event_high_water_run_seq = Column(Integer, nullable=False, default=0)
    source_trade_count = Column(Integer, nullable=False, default=0)
    source_run_updated_at = Column(DateTime, nullable=True)
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
            "report_schema_version": self.report_schema_version,
            "dataset_schema_version": self.dataset_schema_version,
            "builder_source_revision": self.builder_source_revision,
            "storage_schema_version": self.storage_schema_version,
            "artifact_id": self.artifact_id,
            "artifact_path": None,
            "built_at": (self.built_at.isoformat() + "Z") if self.built_at else None,
            "started_at": (self.started_at.isoformat() + "Z") if self.started_at else None,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "stale_reason": self.stale_reason or ("missing_artifact" if effective_status == "stale" else None),
            "cache_key": self.cache_key,
            "input_fingerprint": self.input_fingerprint,
            "input_fingerprint_payload": dict(self.input_fingerprint_payload or {}),
            "source_event_count": int(self.source_event_count or 0),
            "source_event_high_water_run_seq": int(self.source_event_high_water_run_seq or 0),
            "source_trade_count": int(self.source_trade_count or 0),
            "source_run_updated_at": (self.source_run_updated_at.isoformat() + "Z") if self.source_run_updated_at else None,
            "can_view": can_view,
            "can_build": can_build,
            "can_retry": can_retry,
        }


class BotRunLeaseRecord(Base):
    """Runner-agnostic ownership lease for one bot run."""

    __tablename__ = "portal_bot_run_leases"
    __table_args__ = (
        Index("ix_portal_bot_run_leases_bot_status_expires", "bot_id", "status", "expires_at"),
        Index("ix_portal_bot_run_leases_runner_status", "runner_id", "status"),
        Index("ix_portal_bot_run_leases_runner_status_expires", "runner_id", "status", "expires_at"),
        Index("ix_portal_bot_run_leases_status_expires", "status", "expires_at"),
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

    __tablename__ = "portal_bot_run_step_rollups"
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
            name="uq_portal_bot_run_step_rollups_bucket_identity",
        ),
        Index("ix_portal_bot_run_step_rollups_run_bucket", "run_id", "bucket_start"),
        Index(
            "ix_portal_bot_run_step_rollups_run_step_metric_bucket",
            "run_id",
            "step_name",
            "metric_name",
            "bucket_start",
        ),
        Index("ix_portal_bot_run_step_rollups_bot_bucket", "bot_id", "bucket_start"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    bucket_start = Column(DateTime, nullable=False)
    bucket_seconds = Column(Integer, nullable=False, default=60)
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
        Index(
            "ix_portal_bot_run_events_bot_run_series_event_run_seq_id",
            "bot_id",
            "run_id",
            "series_key",
            "event_name",
            "run_seq",
            "id",
        ),
        Index(
            "ix_portal_bot_run_events_bot_run_series_trade_run_seq_id",
            "bot_id",
            "run_id",
            "series_key",
            "trade_id",
            "run_seq",
            "id",
        ),
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

    __tablename__ = "botlens_backend_events"
    __table_args__ = (
        Index("ix_botlens_backend_events_observed_at", "observed_at"),
        Index("ix_botlens_backend_events_event_name_observed_at", "event_name", "observed_at"),
        Index("ix_botlens_backend_events_run_id_observed_at", "run_id", "observed_at"),
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

    __tablename__ = "botlens_backend_metric_rollups"
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
            name="uq_botlens_backend_metric_rollups_bucket_identity",
        ),
        Index("ix_botlens_backend_metric_rollups_bucket_start", "bucket_start"),
        Index(
            "ix_botlens_backend_metric_rollups_metric_bucket",
            "metric_name",
            "bucket_start",
        ),
        Index("ix_botlens_backend_metric_rollups_run_bucket", "run_id", "bucket_start"),
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


class DatabaseCapacitySampleRecord(Base):
    """Bounded database-level capacity and workload snapshot."""

    __tablename__ = "database_capacity_samples"
    __table_args__ = (
        Index("ix_database_capacity_samples_sampled_at", "sampled_at"),
        {"schema": "observability_metrics"},
    )

    sampled_at = Column(DateTime, primary_key=True)
    database_size_bytes = Column(BigInteger, nullable=False)
    relation_count = Column(Integer, nullable=False)
    max_connections = Column(Integer, nullable=False)
    connections_total = Column(Integer, nullable=False)
    connections_active = Column(Integer, nullable=False)
    connections_idle = Column(Integer, nullable=False)
    xact_commit = Column(BigInteger, nullable=False)
    xact_rollback = Column(BigInteger, nullable=False)
    blocks_read = Column(BigInteger, nullable=False)
    blocks_hit = Column(BigInteger, nullable=False)
    tuples_returned = Column(BigInteger, nullable=False)
    tuples_fetched = Column(BigInteger, nullable=False)
    tuples_inserted = Column(BigInteger, nullable=False)
    tuples_updated = Column(BigInteger, nullable=False)
    tuples_deleted = Column(BigInteger, nullable=False)
    temp_files = Column(BigInteger, nullable=False)
    temp_bytes = Column(BigInteger, nullable=False)
    deadlocks = Column(BigInteger, nullable=False)
    block_read_time_ms = Column(Float, nullable=False)
    block_write_time_ms = Column(Float, nullable=False)
    wal_bytes = Column(BigInteger, nullable=False)
    sample_query_ms = Column(Float, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class DatabaseRelationCapacitySampleRecord(Base):
    """Bounded schema/table-level size and activity snapshot."""

    __tablename__ = "database_relation_capacity_samples"
    __table_args__ = (
        Index(
            "ix_database_relation_capacity_samples_relation_time",
            "schema_name",
            "relation_name",
            "sampled_at",
        ),
        Index("ix_database_relation_capacity_samples_sampled_at", "sampled_at"),
        {"schema": "observability_metrics"},
    )

    sampled_at = Column(DateTime, primary_key=True)
    schema_name = Column(String(128), primary_key=True)
    relation_name = Column(String(128), primary_key=True)
    relation_kind = Column(String(32), nullable=False)
    table_bytes = Column(BigInteger, nullable=False)
    index_bytes = Column(BigInteger, nullable=False)
    toast_bytes = Column(BigInteger, nullable=False)
    total_bytes = Column(BigInteger, nullable=False)
    estimated_live_rows = Column(BigInteger, nullable=False)
    estimated_dead_rows = Column(BigInteger, nullable=False)
    inserts_total = Column(BigInteger, nullable=False)
    updates_total = Column(BigInteger, nullable=False)
    deletes_total = Column(BigInteger, nullable=False)
    sequential_scans_total = Column(BigInteger, nullable=False)
    index_scans_total = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AsyncJobRecord(Base):
    """Database-backed async job used by API and worker processes."""

    __tablename__ = "portal_async_jobs"
    __table_args__ = (
        CheckConstraint(
            "claim_generation >= 0",
            name="ck_portal_async_jobs_claim_generation_nonnegative",
        ),
        CheckConstraint(
            "("
            "status = 'running' AND lock_owner IS NOT NULL "
            "AND locked_at IS NOT NULL AND heartbeat_at IS NOT NULL "
            "AND claim_token_hash IS NOT NULL"
            ") OR ("
            "status <> 'running' AND lock_owner IS NULL "
            "AND locked_at IS NULL AND heartbeat_at IS NULL "
            "AND claim_token_hash IS NULL"
            ")",
            name="ck_portal_async_jobs_claim_state",
        ),
        Index(
            "ix_portal_async_jobs_claimable",
            "status",
            "job_type",
            "available_at",
            "created_at",
        ),
        Index(
            "ix_portal_async_jobs_running_heartbeat",
            "status",
            "job_type",
            "heartbeat_at",
        ),
        Index(
            "uq_portal_async_jobs_inflight_request",
            "job_type",
            "partition_key",
            "request_fingerprint",
            unique=True,
            postgresql_where=text(
                "status IN ('queued', 'running', 'retry') "
                "AND request_fingerprint IS NOT NULL"
            ),
        ),
    )

    id = Column(String(64), primary_key=True)
    job_type = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="queued")
    payload = Column(JSONB, nullable=False, default=dict)
    result = Column(JSONB, nullable=True)
    error = Column(String(2048), nullable=True)
    partition_key = Column(String(255), nullable=True)
    partition_hash = Column(Integer, nullable=False, default=0)
    request_fingerprint = Column(String(64), nullable=True)
    attempts = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=3)
    lock_owner = Column(String(128), nullable=True)
    locked_at = Column(DateTime, nullable=True)
    heartbeat_at = Column(DateTime, nullable=True)
    claim_token_hash = Column(String(64), nullable=True)
    claim_generation = Column(Integer, nullable=False, default=0)
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
            "request_fingerprint": self.request_fingerprint,
            "attempts": int(self.attempts or 0),
            "max_attempts": int(self.max_attempts or 0),
            "lock_owner": self.lock_owner,
            "locked_at": (self.locked_at.isoformat() + "Z") if self.locked_at else None,
            "heartbeat_at": (self.heartbeat_at.isoformat() + "Z") if self.heartbeat_at else None,
            "claim_generation": int(self.claim_generation or 0),
            "available_at": (self.available_at.isoformat() + "Z") if self.available_at else None,
            "started_at": (self.started_at.isoformat() + "Z") if self.started_at else None,
            "finished_at": (self.finished_at.isoformat() + "Z") if self.finished_at else None,
            "created_at": (self.created_at or datetime.utcnow()).isoformat() + "Z",
            "updated_at": (self.updated_at or datetime.utcnow()).isoformat() + "Z",
        }
