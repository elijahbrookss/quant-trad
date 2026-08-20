"""Canonical PostgreSQL models for market-data source facts and datasets."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from .models import Base


MARKET_DATA_SCHEMA = "market"


class MarketDataSourceRecord(Base):
    """Stable identity and lineage for one acquisition source."""

    __tablename__ = "sources"
    __table_args__ = (
        UniqueConstraint("identity_key", name="uq_market_sources_identity_key"),
        CheckConstraint("provider <> ''", name="ck_market_sources_provider"),
        CheckConstraint("source_kind <> ''", name="ck_market_sources_kind"),
        CheckConstraint("adapter_version <> ''", name="ck_market_sources_adapter_version"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(BigInteger, Identity(), primary_key=True)
    identity_key = Column(String(64), nullable=False)
    provider = Column(String(64), nullable=False)
    venue = Column(String(128), nullable=False, default="", server_default="")
    source_kind = Column(String(32), nullable=False)
    adapter_version = Column(String(128), nullable=False)
    lineage = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketDataSeriesRecord(Base):
    """Typed fact series keyed to the canonical portal instrument."""

    __tablename__ = "series"
    __table_args__ = (
        UniqueConstraint("identity_key", name="uq_market_series_identity_key"),
        CheckConstraint("fact_type <> ''", name="ck_market_series_fact_type"),
        CheckConstraint(
            "timeframe_seconds IS NULL OR timeframe_seconds > 0",
            name="ck_market_series_timeframe_positive",
        ),
        CheckConstraint(
            "fact_type <> 'candle.ohlcv' OR timeframe_seconds IS NOT NULL",
            name="ck_market_series_candle_timeframe",
        ),
        CheckConstraint(
            "jsonb_typeof(dimensions) = 'object'",
            name="ck_market_series_dimensions_object",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(BigInteger, Identity(), primary_key=True)
    identity_key = Column(String(64), nullable=False)
    instrument_id = Column(
        String(64),
        ForeignKey("portal_instruments.id", ondelete="RESTRICT"),
        nullable=False,
    )
    fact_type = Column(String(64), nullable=False)
    timeframe_seconds = Column(Integer, nullable=True)
    contract_version = Column(String(64), nullable=False)
    dimensions = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))

    __table_args__ = __table_args__[:-1] + (
        Index("ix_market_series_instrument_fact", "instrument_id", "fact_type"),
        {"schema": MARKET_DATA_SCHEMA},
    )


class MarketDataIngestionRunRecord(Base):
    """Auditable acquisition, stream, or migration operation."""

    __tablename__ = "ingestion_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'completed', 'failed')",
            name="ck_market_ingestion_runs_status",
        ),
        CheckConstraint("requested_count >= 0", name="ck_market_ingestion_requested_count"),
        CheckConstraint("inserted_count >= 0", name="ck_market_ingestion_inserted_count"),
        CheckConstraint("corrected_count >= 0", name="ck_market_ingestion_corrected_count"),
        CheckConstraint("noop_count >= 0", name="ck_market_ingestion_noop_count"),
        Index("ix_market_ingestion_runs_source_started", "source_id", "started_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(64), primary_key=True)
    source_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.sources.id", ondelete="RESTRICT"),
        nullable=False,
    )
    status = Column(String(16), nullable=False)
    request = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    source_revision = Column(String(128), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    finished_at = Column(DateTime(timezone=True), nullable=True)
    requested_start = Column(DateTime(timezone=True), nullable=True)
    requested_end = Column(DateTime(timezone=True), nullable=True)
    requested_count = Column(Integer, nullable=False, default=0, server_default="0")
    inserted_count = Column(Integer, nullable=False, default=0, server_default="0")
    corrected_count = Column(Integer, nullable=False, default=0, server_default="0")
    noop_count = Column(Integer, nullable=False, default=0, server_default="0")
    error = Column(Text, nullable=True)


class MarketCollectionDefinitionRecord(Base):
    """Mutable scheduler state for one provider/fact/series polling definition."""

    __tablename__ = "collection_definitions"
    __table_args__ = (
        UniqueConstraint(
            "source_id", "series_id", name="uq_market_collection_source_series"
        ),
        CheckConstraint(
            "poll_interval_seconds > 0",
            name="ck_market_collection_poll_interval_positive",
        ),
        CheckConstraint(
            "max_attempts > 0", name="ck_market_collection_max_attempts_positive"
        ),
        CheckConstraint(
            "lease_generation >= 0",
            name="ck_market_collection_lease_generation_nonnegative",
        ),
        CheckConstraint(
            "consecutive_failures >= 0",
            name="ck_market_collection_failures_nonnegative",
        ),
        CheckConstraint(
            "desired_state IN ('running', 'stopped', 'paused')",
            name="ck_market_collection_desired_state",
        ),
        CheckConstraint(
            "control_generation >= 0",
            name="ck_market_collection_control_generation_nonnegative",
        ),
        CheckConstraint(
            "((lease_owner IS NOT NULL AND lease_token_hash IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (lease_owner IS NULL AND lease_token_hash IS NULL AND lease_expires_at IS NULL))",
            name="ck_market_collection_lease_state",
        ),
        Index(
            "ix_market_collection_claimable",
            "desired_state",
            "enabled",
            "next_scheduled_at",
            "available_at",
        ),
        Index(
            "ix_market_collection_lease_expiry", "lease_expires_at"
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(64), primary_key=True)
    source_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.sources.id", ondelete="RESTRICT"),
        nullable=False,
    )
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    enabled = Column(Boolean, nullable=False, default=False, server_default="false")
    poll_interval_seconds = Column(Integer, nullable=False)
    max_attempts = Column(Integer, nullable=False, default=3, server_default="3")
    next_scheduled_at = Column(DateTime(timezone=True), nullable=False)
    available_at = Column(DateTime(timezone=True), nullable=False)
    consecutive_failures = Column(Integer, nullable=False, default=0, server_default="0")
    desired_state = Column(
        String(16), nullable=False, default="stopped", server_default="stopped"
    )
    control_generation = Column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    control_requested_at = Column(DateTime(timezone=True), nullable=True)
    control_requested_by = Column(String(128), nullable=True)
    control_request_id = Column(String(128), nullable=True)
    lease_owner = Column(String(128), nullable=True)
    lease_token_hash = Column(String(64), nullable=True)
    lease_generation = Column(BigInteger, nullable=False, default=0, server_default="0")
    lease_expires_at = Column(DateTime(timezone=True), nullable=True)
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    last_success_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    config = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketCollectionAttemptRecord(Base):
    """Auditable execution attempt for one scheduled collection occurrence."""

    __tablename__ = "collection_attempts"
    __table_args__ = (
        UniqueConstraint(
            "definition_id",
            "scheduled_for",
            "attempt_number",
            name="uq_market_collection_attempt",
        ),
        CheckConstraint(
            "attempt_number > 0", name="ck_market_collection_attempt_positive"
        ),
        CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'missed')",
            name="ck_market_collection_attempt_status",
        ),
        Index(
            "ix_market_collection_attempt_definition_schedule",
            "definition_id",
            "scheduled_for",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(64), primary_key=True)
    definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.collection_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    scheduled_for = Column(DateTime(timezone=True), nullable=False)
    attempt_number = Column(Integer, nullable=False)
    lease_generation = Column(BigInteger, nullable=False)
    owner_id = Column(String(128), nullable=False)
    status = Column(String(16), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    ingestion_run_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.ingestion_runs.id", ondelete="RESTRICT"),
        nullable=True,
    )
    error = Column(Text, nullable=True)
    evidence = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )


class MarketCollectorWorkerStateRecord(Base):
    """Mutable liveness projection for one scheduled market-fact worker."""

    __tablename__ = "collector_worker_state"
    __table_args__ = (
        CheckConstraint(
            "state IN ('starting', 'idle', 'collecting', 'degraded', 'stopping', 'stopped')",
            name="ck_market_collector_worker_state",
        ),
        Index("ix_market_collector_worker_expiry", "expires_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    worker_id = Column(String(128), primary_key=True)
    worker_role = Column(String(64), nullable=False)
    worker_version = Column(String(64), nullable=False)
    state = Column(String(16), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    heartbeat_at = Column(DateTime(timezone=True), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    last_loop_at = Column(DateTime(timezone=True), nullable=True)
    active_definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.collection_definitions.id", ondelete="SET NULL"),
        nullable=True,
    )
    active_attempt_id = Column(String(64), nullable=True)
    last_error = Column(Text, nullable=True)
    capabilities = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    context = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketProviderRateBudgetRecord(Base):
    """Database-coordinated minimum-spacing budget for provider poll requests."""

    __tablename__ = "provider_rate_budgets"
    __table_args__ = ({"schema": MARKET_DATA_SCHEMA},)

    provider = Column(String(64), primary_key=True)
    next_request_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketGapEvidenceRecord(Base):
    """Range-based quality evidence kept separate from candle identity."""

    __tablename__ = "gap_evidence"
    __table_args__ = (
        CheckConstraint("end_time > start_time", name="ck_market_gap_time_order"),
        CheckConstraint("expected_count >= 0", name="ck_market_gap_expected_count"),
        CheckConstraint("observed_count >= 0", name="ck_market_gap_observed_count"),
        UniqueConstraint(
            "series_id",
            "start_time",
            "end_time",
            "evidence_hash",
            name="uq_market_gap_evidence",
        ),
        Index("ix_market_gap_evidence_series_window", "series_id", "start_time", "end_time"),
        Index("ix_market_gap_evidence_source_window", "source_id", "start_time", "end_time"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(BigInteger, Identity(), primary_key=True)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.sources.id", ondelete="RESTRICT"),
        nullable=True,
    )
    ingestion_run_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.ingestion_runs.id", ondelete="RESTRICT"),
        nullable=True,
    )
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)
    classification = Column(String(64), nullable=False)
    expected_count = Column(Integer, nullable=False, default=0, server_default="0")
    observed_count = Column(Integer, nullable=False, default=0, server_default="0")
    detected_as_of_commit_seq = Column(BigInteger, nullable=False)
    evidence_hash = Column(String(64), nullable=False)
    evidence = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketFactAcquisitionCoverageRecord(Base):
    """Immutable evidence that one bounded provider-neutral source range was scanned."""

    __tablename__ = "fact_acquisition_coverage"
    __table_args__ = (
        CheckConstraint(
            "range_end > range_start",
            name="ck_market_fact_acquisition_coverage_range",
        ),
        CheckConstraint(
            "status IN ('complete', 'partial', 'failed')",
            name="ck_market_fact_acquisition_coverage_status",
        ),
        CheckConstraint(
            "confirmation_depth >= 0",
            name="ck_market_fact_acquisition_confirmation_depth",
        ),
        CheckConstraint(
            "jsonb_typeof(evidence) = 'object'",
            name="ck_market_fact_acquisition_evidence_object",
        ),
        Index(
            "ix_market_fact_acquisition_coverage_lookup",
            "series_id",
            "binding_id",
            "manifest_hash",
            "status",
            "range_start",
            "range_end",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    identity_key = Column(String(64), primary_key=True)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.sources.id", ondelete="RESTRICT"),
        nullable=False,
    )
    binding_id = Column(String(128), nullable=False)
    manifest_hash = Column(String(64), nullable=False)
    interface_version = Column(String(64), nullable=False)
    confirmation_depth = Column(Integer, nullable=False)
    range_start = Column(DateTime(timezone=True), nullable=False)
    range_end = Column(DateTime(timezone=True), nullable=False)
    source_position_start = Column(String(128), nullable=False)
    source_position_end = Column(String(128), nullable=False)
    source_position_head = Column(String(128), nullable=True)
    status = Column(String(16), nullable=False)
    ingestion_run_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.ingestion_runs.id", ondelete="RESTRICT"),
        nullable=True,
    )
    evidence = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketDatasetRecord(Base):
    """Immutable identity for a frozen collection of market facts."""

    __tablename__ = "datasets"
    __table_args__ = (
        UniqueConstraint("dataset_hash", name="uq_market_datasets_hash"),
        CheckConstraint("max_commit_seq >= 0", name="ck_market_dataset_commit_seq"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(64), primary_key=True)
    dataset_hash = Column(String(64), nullable=False)
    name = Column(String(255), nullable=True)
    purpose = Column(String(64), nullable=False, default="research", server_default="research")
    max_commit_seq = Column(BigInteger, nullable=False)
    created_by = Column(String(128), nullable=True)
    metadata_payload = Column(
        "metadata", JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketDatasetSeriesRecord(Base):
    """One frozen series range and its material and quality hashes."""

    __tablename__ = "dataset_series"
    __table_args__ = (
        PrimaryKeyConstraint(
            "dataset_id", "series_id", "range_start", "range_end", name="pk_market_dataset_series"
        ),
        CheckConstraint("range_end > range_start", name="ck_market_dataset_series_range"),
        CheckConstraint("max_commit_seq >= 0", name="ck_market_dataset_series_commit_seq"),
        CheckConstraint("row_count >= 0", name="ck_market_dataset_series_row_count"),
        CheckConstraint(
            "jsonb_typeof(payload_schemas) = 'array'",
            name="ck_market_dataset_series_payload_schemas_array",
        ),
        Index("ix_market_dataset_series_series", "series_id", "range_start", "range_end"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    dataset_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.datasets.id", ondelete="RESTRICT"),
        nullable=False,
    )
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    range_start = Column(DateTime(timezone=True), nullable=False)
    range_end = Column(DateTime(timezone=True), nullable=False)
    max_commit_seq = Column(BigInteger, nullable=False)
    row_count = Column(Integer, nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    source_summary = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    quality_hash = Column(String(64), nullable=False)
    quality_summary = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    quality_evidence = Column(JSONB, nullable=True)
    payload_schemas = Column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb")
    )


class MarketProductDefinitionVersionRecord(Base):
    """Append-only provider product metadata used by unit translations."""

    __tablename__ = "product_definition_versions"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "provider_product_id",
            "effective_at",
            "revision",
            name="uq_market_product_definition_revision",
        ),
        CheckConstraint("revision > 0", name="ck_market_product_definition_revision"),
        CheckConstraint("contract_size IS NULL OR contract_size > 0", name="ck_market_product_contract_size"),
        Index(
            "ix_market_product_definition_product_effective",
            "source_id",
            "provider_product_id",
            "effective_at",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    source_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.sources.id", ondelete="RESTRICT"),
        nullable=False,
    )
    instrument_id = Column(
        String(64), ForeignKey("portal_instruments.id", ondelete="RESTRICT"), nullable=False
    )
    provider_product_id = Column(String(128), nullable=False)
    product_type = Column(String(32), nullable=False)
    venue = Column(String(128), nullable=False)
    status = Column(String(32), nullable=False)
    base_currency = Column(String(32), nullable=False)
    quote_currency = Column(String(32), nullable=False)
    provider_size_unit = Column(String(32), nullable=False)
    price_increment = Column(Numeric(38, 18), nullable=True)
    base_increment = Column(Numeric(38, 18), nullable=True)
    contract_size = Column(Numeric(38, 18), nullable=True)
    expiry_at = Column(DateTime(timezone=True), nullable=True)
    effective_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    revision = Column(Integer, nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    provenance = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketInstrumentRoleMappingVersionRecord(Base):
    """Append-only explicit futures-to-spot or benchmark mapping."""

    __tablename__ = "instrument_role_mapping_versions"
    __table_args__ = (
        UniqueConstraint(
            "primary_instrument_id",
            "role",
            "effective_from",
            "revision",
            name="uq_market_instrument_role_mapping_revision",
        ),
        CheckConstraint("revision > 0", name="ck_market_instrument_role_mapping_revision"),
        CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_market_instrument_role_mapping_range",
        ),
        Index(
            "ix_market_instrument_role_mapping_effective",
            "primary_instrument_id",
            "role",
            "effective_from",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    primary_instrument_id = Column(
        String(64), ForeignKey("portal_instruments.id", ondelete="RESTRICT"), nullable=False
    )
    related_instrument_id = Column(
        String(64), ForeignKey("portal_instruments.id", ondelete="RESTRICT"), nullable=False
    )
    role = Column(String(32), nullable=False)
    mapping_reason = Column(String(255), nullable=False)
    mapping_source = Column(String(64), nullable=False)
    effective_from = Column(DateTime(timezone=True), nullable=False)
    effective_to = Column(DateTime(timezone=True), nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    revision = Column(Integer, nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)


class MarketStreamDefinitionRecord(Base):
    """Mutable bounded acquisition configuration with no credentials."""

    __tablename__ = "stream_definitions"
    __table_args__ = (
        UniqueConstraint("identity_key", name="uq_market_stream_definition_identity"),
        CheckConstraint("generation >= 1", name="ck_market_stream_definition_generation"),
        CheckConstraint(
            "desired_state IN ('running', 'stopped', 'paused')",
            name="ck_market_stream_desired_state",
        ),
        CheckConstraint(
            "control_generation >= 0",
            name="ck_market_stream_control_generation_nonnegative",
        ),
        CheckConstraint("max_spool_bytes > 0", name="ck_market_stream_definition_spool"),
        CheckConstraint("max_segment_bytes > 0", name="ck_market_stream_definition_segment"),
        CheckConstraint(
            "max_segment_bytes <= max_spool_bytes",
            name="ck_market_stream_definition_segment_within_spool",
        ),
        Index(
            "ix_market_stream_definition_enabled",
            "desired_state",
            "enabled",
            "provider",
            "venue",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(64), primary_key=True)
    identity_key = Column(String(64), nullable=False)
    source_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.sources.id", ondelete="RESTRICT"),
        nullable=False,
    )
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider = Column(String(64), nullable=False)
    venue = Column(String(128), nullable=False)
    provider_product_id = Column(String(128), nullable=False)
    channels = Column(JSONB, nullable=False)
    auth_mode = Column(String(32), nullable=False)
    contract_version = Column(String(64), nullable=False)
    enabled = Column(Boolean, nullable=False, default=False, server_default="false")
    max_spool_bytes = Column(BigInteger, nullable=False)
    max_segment_bytes = Column(BigInteger, nullable=False)
    generation = Column(BigInteger, nullable=False, default=1, server_default="1")
    desired_state = Column(
        String(16), nullable=False, default="stopped", server_default="stopped"
    )
    control_generation = Column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    control_requested_at = Column(DateTime(timezone=True), nullable=True)
    control_requested_by = Column(String(128), nullable=True)
    control_request_id = Column(String(128), nullable=True)
    config = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketCollectorSafetyEventRecord(Base):
    """Append-only warning, halt, and acknowledgement evidence."""

    __tablename__ = "collector_safety_events"
    __table_args__ = (
        UniqueConstraint("request_id", name="uq_market_collector_safety_request"),
        CheckConstraint(
            "scope_type IN ('global', 'fleet', 'stream')",
            name="ck_market_collector_safety_scope",
        ),
        CheckConstraint(
            "event_type IN ('warning', 'halted', 'acknowledged')",
            name="ck_market_collector_safety_event_type",
        ),
        CheckConstraint(
            "severity IN ('warning', 'critical', 'operator')",
            name="ck_market_collector_safety_severity",
        ),
        Index(
            "ix_market_collector_safety_scope_time",
            "scope_type",
            "scope_id",
            "occurred_at",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    request_id = Column(String(128), nullable=False)
    scope_type = Column(String(16), nullable=False)
    scope_id = Column(String(128), nullable=False)
    event_type = Column(String(32), nullable=False)
    severity = Column(String(16), nullable=False)
    occurred_at = Column(DateTime(timezone=True), nullable=False)
    actor_id = Column(String(128), nullable=False)
    reason = Column(Text, nullable=False)
    policy_hash = Column(String(64), nullable=False)
    evidence_hash = Column(String(64), nullable=False)
    evidence = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketCollectorOperationRecord(Base):
    """Immutable result of one canonical collector operator request."""

    __tablename__ = "collector_operation_events"
    __table_args__ = (
        UniqueConstraint("request_id", name="uq_market_collector_operation_request"),
        CheckConstraint(
            "collector_kind IN ('scheduled_fact', 'continuous_stream')",
            name="ck_market_collector_operation_kind",
        ),
        CheckConstraint(
            "action IN ('start', 'stop', 'restart', 'pause', 'resume', 'recover')",
            name="ck_market_collector_operation_action",
        ),
        CheckConstraint(
            "status IN ('succeeded', 'failed')",
            name="ck_market_collector_operation_status",
        ),
        CheckConstraint(
            "jsonb_typeof(context) = 'object'",
            name="ck_market_collector_operation_context_object",
        ),
        CheckConstraint(
            "jsonb_typeof(prior_state) = 'object'",
            name="ck_market_collector_operation_prior_object",
        ),
        CheckConstraint(
            "jsonb_typeof(resulting_state) = 'object'",
            name="ck_market_collector_operation_result_object",
        ),
        CheckConstraint(
            "jsonb_typeof(evidence) = 'object'",
            name="ck_market_collector_operation_evidence_object",
        ),
        Index(
            "ix_market_collector_operation_collector_time",
            "collector_id",
            "requested_at",
        ),
        Index("ix_market_collector_operation_recorded", "recorded_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(64), primary_key=True)
    request_id = Column(String(128), nullable=False)
    collector_id = Column(String(64), nullable=False)
    collector_kind = Column(String(32), nullable=False)
    action = Column(String(32), nullable=False)
    status = Column(String(16), nullable=False)
    requested_at = Column(DateTime(timezone=True), nullable=False)
    recorded_at = Column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    actor_id = Column(String(128), nullable=False)
    context = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    prior_state = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    resulting_state = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    evidence = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    error = Column(Text, nullable=True)


class MarketCollectorSafetyStateRecord(Base):
    """Restart-persistent current safety latch derived from immutable events."""

    __tablename__ = "collector_safety_state"
    __table_args__ = (
        PrimaryKeyConstraint(
            "scope_type",
            "scope_id",
            name="pk_market_collector_safety_state",
        ),
        CheckConstraint(
            "scope_type IN ('global', 'fleet', 'stream')",
            name="ck_market_collector_safety_state_scope",
        ),
        Index("ix_market_collector_safety_state_active", "active", "scope_type"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    scope_type = Column(String(16), nullable=False)
    scope_id = Column(String(128), nullable=False)
    active = Column(Boolean, nullable=False, default=False, server_default="false")
    halt_event_id = Column(String(128), nullable=True)
    acknowledged_event_id = Column(String(128), nullable=True)
    reason = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class MarketStreamLeaseStateRecord(Base):
    """Disposable current owner projection; immutable events remain authoritative."""

    __tablename__ = "stream_lease_state"
    __table_args__ = (
        CheckConstraint("lease_generation >= 0", name="ck_market_stream_lease_generation"),
        Index("ix_market_stream_lease_expiry", "expires_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_definitions.id", ondelete="CASCADE"),
        primary_key=True,
    )
    owner_id = Column(String(128), nullable=False)
    token_hash = Column(String(64), nullable=False)
    lease_generation = Column(BigInteger, nullable=False)
    claimed_at = Column(DateTime(timezone=True), nullable=False)
    heartbeat_at = Column(DateTime(timezone=True), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)


class MarketStreamSessionEventRecord(Base):
    """Immutable lifecycle evidence for one logical session/connection epoch."""

    __tablename__ = "stream_session_events"
    __table_args__ = (
        UniqueConstraint("session_id", "event_ordinal", name="uq_market_stream_session_event"),
        CheckConstraint("event_ordinal >= 0", name="ck_market_stream_session_event_ordinal"),
        CheckConstraint("connection_epoch >= 0", name="ck_market_stream_session_epoch"),
        Index("ix_market_stream_session_definition_time", "definition_id", "occurred_at"),
        Index("ix_market_stream_session_id_epoch", "session_id", "connection_epoch"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    session_id = Column(String(64), nullable=False)
    event_ordinal = Column(Integer, nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    owner_id = Column(String(128), nullable=False)
    lease_generation = Column(BigInteger, nullable=False)
    event_type = Column(String(64), nullable=False)
    occurred_at = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    reason = Column(Text, nullable=True)
    evidence_hash = Column(String(64), nullable=False)
    evidence = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketRawArchiveManifestRecord(Base):
    """One acknowledged immutable raw object."""

    __tablename__ = "raw_archive_manifests"
    __table_args__ = (
        UniqueConstraint("object_uri", name="uq_market_raw_archive_object_uri"),
        UniqueConstraint("object_sha256", "content_fingerprint", name="uq_market_raw_archive_content"),
        CheckConstraint("byte_count > 0", name="ck_market_raw_archive_bytes"),
        CheckConstraint("record_count > 0", name="ck_market_raw_archive_records"),
        CheckConstraint("last_receive_ordinal >= first_receive_ordinal", name="ck_market_raw_archive_ordinals"),
        Index("ix_market_raw_archive_definition_time", "definition_id", "first_received_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    session_id = Column(String(64), nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    spool_segment_id = Column(String(80), nullable=False)
    object_uri = Column(Text, nullable=False)
    object_key = Column(Text, nullable=False)
    format = Column(String(32), nullable=False)
    schema_version = Column(String(64), nullable=False)
    compression = Column(String(32), nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    record_count = Column(BigInteger, nullable=False)
    first_receive_ordinal = Column(BigInteger, nullable=False)
    last_receive_ordinal = Column(BigInteger, nullable=False)
    first_received_at = Column(DateTime(timezone=True), nullable=False)
    last_received_at = Column(DateTime(timezone=True), nullable=False)
    uploaded_at = Column(DateTime(timezone=True), nullable=False)
    acknowledged_at = Column(DateTime(timezone=True), nullable=False)
    object_sha256 = Column(String(64), nullable=False)
    content_fingerprint = Column(String(64), nullable=False)


class MarketRawArchiveRangeRecord(Base):
    """Per-object product/channel position and time bounds."""

    __tablename__ = "raw_archive_ranges"
    __table_args__ = (
        PrimaryKeyConstraint("manifest_id", "provider_product_id", "channel", name="pk_market_raw_archive_range"),
        CheckConstraint("record_count > 0", name="ck_market_raw_archive_range_records"),
        Index("ix_market_raw_archive_range_product_time", "provider_product_id", "channel", "min_received_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    manifest_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.raw_archive_manifests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_product_id = Column(String(128), nullable=False)
    channel = Column(String(64), nullable=False)
    first_provider_sequence_num = Column(BigInteger, nullable=True)
    last_provider_sequence_num = Column(BigInteger, nullable=True)
    min_provider_message_at = Column(DateTime(timezone=True), nullable=True)
    max_provider_message_at = Column(DateTime(timezone=True), nullable=True)
    min_received_at = Column(DateTime(timezone=True), nullable=False)
    max_received_at = Column(DateTime(timezone=True), nullable=False)
    record_count = Column(BigInteger, nullable=False)
    gap_count = Column(Integer, nullable=False, default=0, server_default="0")


class MarketRawArchiveRecordMappingRecord(Base):
    """Immutable placement of a preassigned raw record in an acknowledged object."""

    __tablename__ = "raw_archive_record_mappings"
    __table_args__ = (
        PrimaryKeyConstraint("raw_record_id", "manifest_id", name="pk_market_raw_archive_record_mapping"),
        UniqueConstraint("manifest_id", "object_row_index", name="uq_market_raw_archive_row_index"),
        Index("ix_market_raw_archive_mapping_segment", "spool_segment_id", "receive_ordinal"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    raw_record_id = Column(String(80), nullable=False)
    manifest_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.raw_archive_manifests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    spool_segment_id = Column(String(80), nullable=False)
    session_id = Column(String(64), nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    receive_ordinal = Column(BigInteger, nullable=False)
    object_row_group = Column(Integer, nullable=False, default=0, server_default="0")
    object_row_index = Column(BigInteger, nullable=False)
    raw_frame_sha256 = Column(String(64), nullable=False)
    mapped_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)


class MarketRawArchiveCompactionSourceRecord(Base):
    """Append-only lineage from immutable source objects to one replacement."""

    __tablename__ = "raw_archive_compaction_sources"
    __table_args__ = (
        PrimaryKeyConstraint(
            "replacement_manifest_id",
            "source_manifest_id",
            name="pk_market_raw_archive_compaction_source",
        ),
        UniqueConstraint(
            "replacement_manifest_id",
            "source_ordinal",
            name="uq_market_raw_archive_compaction_ordinal",
        ),
        CheckConstraint("source_ordinal >= 0", name="ck_market_raw_archive_compaction_ordinal"),
        Index("ix_market_raw_archive_compaction_source", "source_manifest_id"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    replacement_manifest_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.raw_archive_manifests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_manifest_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.raw_archive_manifests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_ordinal = Column(Integer, nullable=False)
    replacement_content_fingerprint = Column(String(64), nullable=False)
    compacted_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)


class MarketArchiveRetentionPinVersionRecord(Base):
    """Append-only explicit pin/release revisions for raw or checkpoint objects."""

    __tablename__ = "archive_retention_pin_versions"
    __table_args__ = (
        UniqueConstraint(
            "pin_id",
            "revision",
            name="uq_market_archive_retention_pin_revision",
        ),
        CheckConstraint("revision > 0", name="ck_market_archive_retention_pin_revision"),
        CheckConstraint(
            "target_kind IN ('raw_manifest', 'book_checkpoint')",
            name="ck_market_archive_retention_pin_target",
        ),
        CheckConstraint(
            "status IN ('active', 'released')",
            name="ck_market_archive_retention_pin_status",
        ),
        Index(
            "ix_market_archive_retention_pin_target",
            "target_kind",
            "target_id",
            "known_at",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    pin_id = Column(String(128), nullable=False)
    revision = Column(Integer, nullable=False)
    target_kind = Column(String(32), nullable=False)
    target_id = Column(String(128), nullable=False)
    owner_kind = Column(String(32), nullable=False)
    owner_id = Column(String(128), nullable=False)
    status = Column(String(16), nullable=False)
    reason = Column(Text, nullable=False)
    effective_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)


class MarketStorageLifecycleEventRecord(Base):
    """Append-only evidence for compaction, compression, and retention work."""

    __tablename__ = "storage_lifecycle_events"
    __table_args__ = (
        UniqueConstraint(
            "operation_id",
            "event_ordinal",
            name="uq_market_storage_lifecycle_event",
        ),
        CheckConstraint(
            "event_ordinal >= 0",
            name="ck_market_storage_lifecycle_event_ordinal",
        ),
        CheckConstraint(
            "action IN ('archive_compact', 'archive_expire', 'chunk_compress', 'chunk_expire')",
            name="ck_market_storage_lifecycle_action",
        ),
        CheckConstraint(
            "event_type IN ('planned', 'completed', 'skipped', 'failed')",
            name="ck_market_storage_lifecycle_event_type",
        ),
        CheckConstraint(
            "target_kind IN ('raw_manifest_set', 'raw_manifest', 'book_checkpoint', 'hypertable_chunk')",
            name="ck_market_storage_lifecycle_target_kind",
        ),
        Index(
            "ix_market_storage_lifecycle_target",
            "target_kind",
            "target_id",
            "occurred_at",
        ),
        Index(
            "ix_market_storage_lifecycle_action_time",
            "action",
            "occurred_at",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    operation_id = Column(String(128), nullable=False)
    event_ordinal = Column(Integer, nullable=False)
    policy_version = Column(String(64), nullable=False)
    action = Column(String(32), nullable=False)
    event_type = Column(String(16), nullable=False)
    target_kind = Column(String(32), nullable=False)
    target_id = Column(Text, nullable=False)
    cutoff_at = Column(DateTime(timezone=True), nullable=True)
    occurred_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    reason = Column(Text, nullable=True)
    evidence_hash = Column(String(64), nullable=False)
    evidence = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketStreamCoverageIntervalVersionRecord(Base):
    """Typed product/channel delivery coverage, separate from book validity."""

    __tablename__ = "stream_coverage_interval_versions"
    __table_args__ = (
        UniqueConstraint("interval_id", "revision", name="uq_market_stream_coverage_revision"),
        CheckConstraint("revision > 0", name="ck_market_stream_coverage_revision"),
        CheckConstraint("connection_epoch >= 0", name="ck_market_stream_coverage_epoch"),
        CheckConstraint("last_receive_ordinal >= opening_receive_ordinal", name="ck_market_stream_coverage_ordinals"),
        Index("ix_market_stream_coverage_product_time", "provider_product_id", "channel", "opening_effective_at"),
        Index("ix_market_stream_coverage_status", "definition_id", "status", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    interval_id = Column(String(128), nullable=False)
    revision = Column(Integer, nullable=False)
    definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    session_id = Column(String(64), nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    provider_product_id = Column(String(128), nullable=False)
    channel = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False)
    ordering_assurance = Column(String(64), nullable=False)
    archive_status = Column(String(32), nullable=False)
    opening_session_event_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_session_events.id", ondelete="RESTRICT"),
        nullable=False,
    )
    opening_raw_record_id = Column(String(80), nullable=False)
    opening_receive_ordinal = Column(BigInteger, nullable=False)
    opening_effective_at = Column(DateTime(timezone=True), nullable=False)
    last_raw_record_id = Column(String(80), nullable=False)
    last_receive_ordinal = Column(BigInteger, nullable=False)
    last_effective_at = Column(DateTime(timezone=True), nullable=False)
    closing_session_event_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_session_events.id", ondelete="RESTRICT"),
        nullable=True,
    )
    closing_raw_record_id = Column(String(80), nullable=True)
    closing_receive_ordinal = Column(BigInteger, nullable=True)
    closing_effective_at = Column(DateTime(timezone=True), nullable=True)
    first_provider_sequence_num = Column(BigInteger, nullable=True)
    last_provider_sequence_num = Column(BigInteger, nullable=True)
    canonicalization_watermark_ordinal = Column(BigInteger, nullable=False)
    archive_complete_through_ordinal = Column(BigInteger, nullable=False)
    gap_quality_event_ids = Column(JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"))
    opening_evidence = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    closing_evidence = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    material_hash = Column(String(64), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)


class MarketStreamQualityEventRecord(Base):
    """Typed transport anomaly, invalidation, duplicate, or recovery evidence."""

    __tablename__ = "stream_quality_events"
    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "provider_product_id",
            "channel",
            "receive_ordinal",
            "classification",
            "evidence_hash",
            name="uq_market_stream_quality_event",
        ),
        Index("ix_market_stream_quality_product_time", "provider_product_id", "channel", "detected_at"),
        Index("ix_market_stream_quality_classification", "classification", "detected_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    session_id = Column(String(64), nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    provider_product_id = Column(String(128), nullable=False)
    channel = Column(String(64), nullable=False)
    receive_ordinal = Column(BigInteger, nullable=False)
    classification = Column(String(64), nullable=False)
    sequence_before = Column(BigInteger, nullable=True)
    sequence_after = Column(BigInteger, nullable=True)
    heartbeat_before = Column(String(64), nullable=True)
    heartbeat_after = Column(String(64), nullable=True)
    reason = Column(Text, nullable=False)
    detected_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    raw_record_id = Column(String(80), nullable=True)
    raw_archive_manifest_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.raw_archive_manifests.id", ondelete="RESTRICT"),
        nullable=True,
    )
    coverage_interval_id = Column(String(128), nullable=True)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=True,
    )
    gap_evidence_id = Column(BigInteger, nullable=True)
    evidence_hash = Column(String(64), nullable=False)
    evidence = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketTradeIdentityRecord(Base):
    """Natural-key registry enforcing provider identity across time partitions."""

    __tablename__ = "market_trade_identities"
    __table_args__ = (
        UniqueConstraint(
            "source_id", "provider_product_id", "provider_trade_id", name="uq_market_trade_identity"
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    source_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.sources.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_product_id = Column(String(128), nullable=False)
    provider_trade_id = Column(String(128), nullable=False)
    first_material_hash = Column(String(64), nullable=False)
    first_version_id = Column(String(128), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


class MarketBookValidityIntervalVersionRecord(Base):
    """Opening or closing revision of one reconstructable book interval."""

    __tablename__ = "book_validity_interval_versions"
    __table_args__ = (
        UniqueConstraint("interval_id", "revision", name="uq_market_book_validity_revision"),
        CheckConstraint("revision > 0", name="ck_market_book_validity_revision"),
        CheckConstraint(
            "status IN ('open_valid', 'closed_valid', 'closed_invalidated')",
            name="ck_market_book_validity_status",
        ),
        CheckConstraint(
            "last_receive_ordinal >= opening_receive_ordinal",
            name="ck_market_book_validity_position",
        ),
        Index("ix_market_book_validity_series_time", "series_id", "opening_effective_at"),
        Index("ix_market_book_validity_status", "series_id", "status", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    interval_id = Column(String(128), nullable=False)
    revision = Column(Integer, nullable=False)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    status = Column(String(32), nullable=False)
    ordering_assurance = Column(String(64), nullable=False)
    reconstruction_version = Column(String(64), nullable=False)
    opening_snapshot_id = Column(String(128), nullable=False)
    opening_session_id = Column(String(64), nullable=False)
    opening_connection_epoch = Column(Integer, nullable=False)
    opening_sequence_num = Column(BigInteger, nullable=True)
    opening_receive_ordinal = Column(BigInteger, nullable=False)
    opening_event_ordinal = Column(Integer, nullable=False)
    opening_effective_at = Column(DateTime(timezone=True), nullable=False)
    opening_known_at = Column(DateTime(timezone=True), nullable=False)
    last_session_id = Column(String(64), nullable=False)
    last_connection_epoch = Column(Integer, nullable=False)
    last_sequence_num = Column(BigInteger, nullable=True)
    last_receive_ordinal = Column(BigInteger, nullable=False)
    last_event_ordinal = Column(Integer, nullable=False)
    last_valid_effective_at = Column(DateTime(timezone=True), nullable=False)
    last_state_hash = Column(String(64), nullable=False)
    closing_session_id = Column(String(64), nullable=True)
    closing_connection_epoch = Column(Integer, nullable=True)
    closing_sequence_num = Column(BigInteger, nullable=True)
    closing_receive_ordinal = Column(BigInteger, nullable=True)
    closing_event_ordinal = Column(Integer, nullable=True)
    closing_effective_at = Column(DateTime(timezone=True), nullable=True)
    closing_quality_hash = Column(String(64), nullable=True)
    reason = Column(Text, nullable=True)
    known_at = Column(DateTime(timezone=True), nullable=False)


class MarketBookCheckpointManifestRecord(Base):
    """Verified immutable object metadata for one deterministic book checkpoint."""

    __tablename__ = "book_checkpoint_manifests"
    __table_args__ = (
        UniqueConstraint("object_uri", name="uq_market_book_checkpoint_object"),
        CheckConstraint("level_count > 0", name="ck_market_book_checkpoint_levels"),
        CheckConstraint("byte_count > 0", name="ck_market_book_checkpoint_bytes"),
        Index("ix_market_book_checkpoint_series_time", "series_id", "effective_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), primary_key=True)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    validity_interval_id = Column(String(128), nullable=False)
    reconstruction_version = Column(String(64), nullable=False)
    product_definition_version_id = Column(String(128), nullable=False)
    provider_size_unit = Column(String(32), nullable=False)
    session_id = Column(String(64), nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    provider_sequence_num = Column(BigInteger, nullable=True)
    receive_ordinal = Column(BigInteger, nullable=False)
    event_ordinal = Column(Integer, nullable=False)
    effective_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    state_hash = Column(String(64), nullable=False)
    object_uri = Column(Text, nullable=False)
    object_key = Column(Text, nullable=False)
    object_sha256 = Column(String(64), nullable=False)
    content_fingerprint = Column(String(64), nullable=False)
    format = Column(String(32), nullable=False)
    compression = Column(String(32), nullable=False)
    schema_version = Column(String(64), nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    level_count = Column(BigInteger, nullable=False)
    bid_level_count = Column(BigInteger, nullable=False)
    ask_level_count = Column(BigInteger, nullable=False)
    mutation_count_since_prior = Column(BigInteger, nullable=False)
    source_manifest_ids = Column(JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"))
    acknowledged_at = Column(DateTime(timezone=True), nullable=False)


class MarketBookQualityEventLinkRecord(Base):
    """Typed link from transport quality evidence to a book validity interval."""

    __tablename__ = "book_quality_event_links"
    __table_args__ = (
        PrimaryKeyConstraint("quality_event_id", "validity_interval_id", name="pk_market_book_quality_link"),
        Index("ix_market_book_quality_interval", "validity_interval_id"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    quality_event_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_quality_events.id", ondelete="RESTRICT"),
        nullable=False,
    )
    validity_interval_id = Column(String(128), nullable=False)
    link_role = Column(String(32), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)


class MarketBookReconstructionStateRecord(Base):
    """Disposable current projection rebuilt from checkpoints and raw archive."""

    __tablename__ = "book_reconstruction_state"
    __table_args__ = ({"schema": MARKET_DATA_SCHEMA},)

    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="CASCADE"),
        primary_key=True,
    )
    definition_id = Column(String(64), nullable=False)
    session_id = Column(String(64), nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    lifecycle = Column(String(32), nullable=False)
    validity_interval_id = Column(String(128), nullable=True)
    checkpoint_id = Column(String(128), nullable=True)
    provider_sequence_num = Column(BigInteger, nullable=True)
    receive_ordinal = Column(BigInteger, nullable=False)
    event_ordinal = Column(Integer, nullable=False)
    state_hash = Column(String(64), nullable=True)
    lease_generation = Column(BigInteger, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class MarketNormalizationSpecRecord(Base):
    """Immutable executable causal normalization specification."""

    __tablename__ = "normalization_specs"
    __table_args__ = (
        UniqueConstraint(
            "feature_name",
            "semantic_version",
            "spec_hash",
            name="uq_market_normalization_spec_identity",
        ),
        CheckConstraint("feature_name <> ''", name="ck_market_normalization_feature"),
        CheckConstraint("minimum_observations >= 0", name="ck_market_normalization_minimum"),
        CheckConstraint(
            "warmup_observations >= minimum_observations",
            name="ck_market_normalization_warmup",
        ),
        CheckConstraint(
            "window_seconds IS NULL OR window_seconds > 0",
            name="ck_market_normalization_window",
        ),
        Index("ix_market_normalization_feature_version", "feature_name", "semantic_version"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(64), primary_key=True)
    spec_hash = Column(String(64), nullable=False)
    feature_name = Column(String(128), nullable=False)
    semantic_version = Column(String(64), nullable=False)
    input_fact_type = Column(String(128), nullable=False)
    output_fact_type = Column(String(128), nullable=False)
    formula = Column(String(64), nullable=False)
    units = Column(String(64), nullable=False)
    window_seconds = Column(BigInteger, nullable=True)
    minimum_observations = Column(Integer, nullable=False)
    warmup_observations = Column(Integer, nullable=False)
    partition = Column(String(64), nullable=False)
    missing_behavior = Column(String(64), nullable=False)
    materialization_mode = Column(String(64), nullable=False)
    parameters = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_by = Column(String(128), nullable=True)
    approved_by = Column(String(128), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    approved_at = Column(DateTime(timezone=True), nullable=True)


class MarketDatasetNormalizationRefRecord(Base):
    """Immutable binding from a frozen dataset to one normalization spec."""

    __tablename__ = "dataset_normalization_refs"
    __table_args__ = (
        PrimaryKeyConstraint(
            "dataset_id",
            "spec_id",
            "output_series_id",
            name="pk_market_dataset_normalization_ref",
        ),
        CheckConstraint("input_count > 0", name="ck_market_dataset_normalization_count"),
        CheckConstraint("input_watermark > 0", name="ck_market_dataset_normalization_watermark"),
        CheckConstraint("range_end > range_start", name="ck_market_dataset_normalization_range"),
        CheckConstraint(
            "input_range_end >= input_range_start",
            name="ck_market_dataset_normalization_input_range",
        ),
        Index("ix_market_dataset_normalization_spec", "spec_id", "output_series_id"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    dataset_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.datasets.id", ondelete="RESTRICT"),
        nullable=False,
    )
    spec_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.normalization_specs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    output_series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    range_start = Column(DateTime(timezone=True), nullable=False)
    range_end = Column(DateTime(timezone=True), nullable=False)
    input_range_start = Column(DateTime(timezone=True), nullable=False)
    input_range_end = Column(DateTime(timezone=True), nullable=False)
    input_count = Column(BigInteger, nullable=False)
    input_watermark = Column(BigInteger, nullable=False)
    source_series_ids = Column(JSONB, nullable=False)
    input_fingerprint = Column(String(64), nullable=False)
    source_dataset_fingerprints = Column(JSONB, nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality_hash = Column(String(64), nullable=False)
    storage_kind = Column(String(32), nullable=False, default="database_snapshot", server_default="database_snapshot")
    frozen_object_uri = Column(Text, nullable=True)
    frozen_object_sha256 = Column(String(64), nullable=True)
    row_count = Column(BigInteger, nullable=False)


class MarketDatasetArchiveRefRecord(Base):
    """Immutable raw-object retention pin attached to a frozen dataset."""

    __tablename__ = "dataset_archive_refs"
    __table_args__ = (
        PrimaryKeyConstraint("dataset_id", "raw_archive_manifest_id", name="pk_market_dataset_archive_ref"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    dataset_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.datasets.id", ondelete="RESTRICT"),
        nullable=False,
    )
    raw_archive_manifest_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.raw_archive_manifests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    inclusion_role = Column(String(64), nullable=False)
    object_sha256 = Column(String(64), nullable=False)
    content_fingerprint = Column(String(64), nullable=False)


__all__ = [
    "MARKET_DATA_SCHEMA",
    "MarketCollectionAttemptRecord",
    "MarketCollectionDefinitionRecord",
    "MarketCollectorOperationRecord",
    "MarketCollectorSafetyEventRecord",
    "MarketCollectorSafetyStateRecord",
    "MarketCollectorWorkerStateRecord",
    "MarketDataIngestionRunRecord",
    "MarketDataSeriesRecord",
    "MarketDataSourceRecord",
    "MarketDatasetRecord",
    "MarketDatasetSeriesRecord",
    "MarketDatasetNormalizationRefRecord",
    "MarketNormalizationSpecRecord",
    "MarketGapEvidenceRecord",
    "MarketFactAcquisitionCoverageRecord",
    "MarketProviderRateBudgetRecord",
    "MarketDatasetArchiveRefRecord",
    "MarketArchiveRetentionPinVersionRecord",
    "MarketStorageLifecycleEventRecord",
    "MarketInstrumentRoleMappingVersionRecord",
    "MarketBookCheckpointManifestRecord",
    "MarketBookQualityEventLinkRecord",
    "MarketBookReconstructionStateRecord",
    "MarketBookValidityIntervalVersionRecord",
    "MarketProductDefinitionVersionRecord",
    "MarketRawArchiveManifestRecord",
    "MarketRawArchiveCompactionSourceRecord",
    "MarketRawArchiveRangeRecord",
    "MarketRawArchiveRecordMappingRecord",
    "MarketStreamCoverageIntervalVersionRecord",
    "MarketStreamDefinitionRecord",
    "MarketStreamLeaseStateRecord",
    "MarketStreamQualityEventRecord",
    "MarketStreamSessionEventRecord",
    "MarketTradeIdentityRecord",
]
