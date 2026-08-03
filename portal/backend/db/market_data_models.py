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


class MarketCandleVersionRecord(Base):
    """Append-only versions of one logical closed candle."""

    __tablename__ = "candle_versions"
    __table_args__ = (
        PrimaryKeyConstraint(
            "series_id",
            "candle_open_time",
            "revision",
            name="pk_market_candle_versions",
        ),
        CheckConstraint("revision > 0", name="ck_market_candle_revision_positive"),
        CheckConstraint(
            "candle_close_time > candle_open_time",
            name="ck_market_candle_time_order",
        ),
        CheckConstraint("high >= low", name="ck_market_candle_high_low"),
        CheckConstraint(
            "high >= open AND high >= close",
            name="ck_market_candle_high_bounds",
        ),
        CheckConstraint(
            "low <= open AND low <= close",
            name="ck_market_candle_low_bounds",
        ),
        CheckConstraint("volume IS NULL OR volume >= 0", name="ck_market_candle_volume"),
        CheckConstraint(
            "trade_count IS NULL OR trade_count >= 0",
            name="ck_market_candle_trade_count",
        ),
        CheckConstraint(
            "known_at >= candle_close_time",
            name="ck_market_candle_known_after_close",
        ),
        CheckConstraint(
            "source_published_at IS NULL OR known_at >= source_published_at",
            name="ck_market_candle_known_after_publication",
        ),
        CheckConstraint(
            "received_at IS NULL OR (known_at >= received_at AND accepted_at >= received_at)",
            name="ck_market_candle_receipt_order",
        ),
        Index(
            "ix_market_candle_versions_series_time_revision",
            "series_id",
            text("candle_open_time DESC"),
            text("revision DESC"),
        ),
        Index(
            "ix_market_candle_versions_series_commit",
            "series_id",
            "market_commit_seq",
        ),
        Index("ix_market_candle_versions_series_known", "series_id", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    candle_open_time = Column(DateTime(timezone=True), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('market.fact_commit_seq'::regclass)"),
    )
    ingestion_run_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.ingestion_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    candle_close_time = Column(DateTime(timezone=True), nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)
    trade_count = Column(BigInteger, nullable=True)
    source_published_at = Column(DateTime(timezone=True), nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=True)
    accepted_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    known_at_method = Column(String(64), nullable=False)
    provenance = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    row_hash = Column(String(64), nullable=False)


class MarketOpenInterestVersionRecord(Base):
    """Append-only scheduled observations of venue-specific open interest."""

    __tablename__ = "open_interest_versions"
    __table_args__ = (
        PrimaryKeyConstraint(
            "series_id",
            "sample_time",
            "revision",
            name="pk_market_open_interest_versions",
        ),
        CheckConstraint(
            "revision > 0", name="ck_market_open_interest_revision_positive"
        ),
        CheckConstraint(
            "open_interest >= 0", name="ck_market_open_interest_nonnegative"
        ),
        CheckConstraint(
            "known_at >= sample_time",
            name="ck_market_open_interest_known_after_schedule",
        ),
        CheckConstraint(
            "source_published_at IS NULL OR known_at >= source_published_at",
            name="ck_market_open_interest_known_after_publication",
        ),
        CheckConstraint(
            "received_at IS NULL OR (known_at >= received_at AND accepted_at >= received_at)",
            name="ck_market_open_interest_receipt_order",
        ),
        Index(
            "ix_market_open_interest_series_time_revision",
            "series_id",
            text("sample_time DESC"),
            text("revision DESC"),
        ),
        Index(
            "ix_market_open_interest_series_commit",
            "series_id",
            "market_commit_seq",
        ),
        Index(
            "ix_market_open_interest_series_known", "series_id", "known_at"
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    sample_time = Column(DateTime(timezone=True), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('market.fact_commit_seq'::regclass)"),
    )
    ingestion_run_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.ingestion_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    open_interest = Column(Float, nullable=False)
    unit = Column(String(32), nullable=False)
    sample_time_method = Column(String(64), nullable=False)
    source_published_at = Column(DateTime(timezone=True), nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=True)
    accepted_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    known_at_method = Column(String(64), nullable=False)
    provenance = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    row_hash = Column(String(64), nullable=False)


class MarketFundingRateVersionRecord(Base):
    """Append-only scheduled observations of perpetual funding rates."""

    __tablename__ = "funding_rate_versions"
    __table_args__ = (
        PrimaryKeyConstraint(
            "series_id",
            "sample_time",
            "revision",
            name="pk_market_funding_rate_versions",
        ),
        CheckConstraint(
            "revision > 0", name="ck_market_funding_rate_revision_positive"
        ),
        CheckConstraint(
            "funding_interval_seconds > 0",
            name="ck_market_funding_rate_interval_positive",
        ),
        CheckConstraint(
            "known_at >= sample_time",
            name="ck_market_funding_rate_known_after_schedule",
        ),
        CheckConstraint(
            "source_published_at IS NULL OR known_at >= source_published_at",
            name="ck_market_funding_rate_known_after_publication",
        ),
        CheckConstraint(
            "received_at IS NULL OR (known_at >= received_at AND accepted_at >= received_at)",
            name="ck_market_funding_rate_receipt_order",
        ),
        Index(
            "ix_market_funding_rate_series_time_revision",
            "series_id",
            text("sample_time DESC"),
            text("revision DESC"),
        ),
        Index(
            "ix_market_funding_rate_series_commit",
            "series_id",
            "market_commit_seq",
        ),
        Index(
            "ix_market_funding_rate_series_known", "series_id", "known_at"
        ),
        Index(
            "ix_market_funding_rate_series_funding_time",
            "series_id",
            "funding_time",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    sample_time = Column(DateTime(timezone=True), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('market.fact_commit_seq'::regclass)"),
    )
    ingestion_run_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.ingestion_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    funding_rate = Column(Float, nullable=False)
    funding_time = Column(DateTime(timezone=True), nullable=False)
    funding_interval_seconds = Column(Integer, nullable=False)
    unit = Column(String(32), nullable=False)
    sample_time_method = Column(String(64), nullable=False)
    source_published_at = Column(DateTime(timezone=True), nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=True)
    accepted_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    known_at_method = Column(String(64), nullable=False)
    provenance = Column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    row_hash = Column(String(64), nullable=False)


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
            "((lease_owner IS NOT NULL AND lease_token_hash IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (lease_owner IS NULL AND lease_token_hash IS NULL AND lease_expires_at IS NULL))",
            name="ck_market_collection_lease_state",
        ),
        Index(
            "ix_market_collection_claimable",
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
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(BigInteger, Identity(), primary_key=True)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
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
        CheckConstraint("max_spool_bytes > 0", name="ck_market_stream_definition_spool"),
        CheckConstraint("max_segment_bytes > 0", name="ck_market_stream_definition_segment"),
        CheckConstraint(
            "max_segment_bytes <= max_spool_bytes",
            name="ck_market_stream_definition_segment_within_spool",
        ),
        Index("ix_market_stream_definition_enabled", "enabled", "provider", "venue"),
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
    production_admitted = Column(Boolean, nullable=False, default=False, server_default="false")
    max_spool_bytes = Column(BigInteger, nullable=False)
    max_segment_bytes = Column(BigInteger, nullable=False)
    generation = Column(BigInteger, nullable=False, default=1, server_default="1")
    config = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


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


class MarketTradeVersionRecord(Base):
    """Append-only provider trade revisions with causal source positions."""

    __tablename__ = "market_trade_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "provider_event_time", name="pk_market_trade_versions"),
        UniqueConstraint(
            "source_id",
            "provider_product_id",
            "provider_trade_id",
            "provider_event_time",
            "revision",
            name="uq_market_trade_version",
        ),
        CheckConstraint("revision > 0", name="ck_market_trade_revision"),
        CheckConstraint("price > 0", name="ck_market_trade_price"),
        CheckConstraint("provider_size > 0", name="ck_market_trade_provider_size"),
        CheckConstraint("known_at >= accepted_at AND accepted_at >= received_at", name="ck_market_trade_causal_times"),
        Index("ix_market_trade_product_time", "provider_product_id", "provider_event_time"),
        Index("ix_market_trade_series_known", "series_id", "known_at"),
        Index("ix_market_trade_series_commit", "series_id", "market_commit_seq"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
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
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('market.fact_commit_seq'::regclass)"),
    )
    provider_product_id = Column(String(128), nullable=False)
    provider_trade_id = Column(String(128), nullable=False)
    delivery_kind = Column(String(16), nullable=False)
    price = Column(Numeric(38, 18), nullable=False)
    provider_size = Column(Numeric(38, 18), nullable=False)
    provider_size_unit = Column(String(32), nullable=False)
    maker_side = Column(String(8), nullable=False)
    aggressor_side = Column(String(8), nullable=True)
    aggressor_transform_version = Column(String(64), nullable=True)
    contract_quantity = Column(Numeric(38, 18), nullable=True)
    base_quantity = Column(Numeric(38, 18), nullable=True)
    quote_notional = Column(Numeric(38, 18), nullable=True)
    base_currency = Column(String(32), nullable=False)
    quote_currency = Column(String(32), nullable=False)
    product_definition_version_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.product_definition_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_event_time = Column(DateTime(timezone=True), nullable=False)
    provider_message_time = Column(DateTime(timezone=True), nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=False)
    accepted_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    provider_sequence_num = Column(BigInteger, nullable=True)
    connection_epoch = Column(Integer, nullable=False)
    receive_ordinal = Column(BigInteger, nullable=False)
    event_ordinal = Column(Integer, nullable=False)
    trade_ordinal = Column(Integer, nullable=False)
    raw_record_id = Column(String(80), nullable=False)
    coverage_interval_id = Column(String(128), nullable=True)
    material_hash = Column(String(64), nullable=False)
    row_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketTradeFlowAggregateVersionRecord(Base):
    """Append-only one-second or one-minute causal trade-flow bucket."""

    __tablename__ = "trade_flow_aggregate_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "bucket_start", name="pk_market_trade_flow_versions"),
        UniqueConstraint(
            "series_id",
            "interval_seconds",
            "bucket_start",
            "aggregation_version",
            "revision",
            name="uq_market_trade_flow_revision",
        ),
        CheckConstraint("interval_seconds IN (1, 60)", name="ck_market_trade_flow_interval"),
        CheckConstraint("revision > 0", name="ck_market_trade_flow_revision"),
        CheckConstraint("bucket_end > bucket_start", name="ck_market_trade_flow_range"),
        CheckConstraint("trade_count >= 0", name="ck_market_trade_flow_count"),
        Index("ix_market_trade_flow_series_time", "series_id", "interval_seconds", "bucket_start"),
        Index("ix_market_trade_flow_series_known", "series_id", "known_at"),
        Index("ix_market_trade_flow_series_commit", "series_id", "market_commit_seq"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    interval_seconds = Column(Integer, nullable=False)
    bucket_start = Column(DateTime(timezone=True), nullable=False)
    bucket_end = Column(DateTime(timezone=True), nullable=False)
    aggregation_version = Column(String(64), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('market.fact_commit_seq'::regclass)"),
    )
    trade_count = Column(BigInteger, nullable=False)
    maker_buy_count = Column(BigInteger, nullable=False)
    maker_sell_count = Column(BigInteger, nullable=False)
    aggressor_buy_count = Column(BigInteger, nullable=True)
    aggressor_sell_count = Column(BigInteger, nullable=True)
    contract_volume = Column(Numeric(38, 18), nullable=True)
    base_volume = Column(Numeric(38, 18), nullable=True)
    quote_notional = Column(Numeric(38, 18), nullable=True)
    maker_buy_base_volume = Column(Numeric(38, 18), nullable=True)
    maker_sell_base_volume = Column(Numeric(38, 18), nullable=True)
    aggressor_buy_base_volume = Column(Numeric(38, 18), nullable=True)
    aggressor_sell_base_volume = Column(Numeric(38, 18), nullable=True)
    cvd_delta = Column(Numeric(38, 18), nullable=True)
    cvd_unit = Column(String(32), nullable=True)
    open_price = Column(Numeric(38, 18), nullable=True)
    high_price = Column(Numeric(38, 18), nullable=True)
    low_price = Column(Numeric(38, 18), nullable=True)
    close_price = Column(Numeric(38, 18), nullable=True)
    first_trade_id = Column(String(128), nullable=True)
    last_trade_id = Column(String(128), nullable=True)
    first_receive_ordinal = Column(BigInteger, nullable=True)
    last_receive_ordinal = Column(BigInteger, nullable=True)
    coverage_interval_id = Column(String(128), nullable=True)
    coverage_revision = Column(Integer, nullable=True)
    aggregate_complete = Column(Boolean, nullable=False)
    archive_complete = Column(Boolean, nullable=False)
    canonicalization_complete = Column(Boolean, nullable=False)
    late_trade_count = Column(BigInteger, nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    input_fingerprint = Column(String(64), nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketL2SnapshotVersionRecord(Base):
    """One accepted complete aggregated-book snapshot event."""

    __tablename__ = "l2_snapshot_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "effective_at", name="pk_market_l2_snapshot_versions"),
        UniqueConstraint(
            "definition_id",
            "session_id",
            "connection_epoch",
            "receive_ordinal",
            "event_ordinal",
            "effective_at",
            name="uq_market_l2_snapshot_source_position",
        ),
        CheckConstraint("connection_epoch >= 0", name="ck_market_l2_snapshot_epoch"),
        CheckConstraint("receive_ordinal > 0", name="ck_market_l2_snapshot_receive"),
        CheckConstraint("event_ordinal >= 0", name="ck_market_l2_snapshot_event"),
        CheckConstraint("level_count > 0", name="ck_market_l2_snapshot_levels"),
        CheckConstraint(
            "known_at >= accepted_at AND accepted_at >= received_at",
            name="ck_market_l2_snapshot_causal_times",
        ),
        Index("ix_market_l2_snapshot_series_time", "series_id", "effective_at"),
        Index("ix_market_l2_snapshot_series_commit", "series_id", "market_commit_seq"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    session_id = Column(String(64), nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    provider_product_id = Column(String(128), nullable=False)
    product_definition_version_id = Column(
        String(128),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.product_definition_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_sequence_num = Column(BigInteger, nullable=True)
    receive_ordinal = Column(BigInteger, nullable=False)
    event_ordinal = Column(Integer, nullable=False)
    effective_at = Column(DateTime(timezone=True), nullable=False)
    provider_message_time = Column(DateTime(timezone=True), nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=False)
    accepted_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    level_count = Column(BigInteger, nullable=False)
    state_hash = Column(String(64), nullable=False)
    event_material_hash = Column(String(64), nullable=False)
    raw_record_id = Column(String(80), nullable=False)
    validity_interval_id = Column(String(128), nullable=False)
    market_commit_seq = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('market.fact_commit_seq'::regclass)"),
    )
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketL2SnapshotLevelRecord(Base):
    """One typed absolute side/price quantity in an accepted snapshot."""

    __tablename__ = "l2_snapshot_levels"
    __table_args__ = (
        PrimaryKeyConstraint(
            "snapshot_version_id",
            "snapshot_effective_at",
            "side",
            "price",
            name="pk_market_l2_snapshot_level",
        ),
        CheckConstraint("side IN ('bid', 'ask')", name="ck_market_l2_snapshot_level_side"),
        CheckConstraint("price > 0", name="ck_market_l2_snapshot_level_price"),
        CheckConstraint("quantity > 0", name="ck_market_l2_snapshot_level_quantity"),
        Index("ix_market_l2_snapshot_level_order", "snapshot_version_id", "side", "price"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    snapshot_version_id = Column(String(128), nullable=False)
    snapshot_effective_at = Column(DateTime(timezone=True), nullable=False)
    side = Column(String(8), nullable=False)
    price = Column(Numeric(38, 18), nullable=False)
    quantity = Column(Numeric(38, 18), nullable=False)
    provider_size_unit = Column(String(32), nullable=False)
    provider_event_time = Column(DateTime(timezone=True), nullable=False)
    level_ordinal = Column(BigInteger, nullable=False)


class MarketL2MutationBatchRecord(Base):
    """One accepted atomic provider Level 2 update event."""

    __tablename__ = "l2_mutation_batches"
    __table_args__ = (
        PrimaryKeyConstraint("id", "effective_at", name="pk_market_l2_mutation_batches"),
        UniqueConstraint(
            "definition_id",
            "session_id",
            "connection_epoch",
            "receive_ordinal",
            "event_ordinal",
            "effective_at",
            name="uq_market_l2_mutation_source_position",
        ),
        CheckConstraint("mutation_count > 0", name="ck_market_l2_mutation_count"),
        CheckConstraint("unknown_zero_delete_count >= 0", name="ck_market_l2_unknown_delete"),
        CheckConstraint(
            "known_at >= accepted_at AND accepted_at >= received_at",
            name="ck_market_l2_mutation_causal_times",
        ),
        Index("ix_market_l2_mutation_series_time", "series_id", "effective_at"),
        Index("ix_market_l2_mutation_series_commit", "series_id", "market_commit_seq"),
        Index(
            "ix_market_l2_mutation_session_position",
            "session_id",
            "connection_epoch",
            "receive_ordinal",
            "event_ordinal",
        ),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    definition_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.stream_definitions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    session_id = Column(String(64), nullable=False)
    connection_epoch = Column(Integer, nullable=False)
    provider_product_id = Column(String(128), nullable=False)
    product_definition_version_id = Column(String(128), nullable=False)
    provider_sequence_num = Column(BigInteger, nullable=True)
    receive_ordinal = Column(BigInteger, nullable=False)
    event_ordinal = Column(Integer, nullable=False)
    effective_at = Column(DateTime(timezone=True), nullable=False)
    provider_message_time = Column(DateTime(timezone=True), nullable=True)
    received_at = Column(DateTime(timezone=True), nullable=False)
    accepted_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    mutation_count = Column(BigInteger, nullable=False)
    before_state_hash = Column(String(64), nullable=False)
    after_state_hash = Column(String(64), nullable=False)
    event_material_hash = Column(String(64), nullable=False)
    raw_record_id = Column(String(80), nullable=False)
    validity_interval_id = Column(String(128), nullable=False)
    unknown_zero_delete_count = Column(BigInteger, nullable=False)
    market_commit_seq = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('market.fact_commit_seq'::regclass)"),
    )
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketL2MutationRecord(Base):
    """One ordered absolute side/price mutation in a provider event."""

    __tablename__ = "l2_mutations"
    __table_args__ = (
        PrimaryKeyConstraint(
            "batch_id",
            "batch_effective_at",
            "mutation_ordinal",
            name="pk_market_l2_mutation",
        ),
        CheckConstraint("mutation_ordinal >= 0", name="ck_market_l2_mutation_ordinal"),
        CheckConstraint("side IN ('bid', 'ask')", name="ck_market_l2_mutation_side"),
        CheckConstraint("price > 0", name="ck_market_l2_mutation_price"),
        CheckConstraint("new_quantity >= 0", name="ck_market_l2_mutation_quantity"),
        Index("ix_market_l2_mutation_level", "batch_id", "side", "price"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    batch_id = Column(String(128), nullable=False)
    batch_effective_at = Column(DateTime(timezone=True), nullable=False)
    mutation_ordinal = Column(Integer, nullable=False)
    side = Column(String(8), nullable=False)
    price = Column(Numeric(38, 18), nullable=False)
    new_quantity = Column(Numeric(38, 18), nullable=False)
    provider_size_unit = Column(String(32), nullable=False)
    provider_event_time = Column(DateTime(timezone=True), nullable=False)


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


class MarketBboFeatureVersionRecord(Base):
    """Append-only causal one-second BBO, spread, and midpoint fact."""

    __tablename__ = "bbo_feature_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "bucket_start", name="pk_market_bbo_feature_versions"),
        UniqueConstraint("series_id", "bucket_start", "revision", name="uq_market_bbo_feature_revision"),
        CheckConstraint("revision > 0", name="ck_market_bbo_feature_revision"),
        CheckConstraint("bucket_end > bucket_start", name="ck_market_bbo_feature_range"),
        CheckConstraint("bid_price > 0 AND ask_price > bid_price", name="ck_market_bbo_feature_prices"),
        Index("ix_market_bbo_feature_series_time", "series_id", "bucket_start"),
        Index("ix_market_bbo_feature_series_known", "series_id", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    source_l2_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(BigInteger, nullable=False, server_default=text("nextval('market.fact_commit_seq'::regclass)"))
    bucket_start = Column(DateTime(timezone=True), nullable=False)
    bucket_end = Column(DateTime(timezone=True), nullable=False)
    source_effective_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    source_position = Column(JSONB, nullable=False)
    validity_interval_id = Column(String(128), nullable=False)
    product_definition_version_id = Column(String(128), ForeignKey(f"{MARKET_DATA_SCHEMA}.product_definition_versions.id", ondelete="RESTRICT"), nullable=False)
    provider_size_unit = Column(String(32), nullable=False)
    source_state_hash = Column(String(64), nullable=False)
    bid_price = Column(Numeric(38, 18), nullable=False)
    bid_quantity = Column(Numeric(78, 38), nullable=False)
    bid_base_quantity = Column(Numeric(78, 38), nullable=False)
    ask_price = Column(Numeric(38, 18), nullable=False)
    ask_quantity = Column(Numeric(78, 38), nullable=False)
    ask_base_quantity = Column(Numeric(78, 38), nullable=False)
    mid_price = Column(Numeric(38, 18), nullable=False)
    spread = Column(Numeric(38, 18), nullable=False)
    spread_bps = Column(Numeric(78, 38), nullable=False)
    input_fingerprint = Column(String(64), nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketDepthFeatureVersionRecord(Base):
    """Append-only one-second depth and bounded imbalance fact."""

    __tablename__ = "depth_feature_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "bucket_start", name="pk_market_depth_feature_versions"),
        UniqueConstraint("series_id", "bucket_start", "band_bps", "revision", name="uq_market_depth_feature_revision"),
        CheckConstraint("revision > 0", name="ck_market_depth_feature_revision"),
        CheckConstraint("band_bps IN (5, 10, 25)", name="ck_market_depth_feature_band"),
        CheckConstraint("imbalance IS NULL OR (imbalance >= -1 AND imbalance <= 1)", name="ck_market_depth_feature_imbalance"),
        Index("ix_market_depth_feature_series_time", "series_id", "bucket_start"),
        Index("ix_market_depth_feature_series_known", "series_id", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    source_l2_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(BigInteger, nullable=False, server_default=text("nextval('market.fact_commit_seq'::regclass)"))
    bucket_start = Column(DateTime(timezone=True), nullable=False)
    bucket_end = Column(DateTime(timezone=True), nullable=False)
    source_effective_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    source_position = Column(JSONB, nullable=False)
    validity_interval_id = Column(String(128), nullable=False)
    source_state_hash = Column(String(64), nullable=False)
    bbo_input_fingerprint = Column(String(64), nullable=False)
    provider_size_unit = Column(String(32), nullable=False)
    band_bps = Column(Integer, nullable=False)
    mid_price = Column(Numeric(38, 18), nullable=False)
    bid_quantity = Column(Numeric(78, 38), nullable=False)
    ask_quantity = Column(Numeric(78, 38), nullable=False)
    bid_base_quantity = Column(Numeric(78, 38), nullable=False)
    ask_base_quantity = Column(Numeric(78, 38), nullable=False)
    bid_notional = Column(Numeric(78, 38), nullable=True)
    ask_notional = Column(Numeric(78, 38), nullable=True)
    imbalance = Column(Numeric(78, 38), nullable=True)
    input_fingerprint = Column(String(64), nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketTradeFlowFeatureVersionRecord(Base):
    """Append-only enriched complete trade-flow feature bucket."""

    __tablename__ = "trade_flow_feature_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "bucket_start", name="pk_market_trade_flow_feature_versions"),
        UniqueConstraint("series_id", "interval_seconds", "bucket_start", "revision", name="uq_market_trade_flow_feature_revision"),
        CheckConstraint("revision > 0", name="ck_market_trade_flow_feature_revision"),
        CheckConstraint("interval_seconds IN (1, 60)", name="ck_market_trade_flow_feature_interval"),
        CheckConstraint("trade_count > 0", name="ck_market_trade_flow_feature_count"),
        CheckConstraint("cvd_volume_share IS NULL OR (cvd_volume_share >= -1 AND cvd_volume_share <= 1)", name="ck_market_trade_flow_feature_share"),
        Index("ix_market_trade_flow_feature_series_time", "series_id", "bucket_start"),
        Index("ix_market_trade_flow_feature_series_known", "series_id", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    source_trade_flow_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(BigInteger, nullable=False, server_default=text("nextval('market.fact_commit_seq'::regclass)"))
    interval_seconds = Column(Integer, nullable=False)
    bucket_start = Column(DateTime(timezone=True), nullable=False)
    bucket_end = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    aggregate_material_hash = Column(String(64), nullable=False)
    aggregate_input_fingerprint = Column(String(64), nullable=False)
    trade_count = Column(BigInteger, nullable=False)
    quote_notional = Column(Numeric(78, 38), nullable=False)
    aggressor_buy_base_volume = Column(Numeric(78, 38), nullable=False)
    aggressor_sell_base_volume = Column(Numeric(78, 38), nullable=False)
    aggressor_buy_notional = Column(Numeric(78, 38), nullable=False)
    aggressor_sell_notional = Column(Numeric(78, 38), nullable=False)
    cvd_base = Column(Numeric(78, 38), nullable=False)
    cvd_notional = Column(Numeric(78, 38), nullable=False)
    cvd_volume_share = Column(Numeric(78, 38), nullable=True)
    input_fingerprint = Column(String(64), nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketFuturesSpotRelationshipVersionRecord(Base):
    """Append-only causally aligned futures/spot basis fact."""

    __tablename__ = "futures_spot_relationship_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "effective_at", name="pk_market_futures_spot_versions"),
        UniqueConstraint("series_id", "effective_at", "revision", name="uq_market_futures_spot_revision"),
        CheckConstraint("revision > 0", name="ck_market_futures_spot_revision"),
        CheckConstraint("futures_mid > 0 AND spot_mid > 0", name="ck_market_futures_spot_mid"),
        CheckConstraint("futures_staleness_seconds >= 0 AND spot_staleness_seconds >= 0", name="ck_market_futures_spot_staleness"),
        Index("ix_market_futures_spot_series_time", "series_id", "effective_at"),
        Index("ix_market_futures_spot_series_known", "series_id", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(BigInteger, nullable=False, server_default=text("nextval('market.fact_commit_seq'::regclass)"))
    mapping_id = Column(String(128), nullable=False)
    futures_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    spot_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    effective_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    futures_bbo_material_hash = Column(String(64), nullable=False)
    spot_bbo_material_hash = Column(String(64), nullable=False)
    futures_mid = Column(Numeric(38, 18), nullable=False)
    spot_mid = Column(Numeric(38, 18), nullable=False)
    futures_staleness_seconds = Column(Numeric(38, 18), nullable=False)
    spot_staleness_seconds = Column(Numeric(38, 18), nullable=False)
    basis = Column(Numeric(38, 18), nullable=False)
    basis_bps = Column(Numeric(78, 38), nullable=False)
    input_fingerprint = Column(String(64), nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketDerivativeStateVersionRecord(Base):
    """Append-only one-minute OI/funding relationship fact."""

    __tablename__ = "derivative_state_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "effective_at", name="pk_market_derivative_state_versions"),
        UniqueConstraint("series_id", "effective_at", "revision", name="uq_market_derivative_state_revision"),
        CheckConstraint("revision > 0", name="ck_market_derivative_state_revision"),
        Index("ix_market_derivative_state_series_time", "series_id", "effective_at"),
        Index("ix_market_derivative_state_series_known", "series_id", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(BigInteger, nullable=False, server_default=text("nextval('market.fact_commit_seq'::regclass)"))
    instrument_id = Column(String(64), ForeignKey("portal_instruments.id", ondelete="RESTRICT"), nullable=False)
    effective_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    oi_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=True)
    oi_sample_time = Column(DateTime(timezone=True), nullable=True)
    oi_market_commit_seq = Column(BigInteger, nullable=True)
    oi_value = Column(Numeric(78, 38), nullable=True)
    oi_previous_value = Column(Numeric(78, 38), nullable=True)
    oi_log_change = Column(Numeric(78, 38), nullable=True)
    funding_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=True)
    funding_sample_time = Column(DateTime(timezone=True), nullable=True)
    funding_market_commit_seq = Column(BigInteger, nullable=True)
    funding_rate = Column(Numeric(78, 38), nullable=True)
    funding_time = Column(DateTime(timezone=True), nullable=True)
    funding_interval_seconds = Column(Integer, nullable=True)
    funding_semantics = Column(String(32), nullable=True)
    input_fingerprint = Column(String(64), nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


class MarketResponseFeatureVersionRecord(Base):
    """Append-only directional depth replenishment and price-response fact."""

    __tablename__ = "market_response_feature_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "bucket_start", name="pk_market_response_feature_versions"),
        UniqueConstraint("series_id", "bucket_start", "direction", "revision", name="uq_market_response_feature_revision"),
        CheckConstraint("revision > 0", name="ck_market_response_feature_revision"),
        CheckConstraint("direction IN ('BUY', 'SELL')", name="ck_market_response_feature_direction"),
        CheckConstraint("aggressive_notional > 0 AND pre_depth_notional > 0 AND consumed_depth_notional > 0", name="ck_market_response_feature_positive"),
        Index("ix_market_response_feature_series_time", "series_id", "bucket_start"),
        Index("ix_market_response_feature_series_known", "series_id", "known_at"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    source_flow_feature_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    source_l2_series_id = Column(BigInteger, ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"), nullable=False)
    source_flow_material_hash = Column(String(64), nullable=False)
    pre_state_hash = Column(String(64), nullable=False)
    trough_state_hash = Column(String(64), nullable=False)
    post_state_hash = Column(String(64), nullable=False)
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(BigInteger, nullable=False, server_default=text("nextval('market.fact_commit_seq'::regclass)"))
    bucket_start = Column(DateTime(timezone=True), nullable=False)
    bucket_end = Column(DateTime(timezone=True), nullable=False)
    effective_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    direction = Column(String(8), nullable=False)
    first_trade_id = Column(String(128), nullable=False)
    last_trade_id = Column(String(128), nullable=False)
    source_positions = Column(JSONB, nullable=False)
    validity_interval_id = Column(String(128), nullable=False)
    aggressive_notional = Column(Numeric(78, 38), nullable=False)
    signed_aggressive_notional = Column(Numeric(78, 38), nullable=False)
    response_bps = Column(Numeric(78, 38), nullable=False)
    pre_depth_notional = Column(Numeric(78, 38), nullable=False)
    consumed_depth_notional = Column(Numeric(78, 38), nullable=False)
    replenished_depth_notional = Column(Numeric(78, 38), nullable=False)
    depth_replenishment = Column(Numeric(78, 38), nullable=False)
    liquidity_adjusted_impact = Column(Numeric(78, 38), nullable=False)
    price_response_per_flow = Column(Numeric(78, 38), nullable=False)
    input_fingerprint = Column(String(64), nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


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


class MarketNormalizedFeatureVersionRecord(Base):
    """Append-only normalized value or explicit causal unavailability revision."""

    __tablename__ = "normalized_feature_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", "effective_at", name="pk_market_normalized_feature_versions"),
        UniqueConstraint(
            "series_id",
            "spec_id",
            "effective_at",
            "revision",
            name="uq_market_normalized_feature_revision",
        ),
        CheckConstraint("revision > 0", name="ck_market_normalized_feature_revision"),
        CheckConstraint("input_count > 0", name="ck_market_normalized_input_count"),
        CheckConstraint("input_watermark > 0", name="ck_market_normalized_watermark"),
        CheckConstraint("input_end >= input_start", name="ck_market_normalized_input_range"),
        CheckConstraint("effective_at >= input_end", name="ck_market_normalized_effective"),
        CheckConstraint("known_at >= effective_at", name="ck_market_normalized_known"),
        CheckConstraint(
            "status IN ('valid', 'insufficient_history', 'invalid_input', 'zero_denominator', 'zero_variance')",
            name="ck_market_normalized_status",
        ),
        CheckConstraint(
            "(status = 'valid' AND value IS NOT NULL) OR (status <> 'valid' AND value IS NULL)",
            name="ck_market_normalized_value_status",
        ),
        Index("ix_market_normalized_series_time", "series_id", "effective_at"),
        Index("ix_market_normalized_series_known", "series_id", "known_at"),
        Index("ix_market_normalized_spec_time", "spec_id", "effective_at"),
        Index("ix_market_normalized_series_commit", "series_id", "market_commit_seq"),
        {"schema": MARKET_DATA_SCHEMA},
    )

    id = Column(String(128), nullable=False)
    series_id = Column(
        BigInteger,
        ForeignKey(f"{MARKET_DATA_SCHEMA}.series.id", ondelete="RESTRICT"),
        nullable=False,
    )
    spec_id = Column(
        String(64),
        ForeignKey(f"{MARKET_DATA_SCHEMA}.normalization_specs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    revision = Column(Integer, nullable=False)
    market_commit_seq = Column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('market.fact_commit_seq'::regclass)"),
    )
    effective_at = Column(DateTime(timezone=True), nullable=False)
    known_at = Column(DateTime(timezone=True), nullable=False)
    value = Column(Numeric(78, 38), nullable=True)
    status = Column(String(32), nullable=False)
    reason = Column(Text, nullable=True)
    input_start = Column(DateTime(timezone=True), nullable=False)
    input_end = Column(DateTime(timezone=True), nullable=False)
    input_count = Column(Integer, nullable=False)
    input_watermark = Column(BigInteger, nullable=False)
    source_series_ids = Column(JSONB, nullable=False)
    source_material_hashes = Column(JSONB, nullable=False)
    input_fingerprint = Column(String(64), nullable=False)
    material_hash = Column(String(64), nullable=False)
    provenance_hash = Column(String(64), nullable=False)
    quality = Column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))


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
    "MarketBboFeatureVersionRecord",
    "MarketDepthFeatureVersionRecord",
    "MarketDerivativeStateVersionRecord",
    "MarketFuturesSpotRelationshipVersionRecord",
    "MarketResponseFeatureVersionRecord",
    "MarketTradeFlowFeatureVersionRecord",
    "MarketCandleVersionRecord",
    "MarketCollectionAttemptRecord",
    "MarketCollectionDefinitionRecord",
    "MarketDataIngestionRunRecord",
    "MarketDataSeriesRecord",
    "MarketDataSourceRecord",
    "MarketDatasetRecord",
    "MarketDatasetSeriesRecord",
    "MarketDatasetNormalizationRefRecord",
    "MarketNormalizationSpecRecord",
    "MarketNormalizedFeatureVersionRecord",
    "MarketGapEvidenceRecord",
    "MarketOpenInterestVersionRecord",
    "MarketProviderRateBudgetRecord",
    "MarketDatasetArchiveRefRecord",
    "MarketArchiveRetentionPinVersionRecord",
    "MarketInstrumentRoleMappingVersionRecord",
    "MarketBookCheckpointManifestRecord",
    "MarketBookQualityEventLinkRecord",
    "MarketBookReconstructionStateRecord",
    "MarketBookValidityIntervalVersionRecord",
    "MarketL2MutationBatchRecord",
    "MarketL2MutationRecord",
    "MarketL2SnapshotLevelRecord",
    "MarketL2SnapshotVersionRecord",
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
    "MarketTradeFlowAggregateVersionRecord",
    "MarketTradeIdentityRecord",
    "MarketTradeVersionRecord",
]
