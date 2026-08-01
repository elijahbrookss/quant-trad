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


__all__ = [
    "MARKET_DATA_SCHEMA",
    "MarketCandleVersionRecord",
    "MarketCollectionAttemptRecord",
    "MarketCollectionDefinitionRecord",
    "MarketDataIngestionRunRecord",
    "MarketDataSeriesRecord",
    "MarketDataSourceRecord",
    "MarketDatasetRecord",
    "MarketDatasetSeriesRecord",
    "MarketGapEvidenceRecord",
    "MarketOpenInterestVersionRecord",
    "MarketProviderRateBudgetRecord",
]
