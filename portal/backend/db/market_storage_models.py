"""Storage placement and immutable cold-page catalog for canonical Facts.

Fact identity remains in market.fact_versions. These relations describe where
its large JSON documents live; they never create a new market revision.
"""

from sqlalchemy import (
    BigInteger, CheckConstraint, Column, Date, DateTime, ForeignKey, Index,
    Integer, String, Text, UniqueConstraint, text,
)
from sqlalchemy.dialects.postgresql import JSONB

from .models import Base


class MarketFactStorageStateRecord(Base):
    """Explicit clean-install/cutover certificate; runtime cannot complete a migration."""

    __tablename__ = "fact_storage_state"
    __table_args__ = (
        CheckConstraint("state IN ('copying', 'ready')", name="ck_market_fact_storage_state"),
        CheckConstraint(
            "(state = 'copying' AND completed_at IS NULL) OR "
            "(state = 'ready' AND completed_at IS NOT NULL)",
            name="ck_market_fact_storage_completion",
        ),
        CheckConstraint("jsonb_typeof(evidence) = 'object'", name="ck_market_fact_storage_evidence"),
        {"schema": "market"},
    )
    layout_version = Column(String(64), primary_key=True)
    state = Column(String(16), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    completed_at = Column(DateTime(timezone=True), nullable=True)
    evidence = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))


class MarketFactHotPayloadRecord(Base):
    __tablename__ = "fact_hot_payloads"
    __table_args__ = (
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="ck_market_hot_fact_payload_object"),
        CheckConstraint("jsonb_typeof(provenance) = 'object'", name="ck_market_hot_fact_provenance_object"),
        CheckConstraint("jsonb_typeof(quality) = 'object'", name="ck_market_hot_fact_quality_object"),
        CheckConstraint("market.validate_fact_payload(payload_schema_id, payload)", name="ck_market_hot_fact_payload_valid"),
        {"schema": "market", "postgresql_partition_by": "RANGE (storage_day)"},
    )

    storage_day = Column(Date, primary_key=True)
    id = Column(String(64), ForeignKey("market.fact_versions.id", ondelete="RESTRICT"), primary_key=True)
    series_id = Column(BigInteger, nullable=False)
    payload_schema_id = Column(String(128), nullable=False)
    observation_time = Column(DateTime(timezone=True), nullable=False)
    payload = Column(JSONB, nullable=False)
    provenance = Column(JSONB, nullable=False)
    quality = Column(JSONB, nullable=False)


Index("ix_market_fact_payload_gin", MarketFactHotPayloadRecord.payload,
      postgresql_using="gin", postgresql_ops={"payload": "jsonb_path_ops"})
Index("ix_market_fact_provenance_gin", MarketFactHotPayloadRecord.provenance,
      postgresql_using="gin", postgresql_ops={"provenance": "jsonb_path_ops"})
Index(
    "ix_market_fact_exact_value", MarketFactHotPayloadRecord.series_id,
    text("((payload->>'value')::numeric)"), MarketFactHotPayloadRecord.observation_time,
    postgresql_where=MarketFactHotPayloadRecord.payload_schema_id.in_((
        "derivatives.open_interest.v2", "market.reference_price.v1", "market.reserve_balance.v1",
    )),
)
Index(
    "ix_market_fact_exact_rate", MarketFactHotPayloadRecord.series_id,
    text("((payload->>'rate')::numeric)"), MarketFactHotPayloadRecord.observation_time,
    postgresql_where=MarketFactHotPayloadRecord.payload_schema_id == "derivatives.funding_rate.v2",
)
Index(
    "ix_market_fact_funding_time", MarketFactHotPayloadRecord.series_id,
    text("market.canonical_fact_utc_timestamp(payload->>'funding_time')"),
    MarketFactHotPayloadRecord.observation_time,
    postgresql_where=MarketFactHotPayloadRecord.payload_schema_id.in_((
        "derivatives.funding_rate.v1", "derivatives.funding_rate.v2",
    )),
)


class MarketFactRetentionPartitionRecord(Base):
    """Mutable lifecycle progress for one physical hot-payload partition."""

    __tablename__ = "fact_retention_partitions"
    __table_args__ = (
        CheckConstraint("state IN ('open', 'sealed', 'verified', 'reclaimed')", name="ck_market_fact_partition_state"),
        CheckConstraint("expected_rows IS NULL OR expected_rows >= 0", name="ck_market_fact_partition_rows"),
        CheckConstraint("source_bytes IS NULL OR source_bytes >= 0", name="ck_market_fact_partition_source_bytes"),
        CheckConstraint("reclaimed_bytes IS NULL OR reclaimed_bytes >= 0", name="ck_market_fact_partition_reclaimed_bytes"),
        CheckConstraint(
            "(state = 'open' AND sealed_at IS NULL AND expected_rows IS NULL) OR "
            "(state <> 'open' AND sealed_at IS NOT NULL AND expected_rows IS NOT NULL)",
            name="ck_market_fact_partition_sealed",
        ),
        CheckConstraint(
            "(state IN ('open', 'sealed') AND verified_at IS NULL AND manifest_set_hash IS NULL) OR "
            "(state IN ('verified', 'reclaimed') AND verified_at IS NOT NULL AND "
            "manifest_set_hash IS NOT NULL AND manifest_set_hash ~ '^[0-9a-f]{64}$')",
            name="ck_market_fact_partition_verified",
        ),
        CheckConstraint(
            "(state <> 'reclaimed' AND reclaimed_at IS NULL AND reclaimed_bytes IS NULL) OR "
            "(state = 'reclaimed' AND reclaimed_at IS NOT NULL AND reclaimed_bytes IS NOT NULL)",
            name="ck_market_fact_partition_reclaimed",
        ),
        {"schema": "market"},
    )

    storage_day = Column(Date, primary_key=True)
    state = Column(String(16), nullable=False, server_default="open")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    expected_rows = Column(BigInteger, nullable=True)
    source_bytes = Column(BigInteger, nullable=True)
    sealed_at = Column(DateTime(timezone=True), nullable=True)
    verified_at = Column(DateTime(timezone=True), nullable=True)
    manifest_set_hash = Column(String(64), nullable=True)
    reclaimed_at = Column(DateTime(timezone=True), nullable=True)
    reclaimed_bytes = Column(BigInteger, nullable=True)


class MarketFactArchiveManifestRecord(Base):
    """One independently verified, ordered page in a sealed partition."""

    __tablename__ = "fact_archive_manifests"
    __table_args__ = (
        UniqueConstraint("storage_day", "page_ordinal", name="uq_market_fact_archive_page"),
        UniqueConstraint("object_key", name="uq_market_fact_archive_object_key"),
        CheckConstraint("page_ordinal >= 0", name="ck_market_fact_archive_page_ordinal"),
        CheckConstraint("row_count > 0 AND byte_count > 0", name="ck_market_fact_archive_counts"),
        CheckConstraint("first_commit_seq > 0 AND last_commit_seq > 0", name="ck_market_fact_archive_commits"),
        CheckConstraint("(first_commit_seq, first_id) <= (last_commit_seq, last_id)", name="ck_market_fact_archive_cursors"),
        CheckConstraint("object_sha256 ~ '^[0-9a-f]{64}$' AND manifest_hash ~ '^[0-9a-f]{64}$'", name="ck_market_fact_archive_hashes"),
        CheckConstraint("jsonb_typeof(descriptor) = 'object'", name="ck_market_fact_archive_descriptor"),
        CheckConstraint(
            "(descriptor->>'schema_version' = 'market.canonical_fact_archive.v1' AND "
            "descriptor->>'record_selection' = 'all_canonical_revisions.v1' AND "
            "descriptor->>'object_key' = object_key AND descriptor->>'object_sha256' = object_sha256 AND "
            "(descriptor->>'row_count')::bigint = row_count AND "
            "(descriptor->>'byte_count')::bigint = byte_count AND descriptor->>'manifest_id' = id) IS TRUE",
            name="ck_market_fact_archive_descriptor_binding",
        ),
        Index("ix_market_fact_archive_partition_cursor", "storage_day", "last_commit_seq", "last_id"),
        {"schema": "market"},
    )

    id = Column(String(128), primary_key=True)
    storage_day = Column(Date, ForeignKey("market.fact_retention_partitions.storage_day", ondelete="RESTRICT"), nullable=False)
    page_ordinal = Column(Integer, nullable=False)
    object_key = Column(Text, nullable=False)
    object_sha256 = Column(String(64), nullable=False)
    manifest_hash = Column(String(64), nullable=False)
    row_count = Column(BigInteger, nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    first_commit_seq = Column(BigInteger, nullable=False)
    first_id = Column(String(64), nullable=False)
    last_commit_seq = Column(BigInteger, nullable=False)
    last_id = Column(String(64), nullable=False)
    descriptor = Column(JSONB, nullable=False)
    acknowledged_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))


class MarketFactArchiveSeriesRecord(Base):
    """Bounded lookup index derived from, and checked against, each page descriptor."""

    __tablename__ = "fact_archive_series"
    __table_args__ = (
        CheckConstraint("row_count > 0", name="ck_market_fact_archive_series_count"),
        CheckConstraint("first_observation_at <= last_observation_at", name="ck_market_fact_archive_series_observation"),
        CheckConstraint("first_known_at <= last_known_at", name="ck_market_fact_archive_series_known"),
        CheckConstraint("first_accepted_at <= last_accepted_at", name="ck_market_fact_archive_series_accepted"),
        Index("ix_market_fact_archive_series_window", "series_id", "first_observation_at", "last_observation_at"),
        {"schema": "market"},
    )

    manifest_id = Column(String(128), ForeignKey("market.fact_archive_manifests.id", ondelete="RESTRICT"), primary_key=True)
    series_id = Column(BigInteger, ForeignKey("market.series.id", ondelete="RESTRICT"), primary_key=True)
    row_count = Column(BigInteger, nullable=False)
    first_observation_at = Column(DateTime(timezone=True), nullable=False)
    last_observation_at = Column(DateTime(timezone=True), nullable=False)
    first_known_at = Column(DateTime(timezone=True), nullable=False)
    last_known_at = Column(DateTime(timezone=True), nullable=False)
    first_accepted_at = Column(DateTime(timezone=True), nullable=False)
    last_accepted_at = Column(DateTime(timezone=True), nullable=False)


class MarketFactArchiveDependencyRecord(Base):
    """Immutable raw/checkpoint holds; explicit pin release cannot remove these edges."""

    __tablename__ = "fact_archive_dependencies"
    __table_args__ = (
        CheckConstraint("target_kind IN ('raw_manifest', 'book_checkpoint')", name="ck_market_fact_archive_dependency_kind"),
        CheckConstraint("object_sha256 ~ '^[0-9a-f]{64}$'", name="ck_market_fact_archive_dependency_hash"),
        CheckConstraint("target_id <> '' AND object_key <> ''", name="ck_market_fact_archive_dependency_identity"),
        Index("ix_market_fact_archive_dependency_target", "target_kind", "target_id"),
        {"schema": "market"},
    )

    manifest_id = Column(String(128), ForeignKey("market.fact_archive_manifests.id", ondelete="RESTRICT"), primary_key=True)
    target_kind = Column(String(32), primary_key=True)
    target_id = Column(String(128), primary_key=True)
    object_key = Column(Text, nullable=False)
    object_sha256 = Column(String(64), nullable=False)
