"""Durable storage configuration and physical location records.

Policy revisions select destinations for future work. Existing locations remain
explicitly bound to their registered target until a verified movement commits.
"""
from sqlalchemy import BigInteger, CheckConstraint, Column, Date, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB

from .models import Base


class StorageTargetRecord(Base):
    __tablename__ = "portal_storage_targets"
    __table_args__ = (
        UniqueConstraint("filesystem_uuid", name="uq_storage_target_filesystem"),
        CheckConstraint("medium IN ('ssd','hdd')", name="ck_storage_target_medium"),
        CheckConstraint("state IN ('active','draining','disabled')", name="ck_storage_target_state"),
        CheckConstraint("reserved_bytes >= 0", name="ck_storage_target_reserved_bytes"),
    )
    id = Column(String(48), primary_key=True)
    label = Column(String(80), nullable=False)
    filesystem_uuid = Column(String(128), nullable=False)
    root = Column(String(1024), nullable=False)
    medium = Column(String(8), nullable=False)
    roles = Column(JSONB, nullable=False)
    state = Column(String(16), nullable=False, server_default="active")
    reserved_bytes = Column(BigInteger, nullable=False, server_default="0")
    registered_at = Column(DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()"))


class StoragePolicyRecord(Base):
    __tablename__ = "portal_storage_policy"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_storage_policy_singleton"),
        CheckConstraint("revision >= 0", name="ck_storage_policy_revision"),
    )
    id = Column(Integer, primary_key=True)
    revision = Column(BigInteger, nullable=False, server_default="0")
    policy = Column(JSONB, nullable=True)
    applied_plan_id = Column(String(48), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()"))


class StoragePlanRecord(Base):
    __tablename__ = "portal_storage_plans"
    __table_args__ = (
        UniqueConstraint("request_id", name="uq_storage_plan_request"),
        CheckConstraint("base_revision >= 0", name="ck_storage_plan_revision"),
        CheckConstraint("state IN ('planned','queued','running','blocked','completed','cancelled')", name="ck_storage_plan_state"),
        Index("ix_storage_plan_state_created", "state", "created_at"),
    )
    id = Column(String(48), primary_key=True)
    request_id = Column(String(80), nullable=False)
    base_revision = Column(BigInteger, nullable=False)
    policy = Column(JSONB, nullable=False)
    policy_hash = Column(String(64), nullable=False)
    state = Column(String(16), nullable=False, server_default="planned")
    impact = Column(JSONB, nullable=False)
    progress = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()"))


class StorageObjectLocationRecord(Base):
    __tablename__ = "portal_storage_object_locations"
    __table_args__ = (
        UniqueConstraint("object_kind", "object_key", name="uq_storage_object_identity"),
        CheckConstraint("byte_count > 0", name="ck_storage_location_bytes"),
        CheckConstraint("generation > 0", name="ck_storage_location_generation"),
        Index("ix_storage_location_target", "target_id", "id"),
    )
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    object_kind = Column(String(40), nullable=False)
    object_key = Column(String(1024), nullable=False)
    target_id = Column(String(48), ForeignKey("portal_storage_targets.id", ondelete="RESTRICT"), nullable=False)
    sha256 = Column(String(64), nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    generation = Column(BigInteger, nullable=False, server_default="1")
    verified_at = Column(DateTime(timezone=True), nullable=False)


class StorageHeaderBatchRecord(Base):
    """One immutable, bounded header proposal per reviewed storage plan."""
    __tablename__ = "portal_storage_header_batches"
    __table_args__ = (
        CheckConstraint("base_revision >= 0", name="ck_storage_header_batch_revision"),
        CheckConstraint("group_count BETWEEN 0 AND 4096", name="ck_storage_header_batch_count"),
    )
    plan_id = Column(String(48), ForeignKey("portal_storage_plans.id", ondelete="RESTRICT"), primary_key=True)
    review_hash = Column(String(64), nullable=False)
    policy_hash = Column(String(64), nullable=False)
    base_revision = Column(BigInteger, nullable=False)
    database_identity = Column(String(128), nullable=False)
    group_count = Column(Integer, nullable=False)
    captured_at = Column(DateTime(timezone=True), nullable=False)
    verified_at = Column(DateTime(timezone=True), nullable=False)
    cancelled_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()"))


class StorageHeaderMoveRecord(Base):
    """Intent and capacity ownership, not proof that physical movement happened."""
    __tablename__ = "portal_storage_header_moves"
    __table_args__ = (
        UniqueConstraint("plan_id", "storage_day", name="uq_storage_header_move_day"),
        CheckConstraint("heap_oid BETWEEN 1 AND 4294967295", name="ck_storage_header_move_oid"),
        CheckConstraint("reserved_bytes > 0", name="ck_storage_header_move_reservation"),
        CheckConstraint("state IN ('reserved','running','blocked','completed','cancelled')",
                        name="ck_storage_header_move_state"),
        CheckConstraint("jsonb_typeof(source_group) = 'object' AND source_group ? 'indexes' AND "
                        "jsonb_typeof(source_group->'indexes') = 'array' AND "
                        "jsonb_array_length(source_group->'indexes') <= 64 AND "
                        "octet_length(source_group::text) <= 65536",
                        name="ck_storage_header_move_evidence"),
        Index("uq_storage_header_move_active_heap", "database_identity", "heap_oid", unique=True,
              postgresql_where=text("state IN ('reserved','running','blocked')")),
    )
    id = Column(String(48), primary_key=True)
    plan_id = Column(String(48), ForeignKey("portal_storage_header_batches.plan_id", ondelete="RESTRICT"), nullable=False)
    database_identity = Column(String(128), nullable=False)
    storage_day = Column(Date, nullable=False)
    heap_oid = Column(BigInteger, nullable=False)
    target_id = Column(String(48), ForeignKey("portal_storage_targets.id", ondelete="RESTRICT"), nullable=False)
    filesystem_uuid = Column(String(128), nullable=False)
    source_group = Column(JSONB, nullable=False)
    reserved_bytes = Column(BigInteger, nullable=False)
    state = Column(String(16), nullable=False, server_default="reserved")
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()"))
