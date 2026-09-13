"""Durable storage configuration and physical location records.

Policy revisions select destinations for future work. Existing locations remain
explicitly bound to their registered target until a verified movement commits.
"""
from sqlalchemy import BigInteger, CheckConstraint, Column, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint, text
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
