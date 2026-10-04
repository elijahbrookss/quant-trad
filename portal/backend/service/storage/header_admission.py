"""Shared transaction and observation admission for header storage operations."""
from sqlalchemy import select, text

from portal.backend.db.storage_target_models import StorageTargetRecord
from portal.backend.service.storage_management import StorageConflict
from .header_filesystem import VerifiedHeaderPlacement


def lock_header_storage(session):
    if session.connection().get_isolation_level() != "READ COMMITTED":
        raise StorageConflict("storage_journal_requires_read_committed")
    if not session.scalar(text(
        "SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1', 0))"
    )):
        raise StorageConflict("storage_journal_busy")


def fresh_header_database(session, verified):
    if not isinstance(verified, VerifiedHeaderPlacement):
        raise ValueError("storage_journal_verified_inventory_required")
    now = session.scalar(text("SELECT clock_timestamp()"))
    for stamp in (verified.snapshot.captured_at, verified.verified_at):
        if (stamp.tzinfo is None or stamp.utcoffset() is None
                or not 0 <= (now - stamp).total_seconds() <= 60):
            raise StorageConflict("storage_journal_evidence_expired")
    if verified.verified_at < verified.snapshot.captured_at:
        raise StorageConflict("storage_journal_evidence_clock_mismatch")
    identity = session.scalar(text("""
        SELECT c.system_identifier::text || '/' || d.oid::text
        FROM pg_control_system() c CROSS JOIN pg_database d
        WHERE d.datname=current_database()
    """))
    if identity != verified.snapshot.database_identity:
        raise StorageConflict("storage_journal_database_mismatch")
    return identity


def registered_header_targets(session):
    targets = list(session.scalars(select(StorageTargetRecord).order_by(
        StorageTargetRecord.id).limit(33).execution_options(populate_existing=True)))
    if len(targets) > 32:
        raise StorageConflict("storage_journal_target_budget")
    return targets
