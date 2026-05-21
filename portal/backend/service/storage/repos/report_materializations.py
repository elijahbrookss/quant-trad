"""Report materialization artifact/status persistence."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Mapping, Optional, Tuple

from ._shared import (
    ReportMaterializationRecord,
    SQLAlchemyError,
    _json_safe,
    _utcnow,
    db,
    logger,
    text,
)


REPORT_CONTRACT_VERSION = "run_report_v2"
REPORT_STATUS_NOT_STARTED = "not_started"
REPORT_STATUS_BUILDING = "building"
REPORT_STATUS_READY = "ready"
REPORT_STATUS_FAILED = "failed"
REPORT_STATUS_STALE = "stale"
_REPORT_MATERIALIZATION_LOCK_PERSON = b"qt_report"


def _empty_status(run_id: str, *, contract_version: str = REPORT_CONTRACT_VERSION) -> Dict[str, Any]:
    return {
        "run_id": str(run_id or ""),
        "status": REPORT_STATUS_NOT_STARTED,
        "contract_version": contract_version,
        "artifact_id": None,
        "artifact_path": None,
        "built_at": None,
        "started_at": None,
        "duration_ms": None,
        "error": None,
        "stale_reason": None,
        "cache_key": None,
        "can_view": False,
        "can_build": True,
        "can_retry": False,
    }


def _record_status(record: ReportMaterializationRecord | None, run_id: str, *, contract_version: str) -> Dict[str, Any]:
    if record is None:
        return _empty_status(run_id, contract_version=contract_version)
    return record.to_dict()


def _report_materialization_lock_key(run_id: str) -> int:
    digest = hashlib.blake2b(
        str(run_id or "").strip().encode("utf-8"),
        digest_size=8,
        person=_REPORT_MATERIALIZATION_LOCK_PERSON,
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=False) & ((1 << 63) - 1)


def get_report_materialization_status(
    run_id: str,
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
) -> Dict[str, Any]:
    """Return persisted report materialization state for a run."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return _empty_status(normalized_run_id, contract_version=contract_version)
    if not db.available:
        raise RuntimeError("Database not available for report materialization status")
    with db.session() as session:
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        if record is not None and record.contract_version != contract_version:
            status = record.to_dict()
            status["status"] = REPORT_STATUS_STALE
            status["stale_reason"] = "contract_version_changed"
            status["can_view"] = False
            status["can_build"] = True
            status["can_retry"] = False
            return status
        return _record_status(record, normalized_run_id, contract_version=contract_version)


def get_materialized_run_report(
    run_id: str,
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
) -> Optional[Dict[str, Any]]:
    """Return a ready materialized RunReportDTO payload, if present."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not db.available:
        return None
    with db.session() as session:
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        if record is None:
            return None
        if record.contract_version != contract_version:
            return None
        if record.status != REPORT_STATUS_READY or not isinstance(record.artifact, Mapping):
            return None
        return dict(record.artifact)


def claim_report_materialization_build(
    run_id: str,
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
    cache_key: Optional[str] = None,
    force: bool = False,
) -> Tuple[Dict[str, Any], bool, bool]:
    """Mark a run report build as in progress.

    Returns ``(status, claimed, joined)``. ``joined`` means another builder is
    already responsible for this run, so callers must not start a duplicate.
    """

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report materialization")
    if not db.available:
        raise RuntimeError("Database not available for report materialization")
    with db.session() as session:
        session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": _report_materialization_lock_key(normalized_run_id)},
        )
        now = _utcnow()
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        if record is None:
            record = ReportMaterializationRecord(run_id=normalized_run_id)
            record.created_at = now
            session.add(record)
        elif record.contract_version == contract_version and record.to_dict().get("can_view") and not force:
            return record.to_dict(), False, False
        elif record.status == REPORT_STATUS_BUILDING and record.contract_version == contract_version and not force:
            return record.to_dict(), False, True

        record.contract_version = contract_version
        record.status = REPORT_STATUS_BUILDING
        record.cache_key = cache_key
        record.stale_reason = None
        record.error = None
        record.started_at = now
        record.built_at = None
        record.duration_ms = None
        record.updated_at = now
        if force:
            record.artifact = None
            record.artifact_id = None
        return record.to_dict(), True, False


def store_materialized_run_report(
    run_id: str,
    payload: Mapping[str, Any],
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
    cache_key: Optional[str] = None,
    duration_ms: Optional[float] = None,
) -> Dict[str, Any]:
    """Persist a completed RunReportDTO v2 artifact."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report materialization")
    if not db.available:
        raise RuntimeError("Database not available for report materialization")
    with db.session() as session:
        now = _utcnow()
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        if record is None:
            record = ReportMaterializationRecord(run_id=normalized_run_id)
            record.created_at = now
            session.add(record)
        record.contract_version = contract_version
        record.status = REPORT_STATUS_READY
        record.artifact_id = f"{normalized_run_id}:{contract_version}"
        record.artifact = _json_safe(dict(payload))
        record.cache_key = cache_key
        record.error = None
        record.stale_reason = None
        record.built_at = now
        record.duration_ms = duration_ms
        record.updated_at = now
        return record.to_dict()


def mark_report_materialization_failed(
    run_id: str,
    *,
    error: str,
    contract_version: str = REPORT_CONTRACT_VERSION,
    cache_key: Optional[str] = None,
    duration_ms: Optional[float] = None,
) -> Dict[str, Any]:
    """Persist report materialization failure without changing run lifecycle."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report materialization")
    if not db.available:
        raise RuntimeError("Database not available for report materialization")
    with db.session() as session:
        now = _utcnow()
        record = session.get(ReportMaterializationRecord, normalized_run_id)
        if record is None:
            record = ReportMaterializationRecord(run_id=normalized_run_id)
            record.created_at = now
            session.add(record)
        record.contract_version = contract_version
        record.status = REPORT_STATUS_FAILED
        record.cache_key = cache_key
        record.error = str(error or "")[:2048] or "unknown_report_materialization_failure"
        record.stale_reason = None
        record.built_at = now
        record.duration_ms = duration_ms
        record.updated_at = now
        return record.to_dict()


def reset_report_materialization(
    run_id: str,
    *,
    contract_version: str = REPORT_CONTRACT_VERSION,
) -> Dict[str, Any]:
    """Reset a run report status so it can be built again."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for report materialization")
    if not db.available:
        raise RuntimeError("Database not available for report materialization")
    try:
        with db.session() as session:
            now = _utcnow()
            record = session.get(ReportMaterializationRecord, normalized_run_id)
            if record is None:
                record = ReportMaterializationRecord(run_id=normalized_run_id)
                record.created_at = now
                session.add(record)
            record.contract_version = contract_version
            record.status = REPORT_STATUS_NOT_STARTED
            record.artifact = None
            record.artifact_id = None
            record.cache_key = None
            record.error = None
            record.stale_reason = None
            record.started_at = None
            record.built_at = None
            record.duration_ms = None
            record.updated_at = now
            return record.to_dict()
    except SQLAlchemyError as exc:
        logger.error("report_materialization_reset_failed | run_id=%s | error=%s", normalized_run_id, exc)
        raise


__all__ = [
    "REPORT_CONTRACT_VERSION",
    "REPORT_STATUS_BUILDING",
    "REPORT_STATUS_FAILED",
    "REPORT_STATUS_NOT_STARTED",
    "REPORT_STATUS_READY",
    "REPORT_STATUS_STALE",
    "claim_report_materialization_build",
    "get_materialized_run_report",
    "get_report_materialization_status",
    "mark_report_materialization_failed",
    "reset_report_materialization",
    "store_materialized_run_report",
]
