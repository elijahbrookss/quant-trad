from __future__ import annotations

import hashlib
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from sqlalchemy import select

from portal.backend.db import AsyncJobRecord, db


logger = logging.getLogger(__name__)


STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_RETRY = "retry"
TERMINAL_STATUSES = {STATUS_SUCCEEDED, STATUS_FAILED}


@dataclass(frozen=True)
class ClaimedJob:
    id: str
    job_type: str
    payload: Dict[str, Any]
    attempts: int
    max_attempts: int
    partition_key: Optional[str]
    partition_hash: int



def _utcnow() -> datetime:
    return datetime.utcnow()



def _partition_hash(partition_key: Optional[str]) -> int:
    if not partition_key:
        return 0
    digest = hashlib.md5(partition_key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)



def enqueue_job(
    *,
    job_type: str,
    payload: Mapping[str, Any],
    partition_key: Optional[str] = None,
    max_attempts: int = 3,
    available_at: Optional[datetime] = None,
) -> str:
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    now = _utcnow()
    job_id = str(uuid.uuid4())
    record = AsyncJobRecord(
        id=job_id,
        job_type=str(job_type),
        status=STATUS_QUEUED,
        payload=dict(payload or {}),
        partition_key=partition_key,
        partition_hash=_partition_hash(partition_key),
        attempts=0,
        max_attempts=max(1, int(max_attempts)),
        available_at=available_at or now,
        created_at=now,
        updated_at=now,
    )
    with db.session() as session:
        session.add(record)
    logger.info(
        "async_job_enqueued | job_id=%s job_type=%s partition_key=%s partition_hash=%s max_attempts=%s",
        job_id,
        job_type,
        partition_key,
        record.partition_hash,
        record.max_attempts,
    )
    return job_id



def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    if not db.available:
        return None
    with db.session() as session:
        record = session.get(AsyncJobRecord, job_id)
        if record is None:
            return None
        return record.to_dict()



def claim_next_job(
    *,
    worker_id: str,
    job_types: Sequence[str],
    partition_index: int = 0,
    partition_total: int = 1,
) -> Optional[ClaimedJob]:
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    wanted = [str(j).strip() for j in job_types if str(j).strip()]
    if not wanted:
        return None

    now = _utcnow()
    with db.session() as session:
        stmt = (
            select(AsyncJobRecord)
            .where(AsyncJobRecord.status.in_([STATUS_QUEUED, STATUS_RETRY]))
            .where(AsyncJobRecord.job_type.in_(wanted))
            .where(AsyncJobRecord.available_at <= now)
            .order_by(AsyncJobRecord.created_at.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if int(partition_total) > 1:
            stmt = stmt.where((AsyncJobRecord.partition_hash % int(partition_total)) == int(partition_index))

        record = session.execute(stmt).scalars().first()
        if record is None:
            return None

        record.status = STATUS_RUNNING
        record.lock_owner = worker_id
        record.locked_at = now
        record.started_at = record.started_at or now
        record.updated_at = now
        record.attempts = int(record.attempts or 0) + 1

        payload = dict(record.payload or {})
        return ClaimedJob(
            id=str(record.id),
            job_type=str(record.job_type),
            payload=payload,
            attempts=int(record.attempts or 0),
            max_attempts=int(record.max_attempts or 0),
            partition_key=record.partition_key,
            partition_hash=int(record.partition_hash or 0),
        )



def complete_job(job_id: str, result: Mapping[str, Any]) -> None:
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    now = _utcnow()
    with db.session() as session:
        record = session.get(AsyncJobRecord, job_id)
        if record is None:
            raise KeyError(f"async_job_not_found: {job_id}")
        record.status = STATUS_SUCCEEDED
        record.result = dict(result or {})
        record.error = None
        record.finished_at = now
        record.updated_at = now
        record.lock_owner = None
        record.locked_at = None
    logger.info("async_job_succeeded | job_id=%s", job_id)



def fail_job(
    job_id: str,
    *,
    error: str,
    retry_delay_seconds: float = 0.0,
) -> None:
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    now = _utcnow()
    with db.session() as session:
        record = session.get(AsyncJobRecord, job_id)
        if record is None:
            raise KeyError(f"async_job_not_found: {job_id}")
        attempts = int(record.attempts or 0)
        max_attempts = int(record.max_attempts or 0)
        exhausted = attempts >= max_attempts
        if exhausted:
            record.status = STATUS_FAILED
            record.finished_at = now
        else:
            record.status = STATUS_RETRY
            delay = max(0.0, float(retry_delay_seconds or 0.0))
            record.available_at = now + timedelta(seconds=delay)
        record.error = str(error)
        record.updated_at = now
        record.lock_owner = None
        record.locked_at = None
    logger.warning(
        "async_job_failed | job_id=%s exhausted=%s error=%s",
        job_id,
        exhausted,
        error,
    )



def wait_for_job_result(job_id: str, *, timeout_seconds: float = 120.0, poll_interval_seconds: float = 0.2) -> Dict[str, Any]:
    deadline = time.monotonic() + max(0.1, float(timeout_seconds))
    while time.monotonic() < deadline:
        job = get_job(job_id)
        if job is None:
            raise KeyError(f"async_job_not_found: {job_id}")
        status = str(job.get("status") or "")
        if status in TERMINAL_STATUSES:
            return job
        time.sleep(max(0.05, float(poll_interval_seconds)))
    raise TimeoutError(f"async_job_timeout: {job_id}")


def wait_for_database_ready(*, timeout_seconds: float = 60.0, poll_interval_seconds: float = 0.5) -> bool:
    """Block until the shared DB handle reports ready, or timeout."""
    deadline = time.monotonic() + max(0.1, float(timeout_seconds))
    while time.monotonic() < deadline:
        if db.available:
            return True
        time.sleep(max(0.05, float(poll_interval_seconds)))
    return False


__all__ = [
    "ClaimedJob",
    "STATUS_FAILED",
    "STATUS_QUEUED",
    "STATUS_RETRY",
    "STATUS_RUNNING",
    "STATUS_SUCCEEDED",
    "TERMINAL_STATUSES",
    "claim_next_job",
    "complete_job",
    "enqueue_job",
    "fail_job",
    "get_job",
    "wait_for_job_result",
    "wait_for_database_ready",
]
