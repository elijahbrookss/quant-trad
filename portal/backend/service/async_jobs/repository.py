from __future__ import annotations

import hashlib
import logging
import secrets
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from core.settings import get_settings
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from portal.backend.db import AsyncJobRecord, db


logger = logging.getLogger(__name__)
_ASYNC_SETTINGS = get_settings().async_jobs


STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_RETRY = "retry"
TERMINAL_STATUSES = {STATUS_SUCCEEDED, STATUS_FAILED}
INFLIGHT_STATUSES = {STATUS_QUEUED, STATUS_RUNNING, STATUS_RETRY}
DEFAULT_RUNNING_TIMEOUT_SECONDS = float(_ASYNC_SETTINGS.running_timeout_seconds)
_IDEMPOTENCY_CONFLICT_RETRY_LIMIT = 3
_RECLAIM_LAST_MONOTONIC_BY_JOB_TYPES: Dict[tuple[str, ...], float] = {}


@dataclass(frozen=True)
class ClaimedJob:
    id: str
    job_type: str
    payload: Dict[str, Any]
    attempts: int
    max_attempts: int
    partition_key: Optional[str]
    partition_hash: int
    lock_owner: str
    claim_token: str
    claim_generation: int


@dataclass(frozen=True)
class EnqueuedJob:
    id: str
    status: str
    reused: bool


class AsyncJobOwnershipError(RuntimeError):
    """Raised when a worker no longer owns the claim it is trying to mutate."""


class ClaimHeartbeat:
    """Renew a claimed job lease while synchronous worker code is running."""

    def __init__(
        self,
        job: ClaimedJob,
        *,
        interval_seconds: Optional[float] = None,
    ) -> None:
        self._job = job
        timeout_seconds = _running_timeout_seconds()
        if interval_seconds is None:
            interval_seconds = (
                0.0
                if timeout_seconds <= 0
                else max(0.05, min(30.0, timeout_seconds / 3.0))
            )
        self._interval_seconds = max(0.0, float(interval_seconds))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._error: Optional[BaseException] = None

    def __enter__(self) -> "ClaimHeartbeat":
        if self._interval_seconds <= 0:
            return self
        self._thread = threading.Thread(
            target=self._run,
            name=f"async-job-heartbeat-{self._job.id}",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(
                timeout=max(1.0, min(5.0, self._interval_seconds * 2.0))
            )
            if self._thread.is_alive() and self._error is None:
                self._error = RuntimeError(
                    f"async_job_heartbeat_shutdown_timeout: {self._job.id}"
                )
        if self._error is not None:
            if exc is not None:
                logger.warning(
                    "async_job_heartbeat_failed_during_handler_error | "
                    "job_id=%s owner=%s generation=%s heartbeat_error=%s",
                    self._job.id,
                    self._job.lock_owner,
                    self._job.claim_generation,
                    self._error,
                )
                return False
            raise self._error
        return False

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                heartbeat_job(self._job)
            except Exception as exc:  # noqa: BLE001 - cross-thread handoff
                self._error = exc
                self._stop.set()
                return



def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _database_now(session) -> datetime:
    value = session.execute(select(func.now())).scalar_one()
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value



def _partition_hash(partition_key: Optional[str]) -> int:
    if not partition_key:
        return 0
    # Use a stable signed 32-bit hash so values fit SQL INTEGER range.
    digest = hashlib.md5(partition_key.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], byteorder="big", signed=True)


def _claim_token_hash(claim_token: str) -> str:
    normalized = str(claim_token or "").strip()
    if not normalized:
        raise ValueError("async job claim token is required")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _request_fingerprint(
    payload: Mapping[str, Any],
    explicit: Optional[str],
) -> Optional[str]:
    value = (
        str(explicit).strip()
        if explicit is not None
        else str(payload.get("request_fingerprint") or "").strip()
    )
    if not value:
        return None
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value.lower()):
        raise ValueError(
            "async job request_fingerprint must be a 64-character hex digest"
        )
    return value.lower()


def _partition_slot(partition_hash: int, partition_total: int) -> int:
    total = max(1, int(partition_total))
    value = int(partition_hash or 0)
    return ((value % total) + total) % total


def _running_timeout_seconds() -> float:
    return max(0.0, float(_ASYNC_SETTINGS.running_timeout_seconds))


def _reclaim_interval_seconds() -> float:
    return max(0.0, float(_ASYNC_SETTINGS.reclaim_interval_seconds))


def _should_reclaim_stale_running_jobs(job_types: Sequence[str], *, now_monotonic: float) -> bool:
    interval = _reclaim_interval_seconds()
    if interval <= 0:
        return True
    key = tuple(sorted(set(str(job_type) for job_type in job_types)))
    last = _RECLAIM_LAST_MONOTONIC_BY_JOB_TYPES.get(key)
    if last is not None and now_monotonic - last < interval:
        return False
    _RECLAIM_LAST_MONOTONIC_BY_JOB_TYPES[key] = now_monotonic
    return True


def _reclaim_stale_running_jobs(
    *,
    session,
    job_types: Sequence[str],
    now: datetime,
) -> int:
    timeout_seconds = _running_timeout_seconds()
    if timeout_seconds <= 0:
        return 0
    stale_before = now - timedelta(seconds=timeout_seconds)
    common_filters = (
        AsyncJobRecord.status == STATUS_RUNNING,
        AsyncJobRecord.job_type.in_(list(job_types)),
        or_(
            AsyncJobRecord.heartbeat_at.is_(None),
            AsyncJobRecord.heartbeat_at < stale_before,
        ),
    )
    retry_stmt = (
        update(AsyncJobRecord)
        .where(*common_filters)
        .where(AsyncJobRecord.attempts < AsyncJobRecord.max_attempts)
        .values(
            status=STATUS_RETRY,
            available_at=now,
            updated_at=now,
            lock_owner=None,
            locked_at=None,
            heartbeat_at=None,
            claim_token_hash=None,
            claim_generation=AsyncJobRecord.claim_generation + 1,
            error=f"async_job_reclaimed_timeout: running>{int(timeout_seconds)}s",
        )
    )
    exhausted_stmt = (
        update(AsyncJobRecord)
        .where(*common_filters)
        .where(AsyncJobRecord.attempts >= AsyncJobRecord.max_attempts)
        .values(
            status=STATUS_FAILED,
            finished_at=now,
            updated_at=now,
            lock_owner=None,
            locked_at=None,
            heartbeat_at=None,
            claim_token_hash=None,
            claim_generation=AsyncJobRecord.claim_generation + 1,
            error=(
                "async_job_reclaimed_exhausted: "
                f"running>{int(timeout_seconds)}s"
            ),
        )
    )
    retried = int(session.execute(retry_stmt).rowcount or 0)
    exhausted = int(session.execute(exhausted_stmt).rowcount or 0)
    reclaimed = retried + exhausted
    if reclaimed > 0:
        logger.warning(
            "async_job_reclaimed_stale_running | jobs=%s retried=%s "
            "exhausted=%s timeout_seconds=%s stale_before=%s job_types=%s",
            reclaimed,
            retried,
            exhausted,
            int(timeout_seconds),
            stale_before.isoformat() + "Z",
            ",".join(sorted(set(str(j) for j in job_types))),
        )
    return reclaimed


def _fail_exhausted_claimable_jobs(
    *,
    session,
    job_types: Sequence[str],
    now: datetime,
) -> int:
    stmt = (
        update(AsyncJobRecord)
        .where(AsyncJobRecord.status.in_([STATUS_QUEUED, STATUS_RETRY]))
        .where(AsyncJobRecord.job_type.in_(list(job_types)))
        .where(AsyncJobRecord.attempts >= AsyncJobRecord.max_attempts)
        .values(
            status=STATUS_FAILED,
            finished_at=now,
            updated_at=now,
            lock_owner=None,
            locked_at=None,
            heartbeat_at=None,
            claim_token_hash=None,
            error="async_job_attempt_budget_exhausted_before_claim",
        )
    )
    return int(session.execute(stmt).rowcount or 0)


def _json_safe(value: Any) -> Any:
    """Convert nested payload values into JSON-serialisable structures."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception as exc:
            logger.warning(
                "async_job_json_safe_isoformat_failed | value_type=%s error=%s",
                type(value).__name__,
                exc,
            )
    if hasattr(value, "item"):
        try:
            # numpy/pandas scalars often unwrap to datetime/date objects;
            # recurse so nested values are normalised too.
            return _json_safe(value.item())
        except Exception as exc:
            logger.warning(
                "async_job_json_safe_item_unwrap_failed | value_type=%s error=%s",
                type(value).__name__,
                exc,
            )
    return str(value)


def _prune_finished_job_results(
    *,
    session,
    job_type: str,
    before: datetime,
) -> int:
    stmt = (
        update(AsyncJobRecord)
        .where(AsyncJobRecord.job_type == str(job_type))
        .where(AsyncJobRecord.status == STATUS_SUCCEEDED)
        .where(AsyncJobRecord.finished_at.is_not(None))
        .where(AsyncJobRecord.finished_at < before)
        .values(
            result={
                "schema_version": "async_job_result_summary.v1",
                "result_retained": False,
                "pruned_reason": "result_cache_ttl_expired",
            },
            updated_at=_utcnow(),
        )
    )
    outcome = session.execute(stmt)
    return int(outcome.rowcount or 0)



def enqueue_job(
    *,
    job_type: str,
    payload: Mapping[str, Any],
    partition_key: Optional[str] = None,
    max_attempts: int = 3,
    available_at: Optional[datetime] = None,
    request_fingerprint: Optional[str] = None,
) -> str:
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    now = _utcnow()
    job_id = str(uuid.uuid4())
    normalized_payload = dict(payload or {})
    normalized_fingerprint = _request_fingerprint(
        normalized_payload,
        request_fingerprint,
    )
    record = AsyncJobRecord(
        id=job_id,
        job_type=str(job_type),
        status=STATUS_QUEUED,
        payload=normalized_payload,
        partition_key=partition_key,
        partition_hash=_partition_hash(partition_key),
        request_fingerprint=normalized_fingerprint,
        attempts=0,
        max_attempts=max(1, int(max_attempts)),
        available_at=available_at or now,
        created_at=now,
        updated_at=now,
    )
    with db.session() as session:
        if record.job_type.startswith("quantlab_"):
            _prune_finished_job_results(
                session=session,
                job_type=record.job_type,
                before=now - timedelta(seconds=max(0.0, float(_ASYNC_SETTINGS.quantlab_result_cache_ttl_seconds))),
            )
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


def _find_inflight_idempotency_job(
    *,
    session,
    job_type: str,
    partition_key: str,
    request_fingerprint: str,
) -> Optional[AsyncJobRecord]:
    return (
        session.execute(
            select(AsyncJobRecord)
            .where(AsyncJobRecord.job_type == job_type)
            .where(AsyncJobRecord.partition_key == partition_key)
            .where(
                AsyncJobRecord.request_fingerprint
                == request_fingerprint
            )
            .where(AsyncJobRecord.status.in_(INFLIGHT_STATUSES))
            .order_by(AsyncJobRecord.created_at.asc())
            .limit(1)
        )
        .scalars()
        .one_or_none()
    )


def enqueue_or_reuse_job(
    *,
    job_type: str,
    payload: Mapping[str, Any],
    partition_key: str,
    request_fingerprint: str,
    max_attempts: int = 3,
    available_at: Optional[datetime] = None,
) -> EnqueuedJob:
    """Atomically enqueue one in-flight job for an idempotency identity."""

    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    normalized_job_type = str(job_type or "").strip()
    normalized_partition_key = str(partition_key or "").strip()
    if not normalized_job_type:
        raise ValueError("async job job_type is required")
    if not normalized_partition_key:
        raise ValueError(
            "async job partition_key is required for idempotent enqueue"
        )
    normalized_payload = dict(payload or {})
    normalized_fingerprint = _request_fingerprint(
        normalized_payload,
        request_fingerprint,
    )
    assert normalized_fingerprint is not None
    now = _utcnow()
    job_id = str(uuid.uuid4())
    values = {
        "id": job_id,
        "job_type": normalized_job_type,
        "status": STATUS_QUEUED,
        "payload": normalized_payload,
        "partition_key": normalized_partition_key,
        "partition_hash": _partition_hash(normalized_partition_key),
        "request_fingerprint": normalized_fingerprint,
        "attempts": 0,
        "max_attempts": max(1, int(max_attempts)),
        "available_at": available_at or now,
        "created_at": now,
        "updated_at": now,
    }

    with db.session() as session:
        if normalized_job_type.startswith("quantlab_"):
            _prune_finished_job_results(
                session=session,
                job_type=normalized_job_type,
                before=now
                - timedelta(
                    seconds=max(
                        0.0,
                        float(
                            _ASYNC_SETTINGS.quantlab_result_cache_ttl_seconds
                        ),
                    )
                ),
            )
        for _attempt in range(_IDEMPOTENCY_CONFLICT_RETRY_LIMIT):
            statement = (
                pg_insert(AsyncJobRecord)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=[
                        AsyncJobRecord.job_type,
                        AsyncJobRecord.partition_key,
                        AsyncJobRecord.request_fingerprint,
                    ],
                    index_where=text(
                        "status IN ('queued', 'running', 'retry') "
                        "AND request_fingerprint IS NOT NULL"
                    ),
                )
                .returning(AsyncJobRecord.id)
            )
            inserted = session.execute(statement).scalar_one_or_none()
            if inserted is not None:
                return EnqueuedJob(
                    id=str(inserted),
                    status=STATUS_QUEUED,
                    reused=False,
                )

            existing = _find_inflight_idempotency_job(
                session=session,
                job_type=normalized_job_type,
                partition_key=normalized_partition_key,
                request_fingerprint=normalized_fingerprint,
            )
            if existing is not None:
                return EnqueuedJob(
                    id=str(existing.id),
                    status=str(existing.status),
                    reused=True,
                )

        raise RuntimeError(
            "async_job_idempotency_conflict_unstable: "
            f"job_type={normalized_job_type} "
            f"partition_key={normalized_partition_key} "
            f"attempts={_IDEMPOTENCY_CONFLICT_RETRY_LIMIT}"
        )



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
    normalized_worker_id = str(worker_id or "").strip()
    if not normalized_worker_id:
        raise ValueError("async job worker_id is required")
    if len(normalized_worker_id) > 128:
        raise ValueError("async job worker_id must be at most 128 characters")
    normalized_partition_total = int(partition_total)
    normalized_partition_index = int(partition_index)
    if normalized_partition_total < 1:
        raise ValueError("async job partition_total must be positive")
    if not 0 <= normalized_partition_index < normalized_partition_total:
        raise ValueError(
            "async job partition_index must be within "
            "[0, partition_total)"
        )
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    wanted = [str(j).strip() for j in job_types if str(j).strip()]
    if not wanted:
        return None

    with db.session() as session:
        now = _database_now(session)
        if _should_reclaim_stale_running_jobs(wanted, now_monotonic=time.monotonic()):
            _reclaim_stale_running_jobs(session=session, job_types=wanted, now=now)
        _fail_exhausted_claimable_jobs(
            session=session,
            job_types=wanted,
            now=now,
        )
        stmt = (
            select(AsyncJobRecord)
            .where(AsyncJobRecord.status.in_([STATUS_QUEUED, STATUS_RETRY]))
            .where(AsyncJobRecord.job_type.in_(wanted))
            .where(AsyncJobRecord.attempts < AsyncJobRecord.max_attempts)
            .where(AsyncJobRecord.available_at <= now)
            .order_by(AsyncJobRecord.created_at.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if normalized_partition_total > 1:
            total = normalized_partition_total
            normalized_slot = ((AsyncJobRecord.partition_hash % total) + total) % total
            stmt = stmt.where(normalized_slot == normalized_partition_index)

        record = session.execute(stmt).scalars().first()
        if record is None:
            return None

        record.status = STATUS_RUNNING
        record.lock_owner = normalized_worker_id
        record.locked_at = now
        record.heartbeat_at = now
        claim_token = secrets.token_urlsafe(32)
        record.claim_token_hash = _claim_token_hash(claim_token)
        record.claim_generation = int(record.claim_generation or 0) + 1
        record.started_at = record.started_at or now
        record.finished_at = None
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
            lock_owner=str(record.lock_owner),
            claim_token=claim_token,
            claim_generation=int(record.claim_generation),
        )


def find_reusable_job(
    *,
    job_type: str,
    partition_key: Optional[str],
    request_fingerprint: str,
    result_ttl_seconds: float,
    limit: int = 32,
) -> Optional[Dict[str, Any]]:
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    ttl_seconds = max(0.0, float(result_ttl_seconds))
    wanted_type = str(job_type or "").strip()
    wanted_fingerprint = str(request_fingerprint or "").strip()
    if not wanted_type:
        raise ValueError("job_type is required")
    if not wanted_fingerprint:
        raise ValueError("request_fingerprint is required")

    now = _utcnow()
    with db.session() as session:
        if ttl_seconds >= 0:
            _prune_finished_job_results(
                session=session,
                job_type=wanted_type,
                before=now - timedelta(seconds=ttl_seconds),
            )
        stmt = (
            select(AsyncJobRecord)
            .where(AsyncJobRecord.job_type == wanted_type)
            .where(AsyncJobRecord.partition_key == partition_key)
            .where(
                AsyncJobRecord.request_fingerprint == wanted_fingerprint
            )
            .order_by(AsyncJobRecord.created_at.desc())
            .limit(max(1, int(limit)))
        )
        records = list(session.execute(stmt).scalars().all())

    for record in records:
        status = str(record.status or "").strip()
        if status in {STATUS_QUEUED, STATUS_RUNNING, STATUS_RETRY}:
            logger.info(
                "async_job_reused_inflight | job_id=%s job_type=%s partition_key=%s status=%s",
                record.id,
                wanted_type,
                partition_key,
                status,
            )
            return {
                "id": str(record.id),
                "status": status,
                "result": None,
            }
        if status == STATUS_SUCCEEDED:
            logger.info(
                "async_job_reuse_skipped_succeeded_result | job_id=%s job_type=%s partition_key=%s",
                record.id,
                wanted_type,
                partition_key,
            )
            continue
    return None


def _require_current_claim(*, session, job: ClaimedJob) -> AsyncJobRecord:
    record = (
        session.execute(
            select(AsyncJobRecord)
            .where(AsyncJobRecord.id == str(job.id))
            .where(AsyncJobRecord.status == STATUS_RUNNING)
            .where(AsyncJobRecord.lock_owner == str(job.lock_owner))
            .where(
                AsyncJobRecord.claim_token_hash
                == _claim_token_hash(job.claim_token)
            )
            .where(
                AsyncJobRecord.claim_generation == int(job.claim_generation)
            )
            .with_for_update()
        )
        .scalars()
        .one_or_none()
    )
    if record is None:
        raise AsyncJobOwnershipError(
            "async_job_claim_not_current: "
            f"job_id={job.id} owner={job.lock_owner} "
            f"generation={job.claim_generation}"
        )
    return record


def heartbeat_job(job: ClaimedJob) -> None:
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    with db.session() as session:
        record = _require_current_claim(session=session, job=job)
        now = _database_now(session)
        record.heartbeat_at = now
        record.updated_at = now
    logger.debug(
        "async_job_heartbeat | job_id=%s owner=%s generation=%s",
        job.id,
        job.lock_owner,
        job.claim_generation,
    )


def maintain_job_heartbeat(
    job: ClaimedJob,
    *,
    interval_seconds: Optional[float] = None,
) -> ClaimHeartbeat:
    return ClaimHeartbeat(job, interval_seconds=interval_seconds)


def _mark_job_succeeded(
    record: AsyncJobRecord,
    *,
    result: Mapping[str, Any],
    now: datetime,
) -> None:
    record.status = STATUS_SUCCEEDED
    record.result = _json_safe(dict(result or {}))
    record.error = None
    record.finished_at = now
    record.updated_at = now
    record.lock_owner = None
    record.locked_at = None
    record.heartbeat_at = None
    record.claim_token_hash = None


def complete_job_with_owned_effect(
    job: ClaimedJob,
    effect: Callable[[Any], Mapping[str, Any]],
) -> Dict[str, Any]:
    """Commit a job-owned database effect and terminal result atomically."""

    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    with db.session() as session:
        record = _require_current_claim(session=session, job=job)
        result = dict(effect(session) or {})
        now = _database_now(session)
        _mark_job_succeeded(record, result=result, now=now)
    logger.info(
        "async_job_succeeded | job_id=%s owner=%s generation=%s",
        job.id,
        job.lock_owner,
        job.claim_generation,
    )
    return result


def complete_job(job: ClaimedJob, result: Mapping[str, Any]) -> None:
    complete_job_with_owned_effect(job, lambda _session: dict(result or {}))



def fail_job(
    job: ClaimedJob,
    *,
    error: str,
    retry_delay_seconds: float = 0.0,
) -> None:
    if not db.available:
        raise RuntimeError("async_jobs_unavailable: database unavailable")
    with db.session() as session:
        record = _require_current_claim(session=session, job=job)
        now = _database_now(session)
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
        record.heartbeat_at = None
        record.claim_token_hash = None
    logger.warning(
        "async_job_failed | job_id=%s owner=%s generation=%s exhausted=%s error=%s",
        job.id,
        job.lock_owner,
        job.claim_generation,
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
    "AsyncJobOwnershipError",
    "ClaimHeartbeat",
    "ClaimedJob",
    "EnqueuedJob",
    "INFLIGHT_STATUSES",
    "STATUS_FAILED",
    "STATUS_QUEUED",
    "STATUS_RETRY",
    "STATUS_RUNNING",
    "STATUS_SUCCEEDED",
    "TERMINAL_STATUSES",
    "claim_next_job",
    "complete_job",
    "complete_job_with_owned_effect",
    "enqueue_job",
    "enqueue_or_reuse_job",
    "fail_job",
    "get_job",
    "heartbeat_job",
    "maintain_job_heartbeat",
    "wait_for_job_result",
    "wait_for_database_ready",
]
