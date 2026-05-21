"""Per-run ownership leases for runner-agnostic bot execution."""

from __future__ import annotations

import hashlib
import secrets
from datetime import timedelta
from typing import Any, Dict, Mapping, Optional

from ._shared import (
    BotRunLeaseRecord,
    BotRunRecord,
    SQLAlchemyError,
    _json_safe,
    _parse_optional_timestamp,
    _utcnow,
    db,
    logger,
    select,
)

_ACTIVE_STATUS = "active"
_RELEASED_STATUS = "released"
_EXPIRED_STATUS = "expired"
_DEFAULT_TTL_SECONDS = 120.0


class BotRunLeaseConflict(RuntimeError):
    """Raised when a run lease is already owned by another runner/token."""


class BotRunLeaseLost(RuntimeError):
    """Raised when a runner can no longer renew or release its run lease."""


def new_bot_run_lease_token() -> str:
    """Return an opaque lease token suitable for passing to a runner process."""

    return secrets.token_urlsafe(32)


def bot_run_lease_token_hash(lease_token: str) -> str:
    token = str(lease_token or "").strip()
    if not token:
        raise ValueError("lease_token is required")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _ttl_delta(ttl_seconds: float | int | None) -> timedelta:
    ttl = float(ttl_seconds or _DEFAULT_TTL_SECONDS)
    if ttl <= 0:
        ttl = _DEFAULT_TTL_SECONDS
    return timedelta(seconds=ttl)


def _normalise_identity(*, bot_id: str, run_id: str, runner_id: str) -> tuple[str, str, str]:
    normalized_bot_id = str(bot_id or "").strip()
    normalized_run_id = str(run_id or "").strip()
    normalized_runner_id = str(runner_id or "").strip()
    if not normalized_bot_id or not normalized_run_id or not normalized_runner_id:
        raise ValueError("bot_id, run_id, and runner_id are required for run lease persistence")
    return normalized_bot_id, normalized_run_id, normalized_runner_id


def _lease_is_current(record: BotRunLeaseRecord, now: Any) -> bool:
    status = str(record.status or "").strip().lower()
    return status == _ACTIVE_STATUS and record.released_at is None and record.expires_at is not None and record.expires_at > now


def _sanitized_metadata(metadata: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not metadata:
        return {}
    safe = _json_safe(dict(metadata))
    return dict(safe) if isinstance(safe, Mapping) else {}


def _ensure_run_matches_bot(session: Any, *, bot_id: str, run_id: str) -> None:
    run = session.get(BotRunRecord, run_id)
    if run is None:
        raise KeyError(f"Bot run {run_id} was not found")
    if run.bot_id and str(run.bot_id) != str(bot_id):
        raise RuntimeError(f"run {run_id} does not belong to bot {bot_id}")


def acquire_bot_run_lease(
    *,
    bot_id: str,
    run_id: str,
    runner_id: str,
    lease_token: str,
    ttl_seconds: float | int | None = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Acquire the exclusive active lease for a run.

    The raw token is caller-owned and never persisted. Only its hash is stored
    so a runner can prove it owns the lease during renewal/release.
    """

    if not db.available:
        raise RuntimeError("database is required for bot run lease persistence")
    normalized_bot_id, normalized_run_id, normalized_runner_id = _normalise_identity(
        bot_id=bot_id,
        run_id=run_id,
        runner_id=runner_id,
    )
    token_hash = bot_run_lease_token_hash(lease_token)
    now = _utcnow()
    expires_at = now + _ttl_delta(ttl_seconds)
    safe_metadata = _sanitized_metadata(metadata)
    try:
        with db.session() as session:
            _ensure_run_matches_bot(session, bot_id=normalized_bot_id, run_id=normalized_run_id)
            record = (
                session.execute(
                    select(BotRunLeaseRecord)
                    .where(BotRunLeaseRecord.run_id == normalized_run_id)
                    .with_for_update()
                )
                .scalars()
                .first()
            )
            if record is not None and _lease_is_current(record, now) and record.lease_token_hash != token_hash:
                raise BotRunLeaseConflict(
                    "bot_run_lease_conflict: "
                    f"bot_id={normalized_bot_id} run_id={normalized_run_id} "
                    f"owner={record.runner_id} expires_at={record.expires_at.isoformat()}Z"
                )
            if record is None:
                record = BotRunLeaseRecord(
                    run_id=normalized_run_id,
                    bot_id=normalized_bot_id,
                    runner_id=normalized_runner_id,
                    lease_token_hash=token_hash,
                    status=_ACTIVE_STATUS,
                    generation=1,
                    acquired_at=now,
                    renewed_at=now,
                    expires_at=expires_at,
                    lease_metadata=safe_metadata,
                    created_at=now,
                    updated_at=now,
                )
                session.add(record)
            else:
                generation = int(record.generation or 0)
                if record.lease_token_hash != token_hash or str(record.status or "").strip().lower() != _ACTIVE_STATUS:
                    generation += 1
                    record.acquired_at = now
                record.bot_id = normalized_bot_id
                record.runner_id = normalized_runner_id
                record.lease_token_hash = token_hash
                record.status = _ACTIVE_STATUS
                record.generation = max(generation, 1)
                record.renewed_at = now
                record.expires_at = expires_at
                record.released_at = None
                record.lease_metadata = safe_metadata
                record.updated_at = now
            return record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning(
            "bot_run_lease_acquire_failed | bot_id=%s | run_id=%s | runner_id=%s | error=%s",
            normalized_bot_id,
            normalized_run_id,
            normalized_runner_id,
            exc,
        )
        raise


def renew_bot_run_lease(
    *,
    bot_id: str,
    run_id: str,
    runner_id: str,
    lease_token: str,
    ttl_seconds: float | int | None = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    if not db.available:
        raise RuntimeError("database is required for bot run lease persistence")
    normalized_bot_id, normalized_run_id, normalized_runner_id = _normalise_identity(
        bot_id=bot_id,
        run_id=run_id,
        runner_id=runner_id,
    )
    token_hash = bot_run_lease_token_hash(lease_token)
    now = _utcnow()
    expires_at = now + _ttl_delta(ttl_seconds)
    try:
        with db.session() as session:
            record = (
                session.execute(
                    select(BotRunLeaseRecord)
                    .where(BotRunLeaseRecord.run_id == normalized_run_id)
                    .with_for_update()
                )
                .scalars()
                .first()
            )
            if record is None:
                raise BotRunLeaseLost(f"bot_run_lease_missing: bot_id={normalized_bot_id} run_id={normalized_run_id}")
            if str(record.bot_id) != normalized_bot_id:
                raise BotRunLeaseLost(
                    f"bot_run_lease_bot_mismatch: bot_id={normalized_bot_id} run_id={normalized_run_id} lease_bot_id={record.bot_id}"
                )
            if record.lease_token_hash != token_hash:
                raise BotRunLeaseLost(
                    f"bot_run_lease_token_mismatch: bot_id={normalized_bot_id} run_id={normalized_run_id} owner={record.runner_id}"
                )
            if str(record.runner_id) != normalized_runner_id:
                raise BotRunLeaseLost(
                    f"bot_run_lease_runner_mismatch: bot_id={normalized_bot_id} run_id={normalized_run_id} owner={record.runner_id}"
                )
            if str(record.status or "").strip().lower() != _ACTIVE_STATUS or record.released_at is not None:
                raise BotRunLeaseLost(
                    f"bot_run_lease_not_active: bot_id={normalized_bot_id} run_id={normalized_run_id} status={record.status}"
                )
            if record.expires_at is None or record.expires_at <= now:
                record.status = _EXPIRED_STATUS
                record.updated_at = now
                raise BotRunLeaseLost(
                    "bot_run_lease_expired: "
                    f"bot_id={normalized_bot_id} run_id={normalized_run_id} expires_at={record.expires_at.isoformat() if record.expires_at else None}"
                )
            record.renewed_at = now
            record.expires_at = expires_at
            if metadata is not None:
                record.lease_metadata = _sanitized_metadata(metadata)
            record.updated_at = now
            return record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning(
            "bot_run_lease_renew_failed | bot_id=%s | run_id=%s | runner_id=%s | error=%s",
            normalized_bot_id,
            normalized_run_id,
            normalized_runner_id,
            exc,
        )
        raise


def release_bot_run_lease(
    *,
    bot_id: str,
    run_id: str,
    runner_id: str | None = None,
    lease_token: str | None = None,
    status: str = _RELEASED_STATUS,
    metadata: Optional[Mapping[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not db.available:
        raise RuntimeError("database is required for bot run lease persistence")
    normalized_bot_id = str(bot_id or "").strip()
    normalized_run_id = str(run_id or "").strip()
    if not normalized_bot_id or not normalized_run_id:
        raise ValueError("bot_id and run_id are required for run lease release")
    token_hash = bot_run_lease_token_hash(lease_token) if lease_token else None
    normalized_runner_id = str(runner_id or "").strip()
    now = _utcnow()
    release_status = str(status or _RELEASED_STATUS).strip().lower() or _RELEASED_STATUS
    try:
        with db.session() as session:
            record = (
                session.execute(
                    select(BotRunLeaseRecord)
                    .where(BotRunLeaseRecord.run_id == normalized_run_id)
                    .with_for_update()
                )
                .scalars()
                .first()
            )
            if record is None:
                return None
            if str(record.bot_id) != normalized_bot_id:
                raise BotRunLeaseLost(
                    f"bot_run_lease_bot_mismatch: bot_id={normalized_bot_id} run_id={normalized_run_id} lease_bot_id={record.bot_id}"
                )
            if token_hash and record.lease_token_hash != token_hash:
                raise BotRunLeaseLost(
                    f"bot_run_lease_release_token_mismatch: bot_id={normalized_bot_id} run_id={normalized_run_id} owner={record.runner_id}"
                )
            if normalized_runner_id and str(record.runner_id) != normalized_runner_id:
                raise BotRunLeaseLost(
                    f"bot_run_lease_release_runner_mismatch: bot_id={normalized_bot_id} run_id={normalized_run_id} owner={record.runner_id}"
                )
            record.status = release_status
            record.released_at = now
            record.updated_at = now
            if metadata is not None:
                record.lease_metadata = _sanitized_metadata(metadata)
            return record.to_dict()
    except SQLAlchemyError as exc:
        logger.warning(
            "bot_run_lease_release_failed | bot_id=%s | run_id=%s | runner_id=%s | error=%s",
            normalized_bot_id,
            normalized_run_id,
            normalized_runner_id or None,
            exc,
        )
        raise


def get_bot_run_lease(run_id: str) -> Optional[Dict[str, Any]]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not db.available:
        return None
    with db.session() as session:
        record = session.get(BotRunLeaseRecord, normalized_run_id)
        return record.to_dict() if record else None


def run_lease_is_active(lease: Mapping[str, Any] | None, *, now: Any = None) -> bool:
    if not lease:
        return False
    status = str(lease.get("status") or "").strip().lower()
    if status != _ACTIVE_STATUS or lease.get("released_at"):
        return False
    expires_at = _parse_optional_timestamp(lease.get("expires_at"))
    if expires_at is None:
        return False
    return expires_at > (now or _utcnow())


__all__ = [
    "BotRunLeaseConflict",
    "BotRunLeaseLost",
    "acquire_bot_run_lease",
    "bot_run_lease_token_hash",
    "get_bot_run_lease",
    "new_bot_run_lease_token",
    "release_bot_run_lease",
    "renew_bot_run_lease",
    "run_lease_is_active",
]
