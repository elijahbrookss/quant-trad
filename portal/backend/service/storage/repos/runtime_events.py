"""Storage repository module."""

from __future__ import annotations

import os
import time
from typing import Callable, TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError

from ._shared import *

_T = TypeVar("_T")
_DB_WRITE_RETRY_ATTEMPTS = max(1, _coerce_int(os.getenv("PORTAL_DB_WRITE_RETRY_ATTEMPTS")) or 2)


def _is_transient_connection_error(exc: Exception) -> bool:
    if isinstance(exc, DBAPIError) and bool(getattr(exc, "connection_invalidated", False)):
        return True
    message = str(exc).lower()
    transient_markers = (
        "server closed the connection unexpectedly",
        "connection reset by peer",
        "connection refused",
        "could not connect to server",
        "terminating connection due to administrator command",
        "closed the connection",
    )
    return any(marker in message for marker in transient_markers)


def _execute_write_with_retry(
    *,
    operation: str,
    context: Dict[str, Any],
    action: Callable[[], _T],
) -> _T:
    for attempt in range(1, _DB_WRITE_RETRY_ATTEMPTS + 1):
        try:
            return action()
        except (OperationalError, DBAPIError, SQLAlchemyError) as exc:
            retryable = attempt < _DB_WRITE_RETRY_ATTEMPTS and _is_transient_connection_error(exc)
            logger.warning(
                "portal_db_write_error | operation=%s | attempt=%s/%s | retry=%s | run_id=%s | bot_id=%s | error=%s",
                operation,
                attempt,
                _DB_WRITE_RETRY_ATTEMPTS,
                retryable,
                context.get("run_id"),
                context.get("bot_id"),
                exc,
            )
            if not retryable:
                raise
            db.reset_connection_state()
            time.sleep(min(0.05 * attempt, 0.25))


def record_bot_run_step(payload: Dict[str, Any]) -> None:
    """Persist a timed bot runtime step for profiler dashboards."""

    if not db.available:
        return
    run_id = str(payload.get("run_id") or "").strip()
    step_name = str(payload.get("step_name") or "").strip()
    if not run_id or not step_name:
        return
    started_at = _parse_optional_timestamp(payload.get("started_at"))
    ended_at = _parse_optional_timestamp(payload.get("ended_at"))
    duration_ms = _coerce_float(payload.get("duration_ms"))
    if started_at is None or ended_at is None or duration_ms is None:
        return
    try:
        with db.session() as session:
            now = _utcnow()
            record = BotRunStepRecord(
                run_id=run_id,
                bot_id=str(payload.get("bot_id") or "") or None,
                step_name=step_name,
                started_at=started_at,
                ended_at=ended_at,
                duration_ms=float(duration_ms),
                ok=bool(payload.get("ok", True)),
                strategy_id=str(payload.get("strategy_id") or "") or None,
                symbol=str(payload.get("symbol") or "") or None,
                timeframe=str(payload.get("timeframe") or "") or None,
                error=(str(payload.get("error"))[:1024] if payload.get("error") else None),
                context=_json_safe(dict(payload.get("context") or {})),
                created_at=now,
            )
            session.add(record)
    except SQLAlchemyError as exc:
        logger.warning("bot_run_step_persist_failed | run_id=%s | step=%s | error=%s", run_id, step_name, exc)


def record_bot_run_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not db.available:
        raise RuntimeError("database is required for bot snapshots")
    run_id = str(payload.get("run_id") or "").strip()
    bot_id = str(payload.get("bot_id") or "").strip()
    series_key = str(payload.get("series_key") or "").strip()
    snapshot_seq = int(payload.get("snapshot_seq") or 0)
    if not run_id or not bot_id or not series_key:
        raise ValueError("run_id, bot_id and series_key are required for bot snapshot persistence")
    if snapshot_seq <= 0:
        raise ValueError("snapshot_seq must be a positive integer")

    raw_snapshot_payload = payload.get("snapshot_payload")
    if not isinstance(raw_snapshot_payload, dict):
        raise ValueError("snapshot_payload must be a mapping")
    snapshot_payload = _json_safe(dict(raw_snapshot_payload))
    raw_snapshot_schema_version = snapshot_payload.get("schema_version")
    schema_version = int(raw_snapshot_schema_version) if raw_snapshot_schema_version is not None else 1
    if schema_version <= 0:
        raise ValueError("snapshot_payload.schema_version must be >= 1")
    snapshot_payload["schema_version"] = schema_version
    snapshot_payload.setdefault("run_id", run_id)
    snapshot_payload.setdefault("series_key", series_key)
    snapshot_payload.setdefault("snapshot_seq", snapshot_seq)
    snapshot_payload.setdefault("known_at", _utcnow().isoformat() + "Z")

    def _write() -> Dict[str, Any]:
        with db.session() as session:
            row = BotRunSnapshotRecord(
                run_id=run_id,
                bot_id=bot_id,
                series_key=series_key,
                snapshot_seq=snapshot_seq,
                snapshot_payload=snapshot_payload,
                updated_at=_utcnow(),
            )
            session.add(row)
            session.flush()
            return row.to_dict()

    return _execute_write_with_retry(
        operation="record_bot_run_snapshot",
        context={"run_id": run_id, "bot_id": bot_id, "series_key": series_key},
        action=_write,
    )


def get_latest_bot_run_snapshot(
    *,
    bot_id: str,
    run_id: Optional[str] = None,
    series_key: str = "bot",
) -> Optional[Dict[str, Any]]:
    if not db.available:
        return None
    query = (
        select(BotRunSnapshotRecord)
        .where(BotRunSnapshotRecord.bot_id == str(bot_id))
        .where(BotRunSnapshotRecord.series_key == str(series_key))
    )
    if run_id is not None:
        query = query.where(BotRunSnapshotRecord.run_id == str(run_id))
    query = query.order_by(BotRunSnapshotRecord.id.desc()).limit(1)
    with db.session() as session:
        row = session.execute(query).scalars().first()
        return row.to_dict() if row else None


def list_bot_run_snapshots(
    *,
    bot_id: str,
    run_id: str,
    series_key: str = "bot",
    after_seq: int = 0,
    limit: int = 2500,
) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    max_rows = max(1, min(int(limit or 2500), 5000))
    with db.session() as session:
        query = (
            select(BotRunSnapshotRecord)
            .where(BotRunSnapshotRecord.bot_id == str(bot_id))
            .where(BotRunSnapshotRecord.run_id == str(run_id))
            .where(BotRunSnapshotRecord.series_key == str(series_key))
            .where(BotRunSnapshotRecord.snapshot_seq > int(after_seq or 0))
            .order_by(BotRunSnapshotRecord.snapshot_seq.asc(), BotRunSnapshotRecord.id.asc())
            .limit(max_rows)
        )
        rows = session.execute(query).scalars().all()
        return [row.to_dict() for row in rows]


def record_bot_runtime_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not db.available:
        raise RuntimeError("database is required for bot runtime event persistence")
    event_id = str(payload.get("event_id") or "").strip()
    bot_id = str(payload.get("bot_id") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    if not event_id or not bot_id or not run_id:
        raise ValueError("event_id, bot_id and run_id are required for runtime event persistence")
    seq = int(payload.get("seq") or 0)
    if seq <= 0:
        raise ValueError("seq must be a positive integer")
    raw_event_schema_version = payload.get("schema_version")
    schema_version = int(raw_event_schema_version) if raw_event_schema_version is not None else 1
    if schema_version <= 0:
        raise ValueError("schema_version must be >= 1 for runtime event persistence")
    def _write() -> Dict[str, Any]:
        with db.session() as session:
            existing = (
                session.execute(
                    select(BotRunEventRecord)
                    .where(BotRunEventRecord.event_id == event_id)
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if existing is not None:
                return existing.to_dict()
            latest_seq = (
                session.execute(
                    select(func.max(BotRunEventRecord.seq))
                    .where(BotRunEventRecord.bot_id == bot_id)
                    .where(BotRunEventRecord.run_id == run_id)
                )
                .scalar()
            )
            if latest_seq is not None and seq <= int(latest_seq):
                raise ValueError(f"seq must be monotonic per bot/run (incoming={seq}, latest={int(latest_seq)})")
            row = BotRunEventRecord(
                event_id=event_id,
                bot_id=bot_id,
                run_id=run_id,
                seq=seq,
                event_type=str(payload.get("event_type") or "state_delta"),
                critical=bool(payload.get("critical", False)),
                schema_version=schema_version,
                payload=_json_safe(dict(payload.get("payload") or {})),
                event_time=_parse_optional_timestamp(payload.get("event_time")),
                known_at=_utcnow(),
                created_at=_utcnow(),
            )
            session.add(row)
            session.flush()
            return row.to_dict()

    return _execute_write_with_retry(
        operation="record_bot_runtime_event",
        context={"run_id": run_id, "bot_id": bot_id, "event_id": event_id},
        action=_write,
    )


def list_bot_runtime_events(
    *,
    bot_id: str,
    run_id: str,
    after_seq: int = 0,
    limit: int = 1000,
    event_types: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    max_rows = max(1, min(int(limit or 1000), 5000))
    filter_event_types = [str(value).strip() for value in (event_types or []) if str(value).strip()]
    with db.session() as session:
        query = (
            select(BotRunEventRecord)
            .where(BotRunEventRecord.bot_id == str(bot_id))
            .where(BotRunEventRecord.run_id == str(run_id))
            .where(BotRunEventRecord.seq > int(after_seq or 0))
        )
        if filter_event_types:
            query = query.where(BotRunEventRecord.event_type.in_(filter_event_types))
        query = query.order_by(BotRunEventRecord.seq.asc(), BotRunEventRecord.id.asc()).limit(max_rows)
        rows = session.execute(query).scalars().all()
        return [row.to_dict() for row in rows]


def get_latest_bot_runtime_run_id(bot_id: str) -> Optional[str]:
    if not db.available:
        return None
    with db.session() as session:
        row = (
            session.execute(
                select(BotRunEventRecord.run_id)
                .where(BotRunEventRecord.bot_id == str(bot_id))
                .order_by(BotRunEventRecord.id.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        return str(row) if row else None


def get_latest_bot_runtime_event(
    *,
    bot_id: str,
    run_id: Optional[str] = None,
    event_types: Optional[Sequence[str]] = None,
) -> Optional[Dict[str, Any]]:
    if not db.available:
        return None
    filter_event_types = [str(value).strip() for value in (event_types or []) if str(value).strip()]
    with db.session() as session:
        query = select(BotRunEventRecord).where(BotRunEventRecord.bot_id == str(bot_id))
        if run_id:
            query = query.where(BotRunEventRecord.run_id == str(run_id))
            query = query.order_by(BotRunEventRecord.seq.desc(), BotRunEventRecord.id.desc())
        else:
            query = query.order_by(BotRunEventRecord.id.desc())
        if filter_event_types:
            query = query.where(BotRunEventRecord.event_type.in_(filter_event_types))
        row = session.execute(query.limit(1)).scalars().first()
        return row.to_dict() if row else None


def update_bot_runtime_status(*, bot_id: str, run_id: str, status: str, telemetry_degraded: bool = False) -> None:
    if not db.available:
        raise RuntimeError("database is required for bot status persistence")
    def _write() -> None:
        with db.session() as session:
            bot = session.get(BotRecord, bot_id)
            if bot is None:
                raise KeyError(f"Bot {bot_id} was not found")
            bot.status = status
            bot.updated_at = _utcnow()
            run = session.get(BotRunRecord, run_id)
            if run is None:
                run = BotRunRecord(run_id=run_id, bot_id=bot_id, status=status, started_at=_utcnow())
                session.add(run)
            run.status = "telemetry_degraded" if telemetry_degraded else status
            run.updated_at = _utcnow()
            if status in {"stopped", "failed"}:
                run.ended_at = _utcnow()

    _execute_write_with_retry(
        operation="update_bot_runtime_status",
        context={"run_id": run_id, "bot_id": bot_id, "status": status},
        action=_write,
    )
