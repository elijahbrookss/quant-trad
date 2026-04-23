"""Canonical bot lifecycle append plus synchronized convenience tables."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ...bots.botlens_contract import LIFECYCLE_KIND
from ...bots.botlens_domain_events import (
    BotLensDomainEventName,
    _lifecycle_event_name,
    build_botlens_domain_events_from_lifecycle,
)
from ...bots.botlens_projection_batches import projection_batch_from_payload, runtime_event_rows_from_batch
from ...observability import payload_size_bytes
from ....service.bots.startup_lifecycle import deep_merge_dict
from ._shared import (
    BotRecord,
    BotRunEventRecord,
    BotRunLifecycleEventRecord,
    BotRunLifecycleRecord,
    BotRunRecord,
    StorageWriteOutcome,
    _STORAGE_OBSERVER,
    _execute_write_with_retry,
    _json_safe,
    _observe_db_write_outcome,
    _parse_optional_timestamp,
    _utcnow,
    db,
    func,
    select,
    uuid,
)
from .runtime_events import get_latest_bot_runtime_run_id, record_bot_runtime_events_batch

_OBSERVER = _STORAGE_OBSERVER
_TERMINAL_LIFECYCLE_STATUSES = frozenset({"stopped", "failed", "startup_failed", "crashed", "completed"})
_LIFECYCLE_MESSAGE_MAX_CHARS = 1024
_LIFECYCLE_SEQ_LOCK_PERSON = b"qt_lifecycle_seq"
_CANONICAL_LIFECYCLE_EVENT_NAMES = tuple(
    event_name.value
    for event_name in (
        BotLensDomainEventName.RUN_PHASE_REPORTED,
        BotLensDomainEventName.RUN_STARTED,
        BotLensDomainEventName.RUN_READY,
        BotLensDomainEventName.RUN_DEGRADED,
        BotLensDomainEventName.RUN_COMPLETED,
        BotLensDomainEventName.RUN_FAILED,
        BotLensDomainEventName.RUN_STOPPED,
        BotLensDomainEventName.RUN_CANCELLED,
    )
)


def _truncate_lifecycle_message(value: Any) -> Optional[str]:
    text_value = str(value or "").strip()
    if not text_value:
        return None
    if len(text_value) <= _LIFECYCLE_MESSAGE_MAX_CHARS:
        return text_value
    suffix = "... [truncated]"
    return text_value[: _LIFECYCLE_MESSAGE_MAX_CHARS - len(suffix)] + suffix


def _lifecycle_seq_lock_key(run_id: str) -> int:
    digest = hashlib.blake2b(
        str(run_id or "").strip().encode("utf-8"),
        digest_size=8,
        person=_LIFECYCLE_SEQ_LOCK_PERSON,
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=False) & ((1 << 63) - 1)


def _acquire_lifecycle_seq_lock(session: Any, *, run_id: str) -> None:
    session.execute(
        text("SELECT pg_advisory_xact_lock(:key)"),
        {"key": _lifecycle_seq_lock_key(run_id)},
    )


def _canonical_lifecycle_row_from_runtime_row(row: BotRunEventRecord | Mapping[str, Any]) -> Dict[str, Any]:
    payload = row.payload if isinstance(row, BotRunEventRecord) else dict(row.get("payload") or {})
    context = dict(payload.get("context") or {}) if isinstance(payload.get("context"), Mapping) else {}
    created_at = row.created_at if isinstance(row, BotRunEventRecord) else row.get("created_at")
    checkpoint_at = row.event_time if isinstance(row, BotRunEventRecord) else row.get("event_time")
    return {
        "id": int((row.id if isinstance(row, BotRunEventRecord) else row.get("id")) or 0),
        "event_id": str((row.event_id if isinstance(row, BotRunEventRecord) else row.get("event_id")) or ""),
        "run_id": str(context.get("run_id") or (row.run_id if isinstance(row, BotRunEventRecord) else row.get("run_id")) or ""),
        "bot_id": str(context.get("bot_id") or (row.bot_id if isinstance(row, BotRunEventRecord) else row.get("bot_id")) or ""),
        "seq": int((row.seq if isinstance(row, BotRunEventRecord) else row.get("seq")) or 0),
        "phase": str(context.get("phase") or "").strip() or None,
        "status": str(context.get("status") or "").strip() or None,
        "owner": str(context.get("component") or "").strip() or None,
        "message": str(context.get("message") or "").strip() or None,
        "metadata": dict(context.get("metadata") or {}) if isinstance(context.get("metadata"), Mapping) else {},
        "failure": dict(context.get("failure") or {}) if isinstance(context.get("failure"), Mapping) else {},
        "live": bool(context.get("live")),
        "checkpoint_at": checkpoint_at.isoformat() + "Z" if checkpoint_at is not None else None,
        "created_at": created_at.isoformat() + "Z" if created_at is not None else None,
    }


def _latest_canonical_lifecycle_row(run_id: str) -> Optional[Dict[str, Any]]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not db.available:
        return None
    with db.session() as session:
        row = (
            session.execute(
                select(BotRunEventRecord)
                .where(BotRunEventRecord.run_id == normalized_run_id)
                .where(BotRunEventRecord.event_name.in_(_CANONICAL_LIFECYCLE_EVENT_NAMES))
                .order_by(BotRunEventRecord.seq.desc(), BotRunEventRecord.id.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        return _canonical_lifecycle_row_from_runtime_row(row) if row is not None else None


def _list_canonical_lifecycle_rows(run_id: str) -> List[Dict[str, Any]]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not db.available:
        return []
    with db.session() as session:
        rows = (
            session.execute(
                select(BotRunEventRecord)
                .where(BotRunEventRecord.run_id == normalized_run_id)
                .where(BotRunEventRecord.event_name.in_(_CANONICAL_LIFECYCLE_EVENT_NAMES))
                .order_by(BotRunEventRecord.seq.asc(), BotRunEventRecord.id.asc())
            )
            .scalars()
            .all()
        )
        return [_canonical_lifecycle_row_from_runtime_row(row) for row in rows]


def _latest_legacy_lifecycle_row(run_id: str) -> Optional[Dict[str, Any]]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not db.available:
        return None
    with db.session() as session:
        row = session.get(BotRunLifecycleRecord, normalized_run_id)
        return row.to_dict() if row else None


def _list_legacy_lifecycle_rows(run_id: str) -> List[Dict[str, Any]]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not db.available:
        return []
    with db.session() as session:
        rows = (
            session.execute(
                select(BotRunLifecycleEventRecord)
                .where(BotRunLifecycleEventRecord.run_id == normalized_run_id)
                .order_by(BotRunLifecycleEventRecord.seq.asc(), BotRunLifecycleEventRecord.id.asc())
            )
            .scalars()
            .all()
        )
        return [row.to_dict() for row in rows]


def _allocate_next_canonical_seq(run_id: str) -> int:
    with db.session() as session:
        _acquire_lifecycle_seq_lock(session, run_id=run_id)
        return int(
            session.execute(
                select(func.coalesce(func.max(BotRunEventRecord.seq), 0))
                .where(BotRunEventRecord.run_id == run_id)
            ).scalar_one()
            or 0
        ) + 1


def _run_ready_requires_prior_lifecycle(*, run_id: str, phase: str, status: str) -> None:
    if _lifecycle_event_name(phase=phase, status=status) != BotLensDomainEventName.RUN_READY:
        return
    prior = _latest_canonical_lifecycle_row(run_id)
    if prior is None:
        raise RuntimeError(
            "canonical lifecycle requires prior durable startup truth before RUN_READY "
            f"run_id={run_id}"
        )


def _sync_legacy_lifecycle_tables(
    *,
    payload: Mapping[str, Any],
    seq: int,
    replace_metadata: bool,
) -> Dict[str, Any]:
    bot_id = str(payload.get("bot_id") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    phase = str(payload.get("phase") or "").strip()
    status = str(payload.get("status") or "").strip()
    owner = str(payload.get("owner") or "").strip()
    event_id = str(payload.get("event_id") or "").strip()
    message = _truncate_lifecycle_message(payload.get("message"))
    metadata = dict(payload.get("metadata") or {}) if isinstance(payload.get("metadata"), Mapping) else {}
    failure = dict(payload.get("failure") or {}) if isinstance(payload.get("failure"), Mapping) else {}
    checkpoint_at = _parse_optional_timestamp(payload.get("checkpoint_at")) or _utcnow()
    started = time.perf_counter()
    payload_bytes = payload_size_bytes({"metadata": metadata, "failure": failure, "message": message})
    write_context = {"bot_id": bot_id, "run_id": run_id, "seq": int(seq)}

    def _write() -> StorageWriteOutcome:
        with db.session() as session:
            existing = (
                session.execute(
                    select(BotRunLifecycleEventRecord)
                    .where(BotRunLifecycleEventRecord.event_id == event_id)
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if existing is not None:
                row = session.get(BotRunLifecycleRecord, run_id)
                result = row.to_dict() if row is not None else {}
                result["seq"] = int(getattr(existing, "seq", seq) or seq)
                return StorageWriteOutcome(result=result, noop_reason="duplicate_skip", noop_count=1)

            event_row = BotRunLifecycleEventRecord(
                event_id=event_id,
                run_id=run_id,
                bot_id=bot_id,
                seq=int(seq),
                phase=phase,
                status=status,
                owner=owner,
                message=message,
                lifecycle_metadata=_json_safe(metadata),
                failure=_json_safe(failure) if failure else None,
                checkpoint_at=checkpoint_at,
                created_at=_utcnow(),
            )
            session.add(event_row)

            current = session.get(BotRunLifecycleRecord, run_id)
            now = _utcnow()
            rows_written = 2
            if current is None:
                current = BotRunLifecycleRecord(
                    run_id=run_id,
                    bot_id=bot_id,
                    phase=phase,
                    status=status,
                    owner=owner,
                    message=message,
                    lifecycle_metadata=_json_safe(metadata),
                    failure=_json_safe(failure) if failure else None,
                    checkpoint_at=checkpoint_at,
                    created_at=now,
                    updated_at=now,
                )
                session.add(current)
            else:
                current.bot_id = bot_id
                current.phase = phase
                current.status = status
                current.owner = owner
                current.message = message
                current.lifecycle_metadata = _json_safe(
                    metadata
                    if replace_metadata
                    else deep_merge_dict(
                        current.lifecycle_metadata if isinstance(current.lifecycle_metadata, Mapping) else {},
                        metadata,
                    )
                )
                current.failure = _json_safe(failure) if failure else None
                current.checkpoint_at = checkpoint_at
                current.updated_at = now

            bot_row = session.get(BotRecord, bot_id)
            if bot_row is not None:
                bot_row.status = status
                bot_row.updated_at = now
                rows_written += 1

            run_row = session.get(BotRunRecord, run_id)
            if run_row is None:
                run_row = BotRunRecord(
                    run_id=run_id,
                    bot_id=bot_id,
                    bot_name=bot_row.name if bot_row is not None else None,
                    strategy_id=bot_row.strategy_id if bot_row is not None else None,
                    run_type=(bot_row.run_type if bot_row is not None and bot_row.run_type else "backtest"),
                    status=status,
                    started_at=checkpoint_at,
                    backtest_start=bot_row.backtest_start if bot_row is not None else None,
                    backtest_end=bot_row.backtest_end if bot_row is not None else None,
                    updated_at=now,
                )
                session.add(run_row)
            else:
                run_row.bot_id = bot_id
                if not run_row.bot_name and bot_row is not None:
                    run_row.bot_name = bot_row.name
                if not run_row.strategy_id and bot_row is not None:
                    run_row.strategy_id = bot_row.strategy_id
                if not run_row.run_type:
                    run_row.run_type = bot_row.run_type if bot_row is not None and bot_row.run_type else "backtest"
                if run_row.backtest_start is None and bot_row is not None:
                    run_row.backtest_start = bot_row.backtest_start
                if run_row.backtest_end is None and bot_row is not None:
                    run_row.backtest_end = bot_row.backtest_end
                if run_row.started_at is None:
                    run_row.started_at = checkpoint_at
                run_row.updated_at = now
            run_row.status = status
            if status in _TERMINAL_LIFECYCLE_STATUSES:
                run_row.ended_at = checkpoint_at
            rows_written += 1

            return StorageWriteOutcome(
                result={
                    **current.to_dict(),
                    "seq": int(seq),
                },
                rows_written=rows_written,
                payload_bytes=payload_bytes,
            )

    outcome = _execute_write_with_retry(
        operation="sync_bot_run_lifecycle_tables",
        storage_target="portal_bot_run_lifecycle",
        context=write_context,
        action=_write,
    )
    _observe_db_write_outcome(
        storage_target="portal_bot_run_lifecycle",
        context=write_context,
        started=started,
        outcome=outcome,
    )
    return dict(outcome.result)


def record_bot_run_lifecycle_checkpoint(
    payload: Mapping[str, Any],
    *,
    replace_metadata: bool = False,
) -> Dict[str, Any]:
    """Append canonical lifecycle truth, then synchronize legacy lifecycle helpers."""

    if not db.available:
        raise RuntimeError("database is required for bot lifecycle persistence")

    bot_id = str(payload.get("bot_id") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    phase = str(payload.get("phase") or "").strip()
    status = str(payload.get("status") or "").strip()
    owner = str(payload.get("owner") or "").strip()
    if not bot_id or not run_id or not phase or not status or not owner:
        raise ValueError("bot_id, run_id, phase, status, and owner are required for bot lifecycle persistence")

    message = _truncate_lifecycle_message(payload.get("message"))
    metadata = dict(payload.get("metadata") or {}) if isinstance(payload.get("metadata"), Mapping) else {}
    failure = dict(payload.get("failure") or {}) if isinstance(payload.get("failure"), Mapping) else {}
    checkpoint_at = _parse_optional_timestamp(payload.get("checkpoint_at")) or _utcnow()
    event_id = str(payload.get("event_id") or "").strip() or str(uuid.uuid4())
    seq = int(payload.get("seq") or payload.get("run_seq") or 0)
    if seq <= 0:
        seq = _allocate_next_canonical_seq(run_id)

    _run_ready_requires_prior_lifecycle(run_id=run_id, phase=phase, status=status)

    canonical_payload = {
        "event_id": event_id,
        "bot_id": bot_id,
        "run_id": run_id,
        "phase": phase,
        "status": status,
        "owner": owner,
        "message": message,
        "metadata": metadata,
        "failure": failure,
        "checkpoint_at": checkpoint_at,
        "updated_at": checkpoint_at,
        "known_at": checkpoint_at,
        "live": bool(payload.get("live"))
        or phase == "live"
        or status in {"running", "degraded", "telemetry_degraded", "paused"},
    }
    events = build_botlens_domain_events_from_lifecycle(
        bot_id=bot_id,
        run_id=run_id,
        lifecycle=canonical_payload,
    )
    batch = projection_batch_from_payload(
        batch_kind=LIFECYCLE_KIND,
        run_id=run_id,
        bot_id=bot_id,
        symbol_key=None,
        payload=canonical_payload,
        events=tuple(events),
        seq=int(seq),
    )
    rows = runtime_event_rows_from_batch(batch=batch)
    if rows:
        record_bot_runtime_events_batch(
            rows,
            context={
                "bot_id": bot_id,
                "run_id": run_id,
                "message_kind": LIFECYCLE_KIND,
                "pipeline_stage": "botlens_canonical_lifecycle_append",
                "source_emitter": str(payload.get("source_emitter") or owner or "lifecycle").strip() or "lifecycle",
                "source_reason": "producer",
                "event_name": batch.events[0].event_name.value if batch.events else None,
            },
        )

    lifecycle_state = _sync_legacy_lifecycle_tables(
        payload={
            **canonical_payload,
            "event_id": event_id,
        },
        seq=int(seq),
        replace_metadata=replace_metadata,
    )
    lifecycle_state["seq"] = int(seq)
    lifecycle_state["live"] = bool(canonical_payload["live"])
    return lifecycle_state


def get_bot_run_lifecycle(run_id: str) -> Optional[Dict[str, Any]]:
    canonical = _latest_canonical_lifecycle_row(run_id)
    if canonical is not None:
        return canonical
    return _latest_legacy_lifecycle_row(run_id)


def get_latest_bot_run_lifecycle(bot_id: str) -> Optional[Dict[str, Any]]:
    normalized_bot_id = str(bot_id or "").strip()
    if not normalized_bot_id or not db.available:
        return None
    latest_run_id = get_latest_bot_runtime_run_id(normalized_bot_id)
    if latest_run_id:
        canonical = get_bot_run_lifecycle(latest_run_id)
        if canonical is not None:
            return canonical
    with db.session() as session:
        row = (
            session.execute(
                select(BotRunLifecycleRecord)
                .where(BotRunLifecycleRecord.bot_id == normalized_bot_id)
                .order_by(BotRunLifecycleRecord.checkpoint_at.desc(), BotRunLifecycleRecord.updated_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        return row.to_dict() if row else None


def list_bot_run_lifecycle_events(run_id: str) -> List[Dict[str, Any]]:
    canonical = _list_canonical_lifecycle_rows(run_id)
    if canonical:
        return canonical
    return _list_legacy_lifecycle_rows(run_id)


__all__ = [
    "get_bot_run_lifecycle",
    "get_latest_bot_run_lifecycle",
    "list_bot_run_lifecycle_events",
    "record_bot_run_lifecycle_checkpoint",
]
