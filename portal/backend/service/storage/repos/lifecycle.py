"""Durable bot startup/runtime lifecycle state and checkpoint trail."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from ....service.bots.startup_lifecycle import deep_merge_dict
from ._shared import *


def record_bot_run_lifecycle_checkpoint(
    payload: Mapping[str, Any],
    *,
    replace_metadata: bool = False,
) -> Dict[str, Any]:
    """Append a lifecycle checkpoint and update the run's latest lifecycle state."""

    if not db.available:
        raise RuntimeError("database is required for bot lifecycle persistence")

    bot_id = str(payload.get("bot_id") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    phase = str(payload.get("phase") or "").strip()
    status = str(payload.get("status") or "").strip()
    owner = str(payload.get("owner") or "").strip()
    if not bot_id or not run_id or not phase or not status or not owner:
        raise ValueError("bot_id, run_id, phase, status, and owner are required for bot lifecycle persistence")

    message = str(payload.get("message") or "").strip() or None
    metadata = dict(payload.get("metadata") or {}) if isinstance(payload.get("metadata"), Mapping) else {}
    failure = dict(payload.get("failure") or {}) if isinstance(payload.get("failure"), Mapping) else {}
    checkpoint_at = _parse_optional_timestamp(payload.get("checkpoint_at")) or _utcnow()
    event_id = str(payload.get("event_id") or "").strip() or str(uuid.uuid4())

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
            return row.to_dict() if row is not None else {}

        next_seq = int(
            session.execute(
                select(func.coalesce(func.max(BotRunLifecycleEventRecord.seq), 0))
                .where(BotRunLifecycleEventRecord.run_id == run_id)
            ).scalar_one()
            or 0
        ) + 1

        event_row = BotRunLifecycleEventRecord(
            event_id=event_id,
            run_id=run_id,
            bot_id=bot_id,
            seq=next_seq,
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

        return current.to_dict()


def get_bot_run_lifecycle(run_id: str) -> Optional[Dict[str, Any]]:
    if not db.available:
        return None
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return None
    with db.session() as session:
        row = session.get(BotRunLifecycleRecord, normalized_run_id)
        return row.to_dict() if row else None


def get_latest_bot_run_lifecycle(bot_id: str) -> Optional[Dict[str, Any]]:
    if not db.available:
        return None
    normalized_bot_id = str(bot_id or "").strip()
    if not normalized_bot_id:
        return None
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
    if not db.available:
        return []
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
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


__all__ = [
    "get_bot_run_lifecycle",
    "get_latest_bot_run_lifecycle",
    "list_bot_run_lifecycle_events",
    "record_bot_run_lifecycle_checkpoint",
]
