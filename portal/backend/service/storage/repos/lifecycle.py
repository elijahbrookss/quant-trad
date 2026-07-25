"""Canonical bot lifecycle event-ledger persistence and run summaries."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from typing import Any, Dict, List, Optional

from ...bots.botlens_contract import LIFECYCLE_KIND
from ...bots.botlens_domain_events import (
    BotLensDomainEventName,
    _lifecycle_event_name,
    build_botlens_domain_events_from_lifecycle,
)
from ...bots.botlens_projection_batches import projection_batch_from_payload, runtime_event_rows_from_batch
from ...bots.startup_lifecycle import (
    BotLifecyclePhase,
    LifecycleOwner,
    TERMINAL_PHASES,
    status_for_phase,
)
from ._shared import (
    BotRecord,
    BotRunEventRecord,
    BotRunRecord,
    _parse_optional_timestamp,
    _utcnow,
    db,
    func,
    select,
)
from .runtime_events import get_latest_bot_runtime_run_id, record_bot_runtime_events_batch
from .runs import list_latest_bot_runs_by_bot_ids

_TERMINAL_LIFECYCLE_STATUSES = frozenset(
    {"stopped", "failed", "startup_failed", "crashed", "completed", "canceled", "cancelled", "degraded_terminal"}
)
_LIFECYCLE_MESSAGE_MAX_CHARS = 1024
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
_CANONICAL_LIFECYCLE_PHASES = frozenset(phase.value for phase in BotLifecyclePhase)
_CANONICAL_LIFECYCLE_OWNERS = frozenset(owner.value for owner in LifecycleOwner)


def _truncate_lifecycle_message(value: Any) -> Optional[str]:
    text_value = str(value or "").strip()
    if not text_value:
        return None
    if len(text_value) <= _LIFECYCLE_MESSAGE_MAX_CHARS:
        return text_value
    suffix = "... [truncated]"
    return text_value[: _LIFECYCLE_MESSAGE_MAX_CHARS - len(suffix)] + suffix


def _canonical_lifecycle_row_from_runtime_row(row: BotRunEventRecord | Mapping[str, Any]) -> Dict[str, Any]:
    payload = row.payload if isinstance(row, BotRunEventRecord) else dict(row.get("payload") or {})
    context = dict(payload.get("context") or {}) if isinstance(payload.get("context"), Mapping) else {}
    created_at = row.created_at if isinstance(row, BotRunEventRecord) else row.get("created_at")
    checkpoint_at = row.event_time if isinstance(row, BotRunEventRecord) else row.get("event_time")
    row_run_id = row.run_id if isinstance(row, BotRunEventRecord) else row.get("run_id")
    row_bot_id = row.bot_id if isinstance(row, BotRunEventRecord) else row.get("bot_id")
    source_seq = int((row.seq if isinstance(row, BotRunEventRecord) else row.get("seq")) or 0)
    run_seq = int((row.run_seq if isinstance(row, BotRunEventRecord) else row.get("run_seq")) or source_seq or 0)
    return {
        "id": int((row.id if isinstance(row, BotRunEventRecord) else row.get("id")) or 0),
        "event_id": str((row.event_id if isinstance(row, BotRunEventRecord) else row.get("event_id")) or ""),
        "run_id": str(context.get("run_id") or row_run_id or ""),
        "bot_id": str(context.get("bot_id") or row_bot_id or ""),
        "seq": run_seq,
        "run_seq": run_seq,
        "source_seq": source_seq,
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


def _latest_canonical_lifecycle_row(run_id: str, *, session: Any | None = None) -> Optional[Dict[str, Any]]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or (session is None and not db.available):
        return None
    with (nullcontext(session) if session is not None else db.session()) as active_session:
        row = (
            active_session.execute(
                select(BotRunEventRecord)
                .where(BotRunEventRecord.run_id == normalized_run_id)
                .where(BotRunEventRecord.event_name.in_(_CANONICAL_LIFECYCLE_EVENT_NAMES))
                .order_by(
                    func.coalesce(BotRunEventRecord.run_seq, BotRunEventRecord.seq).desc(),
                    BotRunEventRecord.id.desc(),
                )
                .limit(1)
            )
            .scalars()
            .first()
        )
        return _canonical_lifecycle_row_from_runtime_row(row) if row is not None else None


def _canonical_lifecycle_rows_in_session(session: Any, run_id: str) -> List[Dict[str, Any]]:
    rows = (
        session.execute(
            select(BotRunEventRecord)
            .where(BotRunEventRecord.run_id == str(run_id))
            .where(BotRunEventRecord.event_name.in_(_CANONICAL_LIFECYCLE_EVENT_NAMES))
            .order_by(
                func.coalesce(BotRunEventRecord.run_seq, BotRunEventRecord.seq).asc(),
                BotRunEventRecord.id.asc(),
            )
        )
        .scalars()
        .all()
    )
    return [_canonical_lifecycle_row_from_runtime_row(row) for row in rows]


def _validate_canonical_lifecycle_rows(
    rows: List[Mapping[str, Any]],
    *,
    run_id: str,
) -> None:
    if not rows:
        raise RuntimeError(f"canonical lifecycle ledger is empty run_id={run_id}")

    previous_checkpoint = None
    terminal_event_id = None
    for index, row in enumerate(rows):
        row_run_id = str(row.get("run_id") or "").strip()
        phase = str(row.get("phase") or "").strip()
        status = str(row.get("status") or "").strip()
        event_id = str(row.get("event_id") or "").strip()
        if row_run_id != run_id:
            raise RuntimeError(
                "canonical lifecycle run mismatch "
                f"expected_run_id={run_id} row_run_id={row_run_id or '<missing>'}"
            )
        if phase not in _CANONICAL_LIFECYCLE_PHASES:
            raise ValueError(
                f"unknown canonical lifecycle phase run_id={run_id} phase={phase or '<missing>'}"
            )
        expected_status = status_for_phase(phase)
        if status != expected_status:
            raise ValueError(
                "canonical lifecycle phase/status mismatch "
                f"run_id={run_id} phase={phase} status={status or '<missing>'} "
                f"expected_status={expected_status}"
            )
        checkpoint = _parse_optional_timestamp(row.get("checkpoint_at"))
        if checkpoint is None:
            raise ValueError(
                f"canonical lifecycle checkpoint_at is required run_id={run_id} event_id={event_id}"
            )
        if previous_checkpoint is not None and checkpoint < previous_checkpoint:
            raise ValueError(
                "canonical lifecycle checkpoint chronology regression "
                f"run_id={run_id} event_id={event_id} checkpoint_at={checkpoint.isoformat()} "
                f"previous_checkpoint_at={previous_checkpoint.isoformat()}"
            )
        if terminal_event_id is not None:
            raise ValueError(
                "canonical lifecycle cannot append after terminal state "
                f"run_id={run_id} terminal_event_id={terminal_event_id} event_id={event_id}"
            )
        if index == 0 and _lifecycle_event_name(phase=phase, status=status) == BotLensDomainEventName.RUN_READY:
            raise RuntimeError(
                "canonical lifecycle requires prior durable startup truth before RUN_READY "
                f"run_id={run_id}"
            )
        if phase in TERMINAL_PHASES or status in _TERMINAL_LIFECYCLE_STATUSES:
            terminal_event_id = event_id
        previous_checkpoint = checkpoint


def _validated_latest_lifecycle_in_session(session: Any, run_id: str) -> Dict[str, Any]:
    rows = _canonical_lifecycle_rows_in_session(session, run_id)
    _validate_canonical_lifecycle_rows(rows, run_id=run_id)
    return dict(rows[-1])


def _list_canonical_lifecycle_rows(run_id: str) -> List[Dict[str, Any]]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id or not db.available:
        return []
    with db.session() as session:
        return _canonical_lifecycle_rows_in_session(session, normalized_run_id)


def _latest_canonical_lifecycle_rows(run_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    normalized = [str(run_id or "").strip() for run_id in run_ids]
    wanted = [run_id for run_id in dict.fromkeys(normalized) if run_id]
    if not wanted or not db.available:
        return {}
    with db.session() as session:
        ranked_rows = (
            select(
                BotRunEventRecord.id.label("event_row_id"),
                func.row_number()
                .over(
                    partition_by=BotRunEventRecord.run_id,
                    order_by=(
                        func.coalesce(BotRunEventRecord.run_seq, BotRunEventRecord.seq).desc(),
                        BotRunEventRecord.id.desc(),
                    ),
                )
                .label("lifecycle_rank"),
            )
            .where(BotRunEventRecord.run_id.in_(wanted))
            .where(BotRunEventRecord.event_name.in_(_CANONICAL_LIFECYCLE_EVENT_NAMES))
            .subquery()
        )
        rows = (
            session.execute(
                select(BotRunEventRecord)
                .join(ranked_rows, ranked_rows.c.event_row_id == BotRunEventRecord.id)
                .where(ranked_rows.c.lifecycle_rank == 1)
                .order_by(BotRunEventRecord.run_id.asc())
            )
            .scalars()
            .all()
        )
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        run_id = str(row.run_id or "").strip()
        if run_id and run_id not in latest:
            latest[run_id] = _canonical_lifecycle_row_from_runtime_row(row)
    return latest


def _run_ready_requires_prior_lifecycle(*, run_id: str, phase: str, status: str) -> None:
    if _lifecycle_event_name(phase=phase, status=status) != BotLensDomainEventName.RUN_READY:
        return
    prior = _latest_canonical_lifecycle_row(run_id)
    if prior is None:
        raise RuntimeError(
            "canonical lifecycle requires prior durable startup truth before RUN_READY "
            f"run_id={run_id}"
        )


def _project_bot_run_summary_in_session(
    session: Any,
    payload: Mapping[str, Any],
) -> Dict[str, Any]:
    """Project canonical lifecycle status in the event append transaction."""

    bot_id = str(payload.get("bot_id") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    status = str(payload.get("status") or "").strip()
    checkpoint_at = _parse_optional_timestamp(payload.get("checkpoint_at")) or _utcnow()
    if not bot_id or not run_id or not status:
        raise ValueError("bot_id, run_id, and status are required for bot run summary projection")

    now = _utcnow()
    bot_row = session.get(BotRecord, bot_id)
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
            created_at=now,
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
        run_row.status = status
        run_row.updated_at = now
    if status in _TERMINAL_LIFECYCLE_STATUSES:
        run_row.ended_at = checkpoint_at

    return run_row.to_dict()


def record_bot_run_lifecycle_checkpoint(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Append canonical lifecycle truth and refresh the run summary projection."""

    if not db.available:
        raise RuntimeError("database is required for bot lifecycle persistence")

    bot_id = str(payload.get("bot_id") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    phase = str(payload.get("phase") or "").strip()
    status = str(payload.get("status") or "").strip()
    owner = str(payload.get("owner") or "").strip()
    if not bot_id or not run_id or not phase or not status or not owner:
        raise ValueError("bot_id, run_id, phase, status, and owner are required for bot lifecycle persistence")
    if phase not in _CANONICAL_LIFECYCLE_PHASES:
        raise ValueError(f"unknown bot lifecycle phase: {phase}")
    expected_status = status_for_phase(phase)
    if status != expected_status:
        raise ValueError(
            "bot lifecycle status must match phase "
            f"phase={phase} status={status} expected_status={expected_status}"
        )
    if owner not in _CANONICAL_LIFECYCLE_OWNERS:
        raise ValueError(f"unknown bot lifecycle owner: {owner}")

    message = _truncate_lifecycle_message(payload.get("message"))
    metadata = dict(payload.get("metadata") or {}) if isinstance(payload.get("metadata"), Mapping) else {}
    failure = dict(payload.get("failure") or {}) if isinstance(payload.get("failure"), Mapping) else {}
    checkpoint_at = _parse_optional_timestamp(payload.get("checkpoint_at")) or _utcnow()
    source_seq = int(payload.get("seq") or payload.get("run_seq") or 0)
    if source_seq < 0:
        source_seq = 0
    projection_seq = source_seq if source_seq > 0 else 1

    _run_ready_requires_prior_lifecycle(run_id=run_id, phase=phase, status=status)

    canonical_payload = {
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
    if not events:
        raise RuntimeError(
            f"canonical lifecycle builder produced no events run_id={run_id} phase={phase} status={status}"
        )
    canonical_lifecycle_event_id = events[0].event_id
    batch = projection_batch_from_payload(
        batch_kind=LIFECYCLE_KIND,
        run_id=run_id,
        bot_id=bot_id,
        symbol_key=None,
        payload=canonical_payload,
        events=tuple(events),
        seq=int(projection_seq),
    )
    rows = runtime_event_rows_from_batch(batch=batch)
    if not rows:
        raise RuntimeError(
            f"canonical lifecycle event serialization produced no rows run_id={run_id} "
            f"event_id={canonical_lifecycle_event_id}"
        )
    lifecycle_state: Optional[Dict[str, Any]] = None

    def _project_lifecycle_summary(session: Any) -> None:
        nonlocal lifecycle_state
        lifecycle_state = _validated_latest_lifecycle_in_session(session, run_id)
        _project_bot_run_summary_in_session(session, lifecycle_state)

    record_bot_runtime_events_batch(
        rows,
        context={
            "bot_id": bot_id,
            "run_id": run_id,
            "message_kind": LIFECYCLE_KIND,
            "pipeline_stage": "botlens_canonical_lifecycle_append",
            "source_emitter": str(payload.get("source_emitter") or owner or "lifecycle").strip() or "lifecycle",
            "source_reason": "producer",
            "event_name": batch.events[0].event_name.value,
        },
        transactional_projection=_project_lifecycle_summary,
    )

    if lifecycle_state is None:
        raise RuntimeError("canonical lifecycle transactional projection did not run")
    return lifecycle_state


def rebuild_bot_run_lifecycle_summary(run_id: str) -> Dict[str, Any]:
    """Rebuild lifecycle-owned run summary fields from the canonical ledger."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required for lifecycle summary rebuild")
    if not db.available:
        raise RuntimeError("database is required for lifecycle summary rebuild")

    with db.session() as session:
        rows = _canonical_lifecycle_rows_in_session(session, normalized_run_id)
        _validate_canonical_lifecycle_rows(rows, run_id=normalized_run_id)
        first = dict(rows[0])
        latest = dict(rows[-1])
        _project_bot_run_summary_in_session(session, latest)
        run_row = session.get(BotRunRecord, normalized_run_id)
        if run_row is None:
            raise RuntimeError(
                f"lifecycle summary rebuild produced no run row run_id={normalized_run_id}"
            )
        run_row.started_at = _parse_optional_timestamp(first.get("checkpoint_at"))
        if (
            str(latest.get("phase") or "") in TERMINAL_PHASES
            or str(latest.get("status") or "") in _TERMINAL_LIFECYCLE_STATUSES
        ):
            run_row.ended_at = _parse_optional_timestamp(latest.get("checkpoint_at"))
        else:
            run_row.ended_at = None
        run_row.updated_at = _utcnow()
        return run_row.to_dict()


def get_bot_run_lifecycle(run_id: str) -> Optional[Dict[str, Any]]:
    return _latest_canonical_lifecycle_row(run_id)


def get_latest_bot_run_lifecycle(bot_id: str) -> Optional[Dict[str, Any]]:
    normalized_bot_id = str(bot_id or "").strip()
    if not normalized_bot_id or not db.available:
        return None
    latest_run_id = get_latest_bot_runtime_run_id(normalized_bot_id)
    return get_bot_run_lifecycle(latest_run_id) if latest_run_id else None


def list_latest_bot_run_lifecycles(
    bot_ids: List[str],
    *,
    run_ids_by_bot: Mapping[str, str] | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Return latest canonical lifecycle rows keyed by bot id."""

    normalized = [str(bot_id or "").strip() for bot_id in bot_ids]
    wanted = [bot_id for bot_id in dict.fromkeys(normalized) if bot_id]
    if not wanted or not db.available:
        return {}

    resolved_run_ids: Dict[str, str] = {}
    for raw_bot_id, raw_run_id in dict(run_ids_by_bot or {}).items():
        bot_id = str(raw_bot_id or "").strip()
        run_id = str(raw_run_id or "").strip()
        if bot_id in wanted and run_id:
            resolved_run_ids[bot_id] = run_id

    missing = [bot_id for bot_id in wanted if bot_id not in resolved_run_ids]
    if missing:
        latest_runs = list_latest_bot_runs_by_bot_ids(missing)
        for raw_bot_id, run in latest_runs.items():
            bot_id = str(raw_bot_id or "").strip()
            run_id = str((run or {}).get("run_id") or "").strip()
            if bot_id in wanted and run_id:
                resolved_run_ids[bot_id] = run_id

    rows_by_run = _latest_canonical_lifecycle_rows(list(resolved_run_ids.values()))
    result: Dict[str, Dict[str, Any]] = {}
    for bot_id, run_id in resolved_run_ids.items():
        lifecycle = rows_by_run.get(run_id)
        if lifecycle is None:
            continue
        lifecycle_bot_id = str(lifecycle.get("bot_id") or "").strip()
        if lifecycle_bot_id and lifecycle_bot_id != bot_id:
            raise RuntimeError(
                "canonical lifecycle bot/run mismatch "
                f"bot_id={bot_id} run_id={run_id} lifecycle_bot_id={lifecycle_bot_id}"
            )
        result[bot_id] = lifecycle
    return result


def list_bot_run_lifecycle_events(run_id: str) -> List[Dict[str, Any]]:
    return _list_canonical_lifecycle_rows(run_id)


__all__ = [
    "get_bot_run_lifecycle",
    "get_latest_bot_run_lifecycle",
    "list_bot_run_lifecycle_events",
    "list_latest_bot_run_lifecycles",
    "rebuild_bot_run_lifecycle_summary",
    "record_bot_run_lifecycle_checkpoint",
]
