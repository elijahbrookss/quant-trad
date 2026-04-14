"""Runtime-event and BotLens state storage repository module."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Sequence

from engines.bot_runtime.core.series_identity import normalize_series_key as normalize_public_series_key
from sqlalchemy import or_

from ...observability import payload_size_bytes
from ...bots.botlens_contract import RUN_SCOPE_KEY

from ._shared import (
    BotRecord,
    BotRunEventRecord,
    BotRunLifecycleRecord,
    BotRunRecord,
    BotRunStepRecord,
    BotRunViewStateRecord,
    SQLAlchemyError,
    StorageWriteOutcome,
    _STORAGE_OBSERVER,
    _coerce_float,
    _coerce_int,
    _execute_write_with_retry,
    _json_safe,
    _observe_db_write_outcome,
    _parse_optional_timestamp,
    _utcnow,
    db,
    func,
    logger,
    select,
)

_OBSERVER = _STORAGE_OBSERVER


def _normalize_botlens_series_key(value: Any) -> str:
    if str(value or "").strip() == RUN_SCOPE_KEY:
        return RUN_SCOPE_KEY
    return normalize_public_series_key(value)


def _observe_seq_collision(
    *,
    storage_target: str,
    context: Dict[str, Any],
    exc: Exception,
) -> None:
    _OBSERVER.increment(
        "db_write_fail_total",
        bot_id=context.get("bot_id"),
        run_id=context.get("run_id"),
        storage_target=storage_target,
        failure_mode="seq_collision",
    )
    _OBSERVER.event(
        "db_seq_collision",
        level=logging.ERROR,
        bot_id=context.get("bot_id"),
        run_id=context.get("run_id"),
        storage_target=storage_target,
        failure_mode="seq_collision",
        error=str(exc),
    )


def _project_runtime_event_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    projected = dict(row or {})
    payload = projected.get("payload") if isinstance(projected.get("payload"), Mapping) else {}
    series_key = _normalize_botlens_series_key(payload.get("series_key"))
    if series_key:
        projected["series_key"] = series_key
    bridge_session_id = str(payload.get("bridge_session_id") or "").strip()
    if bridge_session_id:
        projected["bridge_session_id"] = bridge_session_id
    bridge_seq = _coerce_int(payload.get("bridge_seq"))
    if bridge_seq is not None:
        projected["bridge_seq"] = int(bridge_seq)
    run_seq = _coerce_int(payload.get("run_seq"))
    if run_seq is not None:
        projected["run_seq"] = int(run_seq)
    for key in ("event_name", "category", "strategy_id", "instrument_id", "symbol", "timeframe", "bar_time"):
        value = payload.get(key)
        if value not in (None, ""):
            projected[key] = value
    return projected


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


def record_bot_run_steps_batch(payloads: Sequence[Dict[str, Any]]) -> int:
    """Persist many runtime step trace rows in one DB transaction."""

    if not db.available:
        return 0
    items = list(payloads or [])
    if not items:
        return 0

    rows: List[BotRunStepRecord] = []
    now = _utcnow()
    for payload in items:
        if not isinstance(payload, dict):
            continue
        run_id = str(payload.get("run_id") or "").strip()
        step_name = str(payload.get("step_name") or "").strip()
        if not run_id or not step_name:
            continue
        started_at = _parse_optional_timestamp(payload.get("started_at"))
        ended_at = _parse_optional_timestamp(payload.get("ended_at"))
        duration_ms = _coerce_float(payload.get("duration_ms"))
        if started_at is None or ended_at is None or duration_ms is None:
            continue
        rows.append(
            BotRunStepRecord(
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
        )
    if not rows:
        return 0

    try:
        with db.session() as session:
            session.add_all(rows)
        return len(rows)
    except SQLAlchemyError as exc:
        logger.warning("bot_run_step_batch_persist_failed | rows=%s | error=%s", len(rows), exc)
        return 0


def upsert_bot_run_view_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Persist the latest derived BotLens materialization for a run scope."""

    if not db.available:
        raise RuntimeError("database is required for bot view-state persistence")
    run_id = str(payload.get("run_id") or "").strip()
    bot_id = str(payload.get("bot_id") or "").strip()
    series_key = _normalize_botlens_series_key(payload.get("series_key"))
    seq = int(payload.get("seq") or 0)
    if not run_id or not bot_id or not series_key:
        raise ValueError("run_id, bot_id and canonical series_key are required for bot view-state persistence")
    if seq <= 0:
        raise ValueError("seq must be a positive integer for bot view-state persistence")

    raw_payload = payload.get("payload")
    if not isinstance(raw_payload, dict):
        raise ValueError("payload must be a mapping")
    view_payload = _json_safe(dict(raw_payload))
    raw_schema_version = payload.get("schema_version")
    schema_version = int(raw_schema_version) if raw_schema_version is not None else 1
    if schema_version <= 0:
        raise ValueError("schema_version must be >= 1 for bot view-state persistence")
    event_time = _parse_optional_timestamp(payload.get("event_time"))
    known_at = _parse_optional_timestamp(payload.get("known_at")) or _utcnow()

    started = time.perf_counter()
    write_context = {"run_id": run_id, "bot_id": bot_id, "series_key": series_key}
    payload_bytes = payload_size_bytes(view_payload)

    def _write() -> StorageWriteOutcome:
        with db.session() as session:
            row = (
                session.execute(
                    select(BotRunViewStateRecord)
                    .where(BotRunViewStateRecord.bot_id == bot_id)
                    .where(BotRunViewStateRecord.run_id == run_id)
                    .where(BotRunViewStateRecord.series_key == series_key)
                    .limit(1)
                )
                .scalars()
                .first()
            )
            now = _utcnow()
            if row is None:
                row = BotRunViewStateRecord(
                    run_id=run_id,
                    bot_id=bot_id,
                    series_key=series_key,
                    seq=seq,
                    schema_version=schema_version,
                    payload=view_payload,
                    event_time=event_time,
                    known_at=known_at,
                    updated_at=now,
                )
                session.add(row)
                session.flush()
                return StorageWriteOutcome(
                    result=row.to_dict(),
                    rows_written=1,
                    payload_bytes=payload_bytes,
                )
            # Ignore stale or duplicate updates to preserve monotonic derived state.
            current_seq = int(row.seq or 0)
            if current_seq >= seq:
                return StorageWriteOutcome(
                    result=row.to_dict(),
                    noop_reason="duplicate_skip" if current_seq == seq else "stale_update",
                    noop_count=1,
                )
            row.seq = seq
            row.schema_version = schema_version
            row.payload = view_payload
            row.event_time = event_time
            row.known_at = known_at
            row.updated_at = now
            session.flush()
            return StorageWriteOutcome(
                result=row.to_dict(),
                rows_written=1,
                payload_bytes=payload_bytes,
            )

    outcome = _execute_write_with_retry(
        operation="upsert_bot_run_view_state",
        storage_target="bot_run_view_state",
        context=write_context,
        action=_write,
    )
    _observe_db_write_outcome(
        storage_target="bot_run_view_state",
        context=write_context,
        started=started,
        outcome=outcome,
    )
    return _project_runtime_event_row(dict(outcome.result))


def get_latest_bot_run_view_state(
    *,
    bot_id: str,
    run_id: Optional[str] = None,
    series_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not db.available:
        return None
    query = select(BotRunViewStateRecord).where(BotRunViewStateRecord.bot_id == str(bot_id))
    if series_key is not None:
        normalized_series_key = _normalize_botlens_series_key(series_key)
        if not normalized_series_key:
            return None
        query = query.where(BotRunViewStateRecord.series_key == normalized_series_key)
    else:
        query = query.where(BotRunViewStateRecord.series_key == RUN_SCOPE_KEY)
    if run_id is not None:
        query = query.where(BotRunViewStateRecord.run_id == str(run_id))
    query = query.order_by(BotRunViewStateRecord.seq.desc(), BotRunViewStateRecord.id.desc()).limit(1)
    with db.session() as session:
        row = session.execute(query).scalars().first()
        return row.to_dict() if row else None


def list_bot_run_view_states(
    *,
    bot_id: str,
    run_id: str,
) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    with db.session() as session:
        rows = (
            session.execute(
                select(BotRunViewStateRecord)
                .where(BotRunViewStateRecord.bot_id == str(bot_id))
                .where(BotRunViewStateRecord.run_id == str(run_id))
                .where(BotRunViewStateRecord.series_key.like("%|%"))
                .where(~BotRunViewStateRecord.series_key.like("%|"))
                .where(~BotRunViewStateRecord.series_key.like("|%"))
                .order_by(BotRunViewStateRecord.series_key.asc(), BotRunViewStateRecord.seq.desc(), BotRunViewStateRecord.id.desc())
            )
            .scalars()
            .all()
        )
        deduped: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            payload = row.to_dict()
            series_key = _normalize_botlens_series_key(payload.get("series_key"))
            if not series_key or series_key in deduped:
                continue
            payload["series_key"] = series_key
            deduped[series_key] = payload
        return list(deduped.values())


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

    started = time.perf_counter()
    write_context = {"run_id": run_id, "bot_id": bot_id, "event_id": event_id}
    persisted_payload = _json_safe(dict(payload.get("payload") or {}))
    persisted_payload_bytes = payload_size_bytes(persisted_payload)

    def _write() -> StorageWriteOutcome:
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
                return StorageWriteOutcome(
                    result=existing.to_dict(),
                    noop_reason="duplicate_skip",
                    noop_count=1,
                )
            existing_seq = (
                session.execute(
                    select(BotRunEventRecord)
                    .where(BotRunEventRecord.bot_id == bot_id)
                    .where(BotRunEventRecord.run_id == run_id)
                    .where(BotRunEventRecord.seq == seq)
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if existing_seq is not None:
                existing_event_id = str(existing_seq.event_id or "").strip()
                if existing_event_id != event_id:
                    raise ValueError(
                        f"seq collision for bot/run (incoming={seq}, existing_event_id={existing_event_id}, event_id={event_id})"
                    )
                return StorageWriteOutcome(
                    result=existing_seq.to_dict(),
                    noop_reason="duplicate_skip",
                    noop_count=1,
                )
            row = BotRunEventRecord(
                event_id=event_id,
                bot_id=bot_id,
                run_id=run_id,
                seq=seq,
                event_type=str(payload.get("event_type") or "state_delta"),
                critical=bool(payload.get("critical", False)),
                schema_version=schema_version,
                payload=persisted_payload,
                event_time=_parse_optional_timestamp(payload.get("event_time")),
                known_at=_parse_optional_timestamp(payload.get("known_at")) or _utcnow(),
                created_at=_utcnow(),
            )
            session.add(row)
            session.flush()
            return StorageWriteOutcome(
                result=row.to_dict(),
                rows_written=1,
                payload_bytes=persisted_payload_bytes,
            )

    try:
        outcome = _execute_write_with_retry(
            operation="record_bot_runtime_event",
            storage_target="bot_runtime_events",
            context=write_context,
            action=_write,
        )
    except ValueError as exc:
        if "seq collision" in str(exc).lower():
            _observe_seq_collision(
                storage_target="bot_runtime_events",
                context=write_context,
                exc=exc,
            )
        raise
    _observe_db_write_outcome(
        storage_target="bot_runtime_events",
        context=write_context,
        started=started,
        outcome=outcome,
    )
    return dict(outcome.result)


def record_bot_runtime_events_batch(payloads: Sequence[Dict[str, Any]]) -> int:
    if not db.available:
        raise RuntimeError("database is required for bot runtime event persistence")
    items = [dict(payload) for payload in (payloads or []) if isinstance(payload, dict)]
    if not items:
        return 0
    started = time.perf_counter()

    normalized: List[Dict[str, Any]] = []
    for payload in items:
        event_id = str(payload.get("event_id") or "").strip()
        bot_id = str(payload.get("bot_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        seq = int(payload.get("seq") or 0)
        if not event_id or not bot_id or not run_id:
            raise ValueError("event_id, bot_id and run_id are required for runtime event persistence")
        if seq <= 0:
            raise ValueError("seq must be a positive integer")
        raw_event_schema_version = payload.get("schema_version")
        schema_version = int(raw_event_schema_version) if raw_event_schema_version is not None else 1
        if schema_version <= 0:
            raise ValueError("schema_version must be >= 1 for runtime event persistence")
        normalized.append(
            {
                "event_id": event_id,
                "bot_id": bot_id,
                "run_id": run_id,
                "seq": seq,
                "event_type": str(payload.get("event_type") or "state_delta"),
                "critical": bool(payload.get("critical", False)),
                "schema_version": schema_version,
                "payload": _json_safe(dict(payload.get("payload") or {})),
                "event_time": _parse_optional_timestamp(payload.get("event_time")),
                "known_at": _parse_optional_timestamp(payload.get("known_at")) or _utcnow(),
            }
        )
    if not normalized:
        return 0

    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in normalized:
        grouped.setdefault((row["bot_id"], row["run_id"]), []).append(row)

    def _write() -> StorageWriteOutcome:
        inserted = 0
        duplicate_skips = 0
        inserted_payload_bytes = 0
        with db.session() as session:
            for (bot_id, run_id), rows in grouped.items():
                rows.sort(key=lambda item: (int(item["seq"]), str(item["event_id"])))
                event_ids = [str(item["event_id"]) for item in rows]
                seqs = [int(item["seq"]) for item in rows]
                existing_rows = (
                    session.execute(
                        select(BotRunEventRecord)
                        .where(BotRunEventRecord.bot_id == bot_id)
                        .where(BotRunEventRecord.run_id == run_id)
                        .where(
                            or_(
                                BotRunEventRecord.event_id.in_(event_ids),
                                BotRunEventRecord.seq.in_(seqs),
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                existing_by_event_id = {str(row.event_id): row for row in existing_rows}
                existing_by_seq = {int(row.seq or 0): row for row in existing_rows}
                pending: List[BotRunEventRecord] = []
                seen_seqs: set[int] = set()
                for row in rows:
                    event_id = str(row["event_id"])
                    seq = int(row["seq"])
                    existing = existing_by_event_id.get(event_id)
                    if existing is not None:
                        duplicate_skips += 1
                        continue
                    conflict = existing_by_seq.get(seq)
                    if conflict is not None:
                        raise ValueError(
                            f"seq collision for bot/run (incoming={seq}, existing_event_id={conflict.event_id}, event_id={event_id})"
                        )
                    if seq in seen_seqs:
                        raise ValueError(f"duplicate seq in runtime event batch (seq={seq}, bot_id={bot_id}, run_id={run_id})")
                    seen_seqs.add(seq)
                    pending.append(
                        BotRunEventRecord(
                            event_id=event_id,
                            bot_id=bot_id,
                            run_id=run_id,
                            seq=seq,
                            event_type=str(row["event_type"]),
                            critical=bool(row["critical"]),
                            schema_version=int(row["schema_version"]),
                            payload=dict(row["payload"]),
                            event_time=row["event_time"],
                            known_at=row["known_at"],
                            created_at=_utcnow(),
                        )
                    )
                    inserted_payload_bytes += payload_size_bytes(row["payload"])
                if pending:
                    session.add_all(pending)
                    inserted += len(pending)
        return StorageWriteOutcome(
            result=inserted,
            rows_written=inserted,
            payload_bytes=inserted_payload_bytes,
            noop_reason="duplicate_skip" if duplicate_skips > 0 else None,
            noop_count=duplicate_skips,
        )

    write_context = {
        "run_id": str(normalized[0].get("run_id") or ""),
        "bot_id": str(normalized[0].get("bot_id") or ""),
        "event_id": str(normalized[0].get("event_id") or ""),
    }
    try:
        outcome = _execute_write_with_retry(
            operation="record_bot_runtime_events_batch",
            storage_target="bot_runtime_events",
            context=write_context,
            action=_write,
        )
    except ValueError as exc:
        lowered = str(exc).lower()
        if "seq collision" in lowered or "duplicate seq" in lowered:
            _observe_seq_collision(
                storage_target="bot_runtime_events",
                context=write_context,
                exc=exc,
            )
        raise
    _observe_db_write_outcome(
        storage_target="bot_runtime_events",
        context=write_context,
        started=started,
        outcome=outcome,
    )
    return int(outcome.result)


def list_bot_runtime_events(
    *,
    bot_id: str,
    run_id: str,
    after_seq: int = 0,
    limit: int = 1000,
    event_types: Optional[Sequence[str]] = None,
    event_type_prefixes: Optional[Sequence[str]] = None,
    series_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    max_rows = max(1, min(int(limit or 1000), 5000))
    filter_event_types = [str(value).strip() for value in (event_types or []) if str(value).strip()]
    filter_prefixes = [str(value).strip() for value in (event_type_prefixes or []) if str(value).strip()]
    normalized_series_key = _normalize_botlens_series_key(series_key) if series_key is not None else None
    with db.session() as session:
        query = (
            select(BotRunEventRecord)
            .where(BotRunEventRecord.bot_id == str(bot_id))
            .where(BotRunEventRecord.run_id == str(run_id))
            .where(BotRunEventRecord.seq > int(after_seq or 0))
        )
        if normalized_series_key:
            query = query.where(BotRunEventRecord.payload["series_key"].astext == normalized_series_key)
        if filter_event_types or filter_prefixes:
            clauses = []
            if filter_event_types:
                clauses.append(BotRunEventRecord.event_type.in_(filter_event_types))
            for prefix in filter_prefixes:
                clauses.append(BotRunEventRecord.event_type.like(f"{prefix}%"))
            query = query.where(or_(*clauses))
        query = query.order_by(BotRunEventRecord.seq.asc(), BotRunEventRecord.id.asc()).limit(max_rows)
        rows = session.execute(query).scalars().all()
        return [_project_runtime_event_row(row.to_dict()) for row in rows]


def get_latest_bot_runtime_run_id(bot_id: str) -> Optional[str]:
    if not db.available:
        return None
    with db.session() as session:
        row = (
            session.execute(
                select(BotRunRecord.run_id)
                .where(BotRunRecord.bot_id == str(bot_id))
                .order_by(
                    func.coalesce(BotRunRecord.started_at, BotRunRecord.updated_at, BotRunRecord.created_at).desc(),
                    BotRunRecord.updated_at.desc(),
                    BotRunRecord.created_at.desc(),
                )
                .limit(1)
            )
            .scalars()
            .first()
        )
        if row:
            return str(row)
        lifecycle_row = (
            session.execute(
                select(BotRunLifecycleRecord.run_id)
                .where(BotRunLifecycleRecord.bot_id == str(bot_id))
                .order_by(BotRunLifecycleRecord.checkpoint_at.desc(), BotRunLifecycleRecord.updated_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        if lifecycle_row:
            return str(lifecycle_row)
        fallback = (
            session.execute(
                select(BotRunEventRecord.run_id)
                .where(BotRunEventRecord.bot_id == str(bot_id))
                .order_by(BotRunEventRecord.id.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
        return str(fallback) if fallback else None


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
        return _project_runtime_event_row(row.to_dict()) if row else None


def update_bot_runtime_status(*, bot_id: str, run_id: str, status: str, telemetry_degraded: bool = False) -> None:
    if not db.available:
        raise RuntimeError("database is required for bot status persistence")
    started = time.perf_counter()
    payloads = {
        "portal_bot_runs": payload_size_bytes({"status": status, "telemetry_degraded": telemetry_degraded}),
        "portal_bots": payload_size_bytes({"status": status}),
    }

    def _write() -> StorageWriteOutcome:
        with db.session() as session:
            bot = session.get(BotRecord, bot_id)
            if bot is None:
                raise KeyError(f"Bot {bot_id} was not found")
            bot.status = status
            bot.updated_at = _utcnow()
            run = session.get(BotRunRecord, run_id)
            if run is None:
                run = BotRunRecord(
                    run_id=run_id,
                    bot_id=bot_id,
                    bot_name=bot.name,
                    strategy_id=bot.strategy_id,
                    run_type=bot.run_type or "backtest",
                    status=status,
                    started_at=_utcnow(),
                    backtest_start=bot.backtest_start,
                    backtest_end=bot.backtest_end,
                )
                session.add(run)
            if not run.bot_name:
                run.bot_name = bot.name
            if not run.strategy_id:
                run.strategy_id = bot.strategy_id
            if not run.run_type:
                run.run_type = bot.run_type or "backtest"
            if run.backtest_start is None:
                run.backtest_start = bot.backtest_start
            if run.backtest_end is None:
                run.backtest_end = bot.backtest_end
            run.status = "telemetry_degraded" if telemetry_degraded else status
            run.updated_at = _utcnow()
            if status in {"stopped", "failed", "startup_failed", "crashed", "completed"}:
                run.ended_at = _utcnow()
        return StorageWriteOutcome(
            result=None,
            rows_written=2,
            payload_bytes=sum(payloads.values()),
        )

    outcome = _execute_write_with_retry(
        operation="update_bot_runtime_status",
        storage_target="portal_bot_runs",
        context={"run_id": run_id, "bot_id": bot_id, "status": status},
        action=_write,
    )
    _observe_db_write_outcome(
        storage_target="portal_bot_runs",
        context={"run_id": run_id, "bot_id": bot_id, "status": status},
        started=started,
        outcome=StorageWriteOutcome(
            result=None,
            rows_written=1,
            payload_bytes=payloads["portal_bot_runs"],
        ),
    )
    _observe_db_write_outcome(
        storage_target="portal_bots",
        context={"run_id": run_id, "bot_id": bot_id, "status": status},
        started=started,
        outcome=StorageWriteOutcome(
            result=None,
            rows_written=1,
            payload_bytes=payloads["portal_bots"],
        ),
    )
