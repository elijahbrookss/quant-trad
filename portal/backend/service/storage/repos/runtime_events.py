"""Runtime-event and BotLens state storage repository module."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from engines.bot_runtime.core.series_identity import normalize_series_key as normalize_public_series_key
from engines.bot_runtime.runtime.event_types import RUNTIME_PREFIX
from sqlalchemy import and_, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ...observability import payload_size_bytes
from ...bots.botlens_candle_continuity import continuity_summary_from_runtime_event_rows, emit_candle_continuity_summary
from ...bots.botlens_contract import BRIDGE_BOOTSTRAP_KIND, RUN_SCOPE_KEY
from ...bots.botlens_domain_events import (
    BOTLENS_DOMAIN_PREFIX,
    botlens_domain_event_type,
    deserialize_botlens_domain_event,
    serialize_botlens_domain_event,
)

from ._shared import (
    BotRecord,
    BotRunEventRecord,
    BotRunLifecycleRecord,
    BotRunRecord,
    BotRunStepRecord,
    SQLAlchemyError,
    StorageWriteOutcome,
    _STORAGE_OBSERVER,
    _coerce_float,
    _coerce_int,
    _execute_write_with_retry,
    _json_safe,
    _observe_db_write_outcome,
    _payload_size_bucket,
    _parse_optional_timestamp,
    _utcnow,
    db,
    func,
    logger,
    select,
)

_OBSERVER = _STORAGE_OBSERVER
_RUNTIME_EVENT_ID_CONFLICT_CONSTRAINT = "uq_portal_bot_run_events_event_id"


def _normalize_botlens_series_key(value: Any) -> str:
    if str(value or "").strip() == RUN_SCOPE_KEY:
        return RUN_SCOPE_KEY
    return normalize_public_series_key(value)


def _runtime_event_context(payload: Mapping[str, Any] | None) -> Dict[str, Any]:
    root = dict(payload or {}) if isinstance(payload, Mapping) else {}
    context = root.get("context") if isinstance(root.get("context"), Mapping) else {}
    series_key = _normalize_botlens_series_key(root.get("series_key") or context.get("series_key"))
    worker_id = str(root.get("worker_id") or context.get("worker_id") or "").strip() or None
    return {
        "series_key": series_key or None,
        "worker_id": worker_id,
    }


def _optional_text(value: Any, *, uppercase: bool = False) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text.upper() if uppercase else text


def _runtime_event_payload_hot_fields(
    *,
    event_type: str,
    payload: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    root = dict(payload or {}) if isinstance(payload, Mapping) else {}
    runtime_context = root.get("context") if isinstance(root.get("context"), Mapping) else {}
    source = runtime_context if event_type.startswith((RUNTIME_PREFIX, BOTLENS_DOMAIN_PREFIX)) else root
    bar_time = source.get("bar_ts") or source.get("bar_time")
    if bar_time in (None, "") and isinstance(source.get("candle"), Mapping):
        bar_time = source["candle"].get("time")
    return {
        "event_name": _optional_text(root.get("event_name"), uppercase=True),
        "series_key": _normalize_botlens_series_key(root.get("series_key") or runtime_context.get("series_key")) or None,
        "correlation_id": _optional_text(root.get("correlation_id")),
        "root_id": _optional_text(root.get("root_id")),
        "bar_time": _parse_optional_timestamp(bar_time),
        "instrument_id": _optional_text(source.get("instrument_id")),
        "symbol": _optional_text(source.get("symbol")),
        "timeframe": _optional_text(source.get("timeframe")),
        "signal_id": _optional_text(source.get("signal_id")),
        "decision_id": _optional_text(source.get("decision_id")),
        "trade_id": _optional_text(source.get("trade_id")),
        "attempt_id": _optional_text(source.get("attempt_id")),
        "order_request_id": _optional_text(source.get("order_request_id")),
        "entry_request_id": _optional_text(source.get("entry_request_id")),
        "settlement_attempt_id": _optional_text(source.get("settlement_attempt_id")),
        "blocking_trade_id": _optional_text(source.get("blocking_trade_id")),
        "reason_code": _optional_text(source.get("reason_code")),
        "bridge_session_id": _optional_text(source.get("bridge_session_id")),
        "bridge_seq": _coerce_int(source.get("bridge_seq")),
        "run_seq": _coerce_int(source.get("run_seq")),
        "strategy_id": _optional_text(source.get("strategy_id")),
        "category": _optional_text(source.get("category")),
    }


def _validate_signal_identity_fields(
    *,
    event_id: str,
    event_type: str,
    hot_fields: Mapping[str, Any],
) -> None:
    event_name = str(hot_fields.get("event_name") or "").strip().upper()
    signal_id = _optional_text(hot_fields.get("signal_id"))
    decision_id = _optional_text(hot_fields.get("decision_id"))
    if event_name == "SIGNAL_EMITTED" and signal_id is None:
        raise ValueError(
            "runtime event persistence requires signal_id for SIGNAL_EMITTED "
            f"event_id={event_id or '<missing>'} event_type={event_type or '<missing>'}"
        )
    if signal_id and decision_id and signal_id == decision_id:
        raise ValueError(
            "runtime event persistence requires signal_id != decision_id "
            f"event_id={event_id or '<missing>'} event_type={event_type or '<missing>'} "
            f"value={signal_id}"
        )


def _iso8601_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat() + "Z"
    text = str(value).strip()
    return text or None


def _runtime_event_row_to_dict(row: BotRunEventRecord | Mapping[str, Any]) -> Dict[str, Any]:
    if isinstance(row, BotRunEventRecord):
        return row.to_dict()
    raw = dict(row or {})
    payload = raw.get("payload") if isinstance(raw.get("payload"), Mapping) else {}
    result = {
        "id": int(raw.get("id") or 0),
        "event_id": _optional_text(raw.get("event_id")) or "",
        "bot_id": _optional_text(raw.get("bot_id")) or "",
        "run_id": _optional_text(raw.get("run_id")) or "",
        "seq": int(raw.get("seq") or 0),
        "event_type": _optional_text(raw.get("event_type")) or "",
        "critical": bool(raw.get("critical", False)),
        "schema_version": int(raw.get("schema_version") or 1),
        "payload": dict(payload),
        "event_name": _optional_text(raw.get("event_name"), uppercase=True),
        "series_key": _normalize_botlens_series_key(raw.get("series_key")) or None,
        "correlation_id": _optional_text(raw.get("correlation_id")),
        "root_id": _optional_text(raw.get("root_id")),
        "bar_time": _iso8601_or_none(raw.get("bar_time")),
        "instrument_id": _optional_text(raw.get("instrument_id")),
        "symbol": _optional_text(raw.get("symbol")),
        "timeframe": _optional_text(raw.get("timeframe")),
        "signal_id": _optional_text(raw.get("signal_id")),
        "decision_id": _optional_text(raw.get("decision_id")),
        "trade_id": _optional_text(raw.get("trade_id")),
        "reason_code": _optional_text(raw.get("reason_code")),
        "event_time": _iso8601_or_none(raw.get("event_time")),
        "known_at": _iso8601_or_none(raw.get("known_at")) or _iso8601_or_none(_utcnow()),
        "created_at": _iso8601_or_none(raw.get("created_at")) or _iso8601_or_none(_utcnow()),
    }
    return result


_WRITE_SOURCE_VALUES = frozenset({"ingest", "producer", "replay", "retry", "bootstrap", "projector", "transport", "unknown"})


def _normalize_write_source_reason(
    value: Any,
    *,
    message_kind: Any = None,
    pipeline_stage: Any = None,
) -> str:
    explicit = str(value or "").strip().lower()
    if explicit in _WRITE_SOURCE_VALUES:
        return explicit
    if "producer" in explicit:
        return "producer"
    metric_kind = str(message_kind or "").strip().lower()
    stage = str(pipeline_stage or "").strip().lower()
    if "replay" in explicit or "replay" in stage or metric_kind == "replay":
        return "replay"
    if "retry" in explicit or "retry" in stage:
        return "retry"
    if "projector" in explicit or "projector" in stage:
        return "projector"
    if "transport" in explicit or "transport" in stage:
        return "transport"
    if "bootstrap" in explicit or "bootstrap" in stage or "bootstrap" in metric_kind:
        return "bootstrap"
    if explicit or stage or metric_kind:
        return "ingest"
    return "unknown"


def _classify_duplicate_reason(
    *,
    source_reason: str,
    existing_event_id: bool = False,
    same_batch_event_id: bool = False,
) -> str:
    if same_batch_event_id:
        return "same_batch_event_id_duplicate"
    if source_reason == "replay":
        return "replay_duplicate"
    if source_reason == "retry":
        return "retry_duplicate"
    if source_reason == "bootstrap":
        return "bootstrap_reemit_duplicate"
    if source_reason == "projector":
        return "projector_rebuild_duplicate"
    if source_reason == "transport":
        return "transport_reemit_duplicate"
    if existing_event_id:
        return "already_persisted_same_event_id"
    return "unknown_duplicate"


def _increment_reason(counter: Dict[str, int], reason: str, value: int = 1) -> None:
    normalized = str(reason or "").strip()
    if not normalized:
        normalized = "unknown_duplicate"
    counter[normalized] = int(counter.get(normalized, 0)) + max(int(value), 0)


def _walk_payload_shape(value: Any, *, path: str, stats: Dict[str, Any]) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            child_bytes = payload_size_bytes(child)
            stats["json_field_count"] += 1
            if child_bytes > stats["largest_json_field_bytes"]:
                stats["largest_json_field_bytes"] = child_bytes
                stats["largest_json_field_name"] = child_path
            _walk_payload_shape(child, path=child_path, stats=stats)
        return
    if isinstance(value, list):
        list_path = f"{path}[]" if path else "[]"
        list_bytes = payload_size_bytes(value)
        if list_bytes > stats["largest_json_field_bytes"]:
            stats["largest_json_field_bytes"] = list_bytes
            stats["largest_json_field_name"] = list_path
        for child in value:
            _walk_payload_shape(child, path=list_path, stats=stats)


def _payload_shape_summary(payloads: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    total_payload_bytes = 0
    top_level_key_count = 0
    stats: Dict[str, Any] = {
        "largest_json_field_name": None,
        "largest_json_field_bytes": 0,
        "json_field_count": 0,
    }
    for payload in payloads:
        payload_mapping = dict(payload or {})
        total_payload_bytes += payload_size_bytes(payload_mapping)
        top_level_key_count = max(top_level_key_count, len(payload_mapping))
        _walk_payload_shape(payload_mapping, path="payload", stats=stats)
    row_count = max(len(payloads), 1)
    bytes_per_row = float(total_payload_bytes) / float(row_count)
    payload_size_bucket = _payload_size_bucket(total_payload_bytes)
    return {
        "payload_bytes": total_payload_bytes,
        "bytes_per_row": bytes_per_row,
        "largest_json_field_name": stats["largest_json_field_name"],
        "largest_json_field_bytes": int(stats["largest_json_field_bytes"]),
        "json_field_count": int(stats["json_field_count"]),
        "top_level_key_count": int(top_level_key_count),
        "blob_bytes": 0,
        "payload_size_bucket": payload_size_bucket,
        "has_large_payload": payload_size_bucket == "large",
    }


def _resolved_event_name(rows: Sequence[Mapping[str, Any]]) -> str | None:
    names = {
        str(payload.get("event_name") or "").strip().upper()
        for row in rows
        for payload in [row.get("payload") if isinstance(row.get("payload"), Mapping) else {}]
        if str(payload.get("event_name") or "").strip()
    }
    if len(names) == 1:
        return next(iter(names))
    return None


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


def _project_runtime_event_row(
    row: Mapping[str, Any],
    *,
    canonicalize_botlens_payloads: bool = False,
) -> Dict[str, Any]:
    projected = dict(row or {})
    event_type = str(projected.get("event_type") or "")
    payload = projected.get("payload") if isinstance(projected.get("payload"), Mapping) else {}
    if canonicalize_botlens_payloads and event_type.startswith(BOTLENS_DOMAIN_PREFIX):
        payload = serialize_botlens_domain_event(deserialize_botlens_domain_event(payload))
        projected["payload"] = payload
    runtime_context = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
    series_key = _normalize_botlens_series_key(projected.get("series_key"))
    if series_key:
        projected["series_key"] = series_key
    bridge_session_id = _optional_text(
        projected.get("bridge_session_id")
        or payload.get("bridge_session_id")
        or runtime_context.get("bridge_session_id")
    )
    if bridge_session_id:
        projected["bridge_session_id"] = bridge_session_id
    bridge_seq = _coerce_int(projected.get("bridge_seq"))
    if bridge_seq is None:
        bridge_seq = _coerce_int(payload.get("bridge_seq"))
    if bridge_seq is None:
        bridge_seq = _coerce_int(runtime_context.get("bridge_seq"))
    if bridge_seq is not None:
        projected["bridge_seq"] = int(bridge_seq)
    run_seq = _coerce_int(projected.get("run_seq"))
    if run_seq is None:
        run_seq = _coerce_int(payload.get("run_seq"))
    if run_seq is None:
        run_seq = _coerce_int(runtime_context.get("run_seq"))
    if run_seq is not None:
        projected["run_seq"] = int(run_seq)
    category = projected.get("category")
    if category in (None, ""):
        category = payload.get("category")
    if category in (None, ""):
        category = runtime_context.get("category")
    if category not in (None, ""):
        projected["category"] = category
    if projected.get("bar_time") not in (None, ""):
        bar_time = projected.get("bar_time")
        if isinstance(bar_time, datetime):
            projected["bar_time"] = bar_time.isoformat() + "Z"
    return projected


def _row_cursor_tuple(row: Mapping[str, Any]) -> tuple[int, int]:
    return (int(row.get("seq") or 0), int(row.get("id") or 0))


def _matches_runtime_event_filters(
    row: Mapping[str, Any],
    *,
    filter_event_types: Sequence[str],
    filter_prefixes: Sequence[str],
    filter_event_names: Sequence[str],
    normalized_series_key: Optional[str],
    typed_filter_values: Mapping[str, str],
) -> bool:
    event_type = str(row.get("event_type") or "")
    if filter_event_types or filter_prefixes:
        if event_type not in filter_event_types and not any(event_type.startswith(prefix) for prefix in filter_prefixes):
            return False
    if filter_event_names:
        if str(row.get("event_name") or "").strip().upper() not in filter_event_names:
            return False
    if normalized_series_key:
        if _normalize_botlens_series_key(row.get("series_key")) != normalized_series_key:
            return False
    for field_name, expected_value in typed_filter_values.items():
        if not expected_value:
            continue
        if str(row.get(field_name) or "").strip() != expected_value:
            return False
    return True


def _allows_duplicate_seq(event_type: str) -> bool:
    return str(event_type or "").startswith(BOTLENS_DOMAIN_PREFIX)


def _normalize_runtime_event_payload(*, event_type: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
    normalized = _json_safe(dict(payload or {}))
    if not str(event_type or "").startswith(BOTLENS_DOMAIN_PREFIX):
        return normalized
    event = deserialize_botlens_domain_event(normalized)
    canonical_event_type = botlens_domain_event_type(event.event_name)
    if str(event_type) != canonical_event_type:
        raise ValueError(
            f"botlens domain event_type mismatch: expected {canonical_event_type}, got {event_type}"
        )
    return serialize_botlens_domain_event(event)


def _runtime_event_row_values(
    *,
    event_id: str,
    bot_id: str,
    run_id: str,
    seq: int,
    event_type: str,
    critical: bool,
    schema_version: int,
    payload: Mapping[str, Any],
    event_time: datetime | None,
    known_at: datetime,
) -> Dict[str, Any]:
    hot_fields = _runtime_event_payload_hot_fields(event_type=event_type, payload=payload)
    _validate_signal_identity_fields(
        event_id=event_id,
        event_type=event_type,
        hot_fields=hot_fields,
    )
    return {
        "event_id": event_id,
        "bot_id": bot_id,
        "run_id": run_id,
        "seq": seq,
        "event_type": event_type,
        "critical": critical,
        "schema_version": schema_version,
        "payload": dict(payload),
        "event_name": hot_fields.get("event_name"),
        "series_key": hot_fields.get("series_key"),
        "correlation_id": hot_fields.get("correlation_id"),
        "root_id": hot_fields.get("root_id"),
        "bar_time": hot_fields.get("bar_time"),
        "instrument_id": hot_fields.get("instrument_id"),
        "symbol": hot_fields.get("symbol"),
        "timeframe": hot_fields.get("timeframe"),
        "signal_id": hot_fields.get("signal_id"),
        "decision_id": hot_fields.get("decision_id"),
        "trade_id": hot_fields.get("trade_id"),
        "reason_code": hot_fields.get("reason_code"),
        "event_time": event_time,
        "known_at": known_at,
        "created_at": _utcnow(),
    }


def _runtime_event_insert_statement(*, rows: Sequence[Mapping[str, Any]], returning_columns: Sequence[Any]) -> Any:
    return (
        pg_insert(BotRunEventRecord.__table__)
        .values([dict(row) for row in rows])
        .on_conflict_do_nothing(constraint=_RUNTIME_EVENT_ID_CONFLICT_CONSTRAINT)
        .returning(*returning_columns)
    )


def _existing_runtime_event_rows_for_seq(
    session: Any,
    *,
    bot_id: str,
    run_id: str,
    seqs: Sequence[int],
) -> List[BotRunEventRecord]:
    normalized_seqs = sorted({int(seq) for seq in seqs if int(seq) > 0})
    if not normalized_seqs:
        return []
    return (
        session.execute(
            select(BotRunEventRecord)
            .where(BotRunEventRecord.bot_id == bot_id)
            .where(BotRunEventRecord.run_id == run_id)
            .where(BotRunEventRecord.seq.in_(normalized_seqs))
        )
        .scalars()
        .all()
    )


def _first_conflicting_seq_row(
    *,
    event_id: str,
    event_type: str,
    conflicts: Sequence[BotRunEventRecord],
) -> BotRunEventRecord | None:
    return next(
        (
            existing
            for existing in conflicts
            if str(existing.event_id or "").strip() != event_id
            and not (
                _allows_duplicate_seq(event_type)
                and _allows_duplicate_seq(str(existing.event_type or ""))
            )
        ),
        None,
    )


def _existing_runtime_event_by_event_id(session: Any, *, event_id: str) -> BotRunEventRecord | None:
    return (
        session.execute(
            select(BotRunEventRecord)
            .where(BotRunEventRecord.event_id == event_id)
            .limit(1)
        )
        .scalars()
        .first()
    )


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


def record_bot_runtime_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not db.available:
        raise RuntimeError("database is required for bot runtime event persistence")
    event_id = str(payload.get("event_id") or "").strip()
    bot_id = str(payload.get("bot_id") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    if not event_id or not bot_id or not run_id:
        raise ValueError("event_id, bot_id and run_id are required for runtime event persistence")
    seq = int(payload.get("seq") or 0)
    event_type = str(payload.get("event_type") or "state_delta")
    if seq <= 0:
        raise ValueError("seq must be a positive integer")
    raw_event_schema_version = payload.get("schema_version")
    schema_version = int(raw_event_schema_version) if raw_event_schema_version is not None else 1
    if schema_version <= 0:
        raise ValueError("schema_version must be >= 1 for runtime event persistence")

    started = time.perf_counter()
    payload_build_started = time.perf_counter()
    persisted_payload = _normalize_runtime_event_payload(
        event_type=event_type,
        payload=payload.get("payload") if isinstance(payload.get("payload"), Mapping) else {},
    )
    payload_shape = _payload_shape_summary([persisted_payload])
    payload_build_ms = max((time.perf_counter() - payload_build_started) * 1000.0, 0.0)
    payload_context = _runtime_event_context(persisted_payload)
    write_context = {
        "run_id": run_id,
        "bot_id": bot_id,
        "event_id": event_id,
        "series_key": payload_context.get("series_key"),
        "worker_id": payload_context.get("worker_id"),
        "message_kind": event_type,
        "pipeline_stage": "runtime_event_persist",
        "batch_size": 1,
        "event_name": _resolved_event_name([{"payload": persisted_payload}]),
        "source_emitter": str(payload.get("source_emitter") or "").strip() or None,
        "source_reason": _normalize_write_source_reason(
            payload.get("source_reason"),
            message_kind=event_type,
            pipeline_stage="runtime_event_persist",
        ),
        "conflict_strategy": "seq_guard_then_insert_on_conflict_do_nothing",
        "conflict_target_name": _RUNTIME_EVENT_ID_CONFLICT_CONSTRAINT,
        "write_contract": "insert_first_event_id_dedupe",
        "precheck_mode": "seq_guard_only",
    }
    db_round_trip_ms = {"value": None}
    row_values = _runtime_event_row_values(
        event_id=event_id,
        bot_id=bot_id,
        run_id=run_id,
        seq=seq,
        event_type=event_type,
        critical=bool(payload.get("critical", False)),
        schema_version=schema_version,
        payload=persisted_payload,
        event_time=_parse_optional_timestamp(payload.get("event_time")),
        known_at=_parse_optional_timestamp(payload.get("known_at")) or _utcnow(),
    )

    def _write() -> StorageWriteOutcome:
        write_started = time.perf_counter()
        with db.session() as session:
            existing_seq_rows = _existing_runtime_event_rows_for_seq(
                session,
                bot_id=bot_id,
                run_id=run_id,
                seqs=[seq],
            )
            conflicting_seq_row = _first_conflicting_seq_row(
                event_id=event_id,
                event_type=event_type,
                conflicts=existing_seq_rows,
            )
            if conflicting_seq_row is not None and _existing_runtime_event_by_event_id(session, event_id=event_id) is None:
                raise ValueError(
                    f"seq collision for bot/run (incoming={seq}, existing_event_id={conflicting_seq_row.event_id}, event_id={event_id})"
                )
            inserted = (
                session.execute(
                    _runtime_event_insert_statement(
                        rows=[row_values],
                        returning_columns=tuple(BotRunEventRecord.__table__.c),
                    )
                )
                .mappings()
                .first()
            )
            if inserted is not None:
                db_round_trip_ms["value"] = max((time.perf_counter() - write_started) * 1000.0, 0.0)
                return StorageWriteOutcome(
                    result=_runtime_event_row_to_dict(inserted),
                    rows_written=1,
                    attempted_rows=1,
                    inserted_rows=1,
                    payload_build_ms=payload_build_ms,
                    db_round_trip_ms=db_round_trip_ms["value"],
                    **payload_shape,
                )
            existing = (
                session.execute(
                    select(BotRunEventRecord)
                    .where(BotRunEventRecord.event_id == event_id)
                    .limit(1)
                )
                .scalars()
                .first()
            )
            db_round_trip_ms["value"] = max((time.perf_counter() - write_started) * 1000.0, 0.0)
            return StorageWriteOutcome(
                result=_runtime_event_row_to_dict(existing or row_values),
                noop_reason="duplicate_skip",
                noop_count=1,
                attempted_rows=1,
                duplicate_rows=1,
                payload_build_ms=payload_build_ms,
                db_round_trip_ms=db_round_trip_ms["value"],
                duplicate_reasons={
                    _classify_duplicate_reason(
                        source_reason=str(write_context.get("source_reason") or ""),
                        existing_event_id=True,
                    ): 1
                },
                **payload_shape,
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
            write_context["conflict_target_name"] = "bot_run_seq_guard"
            _observe_seq_collision(
                storage_target="bot_runtime_events",
                context=write_context,
                exc=exc,
            )
        _observe_db_write_outcome(
            storage_target="bot_runtime_events",
            context=write_context,
            started=started,
            outcome=StorageWriteOutcome(
                result={},
                attempted_rows=1,
                failed_rows=1,
                payload_build_ms=payload_build_ms,
                db_round_trip_ms=db_round_trip_ms["value"],
                **payload_shape,
            ),
            error=exc,
        )
        raise
    _observe_db_write_outcome(
        storage_target="bot_runtime_events",
        context=write_context,
        started=started,
        outcome=outcome,
    )
    return dict(outcome.result)


def record_bot_runtime_events_batch(
    payloads: Sequence[Dict[str, Any]],
    *,
    context: Mapping[str, Any] | None = None,
) -> int:
    if not db.available:
        raise RuntimeError("database is required for bot runtime event persistence")
    items = [dict(payload) for payload in (payloads or []) if isinstance(payload, dict)]
    if not items:
        return 0
    started = time.perf_counter()
    payload_build_started = time.perf_counter()

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
        normalized_payload = _normalize_runtime_event_payload(
            event_type=str(payload.get("event_type") or "state_delta"),
            payload=payload.get("payload") if isinstance(payload.get("payload"), Mapping) else {},
        )
        normalized.append(
            {
                "event_id": event_id,
                "bot_id": bot_id,
                "run_id": run_id,
                "seq": seq,
                "event_type": str(payload.get("event_type") or "state_delta"),
                "critical": bool(payload.get("critical", False)),
                "schema_version": schema_version,
                "payload": normalized_payload,
                "event_time": _parse_optional_timestamp(payload.get("event_time")),
                "known_at": _parse_optional_timestamp(payload.get("known_at")) or _utcnow(),
            }
        )
    if not normalized:
        return 0

    payload_shape = _payload_shape_summary(
        [row.get("payload") if isinstance(row.get("payload"), Mapping) else {} for row in normalized]
    )
    payload_build_ms = max((time.perf_counter() - payload_build_started) * 1000.0, 0.0)

    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in normalized:
        grouped.setdefault((row["bot_id"], row["run_id"]), []).append(row)

    raw_context = dict(context or {})
    payload_context = _runtime_event_context(normalized[0].get("payload"))
    message_kind = str(raw_context.get("message_kind") or normalized[0].get("event_type") or "").strip() or None
    pipeline_stage = str(raw_context.get("pipeline_stage") or "runtime_event_persist").strip()
    source_reason = _normalize_write_source_reason(
        raw_context.get("source_reason"),
        message_kind=message_kind,
        pipeline_stage=pipeline_stage,
    )
    write_context = {
        "run_id": str(normalized[0].get("run_id") or ""),
        "bot_id": str(normalized[0].get("bot_id") or ""),
        "event_id": str(normalized[0].get("event_id") or ""),
        "series_key": _normalize_botlens_series_key(raw_context.get("series_key") or payload_context.get("series_key")) or None,
        "worker_id": str(raw_context.get("worker_id") or payload_context.get("worker_id") or "").strip() or None,
        "message_kind": message_kind,
        "pipeline_stage": pipeline_stage,
        "batch_size": int(raw_context.get("batch_size") or len(normalized)),
        "event_name": _resolved_event_name(normalized),
        "source_emitter": str(raw_context.get("source_emitter") or "").strip() or None,
        "source_reason": source_reason,
        "conflict_strategy": "seq_guard_then_insert_on_conflict_do_nothing",
        "conflict_target_name": _RUNTIME_EVENT_ID_CONFLICT_CONSTRAINT,
        "write_contract": "insert_first_event_id_dedupe",
        "precheck_mode": "seq_guard_only",
    }
    continuity_summary = continuity_summary_from_runtime_event_rows(
        normalized,
        series_key=write_context.get("series_key"),
        source_reason=source_reason,
    )
    if continuity_summary.candle_count > 0 and (
        continuity_summary.candle_count > 1
        or continuity_summary.detected_gap_count > 0
        or message_kind == BRIDGE_BOOTSTRAP_KIND
    ):
        emit_candle_continuity_summary(
            _OBSERVER,
            stage="botlens_ingest_admission",
            summary=continuity_summary,
            bot_id=write_context.get("bot_id"),
            run_id=write_context.get("run_id"),
            series_key=write_context.get("series_key"),
            message_kind=message_kind,
            storage_target="bot_runtime_events",
            source_reason=source_reason,
            boundary_name="ingest_admission",
            extra={
                "batch_size": int(write_context.get("batch_size") or 0),
                "event_name": write_context.get("event_name"),
            },
        )
    db_round_trip_ms = {"value": None}

    def _write() -> StorageWriteOutcome:
        write_started = time.perf_counter()
        duplicate_skips = 0
        duplicate_reason_counts: Dict[str, int] = defaultdict(int)
        pending_rows: List[Dict[str, Any]] = []
        with db.session() as session:
            for (bot_id, run_id), rows in grouped.items():
                rows.sort(key=lambda item: (int(item["seq"]), str(item["event_id"])))
                seqs = [int(item["seq"]) for item in rows]
                existing_rows = _existing_runtime_event_rows_for_seq(
                    session,
                    bot_id=bot_id,
                    run_id=run_id,
                    seqs=seqs,
                )
                existing_by_seq: Dict[int, List[BotRunEventRecord]] = {}
                for existing_row in existing_rows:
                    existing_by_seq.setdefault(int(existing_row.seq or 0), []).append(existing_row)
                pending_event_ids: set[str] = set()
                seen_seq_types: Dict[int, List[str]] = {}
                for row in rows:
                    event_id = str(row["event_id"])
                    seq = int(row["seq"])
                    event_type = str(row["event_type"])
                    if event_id in pending_event_ids:
                        duplicate_skips += 1
                        _increment_reason(
                            duplicate_reason_counts,
                            _classify_duplicate_reason(source_reason=source_reason, same_batch_event_id=True),
                        )
                        continue
                    conflicts = existing_by_seq.get(seq, [])
                    conflicting_seq_row = _first_conflicting_seq_row(
                        event_id=event_id,
                        event_type=event_type,
                        conflicts=conflicts,
                    )
                    if conflicting_seq_row is not None and _existing_runtime_event_by_event_id(session, event_id=event_id) is None:
                        raise ValueError(
                            f"seq collision for bot/run (incoming={seq}, existing_event_id={conflicting_seq_row.event_id}, event_id={event_id})"
                        )
                    if seq in seen_seq_types and any(
                        not (_allows_duplicate_seq(event_type) and _allows_duplicate_seq(existing_type))
                        for existing_type in seen_seq_types[seq]
                    ):
                        raise ValueError(f"duplicate seq in runtime event batch (seq={seq}, bot_id={bot_id}, run_id={run_id})")
                    seen_seq_types.setdefault(seq, []).append(event_type)
                    pending_rows.append(
                        _runtime_event_row_values(
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
                        )
                    )
                    pending_event_ids.add(event_id)
            inserted_event_ids: set[str] = set()
            if pending_rows:
                inserted_event_ids = {
                    str(value)
                    for value in session.execute(
                        _runtime_event_insert_statement(
                            rows=pending_rows,
                            returning_columns=(BotRunEventRecord.__table__.c.event_id,),
                        )
                    )
                    .scalars()
                    .all()
                    if str(value or "").strip()
                }
        inserted = len(inserted_event_ids)
        conflict_duplicates = max(len(pending_rows) - inserted, 0)
        if conflict_duplicates > 0:
            duplicate_skips += conflict_duplicates
            _increment_reason(
                duplicate_reason_counts,
                _classify_duplicate_reason(source_reason=source_reason, existing_event_id=True),
                conflict_duplicates,
            )
        db_round_trip_ms["value"] = max((time.perf_counter() - write_started) * 1000.0, 0.0)
        return StorageWriteOutcome(
            result=inserted,
            rows_written=inserted,
            attempted_rows=len(normalized),
            inserted_rows=inserted,
            duplicate_rows=duplicate_skips,
            noop_reason="duplicate_skip" if duplicate_skips > 0 else None,
            noop_count=duplicate_skips,
            payload_build_ms=payload_build_ms,
            db_round_trip_ms=db_round_trip_ms["value"],
            duplicate_reasons=duplicate_reason_counts,
            **payload_shape,
        )
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
            write_context["conflict_target_name"] = "bot_run_seq_guard"
            _observe_seq_collision(
                storage_target="bot_runtime_events",
                context=write_context,
                exc=exc,
            )
        _observe_db_write_outcome(
            storage_target="bot_runtime_events",
            context=write_context,
            started=started,
            outcome=StorageWriteOutcome(
                result=0,
                attempted_rows=len(normalized),
                failed_rows=len(normalized),
                payload_build_ms=payload_build_ms,
                db_round_trip_ms=db_round_trip_ms["value"],
                **payload_shape,
            ),
            error=exc,
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
    after_row_id: int = 0,
    limit: int = 1000,
    event_types: Optional[Sequence[str]] = None,
    event_type_prefixes: Optional[Sequence[str]] = None,
    event_names: Optional[Sequence[str]] = None,
    series_key: Optional[str] = None,
    root_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    signal_id: Optional[str] = None,
    decision_id: Optional[str] = None,
    trade_id: Optional[str] = None,
    bar_time_gte: Any = None,
    bar_time_lt: Any = None,
    canonicalize_botlens_payloads: bool = False,
) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    max_rows = max(1, min(int(limit or 1000), 5000))
    scan_cursor_seq = max(0, int(after_seq or 0))
    scan_cursor_row_id = max(0, int(after_row_id or 0))
    filter_event_types = [str(value).strip() for value in (event_types or []) if str(value).strip()]
    filter_prefixes = [str(value).strip() for value in (event_type_prefixes or []) if str(value).strip()]
    filter_event_names = [str(value).strip().upper() for value in (event_names or []) if str(value).strip()]
    normalized_series_key = _normalize_botlens_series_key(series_key) if series_key is not None else None
    normalized_bar_time_gte = _parse_optional_timestamp(bar_time_gte)
    normalized_bar_time_lt = _parse_optional_timestamp(bar_time_lt)
    typed_filter_values = {
        "root_id": _optional_text(root_id) or "",
        "correlation_id": _optional_text(correlation_id) or "",
        "signal_id": _optional_text(signal_id) or "",
        "decision_id": _optional_text(decision_id) or "",
        "trade_id": _optional_text(trade_id) or "",
    }
    matched_rows: List[Dict[str, Any]] = []
    with db.session() as session:
        while len(matched_rows) < max_rows:
            remaining = max_rows - len(matched_rows)
            batch_limit = max(1, min(remaining, 5000))
            query = (
                select(BotRunEventRecord)
                .where(BotRunEventRecord.bot_id == str(bot_id))
                .where(BotRunEventRecord.run_id == str(run_id))
            )
            if scan_cursor_seq > 0 or scan_cursor_row_id > 0:
                query = query.where(
                    or_(
                        BotRunEventRecord.seq > scan_cursor_seq,
                        and_(
                            BotRunEventRecord.seq == scan_cursor_seq,
                            BotRunEventRecord.id > scan_cursor_row_id,
                        ),
                    )
                )
            if normalized_series_key:
                query = query.where(BotRunEventRecord.series_key == normalized_series_key)
            if filter_event_names:
                query = query.where(BotRunEventRecord.event_name.in_(filter_event_names))
            if typed_filter_values["root_id"]:
                query = query.where(BotRunEventRecord.root_id == typed_filter_values["root_id"])
            if typed_filter_values["correlation_id"]:
                query = query.where(BotRunEventRecord.correlation_id == typed_filter_values["correlation_id"])
            if typed_filter_values["signal_id"]:
                query = query.where(BotRunEventRecord.signal_id == typed_filter_values["signal_id"])
            if typed_filter_values["decision_id"]:
                query = query.where(BotRunEventRecord.decision_id == typed_filter_values["decision_id"])
            if typed_filter_values["trade_id"]:
                query = query.where(BotRunEventRecord.trade_id == typed_filter_values["trade_id"])
            if normalized_bar_time_gte is not None:
                query = query.where(BotRunEventRecord.bar_time >= normalized_bar_time_gte)
            if normalized_bar_time_lt is not None:
                query = query.where(BotRunEventRecord.bar_time < normalized_bar_time_lt)
            if filter_event_types or filter_prefixes:
                clauses = []
                if filter_event_types:
                    clauses.append(BotRunEventRecord.event_type.in_(filter_event_types))
                for prefix in filter_prefixes:
                    clauses.append(BotRunEventRecord.event_type.like(f"{prefix}%"))
                query = query.where(or_(*clauses))
            query = query.order_by(BotRunEventRecord.seq.asc(), BotRunEventRecord.id.asc()).limit(batch_limit)
            rows = session.execute(query).scalars().all()
            if not rows:
                break
            previous_cursor = (scan_cursor_seq, scan_cursor_row_id)
            for row in rows:
                projected = _project_runtime_event_row(
                    row.to_dict(),
                    canonicalize_botlens_payloads=canonicalize_botlens_payloads,
                )
                if _matches_runtime_event_filters(
                    projected,
                    filter_event_types=filter_event_types,
                    filter_prefixes=filter_prefixes,
                    filter_event_names=filter_event_names,
                    normalized_series_key=normalized_series_key,
                    typed_filter_values=typed_filter_values,
                ):
                    matched_rows.append(projected)
                    if len(matched_rows) >= max_rows:
                        break
            scan_cursor_seq, scan_cursor_row_id = _row_cursor_tuple(rows[-1].to_dict())
            if (scan_cursor_seq, scan_cursor_row_id) <= previous_cursor:
                raise RuntimeError(
                    "list_bot_runtime_events scan cursor did not advance "
                    f"(after_seq={previous_cursor[0]}, after_row_id={previous_cursor[1]}, "
                    f"next_after_seq={scan_cursor_seq}, next_after_row_id={scan_cursor_row_id})"
                )
            if len(rows) < batch_limit:
                break
    return matched_rows


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
        if fallback:
            return str(fallback)
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
        return str(lifecycle_row) if lifecycle_row else None


def get_latest_bot_runtime_event(
    *,
    bot_id: str,
    run_id: Optional[str] = None,
    event_types: Optional[Sequence[str]] = None,
    canonicalize_botlens_payloads: bool = False,
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
        return (
            _project_runtime_event_row(
                row.to_dict(),
                canonicalize_botlens_payloads=canonicalize_botlens_payloads,
            )
            if row
            else None
        )


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
