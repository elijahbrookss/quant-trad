"""Shared storage repository primitives and helpers."""

from __future__ import annotations

import logging
import math
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, TypeVar

from core.settings import get_settings
from sqlalchemy import delete, func, select, text
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError

from ....db import (
    ATMTemplateRecord,
    BotRecord,
    BotRunEventRecord,
    BotRunLifecycleEventRecord,
    BotRunLifecycleRecord,
    BotRunRecord,
    BotRunStepRecord,
    REQUIRED_BOT_RUN_EVENT_INDEXES,
    BotlensBackendEventRecord,
    BotlensBackendMetricSampleRecord,
    BotTradeEventRecord,
    BotTradeRecord,
    IndicatorRecord,
    InstrumentRecord,
    StrategyIndicatorLink,
    StrategyInstrumentLink,
    StrategyRecord,
    StrategyRuleRecord,
    StrategyVariantRecord,
    SymbolPresetRecord,
    db,
)
from ...observability import BackendObserver, normalize_failure_mode
from ...risk.atm import normalise_template

logger = logging.getLogger(__name__)
_DATABASE_SETTINGS = get_settings().database
_DB_WRITE_RETRY_ATTEMPTS = max(1, int(_DATABASE_SETTINGS.write_retry_attempts))
_DB_SLOW_MS = float(get_settings().observability.slow_ms or 250.0)
_STORAGE_OBSERVER = BackendObserver(component="storage_persistence", event_logger=logger)
_T = TypeVar("_T")


def _utcnow() -> datetime:
    """Return a naive UTC timestamp."""

    return datetime.utcnow()


def _parse_optional_timestamp(value: Any) -> Optional[datetime]:
    """Best-effort parsing of ISO8601 strings into naive UTC datetimes."""

    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    """Recursively convert values to JSON-safe primitives."""

    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat() + "Z"
        return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime_time):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


@dataclass(frozen=True)
class StorageWriteOutcome:
    result: Any
    rows_written: int = 0
    payload_bytes: int = 0
    noop_reason: Optional[str] = None
    noop_count: int = 0
    attempted_rows: int = 0
    inserted_rows: int = 0
    duplicate_rows: int = 0
    updated_rows: int = 0
    noop_rows: int = 0
    failed_rows: int = 0
    bytes_per_row: float = 0.0
    payload_build_ms: Optional[float] = None
    db_round_trip_ms: Optional[float] = None
    largest_json_field_name: Optional[str] = None
    largest_json_field_bytes: int = 0
    json_field_count: int = 0
    top_level_key_count: int = 0
    blob_bytes: int = 0
    payload_size_bucket: Optional[str] = None
    has_large_payload: bool = False
    duplicate_reasons: Optional[Mapping[str, int]] = None

    def __post_init__(self) -> None:
        rows_written = max(int(self.rows_written), 0)
        inserted_rows = max(int(self.inserted_rows), 0)
        duplicate_rows = max(int(self.duplicate_rows), 0)
        updated_rows = max(int(self.updated_rows), 0)
        noop_rows = max(int(self.noop_rows), 0)
        failed_rows = max(int(self.failed_rows), 0)
        noop_count = max(int(self.noop_count), 0)

        if inserted_rows <= 0 and rows_written > 0 and updated_rows <= 0:
            inserted_rows = rows_written
        if str(self.noop_reason or "").strip() == "duplicate_skip" and duplicate_rows <= 0:
            duplicate_rows = noop_count or 1
        if str(self.noop_reason or "").strip() == "stale_update" and noop_rows <= 0:
            noop_rows = noop_count or 1

        attempted_rows = max(
            int(self.attempted_rows or 0),
            inserted_rows + duplicate_rows + updated_rows + noop_rows + failed_rows,
            rows_written,
        )
        bytes_per_row = float(self.bytes_per_row or 0.0)
        if attempted_rows > 0 and bytes_per_row <= 0.0:
            bytes_per_row = float(max(int(self.payload_bytes), 0)) / float(attempted_rows)
        payload_size_bucket = str(self.payload_size_bucket or "").strip().lower() or _payload_size_bucket(
            int(self.payload_bytes or 0)
        )

        object.__setattr__(self, "rows_written", rows_written)
        object.__setattr__(self, "attempted_rows", attempted_rows)
        object.__setattr__(self, "inserted_rows", inserted_rows)
        object.__setattr__(self, "duplicate_rows", duplicate_rows)
        object.__setattr__(self, "updated_rows", updated_rows)
        object.__setattr__(self, "noop_rows", noop_rows)
        object.__setattr__(self, "failed_rows", failed_rows)
        object.__setattr__(self, "noop_count", noop_count)
        object.__setattr__(self, "bytes_per_row", bytes_per_row)
        object.__setattr__(self, "largest_json_field_bytes", max(int(self.largest_json_field_bytes or 0), 0))
        object.__setattr__(self, "json_field_count", max(int(self.json_field_count or 0), 0))
        object.__setattr__(self, "top_level_key_count", max(int(self.top_level_key_count or 0), 0))
        object.__setattr__(self, "blob_bytes", max(int(self.blob_bytes or 0), 0))
        object.__setattr__(self, "payload_size_bucket", payload_size_bucket)
        object.__setattr__(self, "has_large_payload", bool(self.has_large_payload or payload_size_bucket == "large"))
        if self.duplicate_reasons is not None:
            object.__setattr__(
                self,
                "duplicate_reasons",
                {
                    str(key): max(int(value), 0)
                    for key, value in dict(self.duplicate_reasons).items()
                    if str(key).strip() and int(value or 0) > 0
                },
            )


_PAYLOAD_SIZE_SMALL_MAX_BYTES = 16 * 1024
_PAYLOAD_SIZE_MEDIUM_MAX_BYTES = 64 * 1024


def _payload_size_bucket(payload_bytes: int) -> str:
    size = max(int(payload_bytes or 0), 0)
    if size >= _PAYLOAD_SIZE_MEDIUM_MAX_BYTES:
        return "large"
    if size >= _PAYLOAD_SIZE_SMALL_MAX_BYTES:
        return "medium"
    return "small"


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
    storage_target: str,
    context: Dict[str, Any],
    action: Callable[[], _T],
) -> _T:
    for attempt in range(1, _DB_WRITE_RETRY_ATTEMPTS + 1):
        try:
            return action()
        except (OperationalError, DBAPIError, SQLAlchemyError) as exc:
            retryable = attempt < _DB_WRITE_RETRY_ATTEMPTS and _is_transient_connection_error(exc)
            failure_mode = normalize_failure_mode(exc)
            if retryable:
                _STORAGE_OBSERVER.increment(
                    "db_write_retry_total",
                    bot_id=context.get("bot_id"),
                    run_id=context.get("run_id"),
                    storage_target=storage_target,
                    failure_mode=failure_mode,
                )
                _STORAGE_OBSERVER.event(
                    "db_write_retried",
                    level=logging.WARN,
                    bot_id=context.get("bot_id"),
                    run_id=context.get("run_id"),
                    storage_target=storage_target,
                    failure_mode=failure_mode,
                    operation=operation,
                    attempt=attempt,
                )
            else:
                _STORAGE_OBSERVER.increment(
                    "db_write_fail_total",
                    bot_id=context.get("bot_id"),
                    run_id=context.get("run_id"),
                    storage_target=storage_target,
                    failure_mode=failure_mode,
                )
                _STORAGE_OBSERVER.event(
                    "db_write_failed",
                    level=logging.ERROR,
                    bot_id=context.get("bot_id"),
                    run_id=context.get("run_id"),
                    storage_target=storage_target,
                    failure_mode=failure_mode,
                    operation=operation,
                    attempt=attempt,
                    error=str(exc),
                )
            if not retryable:
                raise
            db.reset_connection_state()
            time.sleep(min(0.05 * attempt, 0.25))


def _observe_db_write_noop(
    *,
    storage_target: str,
    context: Dict[str, Any],
    noop_reason: Optional[str],
    count: int = 1,
) -> None:
    metric_name = {
        "duplicate_skip": "db_duplicate_skip_total",
        "stale_update": "db_stale_update_total",
    }.get(str(noop_reason or "").strip(), "db_write_noop_total")
    _STORAGE_OBSERVER.increment(
        metric_name,
        value=float(max(int(count), 0)),
        bot_id=context.get("bot_id"),
        run_id=context.get("run_id"),
        storage_target=storage_target,
    )


def _db_write_labels(
    *,
    storage_target: str,
    context: Dict[str, Any],
    payload_size_bucket: str,
) -> Dict[str, Any]:
    return {
        "bot_id": context.get("bot_id"),
        "run_id": context.get("run_id"),
        "series_key": context.get("series_key"),
        "worker_id": context.get("worker_id"),
        "storage_target": storage_target,
        "pipeline_stage": context.get("pipeline_stage"),
        "message_kind": context.get("message_kind"),
        "source_reason": context.get("source_reason"),
        "payload_size_bucket": payload_size_bucket,
    }


def _emit_db_write_attempt_event(
    *,
    storage_target: str,
    context: Dict[str, Any],
    outcome: StorageWriteOutcome,
    elapsed_ms: float,
    error: Exception | None = None,
) -> None:
    duplicate_reason_counts = dict(outcome.duplicate_reasons or {})
    payload_size_bucket = str(outcome.payload_size_bucket or _payload_size_bucket(outcome.payload_bytes))
    labels = _db_write_labels(
        storage_target=storage_target,
        context=context,
        payload_size_bucket=payload_size_bucket,
    )
    details = {
        "event_name": context.get("event_name"),
        "source_emitter": context.get("source_emitter"),
        "source_reason": context.get("source_reason"),
        "write_contract": context.get("write_contract"),
        "precheck_mode": context.get("precheck_mode"),
        "conflict_strategy": context.get("conflict_strategy"),
        "conflict_target_name": context.get("conflict_target_name"),
        "attempted_rows": int(outcome.attempted_rows),
        "inserted_rows": int(outcome.inserted_rows),
        "duplicate_rows": int(outcome.duplicate_rows),
        "updated_rows": int(outcome.updated_rows),
        "noop_rows": int(outcome.noop_rows),
        "failed_rows": int(outcome.failed_rows),
        "payload_bytes": max(int(outcome.payload_bytes), 0),
        "bytes_per_row": round(float(outcome.bytes_per_row or 0.0), 6),
        "batch_size": context.get("batch_size"),
        "write_ms": round(float(elapsed_ms), 6),
        "payload_build_ms": round(float(outcome.payload_build_ms), 6) if outcome.payload_build_ms is not None else None,
        "db_round_trip_ms": round(float(outcome.db_round_trip_ms), 6)
        if outcome.db_round_trip_ms is not None
        else None,
        "largest_json_field_name": outcome.largest_json_field_name,
        "largest_json_field_bytes": int(outcome.largest_json_field_bytes),
        "json_field_count": int(outcome.json_field_count),
        "top_level_key_count": int(outcome.top_level_key_count),
        "blob_bytes": int(outcome.blob_bytes),
        "has_large_payload": bool(outcome.has_large_payload),
        "payload_size_bucket": payload_size_bucket,
        "duplicate_reason_counts": duplicate_reason_counts or None,
    }
    if error is not None:
        details["error"] = str(error)
        details["failure_mode"] = normalize_failure_mode(error)
    event_fields = dict(labels)
    event_fields.update(details)
    event_fields.update(
        {
            "bot_id": context.get("bot_id"),
            "run_id": context.get("run_id"),
            "series_key": context.get("series_key"),
            "worker_id": context.get("worker_id"),
            "storage_target": storage_target,
            "pipeline_stage": context.get("pipeline_stage"),
            "message_kind": context.get("message_kind"),
            "failure_mode": details.get("failure_mode"),
        }
    )
    _STORAGE_OBSERVER.event(
        "db_write_observed",
        level=logging.INFO,
        **event_fields,
    )


def _observe_db_write_outcome(
    *,
    storage_target: str,
    context: Dict[str, Any],
    started: float,
    outcome: StorageWriteOutcome,
    error: Exception | None = None,
) -> None:
    rows_written = max(int(outcome.rows_written), 0)
    if outcome.noop_count > 0 or outcome.noop_reason:
        _observe_db_write_noop(
            storage_target=storage_target,
            context=context,
            noop_reason=outcome.noop_reason,
            count=outcome.noop_count or 1,
        )
    elapsed_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
    payload_size_bucket = str(outcome.payload_size_bucket or _payload_size_bucket(outcome.payload_bytes))
    labels = _db_write_labels(
        storage_target=storage_target,
        context=context,
        payload_size_bucket=payload_size_bucket,
    )

    _STORAGE_OBSERVER.increment("db_write_attempt_total", **labels)
    _STORAGE_OBSERVER.increment(
        "db_write_attempted_rows_total",
        value=float(max(int(outcome.attempted_rows), 0)),
        **labels,
    )
    _STORAGE_OBSERVER.observe("db_write_ms", elapsed_ms, **labels)
    _STORAGE_OBSERVER.observe(
        "db_write_payload_bytes",
        float(max(int(outcome.payload_bytes), 0)),
        **labels,
    )
    _STORAGE_OBSERVER.observe(
        "db_write_bytes_per_row",
        float(max(float(outcome.bytes_per_row or 0.0), 0.0)),
        **labels,
    )
    if outcome.payload_build_ms is not None:
        _STORAGE_OBSERVER.observe("db_write_payload_build_ms", float(max(outcome.payload_build_ms, 0.0)), **labels)
    if outcome.db_round_trip_ms is not None:
        _STORAGE_OBSERVER.observe("db_write_round_trip_ms", float(max(outcome.db_round_trip_ms, 0.0)), **labels)

    row_outcomes = {
        "inserted": int(outcome.inserted_rows),
        "duplicate": int(outcome.duplicate_rows),
        "updated": int(outcome.updated_rows),
        "noop": int(outcome.noop_rows),
        "failed": int(outcome.failed_rows),
    }
    for outcome_name, count in row_outcomes.items():
        if count <= 0:
            continue
        _STORAGE_OBSERVER.increment(
            "db_write_rows_total",
            value=float(count),
            outcome=outcome_name,
            **labels,
        )
    for duplicate_reason, count in dict(outcome.duplicate_reasons or {}).items():
        if int(count or 0) <= 0:
            continue
        _STORAGE_OBSERVER.increment(
            "db_write_duplicate_rows_total",
            value=float(count),
            duplicate_reason=duplicate_reason,
            **labels,
        )

    _emit_db_write_attempt_event(
        storage_target=storage_target,
        context=context,
        outcome=outcome,
        elapsed_ms=elapsed_ms,
        error=error,
    )

    if rows_written > 0:
        _STORAGE_OBSERVER.increment("db_write_total", **labels)
        _STORAGE_OBSERVER.observe("db_write_rows", float(rows_written), **labels)
    if elapsed_ms >= _DB_SLOW_MS:
        event_fields = dict(labels)
        event_fields.update(
            {
                "write_ms": round(elapsed_ms, 6),
                "rows": rows_written,
                "attempted_rows": int(outcome.attempted_rows),
                "inserted_rows": int(outcome.inserted_rows),
                "duplicate_rows": int(outcome.duplicate_rows),
                "updated_rows": int(outcome.updated_rows),
                "noop_rows": int(outcome.noop_rows),
                "failed_rows": int(outcome.failed_rows),
                "payload_bytes": max(int(outcome.payload_bytes), 0),
                "bytes_per_row": round(float(outcome.bytes_per_row or 0.0), 6),
                "payload_size_bucket": payload_size_bucket,
                "largest_json_field_name": outcome.largest_json_field_name,
                "largest_json_field_bytes": int(outcome.largest_json_field_bytes),
                "conflict_target_name": context.get("conflict_target_name"),
                "batch_size": context.get("batch_size"),
            }
        )
        _STORAGE_OBSERVER.event(
            "db_write_slow",
            level=logging.WARN,
            **event_fields,
        )


__all__ = [
    "ATMTemplateRecord",
    "BotRecord",
    "BotRunEventRecord",
    "BotRunLifecycleEventRecord",
    "BotRunLifecycleRecord",
    "BotRunRecord",
    "BotRunStepRecord",
    "BotlensBackendEventRecord",
    "BotlensBackendMetricSampleRecord",
    "BotTradeEventRecord",
    "BotTradeRecord",
    "IndicatorRecord",
    "InstrumentRecord",
    "StrategyIndicatorLink",
    "StrategyInstrumentLink",
    "StrategyRecord",
    "StrategyRuleRecord",
    "StrategyVariantRecord",
    "SymbolPresetRecord",
    "db",
    "delete",
    "func",
    "logger",
    "normalise_template",
    "select",
    "SQLAlchemyError",
    "text",
    "timedelta",
    "uuid",
    "_coerce_float",
    "_coerce_int",
    "_execute_write_with_retry",
    "_observe_db_write_outcome",
    "_payload_size_bucket",
    "_json_safe",
    "_is_transient_connection_error",
    "_parse_optional_timestamp",
    "_STORAGE_OBSERVER",
    "StorageWriteOutcome",
    "_utcnow",
]
