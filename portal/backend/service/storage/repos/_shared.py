"""Shared storage repository primitives and helpers."""

from __future__ import annotations

import logging
import math
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
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
    BotRunViewStateRecord,
    BotRunStepRecord,
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
    if isinstance(value, time):
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


def _observe_db_write_outcome(
    *,
    storage_target: str,
    context: Dict[str, Any],
    started: float,
    outcome: StorageWriteOutcome,
) -> None:
    rows_written = max(int(outcome.rows_written), 0)
    if outcome.noop_count > 0 or outcome.noop_reason:
        _observe_db_write_noop(
            storage_target=storage_target,
            context=context,
            noop_reason=outcome.noop_reason,
            count=outcome.noop_count or 1,
        )
    if rows_written <= 0:
        return
    elapsed_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
    labels = {
        "bot_id": context.get("bot_id"),
        "run_id": context.get("run_id"),
        "storage_target": storage_target,
    }
    _STORAGE_OBSERVER.increment("db_write_total", **labels)
    _STORAGE_OBSERVER.observe("db_write_ms", elapsed_ms, **labels)
    _STORAGE_OBSERVER.observe("db_write_rows", float(rows_written), **labels)
    _STORAGE_OBSERVER.observe(
        "db_write_payload_bytes",
        float(max(int(outcome.payload_bytes), 0)),
        **labels,
    )
    if elapsed_ms >= _DB_SLOW_MS:
        _STORAGE_OBSERVER.event(
            "db_write_slow",
            level=logging.WARN,
            write_ms=round(elapsed_ms, 6),
            rows=rows_written,
            payload_bytes=max(int(outcome.payload_bytes), 0),
            **labels,
        )


__all__ = [
    "ATMTemplateRecord",
    "BotRecord",
    "BotRunEventRecord",
    "BotRunLifecycleEventRecord",
    "BotRunLifecycleRecord",
    "BotRunRecord",
    "BotRunViewStateRecord",
    "BotRunStepRecord",
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
    "_json_safe",
    "_is_transient_connection_error",
    "_parse_optional_timestamp",
    "_STORAGE_OBSERVER",
    "StorageWriteOutcome",
    "_utcnow",
]
