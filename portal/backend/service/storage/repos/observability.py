"""DB-backed BotLens backend observability persistence/query helpers."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List

from ._shared import (
    BotlensBackendEventRecord,
    BotlensBackendMetricSampleRecord,
    _coerce_int,
    _json_safe,
    _parse_optional_timestamp,
    _utcnow,
    db,
    select,
)

logger = logging.getLogger(__name__)


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _clean_int(value: Any) -> int | None:
    parsed = _coerce_int(value)
    return int(parsed) if parsed is not None else None


def _metric_sample_row(payload: Mapping[str, Any]) -> BotlensBackendMetricSampleRecord:
    labels = dict(payload.get("labels") or {}) if isinstance(payload.get("labels"), Mapping) else {}
    return BotlensBackendMetricSampleRecord(
        observed_at=_parse_optional_timestamp(payload.get("observed_at")) or _utcnow(),
        component=_clean_text(payload.get("component")) or "unknown",
        metric_name=_clean_text(payload.get("metric_name")) or "unknown",
        metric_kind=_clean_text(payload.get("metric_kind")) or "unknown",
        value=float(payload.get("value") or 0.0),
        bot_id=_clean_text(payload.get("bot_id")),
        run_id=_clean_text(payload.get("run_id")),
        instrument_id=_clean_text(payload.get("instrument_id")),
        series_key=_clean_text(payload.get("series_key")),
        worker_id=_clean_text(payload.get("worker_id")),
        queue_name=_clean_text(payload.get("queue_name")),
        pipeline_stage=_clean_text(payload.get("pipeline_stage")),
        message_kind=_clean_text(payload.get("message_kind")),
        delta_type=_clean_text(payload.get("delta_type")),
        storage_target=_clean_text(payload.get("storage_target")),
        failure_mode=_clean_text(payload.get("failure_mode")),
        labels=_json_safe(labels),
        created_at=_utcnow(),
    )


def _event_row(payload: Mapping[str, Any]) -> BotlensBackendEventRecord:
    details = dict(payload.get("details") or {}) if isinstance(payload.get("details"), Mapping) else {}
    return BotlensBackendEventRecord(
        observed_at=_parse_optional_timestamp(payload.get("observed_at")) or _utcnow(),
        component=_clean_text(payload.get("component")) or "unknown",
        event_name=_clean_text(payload.get("event_name")) or "unknown",
        level=_clean_text(payload.get("level")) or "INFO",
        bot_id=_clean_text(payload.get("bot_id")),
        run_id=_clean_text(payload.get("run_id")),
        instrument_id=_clean_text(payload.get("instrument_id")),
        series_key=_clean_text(payload.get("series_key")),
        worker_id=_clean_text(payload.get("worker_id")),
        queue_name=_clean_text(payload.get("queue_name")),
        pipeline_stage=_clean_text(payload.get("pipeline_stage")),
        message_kind=_clean_text(payload.get("message_kind")),
        delta_type=_clean_text(payload.get("delta_type")),
        storage_target=_clean_text(payload.get("storage_target")),
        failure_mode=_clean_text(payload.get("failure_mode")),
        phase=_clean_text(payload.get("phase")),
        status=_clean_text(payload.get("status")),
        run_seq=_clean_int(payload.get("run_seq")),
        bridge_session_id=_clean_text(payload.get("bridge_session_id")),
        bridge_seq=_clean_int(payload.get("bridge_seq")),
        message=_clean_text(payload.get("message")),
        details=_json_safe(details),
        created_at=_utcnow(),
    )


def record_observability_metric_samples_batch(payloads: Sequence[Mapping[str, Any]]) -> int:
    """Append many backend observability metric samples in one transaction."""

    if not db.available:
        raise RuntimeError("database is required for observability metric persistence")
    rows = [_metric_sample_row(payload) for payload in payloads if isinstance(payload, Mapping)]
    if not rows:
        return 0
    with db.session() as session:
        session.add_all(rows)
    return len(rows)


def record_observability_events_batch(payloads: Sequence[Mapping[str, Any]]) -> int:
    """Append many backend observability events in one transaction."""

    if not db.available:
        raise RuntimeError("database is required for observability event persistence")
    rows = [_event_row(payload) for payload in payloads if isinstance(payload, Mapping)]
    if not rows:
        return 0
    with db.session() as session:
        session.add_all(rows)
    return len(rows)


def list_observability_metric_samples(*, limit: int = 500) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    max_rows = max(1, min(int(limit or 500), 5000))
    with db.session() as session:
        rows = (
            session.execute(
                select(BotlensBackendMetricSampleRecord)
                .order_by(
                    BotlensBackendMetricSampleRecord.observed_at.desc(),
                    BotlensBackendMetricSampleRecord.id.desc(),
                )
                .limit(max_rows)
            )
            .scalars()
            .all()
        )
        return [row.to_dict() for row in rows]


def list_observability_events(*, limit: int = 500) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    max_rows = max(1, min(int(limit or 500), 5000))
    with db.session() as session:
        rows = (
            session.execute(
                select(BotlensBackendEventRecord)
                .order_by(BotlensBackendEventRecord.observed_at.desc(), BotlensBackendEventRecord.id.desc())
                .limit(max_rows)
            )
            .scalars()
            .all()
        )
        return [row.to_dict() for row in rows]


__all__ = [
    "list_observability_events",
    "list_observability_metric_samples",
    "record_observability_events_batch",
    "record_observability_metric_samples_batch",
]
