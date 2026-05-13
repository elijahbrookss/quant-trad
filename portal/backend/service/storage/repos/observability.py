"""DB-backed BotLens backend observability persistence/query helpers."""

from __future__ import annotations

import logging
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Dict, List

from ._shared import (
    BotlensBackendEventRecord,
    BotlensBackendMetricRollupRecord,
    _coerce_int,
    _json_safe,
    _parse_optional_timestamp,
    _utcnow,
    db,
    select,
    text,
)

logger = logging.getLogger(__name__)
DEFAULT_METRIC_ROLLUP_BUCKET_SECONDS = 10
_ROLLUP_IDENTITY_FIELDS = (
    "component",
    "metric_name",
    "metric_kind",
    "bot_id",
    "run_id",
    "instrument_id",
    "series_key",
    "worker_id",
    "queue_name",
    "pipeline_stage",
    "message_kind",
    "delta_type",
    "storage_target",
    "failure_mode",
)
_BOUNDED_LABEL_KEYS = frozenset(
    {
        "aggregation",
        "direction",
        "duplicate_reason",
        "operation",
        "outcome",
        "payload_size_bucket",
        "reason_code",
        "source_reason",
        "status",
        "tier",
        "trigger",
        "unit",
    }
)


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _clean_int(value: Any) -> int | None:
    parsed = _coerce_int(value)
    return int(parsed) if parsed is not None else None


def _identity_text(value: Any) -> str:
    return str(value or "").strip()


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _bucket_start(value: Any, bucket_seconds: int) -> datetime:
    observed_at = _parse_optional_timestamp(value) or _utcnow()
    observed_at = _naive_utc(observed_at)
    seconds = max(int(bucket_seconds or DEFAULT_METRIC_ROLLUP_BUCKET_SECONDS), 1)
    epoch = int(observed_at.replace(tzinfo=timezone.utc).timestamp())
    bucket_epoch = epoch - (epoch % seconds)
    return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc).replace(tzinfo=None)


def _bounded_labels(labels: Any) -> Dict[str, Any]:
    if not isinstance(labels, Mapping):
        return {}
    normalized: Dict[str, Any] = {}
    for key, value in sorted(labels.items(), key=lambda item: str(item[0])):
        label_key = str(key or "").strip()
        if not label_key or label_key not in _BOUNDED_LABEL_KEYS:
            continue
        if isinstance(value, (dict, list, tuple, set)):
            continue
        label_value = str(value or "").strip()
        if not label_value:
            continue
        normalized[label_key] = label_value[:128]
    return normalized


def _label_hash(labels: Mapping[str, Any]) -> str:
    if not labels:
        return "none"
    payload = json.dumps(labels, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _metric_rollup_row(payload: Mapping[str, Any]) -> BotlensBackendMetricRollupRecord:
    labels = _bounded_labels(payload.get("labels"))
    bucket_seconds = max(int(payload.get("bucket_seconds") or DEFAULT_METRIC_ROLLUP_BUCKET_SECONDS), 1)
    bucket_start = _bucket_start(payload.get("bucket_start") or payload.get("observed_at"), bucket_seconds)
    first_seen = _naive_utc(_parse_optional_timestamp(payload.get("first_seen")) or bucket_start)
    last_seen = _naive_utc(_parse_optional_timestamp(payload.get("last_seen")) or first_seen)
    sample_count = max(int(payload.get("sample_count") or payload.get("raw_sample_count") or 1), 1)
    raw_sample_count = max(int(payload.get("raw_sample_count") or sample_count), sample_count)
    source_metric_record_count = max(int(payload.get("source_metric_record_count") or 1), 1)
    value_sum = _coerce_float(payload.get("value_sum", payload.get("value", 0.0)))
    if "value_sum" not in payload and sample_count > 1:
        value_sum = _coerce_float(payload.get("value")) * sample_count
    return BotlensBackendMetricRollupRecord(
        bucket_start=bucket_start,
        bucket_seconds=bucket_seconds,
        first_seen=first_seen,
        last_seen=last_seen,
        component=_clean_text(payload.get("component")) or "unknown",
        metric_name=_clean_text(payload.get("metric_name")) or "unknown",
        metric_kind=_clean_text(payload.get("metric_kind")) or "unknown",
        bot_id=_identity_text(payload.get("bot_id")),
        run_id=_identity_text(payload.get("run_id")),
        instrument_id=_identity_text(payload.get("instrument_id")),
        series_key=_identity_text(payload.get("series_key")),
        worker_id=_identity_text(payload.get("worker_id")),
        queue_name=_identity_text(payload.get("queue_name")),
        pipeline_stage=_identity_text(payload.get("pipeline_stage")),
        message_kind=_identity_text(payload.get("message_kind")),
        delta_type=_identity_text(payload.get("delta_type")),
        storage_target=_identity_text(payload.get("storage_target")),
        failure_mode=_identity_text(payload.get("failure_mode")),
        label_hash=_clean_text(payload.get("label_hash")) or _label_hash(labels),
        labels=_json_safe(labels),
        sample_count=sample_count,
        value_sum=value_sum,
        value_min=_coerce_float(payload.get("value_min", payload.get("value", 0.0))),
        value_max=_coerce_float(payload.get("value_max", payload.get("value", 0.0))),
        latest_value=_coerce_float(payload.get("latest_value", payload.get("value", 0.0))),
        p95_value=_coerce_float(payload.get("p95_value", payload.get("value_max", payload.get("value", 0.0)))),
        p99_value=_coerce_float(payload.get("p99_value", payload.get("value_max", payload.get("value", 0.0)))),
        raw_sample_count=raw_sample_count,
        source_metric_record_count=source_metric_record_count,
        created_at=_utcnow(),
        updated_at=_utcnow(),
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


def record_observability_metric_rollups_batch(payloads: Sequence[Mapping[str, Any]]) -> int:
    """Upsert backend observability metric rollups in one transaction."""

    if not db.available:
        raise RuntimeError("database is required for observability metric persistence")
    rows = [_metric_rollup_row(payload) for payload in payloads if isinstance(payload, Mapping)]
    if not rows:
        return 0
    statement = text(
        """
        INSERT INTO observability_metrics.botlens_backend_metric_rollups_v1 AS rollup (
            bucket_start,
            bucket_seconds,
            first_seen,
            last_seen,
            component,
            metric_name,
            metric_kind,
            bot_id,
            run_id,
            instrument_id,
            series_key,
            worker_id,
            queue_name,
            pipeline_stage,
            message_kind,
            delta_type,
            storage_target,
            failure_mode,
            label_hash,
            labels,
            sample_count,
            value_sum,
            value_min,
            value_max,
            latest_value,
            p95_value,
            p99_value,
            raw_sample_count,
            source_metric_record_count,
            created_at,
            updated_at
        )
        VALUES (
            :bucket_start,
            :bucket_seconds,
            :first_seen,
            :last_seen,
            :component,
            :metric_name,
            :metric_kind,
            :bot_id,
            :run_id,
            :instrument_id,
            :series_key,
            :worker_id,
            :queue_name,
            :pipeline_stage,
            :message_kind,
            :delta_type,
            :storage_target,
            :failure_mode,
            :label_hash,
            CAST(:labels AS JSONB),
            :sample_count,
            :value_sum,
            :value_min,
            :value_max,
            :latest_value,
            :p95_value,
            :p99_value,
            :raw_sample_count,
            :source_metric_record_count,
            :created_at,
            :updated_at
        )
        ON CONFLICT ON CONSTRAINT uq_botlens_backend_metric_rollups_v1_bucket_identity
        DO UPDATE SET
            first_seen = LEAST(rollup.first_seen, EXCLUDED.first_seen),
            last_seen = GREATEST(rollup.last_seen, EXCLUDED.last_seen),
            sample_count = rollup.sample_count + EXCLUDED.sample_count,
            value_sum = rollup.value_sum + EXCLUDED.value_sum,
            value_min = LEAST(rollup.value_min, EXCLUDED.value_min),
            value_max = GREATEST(rollup.value_max, EXCLUDED.value_max),
            latest_value = EXCLUDED.latest_value,
            p95_value = GREATEST(rollup.p95_value, EXCLUDED.p95_value),
            p99_value = GREATEST(rollup.p99_value, EXCLUDED.p99_value),
            raw_sample_count = rollup.raw_sample_count + EXCLUDED.raw_sample_count,
            source_metric_record_count = rollup.source_metric_record_count + EXCLUDED.source_metric_record_count,
            updated_at = EXCLUDED.updated_at
        """
    )
    params = [
        {
            "bucket_start": row.bucket_start,
            "bucket_seconds": int(row.bucket_seconds or DEFAULT_METRIC_ROLLUP_BUCKET_SECONDS),
            "first_seen": row.first_seen,
            "last_seen": row.last_seen,
            "component": row.component,
            "metric_name": row.metric_name,
            "metric_kind": row.metric_kind,
            "bot_id": row.bot_id or "",
            "run_id": row.run_id or "",
            "instrument_id": row.instrument_id or "",
            "series_key": row.series_key or "",
            "worker_id": row.worker_id or "",
            "queue_name": row.queue_name or "",
            "pipeline_stage": row.pipeline_stage or "",
            "message_kind": row.message_kind or "",
            "delta_type": row.delta_type or "",
            "storage_target": row.storage_target or "",
            "failure_mode": row.failure_mode or "",
            "label_hash": row.label_hash,
            "sample_count": int(row.sample_count or 0),
            "value_sum": float(row.value_sum or 0.0),
            "value_min": float(row.value_min or 0.0),
            "value_max": float(row.value_max or 0.0),
            "latest_value": float(row.latest_value or 0.0),
            "p95_value": float(row.p95_value or 0.0),
            "p99_value": float(row.p99_value or 0.0),
            "raw_sample_count": int(row.raw_sample_count or 0),
            "source_metric_record_count": int(row.source_metric_record_count or 0),
            "labels": json.dumps(dict(row.labels or {}), sort_keys=True, separators=(",", ":"), default=str),
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
        for row in rows
    ]
    with db.session() as session:
        session.execute(statement, params)
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


def list_observability_metric_rollups(*, run_id: str | None = None, limit: int = 500) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    max_rows = max(1, min(int(limit or 500), 5000))
    with db.session() as session:
        query = select(BotlensBackendMetricRollupRecord)
        if run_id:
            query = query.where(BotlensBackendMetricRollupRecord.run_id == str(run_id))
        rows = (
            session.execute(
                query.order_by(
                    BotlensBackendMetricRollupRecord.bucket_start.desc(),
                    BotlensBackendMetricRollupRecord.id.desc(),
                )
                .limit(max_rows)
            )
            .scalars()
            .all()
        )
        return [row.to_dict() for row in rows]


def list_observability_events(*, run_id: str | None = None, limit: int = 500) -> List[Dict[str, Any]]:
    if not db.available:
        return []
    max_rows = max(1, min(int(limit or 500), 5000))
    with db.session() as session:
        query = select(BotlensBackendEventRecord)
        if run_id:
            query = query.where(BotlensBackendEventRecord.run_id == str(run_id))
        rows = (
            session.execute(
                query.order_by(BotlensBackendEventRecord.observed_at.desc(), BotlensBackendEventRecord.id.desc())
                .limit(max_rows)
            )
            .scalars()
            .all()
        )
        return [row.to_dict() for row in rows]


__all__ = [
    "list_observability_events",
    "list_observability_metric_rollups",
    "record_observability_events_batch",
    "record_observability_metric_rollups_batch",
    "_metric_rollup_row",
]
