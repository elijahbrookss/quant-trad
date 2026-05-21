"""Bounded DB-backed exporter for the shared backend observability sink."""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Hashable, List, Optional

from core.settings import get_settings

from core.metrics import Metric

from .observability import EventRecord, InMemoryObservabilitySink, get_observability_sink
from .storage.repos.observability import (
    record_observability_events_batch,
    record_observability_metric_rollups_batch,
)

logger = logging.getLogger(__name__)
_SETTINGS = get_settings().observability
_METRIC_ROLLUP_BUCKET_SECONDS = max(int(_SETTINGS.persist_rollup_bucket_seconds or 30), 1)
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
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _persisted_metric_labels(labels: Dict[str, Any]) -> Dict[str, Any]:
    trimmed = dict(labels or {})
    for key in (
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
    ):
        trimmed.pop(key, None)
    bounded: Dict[str, Any] = {}
    for key, value in sorted(trimmed.items(), key=lambda item: str(item[0])):
        label_key = str(key or "").strip()
        if label_key not in _BOUNDED_LABEL_KEYS:
            continue
        if isinstance(value, (dict, list, tuple, set)):
            continue
        label_value = str(value or "").strip()
        if label_value:
            bounded[label_key] = label_value[:128]
    return bounded


def _metric_payload(record: Metric) -> Dict[str, Any]:
    labels = dict(record.tags or {})
    persisted_labels = _persisted_metric_labels(labels)
    value = float(record.value)
    sample_count = max(int(_clean_int(labels.get("sample_count")) or 1), 1)
    value_sum = _clean_float(labels.get("value_sum"))
    value_min = _clean_float(labels.get("value_min"))
    value_max = _clean_float(labels.get("value_max"))
    latest_value = _clean_float(labels.get("latest_value"))
    return {
        "observed_at": record.to_dict().get("timestamp"),
        "component": _clean_text(record.source) or "unknown",
        "metric_name": record.metric_name,
        "metric_kind": record.metric_type.value,
        "value": value,
        "sample_count": sample_count,
        "raw_sample_count": sample_count,
        "source_metric_record_count": 1,
        "value_sum": float(value_sum if value_sum is not None else value),
        "value_min": float(value_min if value_min is not None else value),
        "value_max": float(value_max if value_max is not None else value),
        "latest_value": float(latest_value if latest_value is not None else value),
        "p95_value": float(value_max if value_max is not None else value),
        "p99_value": float(value_max if value_max is not None else value),
        "bot_id": _clean_text(labels.get("bot_id")),
        "run_id": _clean_text(labels.get("run_id")),
        "instrument_id": _clean_text(labels.get("instrument_id")),
        "series_key": _clean_text(labels.get("series_key")),
        "worker_id": _clean_text(labels.get("worker_id")),
        "queue_name": _clean_text(labels.get("queue_name")),
        "pipeline_stage": _clean_text(labels.get("pipeline_stage")),
        "message_kind": _clean_text(labels.get("message_kind")),
        "delta_type": _clean_text(labels.get("delta_type")),
        "storage_target": _clean_text(labels.get("storage_target")),
        "failure_mode": _clean_text(labels.get("failure_mode")),
        "labels": persisted_labels,
    }


_METRIC_GROUP_FIELDS = (
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
_PRESSURE_GAUGE_SUFFIXES = (
    "_depth",
    "_utilization",
    "_oldest_age_ms",
    "_high_water_mark",
    "_payload_bytes",
    "_bytes",
)
_COALESCIBLE_EVENT_NAMES = frozenset(
    {
        "run_notification_queue_overflow",
        "fanout_channel_overflow",
        "observability_export_queue_overflow",
    }
)
_DURABLE_ALWAYS_CAPTURE_SUFFIXES = (
    "_drop_total",
    "_dropped_total",
    "_error",
    "_error_total",
    "_errors",
    "_errors_total",
    "_failed_total",
    "_fail_total",
    "_invalid_total",
    "_overflow",
    "_overflow_total",
    "_rejected_total",
    "_retries_total",
)
_DURABLE_ALWAYS_CAPTURE_PREFIXES = (
    "db_write_",
    "observability_",
)
_LIVE_ONLY_METRIC_PREFIXES_BY_COMPONENT = {
    "botlens_run_stream": (
        "viewer_broadcast_",
        "viewer_payload_bytes",
        "replay_ring_high_water_mark",
    ),
    "botlens_projector_registry": (
        "fanout_delivery_items_total",
    ),
    "botlens_intake_router": (
        "ingest_messages_total",
    ),
    "botlens_symbol_projector": (
        "symbol_projector_bootstrap_apply_total",
        "symbol_projector_fact_apply_total",
        "symbol_projector_batch_size",
        "symbol_projector_delta_count",
        "symbol_projector_projected_seq",
    ),
    "botlens_run_projector": (
        "run_projector_event_count",
        "run_projector_delta_count",
        "run_projector_projected_seq",
    ),
    "container_runtime_telemetry": (
        "telemetry_emitted_total",
        "telemetry_enqueue_attempt_total",
        "telemetry_enqueue_success_total",
        "telemetry_payload_bytes",
        "telemetry_transport_payload_bytes",
        "telemetry_transport_send_total",
    ),
}


def _labels_key(labels: Any) -> tuple[tuple[str, str], ...]:
    if not isinstance(labels, dict):
        return ()
    return tuple(sorted((str(key), str(value)) for key, value in labels.items()))


def _metric_group_key(row: Dict[str, Any]) -> tuple[Hashable, ...]:
    return tuple(_clean_text(row.get(field)) for field in _METRIC_GROUP_FIELDS) + (
        _labels_key(row.get("labels")),
    )


def _event_group_key(row: Dict[str, Any]) -> tuple[Hashable, ...]:
    return (
        _clean_text(row.get("component")),
        _clean_text(row.get("event_name")),
        _clean_text(row.get("level")),
        _clean_text(row.get("bot_id")),
        _clean_text(row.get("run_id")),
        _clean_text(row.get("instrument_id")),
        _clean_text(row.get("series_key")),
        _clean_text(row.get("queue_name")),
        _clean_text(row.get("failure_mode")),
    )


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _latest_observed_at(rows: List[Dict[str, Any]]) -> Any:
    values = [row.get("observed_at") for row in rows if row.get("observed_at")]
    return max(values) if values else rows[-1].get("observed_at")


def _nearest_percentile(values: List[float], quantile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(value) for value in values)
    index = min(max(int(round((len(ordered) - 1) * float(quantile))), 0), len(ordered) - 1)
    return float(ordered[index])


def _parse_observed_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _bucket_start_for(value: Any, bucket_seconds: int = _METRIC_ROLLUP_BUCKET_SECONDS) -> datetime:
    observed_at = _parse_observed_at(value)
    seconds = max(int(bucket_seconds or _METRIC_ROLLUP_BUCKET_SECONDS), 1)
    epoch = int(math.floor(observed_at.timestamp()))
    return datetime.fromtimestamp(epoch - (epoch % seconds), tz=timezone.utc)


def _coalesced_metric_value(row: Dict[str, Any], values: List[float]) -> tuple[float, str]:
    metric_kind = str(row.get("metric_kind") or "").strip().lower()
    metric_name = str(row.get("metric_name") or "").strip().lower()
    if metric_kind == "counter":
        return sum(values), "batch_sum"
    if metric_kind == "histogram":
        return _nearest_percentile(values, 0.95), "batch_p95"
    if metric_kind == "gauge" and metric_name.endswith(_PRESSURE_GAUGE_SUFFIXES):
        return max(values), "batch_max"
    return values[-1], "batch_latest"


def _coalesce_metric_payloads(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(rows) <= 1:
        return rows
    grouped: Dict[tuple[Hashable, ...], List[Dict[str, Any]]] = {}
    order: list[tuple[Hashable, ...]] = []
    for row in rows:
        key = _metric_group_key(row)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(row)

    coalesced: List[Dict[str, Any]] = []
    for key in order:
        group = grouped[key]
        if len(group) == 1:
            coalesced.append(group[0])
            continue
        row = dict(group[-1])
        values = [_coerce_float(item.get("value")) for item in group]
        value, aggregation = _coalesced_metric_value(row, values)
        labels = dict(row.get("labels") or {})
        labels["export_aggregation"] = aggregation
        labels["export_sample_count"] = len(group)
        row["observed_at"] = _latest_observed_at(group)
        row["value"] = float(value)
        row["labels"] = labels
        coalesced.append(row)
    return coalesced


def _rollup_metric_payloads(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    grouped: Dict[tuple[Hashable, ...], List[Dict[str, Any]]] = defaultdict(list)
    order: list[tuple[Hashable, ...]] = []
    for row in rows:
        bucket_start = _bucket_start_for(row.get("observed_at"))
        key = (_iso_z(bucket_start), _METRIC_ROLLUP_BUCKET_SECONDS) + _metric_group_key(row)
        if key not in grouped:
            order.append(key)
        grouped[key].append(row)

    rollups: List[Dict[str, Any]] = []
    for key in order:
        group = grouped[key]
        observed = [_parse_observed_at(item.get("observed_at")) for item in group]
        sample_counts = [max(int(item.get("sample_count") or 1), 1) for item in group]
        raw_sample_counts = [
            max(int(item.get("raw_sample_count") or sample_counts[index]), sample_counts[index])
            for index, item in enumerate(group)
        ]
        source_metric_record_counts = [
            max(int(item.get("source_metric_record_count") or 1), 1)
            for item in group
        ]
        value_sums = [_coerce_float(item.get("value_sum", item.get("value"))) for item in group]
        value_mins = [_coerce_float(item.get("value_min", item.get("value"))) for item in group]
        value_maxes = [_coerce_float(item.get("value_max", item.get("value"))) for item in group]
        p95_values = [_coerce_float(item.get("p95_value", item.get("value_max", item.get("value")))) for item in group]
        p99_values = [_coerce_float(item.get("p99_value", item.get("value_max", item.get("value")))) for item in group]
        latest_values = [_coerce_float(item.get("latest_value", item.get("value"))) for item in group]
        latest_index = max(range(len(group)), key=lambda index: observed[index])
        latest_row = dict(group[latest_index])
        latest_value = latest_values[latest_index]
        sample_count = sum(sample_counts)
        raw_sample_count = sum(raw_sample_counts)
        source_metric_record_count = sum(source_metric_record_counts)
        latest_row.update(
            {
                "bucket_start": key[0],
                "bucket_seconds": int(key[1]),
                "first_seen": _iso_z(min(observed)),
                "last_seen": _iso_z(max(observed)),
                "sample_count": sample_count,
                "raw_sample_count": raw_sample_count,
                "source_metric_record_count": source_metric_record_count,
                "value_sum": float(sum(value_sums)),
                "value_min": float(min(value_mins)),
                "value_max": float(max(value_maxes)),
                "latest_value": float(latest_value),
                "value": float(latest_value),
                "p95_value": max(p95_values),
                "p99_value": max(p99_values),
                "labels": dict(latest_row.get("labels") or {}),
            }
        )
        rollups.append(latest_row)
    return rollups


def _rollup_buffer_key(row: Dict[str, Any]) -> tuple[Hashable, ...]:
    return (
        _clean_text(row.get("bucket_start")),
        _clean_int(row.get("bucket_seconds")) or _METRIC_ROLLUP_BUCKET_SECONDS,
    ) + _metric_group_key(row)


def _merge_rollup_rows(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    first_seen = min(
        _parse_observed_at(existing.get("first_seen")),
        _parse_observed_at(incoming.get("first_seen")),
    )
    existing_last = _parse_observed_at(existing.get("last_seen"))
    incoming_last = _parse_observed_at(incoming.get("last_seen"))
    latest = incoming if incoming_last >= existing_last else existing

    existing["first_seen"] = _iso_z(first_seen)
    existing["last_seen"] = _iso_z(max(existing_last, incoming_last))
    existing["sample_count"] = int(existing.get("sample_count") or 0) + int(incoming.get("sample_count") or 0)
    existing["raw_sample_count"] = int(existing.get("raw_sample_count") or 0) + int(
        incoming.get("raw_sample_count") or 0
    )
    existing["source_metric_record_count"] = int(existing.get("source_metric_record_count") or 0) + int(
        incoming.get("source_metric_record_count") or 0
    )
    existing["value_sum"] = _coerce_float(existing.get("value_sum")) + _coerce_float(incoming.get("value_sum"))
    existing["value_min"] = min(_coerce_float(existing.get("value_min")), _coerce_float(incoming.get("value_min")))
    existing["value_max"] = max(_coerce_float(existing.get("value_max")), _coerce_float(incoming.get("value_max")))
    existing["p95_value"] = max(_coerce_float(existing.get("p95_value")), _coerce_float(incoming.get("p95_value")))
    existing["p99_value"] = max(_coerce_float(existing.get("p99_value")), _coerce_float(incoming.get("p99_value")))
    existing["latest_value"] = _coerce_float(latest.get("latest_value"))
    existing["value"] = _coerce_float(latest.get("value"))
    existing["observed_at"] = latest.get("observed_at")
    existing["labels"] = dict(latest.get("labels") or {})
    return existing


def _rollup_bucket_end(row: Dict[str, Any]) -> datetime:
    bucket_start = _parse_observed_at(row.get("bucket_start"))
    bucket_seconds = max(int(row.get("bucket_seconds") or _METRIC_ROLLUP_BUCKET_SECONDS), 1)
    return bucket_start + timedelta(seconds=bucket_seconds)


def _coalesce_event_payloads(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if len(rows) <= 1:
        return rows
    grouped: Dict[tuple[Hashable, ...], List[Dict[str, Any]]] = {}
    order: list[tuple[Hashable, ...]] = []
    passthrough: List[Dict[str, Any]] = []
    for row in rows:
        event_name = str(row.get("event_name") or "").strip()
        if event_name not in _COALESCIBLE_EVENT_NAMES:
            passthrough.append(row)
            continue
        key = _event_group_key(row)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(row)

    coalesced = list(passthrough)
    for key in order:
        group = grouped[key]
        if len(group) == 1:
            coalesced.append(group[0])
            continue
        row = dict(group[-1])
        details = dict(row.get("details") or {})
        details["export_aggregation"] = "batch_latest"
        details["export_sample_count"] = len(group)
        details["suppressed_duplicate_events"] = len(group) - 1
        row["observed_at"] = _latest_observed_at(group)
        row["details"] = details
        coalesced.append(row)
    return coalesced


def _is_live_only_metric_payload(row: Dict[str, Any]) -> bool:
    metric_name = str(row.get("metric_name") or "").strip()
    if not metric_name:
        return False
    if metric_name.startswith(_DURABLE_ALWAYS_CAPTURE_PREFIXES):
        return False
    if metric_name.endswith(_DURABLE_ALWAYS_CAPTURE_SUFFIXES):
        return False
    component = str(row.get("component") or "").strip()
    prefixes = _LIVE_ONLY_METRIC_PREFIXES_BY_COMPONENT.get(component, ())
    return any(metric_name == prefix or metric_name.startswith(prefix) for prefix in prefixes)


def _partition_durable_metric_payloads(
    rows: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    durable: List[Dict[str, Any]] = []
    live_only: List[Dict[str, Any]] = []
    for row in rows:
        if _is_live_only_metric_payload(row):
            live_only.append(row)
        else:
            durable.append(row)
    return durable, live_only


def _event_payload(record: EventRecord) -> Dict[str, Any]:
    context = dict(record.context or {})
    message = _clean_text(context.get("message"))
    return {
        "observed_at": record.timestamp,
        "component": _clean_text(context.get("component")) or "unknown",
        "event_name": record.name,
        "level": record.level,
        "bot_id": _clean_text(context.get("bot_id")),
        "run_id": _clean_text(context.get("run_id")),
        "instrument_id": _clean_text(context.get("instrument_id")),
        "series_key": _clean_text(context.get("series_key")),
        "worker_id": _clean_text(context.get("worker_id")),
        "queue_name": _clean_text(context.get("queue_name")),
        "pipeline_stage": _clean_text(context.get("pipeline_stage")),
        "message_kind": _clean_text(context.get("message_kind")),
        "delta_type": _clean_text(context.get("delta_type")),
        "storage_target": _clean_text(context.get("storage_target")),
        "failure_mode": _clean_text(context.get("failure_mode")),
        "phase": _clean_text(context.get("phase")),
        "status": _clean_text(context.get("status")),
        "run_seq": _clean_int(context.get("run_seq")),
        "bridge_session_id": _clean_text(context.get("bridge_session_id")),
        "bridge_seq": _clean_int(context.get("bridge_seq")),
        "message": message,
        "details": context,
    }


def _overflow_event(*, metric_drop_count: int, event_drop_count: int) -> Dict[str, Any]:
    return {
        "observed_at": None,
        "component": "observability_exporter",
        "event_name": "observability_export_queue_overflow",
        "level": "WARN",
        "message": "In-memory observability export queue dropped records before DB persistence.",
        "details": {
            "dropped_metric_records": int(metric_drop_count),
            "dropped_event_records": int(event_drop_count),
        },
    }


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _batch_run_ids(rows: List[Dict[str, Any]]) -> List[str | None]:
    run_ids = sorted({str(row.get("run_id") or "").strip() for row in rows if row.get("run_id")})
    if len(run_ids) > 4:
        return [None]
    return run_ids or [None]


def _exporter_metric_payload(
    *,
    metric_name: str,
    value: float,
    metric_kind: str,
    storage_target: str,
    run_id: str | None = None,
) -> Dict[str, Any]:
    return {
        "observed_at": _utcnow_iso(),
        "component": "observability_exporter",
        "metric_name": metric_name,
        "metric_kind": metric_kind,
        "value": float(value),
        "run_id": _clean_text(run_id),
        "pipeline_stage": "observability_export",
        "storage_target": storage_target,
        "labels": {},
    }


def _has_non_exporter_metrics(rows: List[Dict[str, Any]]) -> bool:
    for row in rows:
        component = _clean_text(row.get("component"))
        metric_name = _clean_text(row.get("metric_name")) or ""
        if component != "observability_exporter" and not metric_name.startswith("observability_"):
            return True
    return False


class ObservabilityExporter:
    """Background worker that drains the in-memory sink into DB-backed tables."""

    def __init__(
        self,
        *,
        sink: Optional[InMemoryObservabilitySink] = None,
        metric_batch_size: Optional[int] = None,
        event_batch_size: Optional[int] = None,
        flush_interval_s: Optional[float] = None,
        rollup_flush_lag_s: Optional[float] = None,
        retry_interval_s: Optional[float] = None,
    ) -> None:
        self._sink = sink or get_observability_sink()
        self._metric_batch_size = max(
            int(metric_batch_size or _SETTINGS.persist_metric_batch_size or 500),
            1,
        )
        self._event_batch_size = max(
            int(event_batch_size or _SETTINGS.persist_event_batch_size or 250),
            1,
        )
        self._flush_interval_s = max(
            float(flush_interval_s or ((_SETTINGS.persist_flush_interval_ms or 1000) / 1000.0)),
            0.01,
        )
        self._retry_interval_s = max(
            float(retry_interval_s or ((_SETTINGS.persist_retry_interval_ms or 1000) / 1000.0)),
            0.01,
        )
        configured_rollup_lag_s = (_SETTINGS.persist_rollup_flush_lag_ms or 0) / 1000.0
        self._metric_rollup_flush_lag_s = max(
            float(configured_rollup_lag_s if rollup_flush_lag_s is None else rollup_flush_lag_s),
            0.0,
        )
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._metric_retry_batch: List[Dict[str, Any]] = []
        self._event_retry_batch: List[Dict[str, Any]] = []
        self._metric_rollup_buffer: Dict[tuple[Hashable, ...], Dict[str, Any]] = {}
        self._metric_rollup_order: List[tuple[Hashable, ...]] = []

    def start(self) -> None:
        if not _SETTINGS.persist_enabled:
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._worker_loop,
                name="botlens-observability-exporter",
                daemon=True,
            )
            self._thread.start()

    def stop(self, *, timeout_s: float = 10.0) -> None:
        with self._lock:
            thread = self._thread
        if thread is None:
            return
        self._stop.set()
        thread.join(timeout=max(float(timeout_s), 0.1))
        if thread.is_alive():
            raise RuntimeError("observability exporter did not stop cleanly")

    def _stage_metric_rollups(self, rows: List[Dict[str, Any]]) -> None:
        for row in rows:
            key = _rollup_buffer_key(row)
            if key in self._metric_rollup_buffer:
                self._metric_rollup_buffer[key] = _merge_rollup_rows(self._metric_rollup_buffer[key], row)
                continue
            self._metric_rollup_buffer[key] = dict(row)
            self._metric_rollup_order.append(key)

    def _drain_metric_rollups(self, *, force: bool = False) -> List[Dict[str, Any]]:
        if not self._metric_rollup_buffer:
            return []
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._metric_rollup_flush_lag_s)
        drained: List[Dict[str, Any]] = []
        retained_order: List[tuple[Hashable, ...]] = []
        for key in self._metric_rollup_order:
            row = self._metric_rollup_buffer.get(key)
            if row is None:
                continue
            if force or self._metric_rollup_flush_lag_s <= 0.0 or _rollup_bucket_end(row) <= cutoff:
                drained.append(row)
                self._metric_rollup_buffer.pop(key, None)
            else:
                retained_order.append(key)
        self._metric_rollup_order = retained_order
        return drained

    def _has_buffered_work(self) -> bool:
        return bool(self._metric_retry_batch or self._event_retry_batch or self._metric_rollup_buffer)

    def flush_once(self, *, force: bool = False) -> bool:
        batch = self._sink.drain_pending(
            metric_limit=self._metric_batch_size,
            event_limit=self._event_batch_size,
        )
        drained_metrics = list(batch.get("metrics") or [])
        drained_events = list(batch.get("events") or [])
        dropped = dict(batch.get("dropped") or {})

        if drained_metrics:
            metric_payloads = [_metric_payload(record) for record in drained_metrics]
            durable_metrics, live_only_metrics = _partition_durable_metric_payloads(metric_payloads)
            self._metric_retry_batch.extend(durable_metrics)
            if live_only_metrics:
                skipped_records = len(live_only_metrics)
                skipped_samples = sum(
                    int(row.get("raw_sample_count") or row.get("sample_count") or 1)
                    for row in live_only_metrics
                )
                for run_id in _batch_run_ids(live_only_metrics):
                    self._metric_retry_batch.extend(
                        [
                            _exporter_metric_payload(
                                metric_name="observability_live_only_metric_records_skipped",
                                value=float(skipped_records),
                                metric_kind="counter",
                                storage_target="observability_metric_rollups",
                                run_id=run_id,
                            ),
                            _exporter_metric_payload(
                                metric_name="observability_live_only_raw_samples_skipped",
                                value=float(skipped_samples),
                                metric_kind="counter",
                                storage_target="observability_metric_rollups",
                                run_id=run_id,
                            ),
                        ]
                    )
        if drained_events:
            self._event_retry_batch.extend(_event_payload(record) for record in drained_events)
        if (dropped.get("metrics") or 0) > 0 or (dropped.get("events") or 0) > 0:
            overflow = _overflow_event(
                metric_drop_count=int(dropped.get("metrics") or 0),
                event_drop_count=int(dropped.get("events") or 0),
            )
            if self._event_retry_batch:
                self._event_retry_batch.append(overflow)
            else:
                self._event_retry_batch = [overflow]

        did_work = bool(drained_metrics or drained_events or dropped.get("metrics") or dropped.get("events"))
        if self._metric_retry_batch:
            raw_metric_batch = list(self._metric_retry_batch)
            self._stage_metric_rollups(_rollup_metric_payloads(raw_metric_batch))
            self._metric_retry_batch = []
            did_work = True
        rollup_batch = self._drain_metric_rollups(force=force)
        if rollup_batch:
            emit_export_metrics = _has_non_exporter_metrics(rollup_batch)
            raw_samples_seen = sum(int(row.get("raw_sample_count") or row.get("sample_count") or 0) for row in rollup_batch)
            source_metric_records_seen = sum(int(row.get("source_metric_record_count") or 1) for row in rollup_batch)
            started = time.perf_counter()
            try:
                rows_written = record_observability_metric_rollups_batch(rollup_batch)
            except Exception as exc:  # noqa: BLE001
                self._stage_metric_rollups(rollup_batch)
                for run_id in _batch_run_ids(rollup_batch):
                    self._metric_retry_batch.append(
                        _exporter_metric_payload(
                            metric_name="observability_export_errors",
                            value=1.0,
                            metric_kind="counter",
                            storage_target="observability_metric_rollups",
                            run_id=run_id,
                        )
                    )
                logger.warning(
                    "botlens_observability_metric_export_failed | batch_size=%s | error=%s",
                    len(raw_metric_batch),
                    exc,
                )
                raise
            else:
                did_work = True
                if emit_export_metrics:
                    elapsed_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
                    reduction_ratio = float(raw_samples_seen) / max(float(rows_written), 1.0)
                    source_reduction_ratio = float(raw_samples_seen) / max(float(source_metric_records_seen), 1.0)
                    for run_id in _batch_run_ids(rollup_batch):
                        self._metric_retry_batch.extend(
                            [
                                _exporter_metric_payload(
                                    metric_name="observability_raw_samples_seen",
                                    value=float(raw_samples_seen),
                                    metric_kind="counter",
                                    storage_target="observability_metric_rollups",
                                    run_id=run_id,
                                ),
                                _exporter_metric_payload(
                                    metric_name="observability_metric_records_seen",
                                    value=float(source_metric_records_seen),
                                    metric_kind="counter",
                                    storage_target="observability_metric_rollups",
                                    run_id=run_id,
                                ),
                                _exporter_metric_payload(
                                    metric_name="observability_rollup_rows_written",
                                    value=float(rows_written),
                                    metric_kind="counter",
                                    storage_target="observability_metric_rollups",
                                    run_id=run_id,
                                ),
                                _exporter_metric_payload(
                                    metric_name="observability_rollup_reduction_ratio",
                                    value=reduction_ratio,
                                    metric_kind="gauge",
                                    storage_target="observability_metric_rollups",
                                    run_id=run_id,
                                ),
                                _exporter_metric_payload(
                                    metric_name="observability_source_budget_reduction_ratio",
                                    value=source_reduction_ratio,
                                    metric_kind="gauge",
                                    storage_target="observability_metric_rollups",
                                    run_id=run_id,
                                ),
                                _exporter_metric_payload(
                                    metric_name="observability_export_db_ms",
                                    value=elapsed_ms,
                                    metric_kind="histogram",
                                    storage_target="observability_metric_rollups",
                                    run_id=run_id,
                                ),
                            ]
                        )
        if self._event_retry_batch:
            event_batch = _coalesce_event_payloads(list(self._event_retry_batch))
            started = time.perf_counter()
            try:
                rows_written = record_observability_events_batch(event_batch)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "botlens_observability_event_export_failed | batch_size=%s | error=%s",
                    len(event_batch),
                    exc,
                )
                raise
            else:
                self._event_retry_batch = []
                elapsed_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
                for run_id in _batch_run_ids(event_batch):
                    self._metric_retry_batch.extend(
                        [
                            _exporter_metric_payload(
                                metric_name="observability_export_db_ms",
                                value=elapsed_ms,
                                metric_kind="histogram",
                                storage_target="observability_events",
                                run_id=run_id,
                            ),
                            _exporter_metric_payload(
                                metric_name="observability_rollup_rows_written",
                                value=float(rows_written),
                                metric_kind="counter",
                                storage_target="observability_events",
                                run_id=run_id,
                            ),
                        ]
                    )
        return did_work

    def _worker_loop(self) -> None:
        while True:
            try:
                if self.flush_once(force=self._stop.is_set()):
                    continue
            except Exception:
                time.sleep(self._retry_interval_s)
                continue
            if self._stop.is_set():
                if not self._sink.wait_for_pending(0.0) and not self._has_buffered_work():
                    break
                continue
            self._sink.wait_for_pending(self._flush_interval_s)


_DEFAULT_EXPORTER = ObservabilityExporter()


def get_observability_exporter() -> ObservabilityExporter:
    return _DEFAULT_EXPORTER


def start_observability_exporter() -> None:
    _DEFAULT_EXPORTER.start()


def stop_observability_exporter(*, timeout_s: float = 10.0) -> None:
    _DEFAULT_EXPORTER.stop(timeout_s=timeout_s)


__all__ = [
    "ObservabilityExporter",
    "get_observability_exporter",
    "start_observability_exporter",
    "stop_observability_exporter",
]
