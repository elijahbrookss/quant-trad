"""Bounded DB-backed exporter for the shared backend observability sink."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from core.settings import get_settings

from core.metrics import Metric

from .observability import EventRecord, InMemoryObservabilitySink, get_observability_sink
from .storage.repos.observability import (
    record_observability_events_batch,
    record_observability_metric_samples_batch,
)

logger = logging.getLogger(__name__)
_SETTINGS = get_settings().observability


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
    return trimmed


def _metric_payload(record: Metric) -> Dict[str, Any]:
    labels = dict(record.tags or {})
    persisted_labels = _persisted_metric_labels(labels)
    return {
        "observed_at": record.to_dict().get("timestamp"),
        "component": _clean_text(record.source) or "unknown",
        "metric_name": record.metric_name,
        "metric_kind": record.metric_type.value,
        "value": float(record.value),
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


class ObservabilityExporter:
    """Background worker that drains the in-memory sink into DB-backed tables."""

    def __init__(
        self,
        *,
        sink: Optional[InMemoryObservabilitySink] = None,
        metric_batch_size: Optional[int] = None,
        event_batch_size: Optional[int] = None,
        flush_interval_s: Optional[float] = None,
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
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._metric_retry_batch: List[Dict[str, Any]] = []
        self._event_retry_batch: List[Dict[str, Any]] = []

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

    def flush_once(self) -> bool:
        batch = self._sink.drain_pending(
            metric_limit=self._metric_batch_size,
            event_limit=self._event_batch_size,
        )
        drained_metrics = list(batch.get("metrics") or [])
        drained_events = list(batch.get("events") or [])
        dropped = dict(batch.get("dropped") or {})

        if drained_metrics:
            self._metric_retry_batch.extend(_metric_payload(record) for record in drained_metrics)
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

        did_work = bool(self._metric_retry_batch or self._event_retry_batch)
        if self._metric_retry_batch:
            try:
                record_observability_metric_samples_batch(self._metric_retry_batch)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "botlens_observability_metric_export_failed | batch_size=%s | error=%s",
                    len(self._metric_retry_batch),
                    exc,
                )
                raise
            else:
                self._metric_retry_batch = []
        if self._event_retry_batch:
            try:
                record_observability_events_batch(self._event_retry_batch)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "botlens_observability_event_export_failed | batch_size=%s | error=%s",
                    len(self._event_retry_batch),
                    exc,
                )
                raise
            else:
                self._event_retry_batch = []
        return did_work

    def _worker_loop(self) -> None:
        while True:
            try:
                if self.flush_once():
                    continue
            except Exception:
                time.sleep(self._retry_interval_s)
                continue
            if self._stop.is_set():
                if not self._sink.wait_for_pending(0.0):
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
