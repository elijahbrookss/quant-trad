"""Backend observability substrate for contract-based instrumentation.

This module intentionally keeps metrics and structured operational events
separate:
  - metrics are emitted into a lightweight in-memory sink for counters,
    histograms, and gauges,
  - events are emitted into the same sink and logged as structured records.

The sink is process-local and test-friendly. Dashboard/export wiring can be
added later without changing call sites.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Mapping, MutableMapping, Optional

from utils.log_context import build_log_context, with_log_context

logger = logging.getLogger(__name__)

_NAME_RE = re.compile(r"[^a-z0-9]+")
_DEFAULT_GAUGE_INTERVAL_S = 1.0
_ALLOWED_METRIC_LABELS = frozenset(
    {
        "bot_id",
        "run_id",
        "instrument_id",
        "series_key",
        "component",
        "worker_id",
        "queue_name",
        "pipeline_stage",
        "message_kind",
        "delta_type",
        "storage_target",
        "failure_mode",
    }
)
_DISALLOWED_METRIC_LABELS = frozenset(
    {
        "viewer_id",
        "viewer_session_id",
        "event_id",
        "request_id",
        "trade_id",
        "exception",
        "error",
        "payload",
        "raw_payload",
    }
)
_DEPRECATED_MESSAGE_KINDS = frozenset({"bot_projection_refresh"})
_ALLOWED_METRIC_MESSAGE_KINDS = frozenset(
    {
        "bootstrap",
        "botlens_lifecycle_event",
        "botlens_open_trades_delta",
        "botlens_run_connected",
        "botlens_run_summary_delta",
        "botlens_runtime_bootstrap_facts",
        "botlens_runtime_facts",
        "botlens_symbol_snapshot",
        "broadcast",
        "connected",
        "ephemeral",
        "facts",
        "ingest_ws",
        "legacy",
        "lifecycle",
        "notification",
        "open_trades_delta",
        "snapshot",
        "snapshot_buffer",
        "snapshot_replay",
        "summary_delta",
        "symbol_candle_delta",
        "symbol_decision_delta",
        "symbol_log_delta",
        "symbol_overlay_delta",
        "symbol_runtime_delta",
        "symbol_trade_delta",
        "typed_delta",
        "unknown",
        "deprecated",
    }
)


def normalize_name(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unknown"
    return _NAME_RE.sub("_", text).strip("_") or "unknown"


def normalize_failure_mode(value: Any) -> str:
    if isinstance(value, BaseException):
        text = value.__class__.__name__
    else:
        text = str(value or "").strip()
    return normalize_name(text or "unknown")


def normalize_metric_message_kind(value: Any) -> str:
    normalized = normalize_name(value or "unknown")
    if normalized in _DEPRECATED_MESSAGE_KINDS:
        return "deprecated"
    if normalized in _ALLOWED_METRIC_MESSAGE_KINDS:
        return normalized
    return "unknown"


def payload_size_bytes(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    try:
        return len(json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"))
    except Exception:
        return len(str(value).encode("utf-8"))


def canonical_context(
    *,
    bot_id: Any = None,
    run_id: Any = None,
    instrument_id: Any = None,
    series_key: Any = None,
    component: Any = None,
    worker_id: Any = None,
    **fields: Any,
) -> Dict[str, Any]:
    context = build_log_context(
        bot_id=str(bot_id).strip() or None if bot_id is not None else None,
        run_id=str(run_id).strip() or None if run_id is not None else None,
        instrument_id=str(instrument_id).strip() or None if instrument_id is not None else None,
        series_key=str(series_key).strip() or None if series_key is not None else None,
        component=normalize_name(component) if component is not None else None,
        worker_id=str(worker_id).strip() or None if worker_id is not None else None,
    )
    context.update(build_log_context(**fields))
    return context


def metric_labels(*contexts: Mapping[str, Any], **fields: Any) -> Dict[str, str]:
    merged: Dict[str, Any] = {}
    for context in contexts:
        for key, value in (context or {}).items():
            merged[key] = value
    merged.update(fields)
    labels: Dict[str, str] = {}
    for key, value in merged.items():
        normalized_key = normalize_name(key)
        if normalized_key in _DISALLOWED_METRIC_LABELS:
            continue
        if normalized_key not in _ALLOWED_METRIC_LABELS:
            continue
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        if normalized_key == "message_kind":
            labels[normalized_key] = normalize_metric_message_kind(text)
            continue
        if normalized_key == "failure_mode":
            labels[normalized_key] = normalize_failure_mode(text)
            continue
        labels[normalized_key] = text
    return labels


@dataclass(frozen=True)
class MetricRecord:
    kind: str
    name: str
    value: float
    labels: Dict[str, str]
    timestamp: str


@dataclass(frozen=True)
class EventRecord:
    name: str
    level: str
    context: Dict[str, Any]
    timestamp: str


@dataclass(frozen=True)
class QueueStateMetricOwner:
    observer: "BackendObserver"
    key: str
    depth_metric: str
    utilization_metric: str
    labels: Dict[str, Any]
    oldest_age_metric: Optional[str] = None

    def emit(
        self,
        *,
        depth: int,
        capacity: int,
        oldest_age_ms: Optional[float] = None,
    ) -> None:
        self.observer.maybe_emit_gauges(
            self.key,
            depth_metric=self.depth_metric,
            utilization_metric=self.utilization_metric,
            oldest_age_metric=self.oldest_age_metric,
            oldest_age_ms=oldest_age_ms,
            depth=depth,
            capacity=capacity,
            **self.labels,
        )


class InMemoryObservabilitySink:
    """Thread-safe process-local sink used by backend instrumentation."""

    def __init__(self, *, max_metrics: int = 50_000, max_events: int = 10_000) -> None:
        self._metrics: deque[MetricRecord] = deque(maxlen=max_metrics)
        self._events: deque[EventRecord] = deque(maxlen=max_events)
        self._lock = threading.Lock()

    def emit_metric(self, record: MetricRecord) -> None:
        with self._lock:
            self._metrics.append(record)

    def emit_event(self, record: EventRecord) -> None:
        with self._lock:
            self._events.append(record)

    def snapshot(self) -> Dict[str, list[Dict[str, Any]]]:
        with self._lock:
            return {
                "metrics": [
                    {
                        "kind": item.kind,
                        "name": item.name,
                        "value": item.value,
                        "labels": dict(item.labels),
                        "timestamp": item.timestamp,
                    }
                    for item in list(self._metrics)
                ],
                "events": [
                    {
                        "name": item.name,
                        "level": item.level,
                        "context": dict(item.context),
                        "timestamp": item.timestamp,
                    }
                    for item in list(self._events)
                ],
            }

    def reset(self) -> None:
        with self._lock:
            self._metrics.clear()
            self._events.clear()


_DEFAULT_SINK = InMemoryObservabilitySink()


def get_observability_sink() -> InMemoryObservabilitySink:
    return _DEFAULT_SINK


def reset_observability_sink() -> None:
    _DEFAULT_SINK.reset()


class BackendObserver:
    """Reusable observer for backend components."""

    def __init__(
        self,
        *,
        component: str,
        sink: Optional[InMemoryObservabilitySink] = None,
        event_logger: Optional[logging.Logger] = None,
        gauge_interval_s: float = _DEFAULT_GAUGE_INTERVAL_S,
    ) -> None:
        self.component = normalize_name(component)
        self._sink = sink or _DEFAULT_SINK
        self._event_logger = event_logger or logger
        self._gauge_interval_s = max(float(gauge_interval_s), 0.1)
        self._gauge_emit_at: Dict[str, float] = {}
        self._lock = threading.Lock()

    def context(self, **fields: Any) -> Dict[str, Any]:
        return canonical_context(component=self.component, **fields)

    def increment(self, name: str, value: float = 1.0, **fields: Any) -> None:
        self._emit_metric("counter", name, value, fields)

    def observe(self, name: str, value: float, **fields: Any) -> None:
        self._emit_metric("histogram", name, value, fields)

    def gauge(self, name: str, value: float, **fields: Any) -> None:
        self._emit_metric("gauge", name, value, fields)

    def event(
        self,
        name: str,
        *,
        level: int = logging.INFO,
        log_to_logger: Optional[bool] = None,
        **fields: Any,
    ) -> None:
        normalized = normalize_name(name)
        context = self.context(**fields)
        record = EventRecord(
            name=normalized,
            level=logging.getLevelName(level),
            context=context,
            timestamp=_utcnow_iso(),
        )
        self._sink.emit_event(record)
        should_log = bool(level >= logging.WARN) if log_to_logger is None else bool(log_to_logger)
        if should_log:
            log_context = dict(context)
            log_context["event_name"] = normalized
            self._event_logger.log(level, with_log_context(normalized, log_context))

    @contextmanager
    def timed(self, metric_name: str, **fields: Any) -> Iterator[MutableMapping[str, Any]]:
        started = time.perf_counter()
        mutable_fields: Dict[str, Any] = dict(fields)
        try:
            yield mutable_fields
        finally:
            elapsed_ms = max((time.perf_counter() - started) * 1000.0, 0.0)
            self.observe(metric_name, elapsed_ms, **mutable_fields)

    def maybe_emit_gauges(
        self,
        key: str,
        *,
        depth_metric: str,
        utilization_metric: str,
        depth: int,
        capacity: int,
        oldest_age_metric: Optional[str] = None,
        oldest_age_ms: Optional[float] = None,
        **fields: Any,
    ) -> None:
        token = f"{self.component}:{normalize_name(key)}"
        now = time.monotonic()
        with self._lock:
            last = self._gauge_emit_at.get(token)
            if last is not None and now - float(last) < self._gauge_interval_s:
                return
            self._gauge_emit_at[token] = now
        depth_value = max(int(depth), 0)
        capacity_value = max(int(capacity), 1)
        self.gauge(depth_metric, float(depth_value), **fields)
        self.gauge(utilization_metric, float(depth_value) / float(capacity_value), **fields)
        if oldest_age_metric and oldest_age_ms is not None:
            self.gauge(oldest_age_metric, max(float(oldest_age_ms), 0.0), **fields)

    def maybe_gauge(self, key: str, name: str, value: float, **fields: Any) -> None:
        token = f"{self.component}:{normalize_name(key)}:{normalize_name(name)}"
        now = time.monotonic()
        with self._lock:
            last = self._gauge_emit_at.get(token)
            if last is not None and now - float(last) < self._gauge_interval_s:
                return
            self._gauge_emit_at[token] = now
        self.gauge(name, value, **fields)

    def _emit_metric(self, kind: str, name: str, value: float, fields: Mapping[str, Any]) -> None:
        normalized = normalize_name(name)
        labels = metric_labels(self.context(), fields)
        record = MetricRecord(
            kind=kind,
            name=normalized,
            value=float(value),
            labels=labels,
            timestamp=_utcnow_iso(),
        )
        self._sink.emit_metric(record)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "BackendObserver",
    "EventRecord",
    "InMemoryObservabilitySink",
    "MetricRecord",
    "canonical_context",
    "get_observability_sink",
    "metric_labels",
    "normalize_failure_mode",
    "normalize_name",
    "payload_size_bytes",
    "reset_observability_sink",
]
