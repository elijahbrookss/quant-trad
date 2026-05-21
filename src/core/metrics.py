"""Shared immutable metric primitives and sink abstractions."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from threading import Lock
from typing import Callable, Deque, Dict, Generic, Optional, Protocol, TypeVar

from .events import normalize_utc_datetime, serialize_datetime


class MetricType(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


@dataclass(frozen=True)
class Metric:
    metric_name: str
    metric_type: MetricType
    value: float
    unit: str
    timestamp: datetime
    source: str
    tags: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.metric_name).strip():
            raise ValueError("metric_name is required")
        if not str(self.source).strip():
            raise ValueError("source is required")
        object.__setattr__(self, "timestamp", normalize_utc_datetime(self.timestamp))
        object.__setattr__(self, "value", float(self.value))
        object.__setattr__(self, "unit", str(self.unit or ""))
        object.__setattr__(
            self,
            "tags",
            {
                str(key): str(value)
                for key, value in dict(self.tags or {}).items()
                if str(key).strip() and str(value).strip()
            },
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "metric_name": self.metric_name,
            "metric_type": self.metric_type.value,
            "value": float(self.value),
            "unit": self.unit,
            "timestamp": serialize_datetime(self.timestamp),
            "source": self.source,
            "tags": dict(self.tags),
        }


TMetric = TypeVar("TMetric")


class MetricSink(Protocol, Generic[TMetric]):
    def emit(self, metric: TMetric) -> None:
        ...


class InMemoryMetricSink(Generic[TMetric]):
    """Thread-safe in-memory metric sink."""

    def __init__(
        self,
        metrics: Deque[TMetric],
        lock: Lock,
        on_metric: Optional[Callable[[], None]] = None,
    ) -> None:
        self._metrics = metrics
        self._lock = lock
        self._on_metric = on_metric

    def emit(self, metric: TMetric) -> None:
        with self._lock:
            self._metrics.append(metric)
            callback = self._on_metric
        if callback is not None:
            callback()


class CompositeMetricSink(Generic[TMetric]):
    """Fan-out sink for multiple metric targets."""

    def __init__(self, sinks: list[MetricSink[TMetric]]) -> None:
        self._sinks = list(sinks)

    def emit(self, metric: TMetric) -> None:
        for sink in self._sinks:
            sink.emit(metric)


__all__ = [
    "CompositeMetricSink",
    "InMemoryMetricSink",
    "Metric",
    "MetricSink",
    "MetricType",
]
