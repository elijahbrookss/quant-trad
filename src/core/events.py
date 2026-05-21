"""Shared immutable event primitives and sink abstractions."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from threading import Lock
from typing import Any, Callable, Deque, Generic, Mapping, Optional, Protocol, TypeVar


def normalize_utc_datetime(value: datetime) -> datetime:
    """Return a timezone-aware UTC datetime."""

    target = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return target.astimezone(timezone.utc)


def serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return normalize_utc_datetime(value).isoformat().replace("+00:00", "Z")


def parse_optional_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return normalize_utc_datetime(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return normalize_utc_datetime(datetime.fromisoformat(text))


def serialize_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return serialize_datetime(value)
    if is_dataclass(value):
        data: dict[str, Any] = {}
        for field_name in getattr(value, "__dataclass_fields__", {}):
            data[field_name] = serialize_value(getattr(value, field_name))
        return data
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return serialize_value(value.to_dict())
    if isinstance(value, Mapping):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize_value(item) for item in value]
    return value


class EventContext(Protocol):
    def to_dict(self) -> dict[str, Any]:
        ...


class EventName(Protocol):
    value: str


@dataclass(frozen=True)
class EventEnvelope:
    schema_version: int
    event_id: str
    event_ts: datetime
    event_name: Enum
    root_id: str
    parent_id: Optional[str]
    correlation_id: str
    context: Any

    def __post_init__(self) -> None:
        if int(self.schema_version) < 1:
            raise ValueError("schema_version must be >= 1")
        if not str(self.event_id).strip():
            raise ValueError("event_id is required")
        if not isinstance(self.event_name, Enum):
            raise ValueError("event_name must be an Enum value")
        if not str(self.root_id).strip():
            raise ValueError("root_id is required")
        if not str(self.correlation_id).strip():
            raise ValueError("correlation_id is required")
        object.__setattr__(self, "event_ts", normalize_utc_datetime(self.event_ts))

    def serialize(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "event_id": self.event_id,
            "event_ts": serialize_datetime(self.event_ts),
            "event_name": self.event_name.value,
            "root_id": self.root_id,
            "parent_id": self.parent_id,
            "correlation_id": self.correlation_id,
            "context": serialize_value(self.context),
        }

TEvent = TypeVar("TEvent")


class EventSink(Protocol, Generic[TEvent]):
    def emit(self, event: TEvent) -> None:
        ...


class InMemoryEventSink(Generic[TEvent]):
    """Thread-safe in-memory event sink."""

    def __init__(
        self,
        events: Deque[TEvent],
        lock: Lock,
        on_event: Optional[Callable[[], None]] = None,
    ) -> None:
        self._events = events
        self._lock = lock
        self._on_event = on_event

    def emit(self, event: TEvent) -> None:
        with self._lock:
            self._events.append(event)
            callback = self._on_event
        if callback is not None:
            callback()


class CompositeEventSink(Generic[TEvent]):
    """Fan-out sink for multiple event targets."""

    def __init__(self, sinks: list[EventSink[TEvent]]) -> None:
        self._sinks = list(sinks)

    def emit(self, event: TEvent) -> None:
        for sink in self._sinks:
            sink.emit(event)


__all__ = [
    "CompositeEventSink",
    "EventEnvelope",
    "EventContext",
    "EventSink",
    "InMemoryEventSink",
    "normalize_utc_datetime",
    "parse_optional_datetime",
    "serialize_datetime",
    "serialize_value",
]
