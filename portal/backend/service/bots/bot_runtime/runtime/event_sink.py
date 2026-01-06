"""Runtime event sink interfaces for bot runtime."""

from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Deque, Mapping, Protocol


class RuntimeEventSink(Protocol):
    """Abstract sink for runtime and decision events."""

    def record_log(self, entry: Mapping[str, object]) -> None:
        ...

    def record_decision(self, entry: Mapping[str, object]) -> None:
        ...


class InMemoryEventSink:
    """Event sink backed by deques."""

    def __init__(
        self,
        logs: Deque[Mapping[str, object]],
        decisions: Deque[Mapping[str, object]],
        lock: Lock,
    ) -> None:
        self._logs = logs
        self._decisions = decisions
        self._lock = lock

    def record_log(self, entry: Mapping[str, object]) -> None:
        with self._lock:
            self._logs.append(dict(entry))

    def record_decision(self, entry: Mapping[str, object]) -> None:
        with self._lock:
            self._decisions.append(dict(entry))


class CompositeEventSink:
    """Fan-out sink for multiple event targets."""

    def __init__(self, sinks: list[RuntimeEventSink]) -> None:
        self._sinks = list(sinks)

    def record_log(self, entry: Mapping[str, object]) -> None:
        for sink in self._sinks:
            sink.record_log(entry)

    def record_decision(self, entry: Mapping[str, object]) -> None:
        for sink in self._sinks:
            sink.record_decision(entry)


__all__ = ["RuntimeEventSink", "InMemoryEventSink", "CompositeEventSink"]
