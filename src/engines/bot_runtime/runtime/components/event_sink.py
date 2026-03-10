"""Runtime event sink interfaces for bot runtime."""

from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Callable, Deque, Mapping, Optional, Protocol


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
        on_log: Optional[Callable[[], None]] = None,
        on_decision: Optional[Callable[[], None]] = None,
    ) -> None:
        self._logs = logs
        self._decisions = decisions
        self._lock = lock
        self._on_log = on_log
        self._on_decision = on_decision

    def record_log(self, entry: Mapping[str, object]) -> None:
        with self._lock:
            self._logs.append(dict(entry))
            callback = self._on_log
        if callback is not None:
            callback()

    def record_decision(self, entry: Mapping[str, object]) -> None:
        with self._lock:
            self._decisions.append(dict(entry))
            callback = self._on_decision
        if callback is not None:
            callback()


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
