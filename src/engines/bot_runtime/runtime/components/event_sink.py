"""Runtime event sink interfaces for bot runtime."""

from __future__ import annotations

from threading import Lock
from typing import Callable, Deque, Optional, Protocol

from core.events import CompositeEventSink as SharedCompositeEventSink
from core.events import InMemoryEventSink as SharedInMemoryEventSink

from engines.bot_runtime.core.runtime_events import RuntimeEvent


class RuntimeEventSink(Protocol):
    """Abstract sink for canonical runtime events."""

    def emit(self, event: RuntimeEvent) -> None:
        ...


class InMemoryEventSink(SharedInMemoryEventSink[RuntimeEvent]):
    """Runtime event sink backed by a deque."""

    def __init__(
        self,
        events: Deque[RuntimeEvent],
        lock: Lock,
        on_event: Optional[Callable[[], None]] = None,
    ) -> None:
        super().__init__(events=events, lock=lock, on_event=on_event)


class CompositeEventSink(SharedCompositeEventSink[RuntimeEvent]):
    """Fan-out sink for multiple runtime event targets."""


__all__ = ["RuntimeEventSink", "InMemoryEventSink", "CompositeEventSink"]
