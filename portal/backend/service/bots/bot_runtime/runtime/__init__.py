"""Runtime orchestration for bot execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import BotRuntime
    from .runtime_policy import RuntimeModePolicy
    from .event_sink import InMemoryEventSink, RuntimeEventSink
    from .run_context import RunContext

__all__ = ["BotRuntime", "RuntimeModePolicy", "InMemoryEventSink", "RuntimeEventSink", "RunContext"]


def __getattr__(name: str):
    if name == "BotRuntime":
        from .runtime import BotRuntime

        return BotRuntime
    if name == "RuntimeModePolicy":
        from .runtime_policy import RuntimeModePolicy

        return RuntimeModePolicy
    if name in {"InMemoryEventSink", "RuntimeEventSink"}:
        from .event_sink import InMemoryEventSink, RuntimeEventSink

        return InMemoryEventSink if name == "InMemoryEventSink" else RuntimeEventSink
    if name == "RunContext":
        from .run_context import RunContext

        return RunContext
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
