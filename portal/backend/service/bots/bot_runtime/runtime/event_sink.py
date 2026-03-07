"""Compatibility wrapper for runtime event sinks."""

from engines.bot_runtime.runtime.components.event_sink import InMemoryEventSink, RuntimeEventSink

__all__ = ["InMemoryEventSink", "RuntimeEventSink"]
