"""Shared runtime event-type names used across runtime and portal read models."""

from __future__ import annotations

from engines.bot_runtime.core.runtime_events import RuntimeEventName

RUNTIME_PREFIX = "runtime."
SERIES_BAR_PREFIX = "series_bar."
BOTLENS_PREFIX = "botlens."

BOTLENS_SERIES_BOOTSTRAP = f"{BOTLENS_PREFIX}series_bootstrap"
BOTLENS_SERIES_DELTA = f"{BOTLENS_PREFIX}series_delta"
SERIES_BAR_TELEMETRY = f"{SERIES_BAR_PREFIX}telemetry"


def runtime_event_type(value: RuntimeEventName | str) -> str:
    name = value.value if isinstance(value, RuntimeEventName) else str(value or "")
    normalized = name.strip().lower()
    if not normalized:
        raise ValueError("runtime event type requires a non-empty event name")
    return f"{RUNTIME_PREFIX}{normalized}"


__all__ = [
    "BOTLENS_SERIES_BOOTSTRAP",
    "BOTLENS_SERIES_DELTA",
    "BOTLENS_PREFIX",
    "RUNTIME_PREFIX",
    "SERIES_BAR_PREFIX",
    "SERIES_BAR_TELEMETRY",
    "runtime_event_type",
]
