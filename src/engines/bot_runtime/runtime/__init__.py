"""Bot runtime package."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import BotRuntime
    from .core import _timeframe_to_seconds

__all__ = ["BotRuntime", "_timeframe_to_seconds"]


def __getattr__(name: str):
    if name == "BotRuntime":
        from .runtime import BotRuntime

        return BotRuntime
    if name == "_timeframe_to_seconds":
        from .core import _timeframe_to_seconds

        return _timeframe_to_seconds
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
