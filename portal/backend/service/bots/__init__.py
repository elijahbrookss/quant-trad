"""Bot services and runtime package."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .bot_runtime import BotRuntime
    from .bot_service import start_bot, stop_bot
    from .bot_stream import BotStreamManager

__all__ = ["BotRuntime", "BotStreamManager", "start_bot", "stop_bot"]


def __getattr__(name: str):
    if name == "BotRuntime":
        from .bot_runtime import BotRuntime

        return BotRuntime
    if name in {"start_bot", "stop_bot"}:
        from .bot_service import start_bot, stop_bot

        return start_bot if name == "start_bot" else stop_bot
    if name == "BotStreamManager":
        from .bot_stream import BotStreamManager

        return BotStreamManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
