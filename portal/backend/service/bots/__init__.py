"""Bot services package with light import-time surface."""

from __future__ import annotations

from .bot_stream import BotStreamManager


def start_bot(bot_id: str):
    from .bot_service import start_bot as _start_bot

    return _start_bot(bot_id)


def stop_bot(bot_id: str, *, preserve_container: bool = False):
    from .bot_service import stop_bot as _stop_bot

    return _stop_bot(bot_id, preserve_container=preserve_container)


__all__ = ["BotStreamManager", "start_bot", "stop_bot"]
