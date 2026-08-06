"""Bot services package with light import-time surface."""

from __future__ import annotations

from .bot_stream import BotStreamManager


def start_bot(
    bot_id: str,
    *,
    economic_claim_intent: str,
    execution_assumptions: dict | None = None,
):
    from .bot_service import start_bot as _start_bot

    overrides = {"economic_claim_intent": economic_claim_intent}
    if execution_assumptions is not None:
        overrides["execution_assumptions"] = dict(execution_assumptions)
    return _start_bot(bot_id, start_overrides=overrides)


def stop_bot(bot_id: str, *, preserve_container: bool = False):
    from .bot_service import stop_bot as _stop_bot

    return _stop_bot(bot_id, preserve_container=preserve_container)


__all__ = ["BotStreamManager", "start_bot", "stop_bot"]
