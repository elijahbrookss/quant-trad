"""Compatibility wrapper for bot runtime implementation.

The canonical implementation now lives in `engines.bot_runtime.runtime`.
"""

from __future__ import annotations

from engines.bot_runtime.runtime import BotRuntime, _timeframe_to_seconds

__all__ = ["BotRuntime", "_timeframe_to_seconds"]
