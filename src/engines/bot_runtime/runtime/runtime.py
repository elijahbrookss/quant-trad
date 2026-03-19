"""Bot runtime orchestration assembly."""

from __future__ import annotations

from .mixins import (
    RuntimeExecutionLoopMixin,
    RuntimeEventsMixin,
    RuntimePersistenceMixin,
    RuntimeProjectionMixin,
    RuntimePushStreamMixin,
    RuntimeSetupPrepareMixin,
)


class BotRuntime(
    RuntimeSetupPrepareMixin,
    RuntimeExecutionLoopMixin,
    RuntimeEventsMixin,
    RuntimePersistenceMixin,
    RuntimeProjectionMixin,
    RuntimePushStreamMixin,
):
    """Simulated bot runtime that iterates over real candles and emits stats."""


__all__ = ["BotRuntime"]
