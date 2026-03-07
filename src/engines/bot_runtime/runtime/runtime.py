"""Bot runtime orchestration assembly."""

from __future__ import annotations

from signals.overlays.registry import register_overlay_type

from .mixins import (
    RuntimeExecutionLoopMixin,
    RuntimeEventsMixin,
    RuntimeSetupPrepareMixin,
    RuntimeStateStreamingMixin,
)

register_overlay_type(
    "bot_trade_rays",
    label="Trade Rays",
    pane_views=("segment",),
    description="Active trade stop/target rays for bot playback.",
    renderers={"lightweight": "segment", "mpl": "line"},
    payload_keys=("segments",),
    ui_color="#22d3ee",
)


class BotRuntime(
    RuntimeSetupPrepareMixin,
    RuntimeExecutionLoopMixin,
    RuntimeEventsMixin,
    RuntimeStateStreamingMixin,
):
    """Simulated bot runtime that iterates over real candles and emits stats."""


__all__ = ["BotRuntime"]
