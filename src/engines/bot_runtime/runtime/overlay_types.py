"""Explicit registration for bot-runtime-owned overlay contracts."""

from __future__ import annotations

from signals.overlays.registry import register_overlay_type

_REGISTERED = False


def ensure_runtime_overlay_types_registered() -> None:
    global _REGISTERED
    if _REGISTERED:
        return
    register_overlay_type(
        "bot_trade_rays",
        label="Trade Rays",
        pane_views=("segment",),
        description="Active trade stop/target rays for bot playback.",
        renderers={"lightweight": "segment", "mpl": "line"},
        payload_keys=("segments",),
        ui_color="#22d3ee",
    )
    _REGISTERED = True


__all__ = ["ensure_runtime_overlay_types_registered"]
