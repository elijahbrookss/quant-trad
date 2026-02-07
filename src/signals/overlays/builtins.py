"""Built-in overlay registrations to ensure registry coverage."""

from __future__ import annotations

import logging
from typing import Optional

from .registry import list_overlay_specs, register_overlay_type

_REGISTERED = False
logger = logging.getLogger(__name__)


def ensure_builtin_overlays_registered() -> None:
    global _REGISTERED
    if _REGISTERED:
        return

    register_overlay_type(
        ["market_profile", "market-profile", "mpf"],
        label="Market Profile",
        pane_views=("va_box", "touch"),
        description="Market profile value area boxes and touch markers.",
        renderers={"lightweight": "va_box", "mpl": "box"},
        payload_keys=("boxes", "markers", "bubbles"),
        ui_color="#38bdf8",
    )
    register_overlay_type(
        "trendline",
        label="Trendline",
        pane_views=("polyline", "touch"),
        description="Trendline segments and touch markers.",
        renderers={"lightweight": "polyline", "mpl": "line"},
        payload_keys=("polylines", "markers"),
        ui_color="#a855f7",
    )
    register_overlay_type(
        "vwap",
        label="VWAP",
        pane_views=("polyline", "touch"),
        description="VWAP and deviation bands with touch markers.",
        renderers={"lightweight": "polyline", "mpl": "line"},
        payload_keys=("polylines", "markers"),
        ui_color="#f97316",
    )
    register_overlay_type(
        "pivot_level",
        label="Pivot Levels",
        pane_views=("signal_bubble", "touch"),
        description="Pivot level retest/breakout bubbles and touches.",
        renderers={"lightweight": "signal_bubble", "mpl": "scatter"},
        payload_keys=("bubbles", "markers", "price_lines"),
        ui_color="#facc15",
    )
    register_overlay_type(
        "strategy_signal",
        label="Strategy Signals",
        pane_views=("marker",),
        description="Strategy evaluation signal markers.",
        renderers={"lightweight": "marker", "mpl": "scatter"},
        payload_keys=("markers",),
        ui_color="#10b981",
    )
    register_overlay_type(
        "regime_overlay",
        label="Market Structure",
        pane_views=("va_box", "segment"),
        description="Regime structure bands with optional change markers.",
        renderers={"lightweight": "va_box", "mpl": "box"},
        payload_keys=("boxes", "segments", "regime_points"),
        ui_color="#94a3b8",
    )
    register_overlay_type(
        "regime_markers",
        label="Regime Markers",
        pane_views=("marker",),
        description="Regime change markers for research context.",
        renderers={"lightweight": "marker", "mpl": "scatter"},
        payload_keys=("markers",),
        ui_color="#94a3b8",
        ui_default_visible=False,
    )

    # Per-lens regime overlays (structure, expansion, liquidity, volatility, etc.)
    lens_colors = {
        "structure": "#f59e0b",
        "expansion": "#a855f7",
        "liquidity": "#22d3ee",
        "volatility": "#0ea5e9",
    }
    for lens in ("structure", "expansion", "liquidity", "volatility"):
        register_overlay_type(
            f"regime_overlay_{lens}",
            label=f"{lens.title()} Band",
            pane_views=("va_box",),
            description=f"Regime {lens} lens bands.",
            renderers={"lightweight": "va_box", "mpl": "box"},
            payload_keys=("boxes",),
            ui_color=lens_colors.get(lens, "#94a3b8"),
            ui_default_visible=False,
        )

    _REGISTERED = True
    logger.info("overlay_registry_initialized | overlay_types=%d", len(list_overlay_specs()))


__all__ = ["ensure_builtin_overlays_registered"]
