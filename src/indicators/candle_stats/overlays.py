"""Overlay registrations for candle stats runtime outputs."""

from __future__ import annotations

from overlays.registry import register_overlay_type


register_overlay_type(
    "candle_stats_atr_short",
    label="ATR Short",
    pane_key="volatility",
    pane_views=("polyline",),
    description="Short ATR line rendered in the volatility pane.",
    renderers={"lightweight": "polyline", "mpl": "line"},
    payload_keys=("polylines",),
    ui_color="#ef4444",
)

register_overlay_type(
    "candle_stats_atr_long",
    label="ATR Long",
    pane_key="volatility",
    pane_views=("polyline",),
    description="Long ATR baseline rendered in the volatility pane.",
    renderers={"lightweight": "polyline", "mpl": "line"},
    payload_keys=("polylines",),
    ui_color="#22c55e",
)

register_overlay_type(
    "candle_stats_atr_zscore",
    label="ATR Z-Score",
    pane_key="oscillator",
    pane_views=("polyline",),
    description="ATR z-score rendered in the oscillator pane.",
    renderers={"lightweight": "polyline", "mpl": "line"},
    payload_keys=("polylines",),
    ui_color="#38bdf8",
)


__all__ = []
