"""Trendline overlay registration."""

from overlays.registry import register_overlay_type


register_overlay_type(
    "trendline",
    label="Trendline",
    pane_views=("polyline", "touch"),
    description="Trendline segments and touch markers.",
    renderers={"lightweight": "polyline", "mpl": "line"},
    payload_keys=("polylines", "markers"),
    ui_color="#a855f7",
)


__all__: list[str] = []
