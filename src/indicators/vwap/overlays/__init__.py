"""VWAP overlay registration."""

from overlays.registry import register_overlay_type


register_overlay_type(
    "vwap_bands",
    label="VWAP",
    pane_views=("polyline", "touch"),
    description="VWAP and deviation bands with touch markers.",
    renderers={"lightweight": "polyline", "mpl": "line"},
    payload_keys=("polylines", "markers"),
    ui_color="#f97316",
)


__all__: list[str] = []
