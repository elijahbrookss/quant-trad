"""Market Profile overlay registration and transform exports."""

from overlays.registry import register_overlay_type

from .adapter import market_profile_overlay_transformer


register_overlay_type(
    ["market_profile", "market-profile", "mpf"],
    label="Market Profile",
    pane_views=("va_box", "touch"),
    description="Market profile value area boxes and touch markers.",
    renderers={"lightweight": "va_box", "mpl": "box"},
    payload_keys=("boxes", "markers", "bubbles"),
    ui_color="#38bdf8",
)


__all__ = ["market_profile_overlay_transformer"]
