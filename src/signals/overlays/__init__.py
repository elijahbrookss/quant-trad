"""Overlay payload schema and helpers."""

from .registry import (
    OverlaySpec,
    get_overlay_spec,
    list_overlay_specs,
    overlay_type,
    register_overlay_type,
    validate_overlay_payload,
)
from .schema import (
    BoxPayload,
    BubblePayload,
    ChartOverlay,
    MarkerPayload,
    OverlayPayload,
    PolylinePayload,
    SegmentPayload,
    TouchPointPayload,
    build_overlay,
    coerce_overlay_payload,
    normalize_overlays,
)
from .builtins import ensure_builtin_overlays_registered

__all__ = [
    "OverlaySpec",
    "get_overlay_spec",
    "list_overlay_specs",
    "overlay_type",
    "register_overlay_type",
    "validate_overlay_payload",
    "BoxPayload",
    "BubblePayload",
    "ChartOverlay",
    "MarkerPayload",
    "OverlayPayload",
    "PolylinePayload",
    "SegmentPayload",
    "TouchPointPayload",
    "build_overlay",
    "coerce_overlay_payload",
    "normalize_overlays",
    "ensure_builtin_overlays_registered",
]
