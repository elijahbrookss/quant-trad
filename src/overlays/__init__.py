"""Shared chart-overlay contracts and helpers."""

from .builders import build_line_overlay
from .builtins import ensure_builtin_overlays_registered
from .registry import (
    OverlaySpec,
    get_overlay_spec,
    list_overlay_specs,
    overlay_type,
    register_overlay_type,
    validate_overlay_payload,
)
from .schema import (
    BubblePayload,
    ChartOverlay,
    MarkerPayload,
    OverlayPayload,
    build_overlay,
    coerce_overlay_payload,
)
from .transformers import (
    apply_overlay_transform,
    normalize_overlay_epoch,
    overlay_transformer,
)

__all__ = [
    "BubblePayload",
    "ChartOverlay",
    "MarkerPayload",
    "OverlayPayload",
    "OverlaySpec",
    "apply_overlay_transform",
    "build_line_overlay",
    "build_overlay",
    "coerce_overlay_payload",
    "ensure_builtin_overlays_registered",
    "get_overlay_spec",
    "list_overlay_specs",
    "normalize_overlay_epoch",
    "overlay_transformer",
    "overlay_type",
    "register_overlay_type",
    "validate_overlay_payload",
]
