"""Lightweight, chart-agnostic overlay payload shapes.

Each overlay adapter should emit a list of :class:`ChartOverlay` objects. The
payload is intentionally minimal and free of plotting-library specifics so that
different renderers (lightweight-charts, mplfinance, custom front-ends) can
translate the same artefacts.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Literal, Mapping, MutableMapping, Optional, Sequence, TypedDict


class MarkerPayload(TypedDict, total=False):
    time: int
    price: float
    shape: str
    color: str
    text: Optional[str]
    position: Optional[str]
    subtype: Optional[str]


class PriceLinePayload(TypedDict, total=False):
    price: float
    color: str
    extend: Literal["none", "right", "left", "both"]
    lineWidth: int
    lineStyle: int
    axisLabelVisible: bool
    title: Optional[str]
    originTime: Optional[int]
    endTime: Optional[int]


class BubblePayload(TypedDict, total=False):
    time: int
    price: float
    label: str
    detail: Optional[str]
    meta: Optional[str]
    bias: Optional[str]
    direction: Optional[str]
    accentColor: Optional[str]
    backgroundColor: Optional[str]
    textColor: Optional[str]
    subtype: Literal["bubble"]


class PolylinePoint(TypedDict):
    time: int
    price: float


class PolylinePayload(TypedDict, total=False):
    points: List[PolylinePoint]
    color: Optional[str]
    lineWidth: Optional[float]
    lineStyle: Optional[int]
    role: Optional[str]
    band: Optional[float]
    side: Optional[str]
    shade: Optional[bool]


class BoxBorderPayload(TypedDict, total=False):
    color: Optional[str]
    width: Optional[float]


class BoxPayload(TypedDict, total=False):
    x1: int
    x2: int
    y1: float
    y2: float
    color: Optional[str]
    border: Optional[BoxBorderPayload]
    precision: Optional[int]
    extend: Optional[bool]


class SegmentPayload(TypedDict, total=False):
    x1: int
    x2: int
    y1: float
    y2: float
    color: Optional[str]
    lineWidth: Optional[float]
    lineStyle: Optional[int]
    role: Optional[str]


class TouchPointPayload(TypedDict, total=False):
    time: int
    price: float
    color: Optional[str]
    size: Optional[float]


class OverlayPayload(TypedDict, total=False):
    bubbles: List[BubblePayload]
    markers: List[MarkerPayload]
    price_lines: List[PriceLinePayload]
    polylines: List[PolylinePayload]
    boxes: List[BoxPayload]
    segments: List[SegmentPayload]
    touch_points: List[TouchPointPayload]
    summary: Optional[Mapping[str, Any]]


class ChartOverlay(TypedDict, total=False):
    type: str
    payload: OverlayPayload
    pane_views: Sequence[str]
    renderers: Mapping[str, Any]
    ui: Mapping[str, Any]


def _default_payload() -> OverlayPayload:
    return {
        "bubbles": [],
        "markers": [],
        "price_lines": [],
        "polylines": [],
        "boxes": [],
        "segments": [],
        "touch_points": [],
    }


def coerce_overlay_payload(payload: Mapping[str, Any] | None) -> OverlayPayload:
    """Ensure overlay payload keys exist with list defaults.

    Adapters can emit partial payloads; this helper fills missing collections so
    downstream renderers can rely on their presence.
    """

    base: OverlayPayload = _default_payload()
    if payload is None:
        return base

    mutable: MutableMapping[str, Any] = dict(payload)
    if "touch_points" not in mutable and "touchPoints" in mutable:
        mutable["touch_points"] = mutable.get("touchPoints")
    for key, default_value in base.items():
        if key not in mutable:
            mutable[key] = default_value
    return mutable  # type: ignore[return-value]


logger = logging.getLogger(__name__)


def build_overlay(indicator_type: str, payload: Mapping[str, Any] | None) -> ChartOverlay:
    """Create a :class:`ChartOverlay` with a normalized payload."""

    overlay: ChartOverlay = {
        "type": indicator_type,
        "payload": coerce_overlay_payload(payload),
    }
    from .builtins import ensure_builtin_overlays_registered
    from .registry import get_overlay_spec, validate_overlay_payload

    ensure_builtin_overlays_registered()
    spec = get_overlay_spec(indicator_type)
    if not spec:
        logger.warning("overlay_spec_missing | type=%s", indicator_type)
        raise ValueError(f"overlay spec missing for type '{indicator_type}'")
    if not spec.pane_views:
        logger.warning("overlay_spec_missing_pane_views | type=%s", indicator_type)
        raise ValueError(f"overlay spec missing pane_views for type '{indicator_type}'")
    overlay["pane_views"] = list(spec.pane_views)
    if spec.renderers:
        overlay["renderers"] = dict(spec.renderers)
    overlay["ui"] = {
        "label": spec.label,
        "color": spec.ui_color,
        "default_visible": spec.ui_default_visible,
    }
    logger.debug(
        "overlay_spec_resolved | type=%s | pane_views=%s | payload_keys=%s | ui_color=%s | ui_default_visible=%s",
        indicator_type,
        spec.pane_views,
        spec.payload_keys,
        spec.ui_color,
        spec.ui_default_visible,
    )
    validate_overlay_payload(indicator_type, overlay["payload"])
    return overlay


def normalize_overlays(
    indicator_type: str, overlays: List[Mapping[str, Any]] | None
) -> List[ChartOverlay]:
    """Normalize arbitrary overlay adapter output to the canonical schema."""

    if not overlays:
        return []

    normalised: List[ChartOverlay] = []
    for entry in overlays:
        if entry is None:
            continue
        if "type" in entry and "payload" in entry:
            overlay_type = str(entry.get("type"))
            payload = coerce_overlay_payload(entry.get("payload"))
            overlay = build_overlay(overlay_type, payload)
            if "pane_views" in entry:
                overlay["pane_views"] = entry.get("pane_views")
            if "renderers" in entry:
                overlay["renderers"] = entry.get("renderers")
            normalised.append(overlay)
        else:
            normalised.append(build_overlay(indicator_type, entry))
    return normalised


__all__ = [
    "MarkerPayload",
    "PriceLinePayload",
    "BubblePayload",
    "PolylinePayload",
    "BoxPayload",
    "SegmentPayload",
    "TouchPointPayload",
    "OverlayPayload",
    "ChartOverlay",
    "build_overlay",
    "coerce_overlay_payload",
    "normalize_overlays",
]
