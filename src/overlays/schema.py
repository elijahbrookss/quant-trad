"""Lightweight, chart-agnostic overlay payload shapes."""

from __future__ import annotations

import logging
import threading
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
    known_at: Optional[int | float | str]
    trigger_price: Optional[float]
    reference: Optional[Mapping[str, Any]]
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
    pane_key: str
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
    """Ensure overlay payload keys exist with list defaults."""

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
_OVERLAY_SPEC_LOGGED: set[tuple[str, tuple[str, ...], tuple[str, ...], str | None, bool | None]] = set()
_OVERLAY_SPEC_LOGGED_LOCK = threading.Lock()


def _log_overlay_spec_resolved_once(
    *,
    indicator_type: str,
    pane_key: str,
    pane_views: Sequence[str],
    payload_keys: Sequence[str],
    ui_color: str | None,
    ui_default_visible: bool | None,
) -> None:
    signature = (
        indicator_type,
        pane_key,
        tuple(str(v) for v in pane_views),
        tuple(str(v) for v in payload_keys),
        ui_color,
        ui_default_visible,
    )
    with _OVERLAY_SPEC_LOGGED_LOCK:
        if signature in _OVERLAY_SPEC_LOGGED:
            return
        _OVERLAY_SPEC_LOGGED.add(signature)
    logger.debug(
        "overlay_spec_resolved | type=%s | pane_key=%s | pane_views=%s | payload_keys=%s | ui_color=%s | ui_default_visible=%s",
        indicator_type,
        pane_key,
        pane_views,
        payload_keys,
        ui_color,
        ui_default_visible,
    )


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
    overlay["pane_key"] = str(spec.pane_key or "price")
    overlay["pane_views"] = list(spec.pane_views)
    if spec.renderers:
        overlay["renderers"] = dict(spec.renderers)
    overlay["ui"] = {
        "label": spec.label,
        "color": spec.ui_color,
        "default_visible": spec.ui_default_visible,
    }
    _log_overlay_spec_resolved_once(
        indicator_type=indicator_type,
        pane_key=str(spec.pane_key or "price"),
        pane_views=spec.pane_views,
        payload_keys=spec.payload_keys,
        ui_color=spec.ui_color,
        ui_default_visible=spec.ui_default_visible,
    )
    validate_overlay_payload(indicator_type, overlay["payload"])
    return overlay


__all__ = [
    "BubblePayload",
    "ChartOverlay",
    "MarkerPayload",
    "OverlayPayload",
    "build_overlay",
    "coerce_overlay_payload",
]
