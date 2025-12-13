"""Lightweight, chart-agnostic overlay payload shapes.

Each overlay adapter should emit a list of :class:`ChartOverlay` objects. The
payload is intentionally minimal and free of plotting-library specifics so that
different renderers (lightweight-charts, mplfinance, custom front-ends) can
translate the same artefacts.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Mapping, MutableMapping, Optional, TypedDict


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


class OverlayPayload(TypedDict, total=False):
    bubbles: List[BubblePayload]
    markers: List[MarkerPayload]
    price_lines: List[PriceLinePayload]
    polylines: List[PolylinePayload]
    summary: Optional[Mapping[str, Any]]


class ChartOverlay(TypedDict):
    type: str
    payload: OverlayPayload


def _default_payload() -> OverlayPayload:
    return {
        "bubbles": [],
        "markers": [],
        "price_lines": [],
        "polylines": [],
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
    for key, default_value in base.items():
        if key not in mutable:
            mutable[key] = default_value
    return mutable  # type: ignore[return-value]


def build_overlay(indicator_type: str, payload: Mapping[str, Any] | None) -> ChartOverlay:
    """Create a :class:`ChartOverlay` with a normalized payload."""

    return {
        "type": indicator_type,
        "payload": coerce_overlay_payload(payload),
    }


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
            normalised.append(
                {
                    "type": str(entry.get("type")),
                    "payload": coerce_overlay_payload(entry.get("payload")),
                }
            )
        else:
            normalised.append(build_overlay(indicator_type, entry))
    return normalised


__all__ = [
    "MarkerPayload",
    "PriceLinePayload",
    "BubblePayload",
    "PolylinePayload",
    "OverlayPayload",
    "ChartOverlay",
    "build_overlay",
    "coerce_overlay_payload",
    "normalize_overlays",
]
