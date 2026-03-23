"""Small helpers for common overlay payload construction."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .schema import ChartOverlay, build_overlay


def build_line_overlay(
    overlay_type: str,
    *,
    points: Sequence[Mapping[str, Any]],
    color: str | None = None,
    role: str | None = None,
    line_width: float = 1.5,
    line_style: int = 0,
    band: float | None = None,
    side: str | None = None,
    shade: bool | None = None,
) -> ChartOverlay:
    """Build a single-polyline overlay for simple line/oscillator series."""

    polyline: dict[str, Any] = {
        "points": [dict(point) for point in points],
        "lineWidth": line_width,
        "lineStyle": line_style,
    }
    if color is not None:
        polyline["color"] = color
    if role is not None:
        polyline["role"] = role
    if band is not None:
        polyline["band"] = band
    if side is not None:
        polyline["side"] = side
    if shade is not None:
        polyline["shade"] = shade
    return build_overlay(overlay_type, {"polylines": [polyline]})


__all__ = ["build_line_overlay"]
