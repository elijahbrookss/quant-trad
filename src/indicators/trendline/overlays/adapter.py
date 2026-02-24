"""Overlay adapter for trendline artefacts."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

import pandas as pd

from ..compute.engine import TL, TrendlineIndicator
from signals.base import BaseSignal
from signals.overlays.registry import overlay_type
from signals.overlays.schema import build_overlay, PolylinePayload


def _to_unix_seconds(ts: pd.Timestamp) -> int:
    ts = pd.Timestamp(ts)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return int(ts.timestamp())


def _line_segment(line: TL, index: pd.DatetimeIndex) -> PolylinePayload:
    start = index[line.i_from]
    end_idx = min(line.i_to, len(index) - 1)
    end = index[end_idx]

    start_time = _to_unix_seconds(start)
    end_time = _to_unix_seconds(end)
    start_price = float(line.slope * line.i_from + line.intercept)
    end_price = float(line.slope * line.i_to + line.intercept)

    return {
        "points": [
            {"time": start_time, "price": start_price},
            {"time": end_time, "price": end_price},
        ],
        "color": "#38bdf8" if line.side == "support" else "#f97316",
        "lineWidth": 1.25,
        "lineStyle": 0,
        "role": line.side,
    }


@overlay_type(
    TrendlineIndicator.NAME,
    label="Trendline",
    pane_views=("polyline", "touch"),
    description="Trendline segments and touch markers.",
    renderers={"lightweight": "polyline", "mpl": "line"},
    payload_keys=("polylines", "markers"),
    ui_color="#a855f7",
)
def trendline_overlay_adapter(
    signals: Sequence[BaseSignal],
    plot_df: pd.DataFrame,
    indicator: TrendlineIndicator | None = None,
    **_: Any,
) -> List[Dict[str, Any]]:
    """Convert computed trendlines into chart-agnostic polylines."""

    polylines: List[PolylinePayload] = []
    markers: List[Dict[str, Any]] = []

    if indicator is not None and indicator.lines:
        index = indicator.df.index
        for line in indicator.lines:
            polylines.append(_line_segment(line, index))
            for touch in getattr(line, "touches", []) or []:
                try:
                    touch_time = _to_unix_seconds(touch)
                    markers.append(
                        {
                            "time": touch_time,
                            "price": float(indicator.df.loc[touch, "close"]),
                            "shape": "circle",
                            "color": "#c084fc",
                            "position": "aboveBar" if line.side == "support" else "belowBar",
                            "subtype": "touch",
                        }
                    )
                except Exception:
                    continue

    if indicator is None and signals:
        for sig in signals:
            meta = sig.metadata or {}
            points = meta.get("points") or meta.get("line_points")
            if points:
                polylines.append(
                    {
                        "points": [
                            {"time": int(pt["time"]), "price": float(pt["price"])}
                            for pt in points
                            if "time" in pt and "price" in pt
                        ],
                        "color": meta.get("color"),
                        "lineWidth": meta.get("lineWidth", 1.0),
                        "lineStyle": meta.get("lineStyle", 0),
                        "role": meta.get("role"),
                    }
                )
            touch_price = meta.get("touch_price")
            touch_time = meta.get("touch_time")
            if touch_price is not None and touch_time is not None:
                markers.append(
                    {
                        "time": int(touch_time),
                        "price": float(touch_price),
                        "shape": "circle",
                        "color": meta.get("color", "#c084fc"),
                        "position": meta.get("position"),
                        "subtype": meta.get("subtype", "touch"),
                    }
                )

    if not polylines and not markers:
        return []

    payload = {"polylines": polylines, "markers": markers}
    return [build_overlay(TrendlineIndicator.NAME, payload)]


__all__ = ["trendline_overlay_adapter"]
