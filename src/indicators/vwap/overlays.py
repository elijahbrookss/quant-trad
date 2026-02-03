"""Overlay adapter for VWAP and its deviation bands."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

import pandas as pd

from .indicator import VWAPIndicator
from signals.base import BaseSignal
from signals.engine.signal_generator import overlay_adapter
from signals.overlays.registry import overlay_type
from signals.overlays.schema import build_overlay, PolylinePayload


def _to_unix_seconds(ts: pd.Timestamp) -> int:
    ts = pd.Timestamp(ts)
    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    return int(ts.timestamp())


def _aligned_series(indicator: VWAPIndicator, plot_df: pd.DataFrame, column: str) -> List[float]:
    values = indicator.df[column]
    n = len(plot_df.index)
    if len(values) >= n:
        return [float(x) for x in values.values[:n]]
    return [float(x) for x in values.reindex(plot_df.index, method="nearest").values]


@overlay_type(
    VWAPIndicator.NAME,
    label="VWAP",
    pane_views=("polyline", "touch"),
    description="VWAP and deviation bands with touch markers.",
    renderers={"lightweight": "polyline", "mpl": "line"},
    payload_keys=("polylines", "markers"),
    ui_color="#f97316",
)
@overlay_adapter(VWAPIndicator)
def vwap_overlay_adapter(
    signals: Sequence[BaseSignal],
    plot_df: pd.DataFrame,
    indicator: VWAPIndicator | None = None,
    include_touches: bool = True,
    **_: Any,
) -> List[Dict[str, Any]]:
    """Emit VWAP polylines and touch markers as chart-agnostic overlays."""

    if indicator is None or plot_df is None or plot_df.empty:
        return []

    times = [_to_unix_seconds(ts) for ts in plot_df.index]
    polylines: List[PolylinePayload] = []
    markers: List[Dict[str, Any]] = []

    vwap_values = _aligned_series(indicator, plot_df, "vwap")
    polylines.append(
        {
            "points": [{"time": times[i], "price": vwap_values[i]} for i in range(len(times))],
            "lineStyle": 0,
            "lineWidth": 1,
            "color": "#6b7280",
            "role": "vwap",
        }
    )

    for m in indicator.stddev_multipliers:
        up = _aligned_series(indicator, plot_df, f"upper_{int(m)}std")
        lo = _aligned_series(indicator, plot_df, f"lower_{int(m)}std")
        polylines.append(
            {
                "points": [{"time": times[i], "price": up[i]} for i in range(len(times))],
                "lineStyle": 2,
                "lineWidth": 0.75,
                "color": "#9ca3af",
                "band": float(m),
                "side": "upper",
                "shade": True,
            }
        )
        polylines.append(
            {
                "points": [{"time": times[i], "price": lo[i]} for i in range(len(times))],
                "lineStyle": 2,
                "lineWidth": 0.75,
                "color": "#9ca3af",
                "band": float(m),
                "side": "lower",
                "shade": True,
            }
        )

        if include_touches:
            for i, ts in enumerate(plot_df.index):
                low = float(plot_df.at[ts, "low"])
                high = float(plot_df.at[ts, "high"])
                if low <= up[i] <= high:
                    markers.append(
                        {
                            "time": times[i],
                            "position": "belowBar",
                            "shape": "circle",
                            "color": "#6b7280",
                            "price": float(up[i]),
                            "subtype": "touch",
                        }
                    )
                if low <= lo[i] <= high:
                    markers.append(
                        {
                            "time": times[i],
                            "position": "aboveBar",
                            "shape": "circle",
                            "color": "#6b7280",
                            "price": float(lo[i]),
                            "subtype": "touch",
                        }
                    )

    payload = {"polylines": polylines, "markers": markers}
    return [build_overlay(VWAPIndicator.NAME, payload)]


__all__ = ["vwap_overlay_adapter"]
