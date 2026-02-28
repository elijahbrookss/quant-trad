"""Indicator package exports."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .market_profile import MarketProfileIndicator, market_profile_overlay_adapter
    from .pivot_level import PivotLevelIndicator
    from .trendline import TrendlineIndicator, trendline_overlay_adapter
    from .vwap import VWAPIndicator, vwap_overlay_adapter

__all__ = [
    "MarketProfileIndicator",
    "PivotLevelIndicator",
    "TrendlineIndicator",
    "VWAPIndicator",
    "market_profile_overlay_adapter",
    "trendline_overlay_adapter",
    "vwap_overlay_adapter",
]


def __getattr__(name: str):
    if name in {"MarketProfileIndicator", "market_profile_overlay_adapter"}:
        from .market_profile import MarketProfileIndicator, market_profile_overlay_adapter

        return MarketProfileIndicator if name == "MarketProfileIndicator" else market_profile_overlay_adapter
    if name == "PivotLevelIndicator":
        from .pivot_level import PivotLevelIndicator

        return PivotLevelIndicator
    if name in {"TrendlineIndicator", "trendline_overlay_adapter"}:
        from .trendline import TrendlineIndicator, trendline_overlay_adapter

        return TrendlineIndicator if name == "TrendlineIndicator" else trendline_overlay_adapter
    if name in {"VWAPIndicator", "vwap_overlay_adapter"}:
        from .vwap import VWAPIndicator, vwap_overlay_adapter

        return VWAPIndicator if name == "VWAPIndicator" else vwap_overlay_adapter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
