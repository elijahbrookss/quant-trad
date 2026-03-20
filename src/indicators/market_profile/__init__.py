"""Market profile package with lazy exports."""

from __future__ import annotations

from typing import Any

__all__ = [
    "MANIFEST",
    "MarketProfileIndicator",
    "Profile",
    "ValueArea",
    "market_profile_overlay_adapter",
]


def __getattr__(name: str) -> Any:
    if name in {"MarketProfileIndicator", "Profile", "ValueArea"}:
        from .compute import MarketProfileIndicator, Profile, ValueArea

        exports = {
            "MarketProfileIndicator": MarketProfileIndicator,
            "Profile": Profile,
            "ValueArea": ValueArea,
        }
        return exports[name]
    if name == "MANIFEST":
        from .manifest import MANIFEST

        return MANIFEST
    if name == "market_profile_overlay_adapter":
        from .overlays import market_profile_overlay_adapter

        return market_profile_overlay_adapter
    raise AttributeError(name)
