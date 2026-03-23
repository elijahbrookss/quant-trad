"""Trendline indicator package."""

from __future__ import annotations

from typing import Any

__all__ = [
    "MANIFEST",
    "TL",
    "TrendlineIndicator",
    "TrendlineIndicatorDefinition",
]


def __getattr__(name: str) -> Any:
    if name == "MANIFEST":
        from .manifest import MANIFEST

        return MANIFEST
    if name == "TrendlineIndicatorDefinition":
        from .definition import TrendlineIndicatorDefinition

        return TrendlineIndicatorDefinition
    if name in {"TrendlineIndicator", "TL"}:
        from .compute import TL, TrendlineIndicator

        exports = {
            "TL": TL,
            "TrendlineIndicator": TrendlineIndicator,
        }
        return exports[name]
    raise AttributeError(name)
