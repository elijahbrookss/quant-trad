"""Pivot level indicator package."""

from __future__ import annotations

from typing import Any

__all__ = [
    "MANIFEST",
    "Level",
    "PivotLevelIndicator",
    "PivotLevelIndicatorDefinition",
]


def __getattr__(name: str) -> Any:
    if name == "MANIFEST":
        from .manifest import MANIFEST

        return MANIFEST
    if name == "PivotLevelIndicatorDefinition":
        from .definition import PivotLevelIndicatorDefinition

        return PivotLevelIndicatorDefinition
    if name in {"PivotLevelIndicator", "Level"}:
        from .compute import Level, PivotLevelIndicator

        exports = {
            "Level": Level,
            "PivotLevelIndicator": PivotLevelIndicator,
        }
        return exports[name]
    raise AttributeError(name)
