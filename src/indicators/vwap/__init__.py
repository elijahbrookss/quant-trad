"""VWAP indicator package."""

from __future__ import annotations

from typing import Any

__all__ = [
    "MANIFEST",
    "VWAPIndicator",
    "VWAPIndicatorDefinition",
]


def __getattr__(name: str) -> Any:
    if name == "MANIFEST":
        from .manifest import MANIFEST

        return MANIFEST
    if name == "VWAPIndicatorDefinition":
        from .definition import VWAPIndicatorDefinition

        return VWAPIndicatorDefinition
    if name == "VWAPIndicator":
        from .compute import VWAPIndicator

        return VWAPIndicator
    raise AttributeError(name)
