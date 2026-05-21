"""Market Profile compute exports with lazy loading."""

from __future__ import annotations

from typing import Any

__all__ = ["MarketProfileIndicator", "Profile", "ValueArea"]


def __getattr__(name: str) -> Any:
    if name == "MarketProfileIndicator":
        from .engine import MarketProfileIndicator

        return MarketProfileIndicator
    if name in {"Profile", "ValueArea"}:
        from .models import Profile, ValueArea

        exports = {
            "Profile": Profile,
            "ValueArea": ValueArea,
        }
        return exports[name]
    raise AttributeError(name)
