"""Market Profile typed runtime exports."""

from __future__ import annotations

from typing import Any

__all__ = ["TypedMarketProfileIndicator"]


def __getattr__(name: str) -> Any:
    if name != "TypedMarketProfileIndicator":
        raise AttributeError(name)
    from .typed_indicator import TypedMarketProfileIndicator

    return TypedMarketProfileIndicator
