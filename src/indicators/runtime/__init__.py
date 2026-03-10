"""Indicator runtime utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .indicator_breakout_cache import IndicatorBreakoutCache, default_breakout_cache
    from .indicator_overlay_cache import IndicatorOverlayCache, default_overlay_cache
    from .incremental_cache import IncrementalCache, default_incremental_cache

__all__ = [
    "IndicatorBreakoutCache",
    "default_breakout_cache",
    "IndicatorOverlayCache",
    "default_overlay_cache",
    "IncrementalCache",
    "default_incremental_cache",
]


def __getattr__(name: str):
    if name in {"IndicatorBreakoutCache", "default_breakout_cache"}:
        from .indicator_breakout_cache import IndicatorBreakoutCache, default_breakout_cache

        return IndicatorBreakoutCache if name == "IndicatorBreakoutCache" else default_breakout_cache
    if name in {"IndicatorOverlayCache", "default_overlay_cache"}:
        from .indicator_overlay_cache import IndicatorOverlayCache, default_overlay_cache

        return IndicatorOverlayCache if name == "IndicatorOverlayCache" else default_overlay_cache
    if name in {"IncrementalCache", "default_incremental_cache"}:
        from .incremental_cache import IncrementalCache, default_incremental_cache

        return IncrementalCache if name == "IncrementalCache" else default_incremental_cache
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
