"""Registry and protocol for indicators that support incremental caching.

This module provides a declarative way for indicators to opt into incremental caching
without requiring hard-coded type checks in the service layer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Protocol

if TYPE_CHECKING:
    from indicators.config import DataContext
    from .incremental_cache import IncrementalCache


class IncrementalCacheable(Protocol):
    """
    Protocol for indicators that support incremental caching.

    Indicators implementing this protocol can cache intermediate computation
    results (e.g., daily profiles, session data, pivot levels) to avoid
    recomputing them on subsequent runs.

    The protocol is intentionally minimal - it only requires implementing
    a method to instantiate with cache support. What you cache and how
    you use it is entirely up to your indicator.
    """

    @classmethod
    def from_context_with_incremental_cache(
        cls,
        provider: Any,
        ctx: "DataContext",
        cache: "IncrementalCache",
        inst_id: str,
        **kwargs,
    ) -> "IncrementalCacheable":
        """
        Instantiate indicator with incremental caching support.

        This method should:
        1. Check the cache for previously computed data
        2. Only fetch/compute missing data
        3. Store newly computed data in the cache
        4. Return an instance with combined cached + fresh data

        Args:
            provider: Data provider
            ctx: DataContext with symbol, start, end, interval
            cache: IncrementalCache instance for storing/retrieving data
            inst_id: Indicator instance ID (used for cache keys)
            **kwargs: Additional indicator-specific parameters

        Returns:
            Indicator instance with data from cache + fresh computation

        Example:
            @classmethod
            def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
                # Check cache for daily data
                cached_data = cache.get_range(
                    inst_id,
                    ctx.symbol,
                    ["2025-01-10", "2025-01-11", ...]
                )

                if len(cached_data) >= expected:
                    return cls._from_cached(cached_data, **kwargs)

                # Fetch and compute missing data
                df = provider.get_ohlcv(ctx)
                instance = cls(df, **kwargs)

                # Cache newly computed data
                for date, data in instance.get_daily_data().items():
                    cache.set(inst_id, ctx.symbol, date, data)

                return instance
        """
        ...


_REGISTRY: Dict[str, Dict[str, Any]] = {}


def incremental_cacheable(
    indicator_type: Optional[str] = None,
    *,
    enabled: bool = True,
) -> Callable:
    """
    Decorator to mark an indicator as supporting incremental caching.

    This is a zero-configuration opt-in - just add the decorator and implement
    the required method. The framework handles the rest.

    Usage:
        @incremental_cacheable("my_indicator")
        @indicator(name="my_indicator", inputs=["ohlc"], outputs=["data"])
        class MyIndicator(ComputeIndicator):
            NAME = "my_indicator"

            @classmethod
            def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
                # Your caching logic here
                ...

    Args:
        indicator_type: Name to register (defaults to class NAME attribute)
        enabled: Whether caching is enabled (default: True)

    Returns:
        Decorator function
    """

    def wrapper(cls):
        name = indicator_type or getattr(cls, "NAME", None) or cls.__name__
        if name:
            # Verify the class implements the required method
            if not hasattr(cls, "from_context_with_incremental_cache"):
                raise TypeError(
                    f"{cls.__name__} must implement from_context_with_incremental_cache() "
                    f"to be @incremental_cacheable"
                )

            _REGISTRY[str(name).lower()] = {
                "enabled": bool(enabled),
                "indicator_class": cls,
            }
        return cls

    return wrapper


def is_incremental_cacheable(indicator_type: str) -> bool:
    """Check if an indicator type supports incremental caching."""
    entry = _REGISTRY.get(str(indicator_type).lower())
    return entry is not None and entry.get("enabled", False)


def get_incremental_cacheable_class(indicator_type: str) -> Optional[type]:
    """Get the indicator class for an incremental-cacheable type."""
    entry = _REGISTRY.get(str(indicator_type).lower())
    if entry and entry.get("enabled"):
        return entry.get("indicator_class")
    return None


def get_incremental_cacheable_types() -> list[str]:
    """Get all registered incremental-cacheable indicator types."""
    return [name for name, meta in _REGISTRY.items() if meta.get("enabled")]


__all__ = [
    "IncrementalCacheable",
    "incremental_cacheable",
    "is_incremental_cacheable",
    "get_incremental_cacheable_class",
    "get_incremental_cacheable_types",
]
