"""Registry for indicators that opt into overlay caching."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Optional

_REGISTRY: Dict[str, Dict[str, Any]] = {}


def overlay_cacheable(indicator_type: Optional[str] = None, *, enabled: bool = True) -> Callable:
    """Decorator to mark an indicator type as overlay-cacheable."""

    def wrapper(cls):
        name = indicator_type or getattr(cls, "NAME", None) or cls.__name__
        if name:
            _REGISTRY[str(name).lower()] = {"enabled": bool(enabled)}
        return cls

    return wrapper


def get_overlay_cache_types() -> Iterable[str]:
    return [name for name, meta in _REGISTRY.items() if meta.get("enabled")]


__all__ = ["overlay_cacheable", "get_overlay_cache_types"]
