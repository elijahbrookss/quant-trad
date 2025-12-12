"""Helpers for managing rule coordination caches."""

from __future__ import annotations

from typing import Any, Callable, Iterable, MutableMapping, Optional

from signals.rules.patterns import maybe_mutable_context


def ensure_cache(
    context: Any,
    key: str,
    default_factory: Callable[[], Any],
    *,
    ready_flag: Optional[str] = None,
    initialised_flag: Optional[str] = None,
) -> Optional[MutableMapping[str, Any]]:
    """Ensure a cache exists within the mutable context and reset readiness flags."""

    mutable = maybe_mutable_context(context)
    if mutable is None:
        return None

    cache = mutable.get(key)
    default_value = default_factory()
    if not isinstance(cache, type(default_value)):
        mutable[key] = default_value
        if initialised_flag is not None:
            mutable[initialised_flag] = True
    if ready_flag is not None:
        mutable[ready_flag] = False
    return mutable


def append_to_cache(context: Any, key: str, items: Iterable[Any]) -> Optional[MutableMapping[str, Any]]:
    """Append iterable ``items`` into a list cache on the context."""

    mutable = maybe_mutable_context(context)
    if mutable is None:
        return None

    cache = mutable.get(key)
    if isinstance(cache, list):
        cache.extend(items)
    else:
        mutable[key] = list(items)
    return mutable


def mark_ready(context: Any, ready_flag: str, *, ready: bool = True) -> Optional[MutableMapping[str, Any]]:
    """Mark a readiness flag in the mutable context when available."""

    mutable = maybe_mutable_context(context)
    if mutable is None:
        return None

    mutable[ready_flag] = ready
    return mutable
