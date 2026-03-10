"""Generic incremental caching for indicators.

This module provides a flexible caching system that indicators can use to cache
any intermediate computation results, enabling efficient incremental updates.
"""

from __future__ import annotations

from collections import OrderedDict
import logging
import os
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Hashable, Optional, Set, Tuple

from utils.perf_log import get_obs_enabled

logger = logging.getLogger(__name__)

@dataclass
class IncrementalCache:
    """
    Generic cache for storing indicator computation artifacts.

    Unlike overlay caching (which caches final rendered payloads), this cache
    stores intermediate computation results that can be reused across runs.

    The cache is completely agnostic to what you're caching - it could be:
    - Daily profiles (Market Profile)
    - Pivot levels by session (Pivot Points)
    - Volume nodes (Volume Profile)
    - Anchored VWAP calculations
    - Any other incrementally computable data

    Usage:
        cache = IncrementalCache()

        # Store a single item
        cache.set("indicator-123", "BTCUSD", "2025-01-13", my_data)

        # Retrieve it
        data = cache.get("indicator-123", "BTCUSD", "2025-01-13")

        # Store multiple items
        cache.set_many("indicator-123", "BTCUSD", {
            "2025-01-10": data1,
            "2025-01-11": data2,
        })

        # Get a range of items
        items = cache.get_range("indicator-123", "BTCUSD", ["2025-01-10", "2025-01-11"])
    """

    max_entries: int = 10000  # Increased default for more capacity

    def __post_init__(self) -> None:
        # Cache key: (inst_id, symbol, key) -> value
        # Key can be anything: date string, timestamp, session ID, etc.
        # NOTE: In-memory LRU cache; per-process only, no locks for concurrent writers.
        self._cache: "OrderedDict[Tuple[str, str, Hashable], Any]" = OrderedDict()
        # Scope index to avoid full-cache scans for per-indicator fingerprint lookups.
        self._scope_index: Dict[Tuple[str, str], Set[Tuple[str, str, Hashable]]] = {}

    def _build_key(
        self, inst_id: str, symbol: str, key: Hashable
    ) -> Tuple[str, str, Hashable]:
        """Build cache key from components."""
        return (inst_id, symbol, key)

    def get(self, inst_id: str, symbol: str, key: Hashable) -> Optional[Any]:
        """
        Retrieve a single cached item.

        Args:
            inst_id: Indicator instance ID
            symbol: Trading symbol
            key: Arbitrary key (date, timestamp, session ID, etc.)

        Returns:
            Cached value or None if not found
        """
        cache_key = self._build_key(inst_id, symbol, key)
        cached = self._cache.get(cache_key)
        if cached is None:
            return None
        # Move to end for LRU ordering
        self._cache.move_to_end(cache_key)
        return deepcopy(cached)

    def set(self, inst_id: str, symbol: str, key: Hashable, value: Any) -> None:
        """
        Store a single item in the cache.

        Args:
            inst_id: Indicator instance ID
            symbol: Trading symbol
            key: Arbitrary key (date, timestamp, session ID, etc.)
            value: Value to cache (will be deep copied)
        """
        cache_key = self._build_key(inst_id, symbol, key)
        self._cache[cache_key] = deepcopy(value)
        self._cache.move_to_end(cache_key)
        self._scope_add(cache_key)
        self._enforce_limit()

    def get_range(
        self, inst_id: str, symbol: str, keys: list[Hashable]
    ) -> Dict[Hashable, Any]:
        """
        Retrieve multiple cached items.

        Args:
            inst_id: Indicator instance ID
            symbol: Trading symbol
            keys: List of keys to retrieve

        Returns:
            Dict mapping keys to cached values (missing keys omitted)
        """
        results = {}
        for key in keys:
            value = self.get(inst_id, symbol, key)
            if value is not None:
                results[key] = value
        return results

    def set_many(
        self, inst_id: str, symbol: str, items: Dict[Hashable, Any]
    ) -> None:
        """
        Store multiple items at once.

        Args:
            inst_id: Indicator instance ID
            symbol: Trading symbol
            items: Dict mapping keys to values
        """
        for key, value in items.items():
            self.set(inst_id, symbol, key, value)

    def has(self, inst_id: str, symbol: str, key: Hashable) -> bool:
        """Check if a key exists in cache."""
        cache_key = self._build_key(inst_id, symbol, key)
        return cache_key in self._cache

    def purge_indicator(self, inst_id: str) -> None:
        """Remove all cached items for a given indicator instance."""
        if not inst_id:
            return
        stale_keys = [key for key in self._cache.keys() if key[0] == inst_id]
        for cache_key in stale_keys:
            self._cache.pop(cache_key, None)
            self._scope_discard(cache_key)

    def purge_symbol(self, inst_id: str, symbol: str) -> None:
        """Remove all cached items for a given indicator+symbol combination."""
        if not inst_id or not symbol:
            return
        stale_keys = [
            key for key in self._cache.keys() if key[0] == inst_id and key[1] == symbol
        ]
        for cache_key in stale_keys:
            self._cache.pop(cache_key, None)
            self._scope_discard(cache_key)

    def clear(self) -> None:
        """Clear all cached items."""
        self._cache.clear()
        self._scope_index.clear()

    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            "total_entries": len(self._cache),
            "max_entries": self.max_entries,
            "indicators": len({key[0] for key in self._cache.keys()}),
            "symbols": len({(key[0], key[1]) for key in self._cache.keys()}),
        }

    def fingerprint_for(self, inst_id: str, symbol: str) -> Tuple[int, Optional[str]]:
        """Return a lightweight fingerprint for cached artifacts of one indicator+symbol.

        The fingerprint is stable when no cache entries were added/removed for this pair.
        """
        if not inst_id or not symbol:
            return (0, None)
        scoped = self._scope_index.get((inst_id, symbol))
        if not scoped:
            return (0, None)
        try:
            ordered = sorted(scoped, key=lambda item: repr(item[2]))
        except Exception:
            ordered = list(scoped)
        last = ordered[-1]
        return (len(ordered), repr(last[2]))

    def _scope_add(self, cache_key: Tuple[str, str, Hashable]) -> None:
        scope = (cache_key[0], cache_key[1])
        entries = self._scope_index.setdefault(scope, set())
        entries.add(cache_key)

    def _scope_discard(self, cache_key: Tuple[str, str, Hashable]) -> None:
        scope = (cache_key[0], cache_key[1])
        entries = self._scope_index.get(scope)
        if not entries:
            return
        entries.discard(cache_key)
        if not entries:
            self._scope_index.pop(scope, None)

    def _enforce_limit(self) -> None:
        """Remove oldest entries when cache exceeds max size."""
        should_log = get_obs_enabled()
        while len(self._cache) > self.max_entries:
            evict_start = time.perf_counter() if should_log else 0.0
            cache_key, _ = self._cache.popitem(last=False)
            self._scope_discard(cache_key)
            if should_log:
                evict_ms = (time.perf_counter() - evict_start) * 1000.0
                cache_key_summary = f"{cache_key[0]}:{cache_key[1]}:{cache_key[2]}"
                logger.debug(
                    "cache.eviction | event=cache.eviction cache_name=incremental_profile_cache cache_scope=process "
                    "cache_key_summary=%s time_taken_ms=%.4f pid=%s thread_name=%s",
                    cache_key_summary,
                    evict_ms,
                    os.getpid(),
                    threading.current_thread().name,
                )


def default_incremental_cache() -> IncrementalCache:
    """Create a default incremental cache instance."""
    return IncrementalCache(max_entries=10000)


__all__ = ["IncrementalCache", "default_incremental_cache"]
