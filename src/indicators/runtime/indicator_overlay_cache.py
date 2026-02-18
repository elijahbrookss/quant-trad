"""In-memory cache for indicator overlay payloads."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Set, Tuple

from utils.perf_log import get_obs_enabled

logger = logging.getLogger(__name__)

@dataclass
class IndicatorOverlayCache:
    """Store overlay payloads for reuse across runtime sessions."""

    max_entries: Optional[int] = None
    enabled_types: Set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        # NOTE: In-memory LRU overlay cache keyed by inst_id/type/symbol/interval/window/datasource/exchange/signature.
        # NOTE: Per-process cache with no locks; not thread-safe for concurrent writers.
        self._cache: "OrderedDict[Tuple[Any, ...], Dict[str, Any]]" = OrderedDict()

    def enable_type(self, indicator_type: str) -> None:
        if indicator_type:
            self.enabled_types.add(indicator_type.lower())

    def disable_type(self, indicator_type: str) -> None:
        if indicator_type:
            self.enabled_types.discard(indicator_type.lower())

    def is_enabled(self, indicator_type: Optional[str]) -> bool:
        if not indicator_type:
            return False
        return indicator_type.lower() in self.enabled_types

    def clear(self) -> None:
        self._cache.clear()

    def purge_indicator(self, inst_id: str) -> None:
        if not inst_id:
            return
        stale_keys = [key for key in self._cache.keys() if key and key[0] == inst_id]
        for cache_key in stale_keys:
            self._cache.pop(cache_key, None)

    def build_cache_key(
        self,
        inst_id: str,
        indicator_type: str,
        symbol: str,
        interval: str,
        start: str,
        end: str,
        *,
        datasource: Optional[str],
        exchange: Optional[str],
        signature: str,
        updated_at: str,
    ) -> Tuple[Any, ...]:
        return (
            inst_id,
            indicator_type,
            symbol,
            interval,
            start,
            end,
            datasource or "",
            exchange or "",
            updated_at or "",
            signature,
        )

    def get(self, cache_key: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
        cached = self._cache.get(cache_key)
        if cached is None:
            return None
        self._cache.move_to_end(cache_key)
        return deepcopy(cached)

    def set(self, cache_key: Tuple[Any, ...], payload: Mapping[str, Any]) -> None:
        self._cache[cache_key] = deepcopy(dict(payload))
        self._cache.move_to_end(cache_key)
        self._enforce_limit()

    def build_signature(
        self,
        params: Mapping[str, Any],
        overlay_options: Optional[Mapping[str, Any]],
    ) -> str:
        packed = {
            "params": self._normalize(params or {}),
            "overlay_options": self._normalize(overlay_options or {}),
        }
        return json.dumps(packed, sort_keys=True, separators=(",", ":"), default=str)

    def _enforce_limit(self) -> None:
        if self.max_entries is None:
            return
        should_log = get_obs_enabled()
        while len(self._cache) > self.max_entries:
            evict_start = time.perf_counter() if should_log else 0.0
            cache_key, _ = self._cache.popitem(last=False)
            if should_log:
                evict_ms = (time.perf_counter() - evict_start) * 1000.0
                cache_key_summary = f"{cache_key[0]}:{cache_key[2]}:{cache_key[3]}"
                logger.debug(
                    "cache.eviction | event=cache.eviction cache_name=indicator_overlay_cache cache_scope=process "
                    "cache_key_summary=%s time_taken_ms=%.4f pid=%s thread_name=%s",
                    cache_key_summary,
                    evict_ms,
                    os.getpid(),
                    threading.current_thread().name,
                )

    def _normalize(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): self._normalize(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize(v) for v in value]
        if isinstance(value, set):
            return sorted(self._normalize(v) for v in value)
        return value


def default_overlay_cache() -> IndicatorOverlayCache:
    return IndicatorOverlayCache(max_entries=256)


__all__ = ["IndicatorOverlayCache", "default_overlay_cache"]
