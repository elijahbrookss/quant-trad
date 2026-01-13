"""In-memory cache for indicator overlay payloads."""

from __future__ import annotations

import json
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Set, Tuple


@dataclass
class IndicatorOverlayCache:
    """Store overlay payloads for reuse across runtime sessions."""

    max_entries: Optional[int] = None
    enabled_types: Set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
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
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)

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
