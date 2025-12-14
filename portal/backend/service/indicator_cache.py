from __future__ import annotations

"""In-memory indicator cache with backfill support."""

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Mapping, MutableMapping, Optional

from .indicator_repository import IndicatorRepository


@dataclass
class IndicatorCacheEntry:
    meta: Dict[str, Any]
    instance: Any
    updated_at: Optional[str] = None


class IndicatorCacheManager:
    """Manage indicator instances built from persisted metadata."""

    def __init__(
        self,
        repository: IndicatorRepository,
        *,
        factory,
        context_keys: tuple[str, ...] = ("symbol", "start", "end", "interval"),
    ) -> None:
        self._repo = repository
        self._factory = factory
        self._cache: Dict[str, IndicatorCacheEntry] = {}
        self._context_keys = context_keys

    def evict(self, inst_id: str) -> None:
        if not inst_id:
            return
        self._cache.pop(inst_id, None)

    def get_entry(
        self,
        inst_id: str,
        *,
        fallback_context: Optional[Mapping[str, Any]] = None,
        persist_backfill: bool = False,
    ) -> IndicatorCacheEntry:
        record = self._repo.get(inst_id)
        if not record:
            raise KeyError("Indicator not found")

        if fallback_context:
            record = self._maybe_backfill_context(
                record, fallback_context, persist=persist_backfill
            )

        record_version = str(record.get("updated_at") or "")
        cached = self._cache.get(inst_id)
        if cached and cached.updated_at == record_version and cached.instance is not None:
            return cached

        meta = self._factory.build_meta_from_record(record)
        # Allow fallback context to supply runtime provider overrides so
        # indicator instances created for ad-hoc overlays use the UI-selected
        # datasource/exchange instead of any persisted snapshot values.
        fb = fallback_context or {}
        fb_ds = fb.get("datasource")
        fb_ex = fb.get("exchange")
        inst = self._factory.build_indicator_instance(meta, datasource=fb_ds, exchange=fb_ex)
        entry = IndicatorCacheEntry(meta=meta, instance=inst, updated_at=record_version)
        self._cache[inst_id] = entry
        return entry

    def cache_indicator(
        self, inst_id: str, meta: Mapping[str, Any], inst: Any, updated_at: Optional[str]
    ) -> None:
        if not inst_id:
            return
        self._cache[inst_id] = IndicatorCacheEntry(
            meta=deepcopy(dict(meta)), instance=inst, updated_at=updated_at
        )

    def _normalize_context_values(
        self, payload: Optional[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        if not payload:
            return {}
        context: Dict[str, Any] = {}
        for key in self._context_keys:
            value = payload.get(key)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            context[key] = value
        return context

    def _maybe_backfill_context(
        self,
        record: Mapping[str, Any],
        fallback: Optional[Mapping[str, Any]] = None,
        *,
        persist: bool = False,
    ) -> Mapping[str, Any]:
        params = dict(record.get("params") or {})
        missing = [key for key in self._context_keys if not params.get(key)]
        if not missing:
            return record

        ctx_patch = self._normalize_context_values(fallback)
        if not ctx_patch:
            return record

        updated = False
        for key in missing:
            value = ctx_patch.get(key)
            if value is None:
                continue
            params[key] = value
            updated = True

        if not updated:
            return record

        patched: MutableMapping[str, Any] = dict(record)
        patched["params"] = params

        if not persist:
            return patched

        self._repo.upsert(patched)
        refreshed = self._repo.get(str(record.get("id")))
        return refreshed or patched


def default_cache_manager(repository: IndicatorRepository, *, factory) -> IndicatorCacheManager:
    return IndicatorCacheManager(repository=repository, factory=factory)
