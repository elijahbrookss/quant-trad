from __future__ import annotations

import logging
from dataclasses import dataclass

from ...providers.data_provider_resolver import DataProviderResolver, default_resolver
from indicators.runtime.indicator_breakout_cache import IndicatorBreakoutCache, default_breakout_cache
from indicators.runtime.indicator_overlay_cache import IndicatorOverlayCache, default_overlay_cache
from indicators.runtime.incremental_cache import IncrementalCache, default_incremental_cache
from ..indicator_factory import IndicatorFactory
from ..indicator_repository import IndicatorRepository, default_repository


logger = logging.getLogger(__name__)


@dataclass
class IndicatorServiceContext:
    """Container for indicator service dependencies."""

    repository: IndicatorRepository
    resolver: DataProviderResolver
    factory: IndicatorFactory
    breakout_cache: IndicatorBreakoutCache
    overlay_cache: IndicatorOverlayCache
    incremental_cache: IncrementalCache
    cache_owner: str
    cache_scope_id: str

    @classmethod
    def for_quantlab_worker(cls, *, cache_scope_id: str) -> "IndicatorServiceContext":
        return cls._build(cache_owner="quantlab_worker", cache_scope_id=cache_scope_id)

    @classmethod
    def for_bot_runtime(cls, *, cache_scope_id: str) -> "IndicatorServiceContext":
        return cls._build(cache_owner="bot_runtime", cache_scope_id=cache_scope_id)

    @classmethod
    def _build(cls, *, cache_owner: str, cache_scope_id: str) -> "IndicatorServiceContext":
        repository = default_repository()
        resolver = default_resolver()
        factory = IndicatorFactory(resolver=resolver)
        context = cls(
            repository=repository,
            resolver=resolver,
            factory=factory,
            breakout_cache=default_breakout_cache(),
            overlay_cache=default_overlay_cache(),
            incremental_cache=default_incremental_cache(),
            cache_owner=cache_owner,
            cache_scope_id=cache_scope_id,
        )
        factory._ctx = context
        logger.info(
            "indicator_service_context_created | cache_owner=%s | cache_scope_id=%s",
            context.cache_owner,
            context.cache_scope_id,
        )
        return context

    @classmethod
    def fork_with_overlay_cache(
        cls, base: "IndicatorServiceContext", overlay_cache: IndicatorOverlayCache
    ) -> "IndicatorServiceContext":
        resolver = base.resolver
        repository = base.repository
        factory = IndicatorFactory(resolver=resolver)
        context = cls(
            repository=repository,
            resolver=resolver,
            factory=factory,
            breakout_cache=base.breakout_cache,
            overlay_cache=overlay_cache,
            incremental_cache=base.incremental_cache,
            cache_owner="series_process",
            cache_scope_id=base.cache_scope_id,
        )
        factory._ctx = context
        logger.info(
            "indicator_service_context_created | cache_owner=%s | cache_scope_id=%s",
            context.cache_owner,
            context.cache_scope_id,
        )
        return context

# Backward-compatible explicit process context for API-only call paths.
_context = IndicatorServiceContext.for_quantlab_worker(cache_scope_id="portal_api")
