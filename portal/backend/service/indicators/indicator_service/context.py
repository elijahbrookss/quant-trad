from __future__ import annotations

from dataclasses import dataclass

from ...providers.data_provider_resolver import DataProviderResolver, default_resolver
from indicators.runtime.indicator_breakout_cache import IndicatorBreakoutCache, default_breakout_cache
from indicators.runtime.indicator_overlay_cache import IndicatorOverlayCache, default_overlay_cache
from indicators.runtime.incremental_cache import IncrementalCache, default_incremental_cache
from ..indicator_factory import IndicatorFactory
from ..indicator_repository import IndicatorRepository, default_repository
from indicators.runtime.indicator_signal_runner import IndicatorSignalRunner, default_signal_runner


@dataclass
class IndicatorServiceContext:
    """Container for indicator service dependencies.

    Indicators are built fresh from DB on each access to avoid stale params.
    Overlay payloads may be cached separately for runtime performance.
    """

    repository: IndicatorRepository
    resolver: DataProviderResolver
    factory: IndicatorFactory
    signal_runner: IndicatorSignalRunner
    breakout_cache: IndicatorBreakoutCache
    overlay_cache: IndicatorOverlayCache
    incremental_cache: IncrementalCache

    @classmethod
    def default(cls) -> "IndicatorServiceContext":
        repository = default_repository()
        resolver = default_resolver()
        factory = IndicatorFactory(resolver=resolver)

        # Create context first, then inject it back into factory
        context = cls(
            repository=repository,
            resolver=resolver,
            factory=factory,
            signal_runner=default_signal_runner(),
            breakout_cache=default_breakout_cache(),
            overlay_cache=default_overlay_cache(),
            incremental_cache=default_incremental_cache(),
        )

        # Inject context back into factory so it can attach signal catalogs
        factory._ctx = context

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
            signal_runner=base.signal_runner,
            breakout_cache=base.breakout_cache,
            overlay_cache=overlay_cache,
            incremental_cache=base.incremental_cache,
        )
        factory._ctx = context
        return context


_context = IndicatorServiceContext.default()
