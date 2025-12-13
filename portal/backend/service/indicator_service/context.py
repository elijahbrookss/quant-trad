from __future__ import annotations

from dataclasses import dataclass

from ..data_provider_resolver import DataProviderResolver, default_resolver
from ..indicator_breakout_cache import IndicatorBreakoutCache, default_breakout_cache
from ..indicator_cache import IndicatorCacheManager, default_cache_manager
from ..indicator_factory import IndicatorFactory
from ..indicator_repository import IndicatorRepository, default_repository
from ..indicator_signal_runner import IndicatorSignalRunner, default_signal_runner


@dataclass
class IndicatorServiceContext:
    """Container for indicator service dependencies."""

    repository: IndicatorRepository
    resolver: DataProviderResolver
    factory: IndicatorFactory
    cache_manager: IndicatorCacheManager
    signal_runner: IndicatorSignalRunner
    breakout_cache: IndicatorBreakoutCache

    @classmethod
    def default(cls) -> "IndicatorServiceContext":
        repository = default_repository()
        resolver = default_resolver()
        factory = IndicatorFactory(resolver=resolver)
        cache_manager = default_cache_manager(repository, factory=factory)

        # Create context first, then inject it back into factory
        context = cls(
            repository=repository,
            resolver=resolver,
            factory=factory,
            cache_manager=cache_manager,
            signal_runner=default_signal_runner(),
            breakout_cache=default_breakout_cache(),
        )

        # Inject context back into factory so it can attach signal catalogs
        factory._ctx = context

        return context


_context = IndicatorServiceContext.default()
