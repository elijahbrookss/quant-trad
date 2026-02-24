from __future__ import annotations

from dataclasses import dataclass, field
import os
import threading
import time
from typing import Dict, Optional, Tuple

from core.logger import logger
from utils.perf_log import get_obs_enabled

from ..config import runtime_config_from_env
from ..services import DataPersistence, NullPersistence
from ..registry import (
    exchange_slug_for_venue,
    get_provider_config,
    get_venue_config,
    normalize_provider_id,
    normalize_venue_id,
    provider_for_venue,
)
from .alpaca import AlpacaProvider
from .base import BaseDataProvider, DataSource
from .ccxt import CCXTProvider
from .coinbase import CoinbaseProvider
from .interactive_brokers import InteractiveBrokersProvider
from .yahoo import YahooFinanceProvider


@dataclass
class ProviderRegistry:
    """Registry for provider instances and persistence wiring."""

    runtime_config: object = field(default_factory=runtime_config_from_env)
    persistence_factory: Optional[callable] = None
    persistence: Optional[DataPersistence] = None
    cache: Dict[Tuple[str, str], BaseDataProvider] = field(default_factory=dict)

    def configure_persistence_factory(self, factory) -> None:
        self.persistence_factory = factory
        self.persistence = None
        self.cache = {}

    def get_persistence(self) -> DataPersistence:
        if self.persistence is None:
            if self.persistence_factory is None:
                self.persistence = NullPersistence()
            else:
                self.persistence = self.persistence_factory()
        return self.persistence

    def get_provider(
        self,
        provider_id: Optional[str] = None,
        *,
        venue: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> BaseDataProvider:
        provider, resolved_venue = _resolve_ids(provider_id, venue or exchange)
        cache_key = (provider, resolved_venue or "")
        should_log = get_obs_enabled()
        get_started = time.perf_counter() if should_log else 0.0

        if cache_key in self.cache:
            return self.cache[cache_key]

        if should_log:
            get_ms = (time.perf_counter() - get_started) * 1000.0
            logger.debug(
                "cache.miss | event=cache.miss cache_name=provider_registry cache_scope=process "
                "cache_key_summary=%s time_taken_ms=%.4f pid=%s thread_name=%s",
                f"{provider}:{resolved_venue or ''}",
                get_ms,
                os.getpid(),
                threading.current_thread().name,
            )

        # NOTE: In-memory provider instance cache (per-process, no eviction).
        # NOTE: No locks; not thread-safe for concurrent writes.
        # NOTE: Multiprocessing/container-per-bot will duplicate provider instances.
        build_started = time.perf_counter() if should_log else 0.0
        provider_cfg = get_provider_config(provider)
        if not provider_cfg:
            raise ValueError(f"Unsupported provider: {provider}")

        persistence = self.get_persistence()

        if provider == DataSource.ALPACA.value or provider == "ALPACA":
            instance = AlpacaProvider(persistence=persistence, settings=self.runtime_config)
        elif provider == DataSource.YFINANCE.value or provider == "YAHOO":
            instance = YahooFinanceProvider(persistence=persistence, settings=self.runtime_config)
        elif provider == DataSource.IBKR.value or provider == "INTERACTIVE_BROKERS":
            instance = InteractiveBrokersProvider(
                exchange=exchange or resolved_venue,
                persistence=persistence,
                settings=self.runtime_config,
            )
        elif provider == DataSource.CCXT.value or provider == "CCXT":
            slug = exchange_slug_for_venue(resolved_venue) or (exchange or "").lower()
            if not slug:
                raise ValueError("CCXT provider requires a venue/exchange identifier")
            instance = CCXTProvider(slug, persistence=persistence, settings=self.runtime_config)
        elif provider == DataSource.COINBASE.value or provider == "COINBASE":
            instance = CoinbaseProvider(persistence=persistence, settings=self.runtime_config)
        else:
            raise ValueError(f"No provider implementation for {provider}")

        self.cache[cache_key] = instance
        logger.debug("provider_factory_cached | provider=%s venue=%s", provider, resolved_venue)
        if should_log:
            build_ms = (time.perf_counter() - build_started) * 1000.0
            logger.debug(
                "cache.set | event=cache.set cache_name=provider_registry cache_scope=process "
                "cache_key_summary=%s time_taken_ms=%.4f pid=%s thread_name=%s",
                f"{provider}:{resolved_venue or ''}",
                build_ms,
                os.getpid(),
                threading.current_thread().name,
            )
        return instance


_REGISTRY = ProviderRegistry()


def configure_persistence_factory(factory):
    """Provide a service-layer persistence builder for provider instances."""

    _REGISTRY.configure_persistence_factory(factory)


def _resolve_ids(provider_id: Optional[str], venue_id: Optional[str]) -> Tuple[str, Optional[str]]:
    provider = normalize_provider_id(provider_id) or "ALPACA"
    venue = normalize_venue_id(venue_id)

    if venue and not provider_id:
        provider = provider_for_venue(venue) or provider

    if venue:
        venue_cfg = get_venue_config(venue)
        if not venue_cfg:
            return provider, venue
        if venue_cfg.provider_id != provider:
            raise ValueError(f"Venue {venue} is not supported by provider {provider}")
    else:
        provider_cfg = get_provider_config(provider)
        if provider_cfg and len(provider_cfg.supported_venues) == 1:
            venue = provider_cfg.supported_venues[0]

    return provider, venue


def get_provider(provider_id: Optional[str] = None, *, venue: Optional[str] = None, exchange: Optional[str] = None) -> BaseDataProvider:
    """Return a data provider instance for the requested provider/venue."""

    return _REGISTRY.get_provider(provider_id, venue=venue, exchange=exchange)
