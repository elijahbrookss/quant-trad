from __future__ import annotations

from typing import Dict, Optional, Tuple

from core.logger import logger

from .alpaca_provider import AlpacaProvider
from .ccxt_provider import CCXTProvider
from .interactive_brokers_provider import InteractiveBrokersProvider
from .yahoo_provider import YahooFinanceProvider
from .base_provider import BaseDataProvider, DataSource
from .registry import (
    exchange_slug_for_venue,
    get_provider_config,
    get_venue_config,
    normalize_provider_id,
    normalize_venue_id,
    provider_for_venue,
)


_PROVIDER_CACHE: Dict[Tuple[str, str], BaseDataProvider] = {}


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

    provider, resolved_venue = _resolve_ids(provider_id, venue or exchange)
    cache_key = (provider, resolved_venue or "")

    if cache_key in _PROVIDER_CACHE:
        return _PROVIDER_CACHE[cache_key]

    provider_cfg = get_provider_config(provider)
    if not provider_cfg:
        raise ValueError(f"Unsupported provider: {provider}")

    if provider == DataSource.ALPACA.value or provider == "ALPACA":
        instance = AlpacaProvider()
    elif provider == DataSource.YFINANCE.value or provider == "YAHOO":
        instance = YahooFinanceProvider()
    elif provider == DataSource.IBKR.value or provider == "INTERACTIVE_BROKERS":
        instance = InteractiveBrokersProvider(exchange=exchange or resolved_venue)
    elif provider == DataSource.CCXT.value or provider == "CCXT":
        slug = exchange_slug_for_venue(resolved_venue) or (exchange or "").lower()
        if not slug:
            raise ValueError("CCXT provider requires a venue/exchange identifier")
        instance = CCXTProvider(slug)
    else:
        raise ValueError(f"No provider implementation for {provider}")

    _PROVIDER_CACHE[cache_key] = instance
    logger.debug("provider_factory_cached provider=%s venue=%s", provider, resolved_venue)
    return instance
