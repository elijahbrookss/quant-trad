from __future__ import annotations

from typing import Dict, Tuple, Optional

from core.logger import logger

from .base_provider import BaseDataProvider, DataSource
from .alpaca_provider import AlpacaProvider
from .yahoo_provider import YahooFinanceProvider
from .interactive_brokers_provider import InteractiveBrokersProvider
from .ccxt_provider import CCXTProvider


_PROVIDER_CACHE: Dict[Tuple[str, str], BaseDataProvider] = {}


def _normalise_datasource(value: Optional[str]) -> str:
    if not value:
        return DataSource.ALPACA.value
    return str(value).strip().upper() or DataSource.ALPACA.value


def _normalise_exchange(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value).strip().lower()


def get_provider(datasource: Optional[str] = None, *, exchange: Optional[str] = None) -> BaseDataProvider:
    """Return a data provider instance for the requested datasource/exchange."""

    ds = _normalise_datasource(datasource)
    ex = _normalise_exchange(exchange)
    cache_key = (ds, ex)

    if cache_key in _PROVIDER_CACHE:
        return _PROVIDER_CACHE[cache_key]

    if ds == DataSource.ALPACA.value:
        if ex in {"yfinance", "yahoo", "yahoo_finance", "yf"}:
            provider = YahooFinanceProvider()
        else:
            provider = AlpacaProvider()
    elif ds == DataSource.YFINANCE.value:
        provider = YahooFinanceProvider()
    elif ds == DataSource.IBKR.value:
        provider = InteractiveBrokersProvider(exchange=exchange)
    elif ds in {DataSource.CCXT.value, "CRYPTO", "CRYPTOCURRENCY"}:
        if not ex:
            raise ValueError("CCXT datasource requires an exchange identifier")
        provider = CCXTProvider(ex)
    else:
        # Allow direct exchange identifiers to route through CCXT for flexibility.
        provider = CCXTProvider(ex or ds)

    _PROVIDER_CACHE[cache_key] = provider
    logger.debug("provider_factory_cached datasource=%s exchange=%s", ds, ex)
    return provider
