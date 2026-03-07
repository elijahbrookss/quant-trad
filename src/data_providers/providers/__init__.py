"""Provider implementations and base interfaces."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from .base import BaseDataProvider, DataSource, InstrumentMetadata, InstrumentType, ProviderInterface
from .factory import get_provider

_PROVIDER_EXPORTS = {
    "AlpacaProvider": "data_providers.providers.alpaca",
    "CCXTProvider": "data_providers.providers.ccxt",
    "CoinbaseProvider": "data_providers.providers.coinbase",
    "InteractiveBrokersProvider": "data_providers.providers.interactive_brokers",
    "YahooFinanceProvider": "data_providers.providers.yahoo",
}


def __getattr__(name: str) -> Any:
    module_name = _PROVIDER_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    return getattr(module, name)


__all__ = [
    "AlpacaProvider",
    "BaseDataProvider",
    "CCXTProvider",
    "CoinbaseProvider",
    "get_provider",
    "DataSource",
    "InstrumentMetadata",
    "InstrumentType",
    "InteractiveBrokersProvider",
    "ProviderInterface",
    "YahooFinanceProvider",
]
