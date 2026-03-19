"""Provider package with lazy exports."""

from __future__ import annotations

from typing import Any

from .base import BaseDataProvider, DataSource, InstrumentMetadata, InstrumentType, ProviderInterface
from .factory import get_provider

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


def __getattr__(name: str) -> Any:
    if name == "AlpacaProvider":
        from .alpaca import AlpacaProvider

        return AlpacaProvider
    if name == "CCXTProvider":
        from .ccxt import CCXTProvider

        return CCXTProvider
    if name == "CoinbaseProvider":
        from .coinbase import CoinbaseProvider

        return CoinbaseProvider
    if name == "InteractiveBrokersProvider":
        from .interactive_brokers import InteractiveBrokersProvider

        return InteractiveBrokersProvider
    if name == "YahooFinanceProvider":
        from .yahoo import YahooFinanceProvider

        return YahooFinanceProvider
    raise AttributeError(name)
