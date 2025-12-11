"""Shared datasource/exchange normalization and provider resolution helpers."""

from __future__ import annotations

from typing import Optional

from data_providers.alpaca_provider import AlpacaProvider
from data_providers.base_provider import DataSource
from data_providers.factory import get_provider


class DataProviderResolver:
    """Normalize datasource/exchange inputs and return a provider instance."""

    def normalize_datasource(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = str(value).strip().upper()
        return cleaned or None

    def normalize_exchange(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = str(value).strip().lower()
        return cleaned or None

    def resolve(self, datasource: Optional[str], *, exchange: Optional[str] = None):
        datasource_normalized = self.normalize_datasource(datasource)
        exchange_normalized = self.normalize_exchange(exchange)

        if exchange_normalized and not datasource_normalized:
            datasource_normalized = DataSource.CCXT.value

        if not datasource_normalized:
            datasource_normalized = DataSource.ALPACA.value

        if datasource_normalized == DataSource.ALPACA.value:
            return AlpacaProvider()

        return get_provider(datasource_normalized, exchange=exchange_normalized)


def default_resolver() -> DataProviderResolver:
    return DataProviderResolver()
