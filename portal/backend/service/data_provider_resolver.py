"""Shared datasource/exchange normalization and provider resolution helpers."""

from __future__ import annotations

from typing import Optional

from data_providers import AlpacaProvider, DataSource
from data_providers.providers.factory import get_provider


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
        """Resolve data provider from datasource and exchange.

        No defaults: datasource must be explicitly provided.
        Raises ValueError if datasource is None.
        """
        datasource_normalized = self.normalize_datasource(datasource)
        exchange_normalized = self.normalize_exchange(exchange)

        # No hardcoded defaults - fail loudly if datasource is missing
        if not datasource_normalized:
            raise ValueError(
                f"datasource is required to resolve data provider "
                f"(got datasource={datasource}, exchange={exchange})"
            )

        if datasource_normalized == DataSource.ALPACA.value:
            return AlpacaProvider()

        return get_provider(datasource_normalized, exchange=exchange_normalized)


def default_resolver() -> DataProviderResolver:
    return DataProviderResolver()
