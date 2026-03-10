"""Data provider package exposing providers, configuration, and services."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from .config import PersistenceConfig, ProviderRuntimeConfig, runtime_config_from_env
from .services import DataPersistence, NullPersistence
from .utils import (
    collect_missing_ranges,
    compute_tr_atr,
    interval_to_timedelta,
    split_history_range,
    subtract_ranges,
)

_PROVIDER_EXPORTS = {
    "AlpacaProvider": "data_providers.providers",
    "BaseDataProvider": "data_providers.providers",
    "CCXTProvider": "data_providers.providers",
    "get_provider": "data_providers.providers",
    "DataSource": "data_providers.providers",
    "InstrumentMetadata": "data_providers.providers",
    "InstrumentType": "data_providers.providers",
    "InteractiveBrokersProvider": "data_providers.providers",
    "ProviderInterface": "data_providers.providers",
    "YahooFinanceProvider": "data_providers.providers",
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
    "DataPersistence",
    "NullPersistence",
    "get_provider",
    "DataSource",
    "InstrumentMetadata",
    "InstrumentType",
    "InteractiveBrokersProvider",
    "PersistenceConfig",
    "ProviderInterface",
    "ProviderRuntimeConfig",
    "YahooFinanceProvider",
    "collect_missing_ranges",
    "compute_tr_atr",
    "interval_to_timedelta",
    "runtime_config_from_env",
    "split_history_range",
    "subtract_ranges",
]
