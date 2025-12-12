"""Data provider package exposing providers, configuration, and services."""

from .config import PersistenceConfig, ProviderRuntimeConfig, runtime_config_from_env
from .providers import (
    AlpacaProvider,
    BaseDataProvider,
    CCXTProvider,
    get_provider,
    DataSource,
    InstrumentMetadata,
    InstrumentType,
    InteractiveBrokersProvider,
    ProviderInterface,
    YahooFinanceProvider,
)
from .services import DataPersistenceService
from .utils import (
    collect_missing_ranges,
    compute_tr_atr,
    interval_to_timedelta,
    split_history_range,
    subtract_ranges,
)

__all__ = [
    "AlpacaProvider",
    "BaseDataProvider",
    "CCXTProvider",
    "DataPersistenceService",
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
