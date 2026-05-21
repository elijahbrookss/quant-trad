"""Data provider package exposing providers, configuration, and services."""

from .config import PersistenceConfig, ProviderRuntimeConfig, runtime_config_from_env
from .providers.base import BaseDataProvider, DataSource, InstrumentMetadata, InstrumentType, ProviderInterface
from .providers.factory import get_provider
from .services import DataPersistence, NullPersistence
from .utils import (
    collect_missing_ranges,
    compute_tr_atr,
    interval_to_timedelta,
    split_history_range,
    subtract_ranges,
)

__all__ = [
    "BaseDataProvider",
    "DataPersistence",
    "NullPersistence",
    "get_provider",
    "DataSource",
    "InstrumentMetadata",
    "InstrumentType",
    "PersistenceConfig",
    "ProviderInterface",
    "ProviderRuntimeConfig",
    "collect_missing_ranges",
    "compute_tr_atr",
    "interval_to_timedelta",
    "runtime_config_from_env",
    "split_history_range",
    "subtract_ranges",
]
