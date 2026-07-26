"""Data provider package exposing providers, configuration, and services."""

from .providers.base import BaseDataProvider, DataSource, InstrumentMetadata, InstrumentType, ProviderInterface
from .providers.factory import get_provider
from .utils import (
    collect_missing_ranges,
    compute_tr_atr,
    interval_to_timedelta,
    split_history_range,
    subtract_ranges,
)

__all__ = [
    "BaseDataProvider",
    "get_provider",
    "DataSource",
    "InstrumentMetadata",
    "InstrumentType",
    "ProviderInterface",
    "collect_missing_ranges",
    "compute_tr_atr",
    "interval_to_timedelta",
    "split_history_range",
    "subtract_ranges",
]
