"""Canonical market-data contracts and feed boundaries."""

from .store import FrozenDataset, IngestionOutcome, MarketDataStore
from .contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    CandleFact,
    CandleRecord,
    DatasetSeriesRequest,
    MarketDataRequirement,
    MarketDataWindow,
    SourceIdentity,
    build_candle_material_hash,
    build_provenance_hash,
    build_quality_hash,
)

__all__ = [
    "FrozenDataset",
    "IngestionOutcome",
    "MarketDataStore",
    "CANDLE_FACT_TYPE",
    "CANDLE_FACT_VERSION",
    "CandleFact",
    "CandleRecord",
    "DatasetSeriesRequest",
    "MarketDataRequirement",
    "MarketDataWindow",
    "SourceIdentity",
    "build_candle_material_hash",
    "build_provenance_hash",
    "build_quality_hash",
]
