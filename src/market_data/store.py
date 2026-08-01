"""Provider-neutral persistence boundary for canonical market-data facts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Protocol

from .contracts import (
    CandleFact,
    CandleRecord,
    DatasetSeriesRequest,
    MarketDataRecord,
    OpenInterestFact,
    OpenInterestRecord,
    SourceIdentity,
)


@dataclass(frozen=True)
class IngestionOutcome:
    ingestion_run_id: str
    requested_count: int
    inserted_count: int
    corrected_count: int
    noop_count: int
    max_commit_seq: int


@dataclass(frozen=True)
class FrozenDataset:
    dataset_id: str
    dataset_hash: str
    max_commit_seq: int
    series: tuple[Mapping[str, Any], ...]
    contract_version: str = "market_dataset.v1"
    name: Optional[str] = None
    purpose: str = "research"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    reused_existing: bool = field(default=False, compare=False)


class MarketDataStore(Protocol):
    """Storage operations used by ingestion, replay, and paper feed services."""

    def current_commit_seq(self) -> int:
        ...

    def list_series(self, *, instrument_id: Optional[str] = None) -> list[Mapping[str, Any]]:
        ...

    def get_dataset(self, dataset_id: str) -> FrozenDataset:
        ...

    def register_source(
        self,
        identity: SourceIdentity,
        *,
        lineage: Optional[Mapping[str, Any]] = None,
    ) -> int:
        ...

    def register_series(
        self,
        *,
        instrument_id: str,
        fact_type: str,
        timeframe_seconds: Optional[int],
        contract_version: str,
    ) -> int:
        ...

    def resolve_series_id(
        self,
        *,
        instrument_id: str,
        fact_type: str,
        timeframe_seconds: Optional[int],
        contract_version: str,
    ) -> int:
        ...

    def ingest_candles(
        self,
        *,
        series_id: int,
        source_id: int,
        facts: Iterable[CandleFact],
        request: Optional[Mapping[str, Any]] = None,
        source_revision: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        allow_corrections: bool = True,
    ) -> IngestionOutcome:
        ...

    def read_candles(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
    ) -> list[CandleRecord]:
        ...

    def ingest_open_interest(
        self,
        *,
        series_id: int,
        source_id: int,
        facts: Iterable[OpenInterestFact],
        request: Optional[Mapping[str, Any]] = None,
        provenance: Optional[Mapping[str, Any]] = None,
        source_revision: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        allow_corrections: bool = True,
        collection_fence: Optional[Mapping[str, Any]] = None,
    ) -> IngestionOutcome:
        ...

    def read_open_interest(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[datetime] = None,
    ) -> list[OpenInterestRecord]:
        ...

    def record_gap_evidence(self, **kwargs: Any) -> str:
        ...

    def list_gap_evidence(self, **kwargs: Any) -> list[Mapping[str, Any]]:
        ...

    def freeze_dataset(
        self,
        requests: Sequence[DatasetSeriesRequest],
        *,
        name: Optional[str] = None,
        purpose: str = "research",
        created_by: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> FrozenDataset:
        ...

    def read_dataset_series(
        self,
        *,
        dataset_id: str,
        series_id: int,
        known_at_lte: Optional[datetime] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> list[MarketDataRecord]:
        ...


__all__ = ["FrozenDataset", "IngestionOutcome", "MarketDataStore"]
