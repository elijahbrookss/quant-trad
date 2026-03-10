from __future__ import annotations

from typing import List, Protocol, Tuple

import pandas as pd

from indicators.config import DataContext


class DataPersistence(Protocol):
    """Interface for persistence backends used by data providers."""

    @property
    def engine_available(self) -> bool:
        ...

    def ensure_schema(self) -> None:
        ...

    def fetch_ohlcv(self, ctx: DataContext, datasource: str) -> pd.DataFrame:
        ...

    def load_closure_ranges(
        self,
        ctx: DataContext,
        datasource: str,
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        ...

    def record_closure_range(
        self,
        ctx: DataContext,
        datasource: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        ...

    def write_dataframe(self, df: pd.DataFrame, ctx: DataContext) -> int:
        ...


class NullPersistence:
    """No-op persistence used when the service layer is not configured."""

    engine_available = False

    def ensure_schema(self) -> None:
        return None

    def fetch_ohlcv(self, ctx: DataContext, datasource: str) -> pd.DataFrame:
        return pd.DataFrame()

    def load_closure_ranges(
        self,
        ctx: DataContext,
        datasource: str,
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        return []

    def record_closure_range(
        self,
        ctx: DataContext,
        datasource: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        return None

    def write_dataframe(self, df: pd.DataFrame, ctx: DataContext) -> int:
        return 0


__all__ = ["DataPersistence", "NullPersistence"]
