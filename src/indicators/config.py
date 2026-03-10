from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class DataContext:
    symbol: Optional[str]
    start: Optional[str]
    end: Optional[str]
    interval: Optional[str]
    instrument_id: Optional[str] = None

    def __post_init__(self):
        self.validate()

    def validate(self):
        if not self.symbol:
            raise ValueError("DataContext validation failed: 'symbol' is required.")
        if not self.start:
            raise ValueError("DataContext validation failed: 'start' date is required.")
        if not self.end:
            raise ValueError("DataContext validation failed: 'end' date is required.")
        if not self.interval:
            raise ValueError("DataContext validation failed: 'interval' is required.")

    @staticmethod
    def _to_utc_timestamp(value: str, *, field_name: str) -> pd.Timestamp:
        try:
            ts = pd.Timestamp(value)
        except Exception as exc:  # noqa: BLE001 - normalize caller input with context
            raise ValueError(
                f"DataContext validation failed: '{field_name}' could not be parsed as a timestamp."
            ) from exc
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    def start_utc(self) -> pd.Timestamp:
        return self._to_utc_timestamp(str(self.start), field_name="start")

    def end_utc(self) -> pd.Timestamp:
        return self._to_utc_timestamp(str(self.end), field_name="end")
