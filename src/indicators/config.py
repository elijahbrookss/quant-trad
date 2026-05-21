from dataclasses import dataclass
from typing import Optional


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
    def _to_utc_timestamp(value: str, *, field_name: str):
        import pandas as pd

        try:
            ts = pd.Timestamp(value)
        except Exception as exc:  # noqa: BLE001 - normalize caller input with context
            raise ValueError(
                f"DataContext validation failed: '{field_name}' could not be parsed as a timestamp."
            ) from exc
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    def start_utc(self):
        return self._to_utc_timestamp(str(self.start), field_name="start")

    def end_utc(self):
        return self._to_utc_timestamp(str(self.end), field_name="end")


@dataclass(frozen=True)
class IndicatorExecutionContext:
    symbol: Optional[str]
    start: Optional[str]
    end: Optional[str]
    interval: Optional[str]
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    instrument_id: Optional[str] = None

    def validate(self) -> None:
        DataContext(
            symbol=self.symbol,
            start=self.start,
            end=self.end,
            interval=self.interval,
            instrument_id=self.instrument_id,
        ).validate()

    def data_context(
        self,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        interval: Optional[str] = None,
    ) -> DataContext:
        return DataContext(
            symbol=self.symbol,
            start=start or self.start,
            end=end or self.end,
            interval=interval or self.interval,
            instrument_id=self.instrument_id,
        )
