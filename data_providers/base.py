from abc import ABC, abstractmethod
from enum import Enum
import datetime as dt
import pandas as pd

class BaseDataProvider(ABC):
    @abstractmethod
    def get_ohlcv(
        self,
        symbol: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str
    ) -> pd.DataFrame:
        """
        Retrieve OHLCV data for the given symbol between start and end datetimes.

        Returns a DataFrame with the following required columns:
        ['ts', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'datasource']
        """
        pass

    @abstractmethod
    def get_datasource(self) -> str:
        """
        Return the name of the data source as a string.
        """
        pass

class DataSource(str, Enum):
    YFINANCE = "YFINANCE"
    ALPACA = "ALPACA"
    UNKNOWN = "UNKNOWN"