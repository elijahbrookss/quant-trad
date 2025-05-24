from abc import ABC, abstractmethod
from enum import Enum
import datetime as dt
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("secrets.env")


class BaseDataProvider(ABC):
    _dsn = os.getenv("PG_DSN")
    _table = os.getenv("OHLC_TABLE")
    _engine = create_engine(_dsn)

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
        ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'symbol']
        """
        pass

    @abstractmethod
    def get_datasource(self) -> str:
        """
        Return the name of the data source as a string.
        """
        pass

    def get_ohlcv(self, symbol: str, start: str, end: str, interval: str = "1d") -> pd.DataFrame:
        query = text(f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {self._table}
            WHERE symbol = :symbol
              AND datasource = :ds
              AND interval = :interval
              AND timestamp BETWEEN :start AND :end
            ORDER BY timestamp
        """)

        df = pd.read_sql(query, self._engine, params={
            "symbol": symbol,
            "ds": self.get_datasource(),
            "interval": interval,
            "start": start,
            "end": end,
        })

        if df.empty:
            return df

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df
    
    @abstractmethod
    def fetch_from_api(self, symbol: str, start: str, end: str, interval: str) -> pd.DataFrame:
        """Fetch data from API"""
        pass

class DataSource(str, Enum):
    YFINANCE = "YFINANCE"
    ALPACA = "ALPACA"
    UNKNOWN = "UNKNOWN"