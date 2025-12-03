import datetime as dt
import pandas as pd
import yfinance as yf
from core.logger import logger
from .base_provider import DataSource, BaseDataProvider, InstrumentMetadata, InstrumentType

class YahooFinanceProvider(BaseDataProvider):
    def fetch_from_api(
        self,
        symbol: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str
    ) -> pd.DataFrame:
        try:
            df = yf.download(
                symbol,
                start=start,
                end=end,
                interval=interval,
                progress=False,
                threads=False,
            )
        except Exception as e:
            raise RuntimeError(f"YahooFinance download failed: {e}")

        if df is None or df.empty:
            return pd.DataFrame()
        
        # Flatten if MultiIndex or Ticker-based headers
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        elif isinstance(df.columns.name, str) and df.columns.name.lower() == "ticker":
            df.columns.name = None  # Drop the 'Ticker' name 

        df = df.tz_convert(None).reset_index()
        df.columns = [col.lower() for col in df.columns]
        logger.debug("DataFrame columns after cleanup - YFINANCE: %s", df.columns)

        df["timestamp"] = df["datetime"]
        
        return df[["timestamp", "open", "high", "low", "close", "volume"]]
    
    def get_datasource(self):
        return DataSource.YFINANCE.value

    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Yahoo Finance exposes spot instruments (equities, ETFs, FX, crypto)."""

        return InstrumentType.SPOT

    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        """Return spot metadata expressed per share/coin."""

        return self._normalize_metadata(tick_size=0.01, contract_size=1.0)
