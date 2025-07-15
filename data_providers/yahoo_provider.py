
# YahooFinanceProvider integrates Yahoo Finance data into the quant-trad project.
# It implements the BaseDataProvider interface, allowing the system to fetch historical market data
# from Yahoo Finance using the yfinance library. This data is then standardized for use by the
# project's backtesting and analysis engines.

import datetime as dt
import pandas as pd
import yfinance as yf
from classes.Logger import logger
from .base import DataSource
from .base import BaseDataProvider

class YahooFinanceProvider(BaseDataProvider):
    """
    Data provider for Yahoo Finance. Fetches historical OHLCV data using yfinance,
    cleans and standardizes it for use in the quant-trad framework.
    Inherits from BaseDataProvider, ensuring compatibility with the rest of the system.
    """

    def fetch_from_api(
        self,
        symbol: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str
    ) -> pd.DataFrame:
        """
        Download historical data for a given symbol and time range from Yahoo Finance.
        Returns a DataFrame with standardized columns for use in backtesting/analysis.
        """
        try:
            # Use yfinance to download OHLCV data
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
            # Return empty DataFrame if no data is found
            return pd.DataFrame()
        
        # Flatten if MultiIndex or Ticker-based headers (can happen with yfinance)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        elif isinstance(df.columns.name, str) and df.columns.name.lower() == "ticker":
            df.columns.name = None  # Drop the 'Ticker' name 

        # Remove timezone info and reset index for consistency
        df = df.tz_convert(None).reset_index()
        # Standardize column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        logger.debug("DataFrame columns after cleanup - YFINANCE: %s", df.columns)

        # Add a 'timestamp' column for compatibility with the rest of the project
        df["timestamp"] = df["datetime"]
        
        # Return only the columns needed by the system
        return df[["timestamp", "open", "high", "low", "close", "volume"]]
    
    def get_datasource(self):
        """
        Returns the identifier for this data source (YFINANCE).
        Used by the system to distinguish between providers.
        """
        return DataSource.YFINANCE.value
