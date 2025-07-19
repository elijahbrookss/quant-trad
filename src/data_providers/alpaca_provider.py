
# AlpacaProvider integrates Alpaca's historical market data into the quant-trad project.
# It implements the BaseDataProvider interface, allowing the system to fetch historical OHLCV data
# from Alpaca using their official API. This data is then standardized for use by the
# project's backtesting and analysis engines.

import os
import datetime as dt
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from core.logger import logger
from .base_provider import DataSource
from .base_provider import BaseDataProvider



# Load API keys from environment file for secure authentication
load_dotenv("secrets.env")


class AlpacaProvider(BaseDataProvider):
    """
    Data provider for Alpaca. Fetches historical OHLCV data using Alpaca's API,
    cleans and standardizes it for use in the quant-trad framework.
    Inherits from BaseDataProvider, ensuring compatibility with the rest of the system.
    """

    def __init__(self):
        # Initialize Alpaca API client with credentials from environment
        self.client = StockHistoricalDataClient(
            os.getenv("ALPACA_API_KEY"),
            os.getenv("ALPACA_SECRET_KEY"),
        )

    def fetch_from_api(
        self,
        symbol: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str
    ) -> pd.DataFrame:
        """
        Download historical data for a given symbol and time range from Alpaca.
        Returns a DataFrame with standardized columns for use in backtesting/analysis.
        """
        # Map string intervals to Alpaca's TimeFrame objects
        tf = {
            "1m": TimeFrame.Minute,
            "5m": TimeFrame(5, TimeFrameUnit.Minute),
            "15m": TimeFrame(15, TimeFrameUnit.Minute),
            "30m": TimeFrame(30, TimeFrameUnit.Minute),
            "1h": TimeFrame.Hour,
            "4h": TimeFrame(4, TimeFrameUnit.Hour),
            "1d": TimeFrame.Day
        }.get(interval)

        if tf is None:
            # Raise error if interval is not supported
            raise ValueError(f"Unsupported interval for Alpaca: {interval}")

        logger.debug("Timeframe for Alpaca: %s", tf)
        # Fetch OHLCV bars from Alpaca
        bars = self.client.get_stock_bars(
            StockBarsRequest(
                symbol_or_symbols=[symbol],
                start=start,
                end=end,
                timeframe=tf,
                feed=DataFeed.IEX,  # IEX feed is free for Alpaca users
                # feed=DataFeed.SIP,  # SIP feed is paid
            )
        )

        df = bars.df
        if df.empty:
            # Return empty DataFrame if no data is found
            return pd.DataFrame()
        
        # Bring index columns (symbol, timestamp) into DataFrame columns
        df.reset_index(inplace=True)

        logger.debug("DataFrame columns after cleanup - ALPACA: %s", df.columns)
        # Return only the columns needed by the system
        return df[["timestamp", "open", "high", "low", "close", "volume"]]
    
    def get_datasource(self):
        """
        Returns the identifier for this data source (ALPACA).
        Used by the system to distinguish between providers.
        """
        return DataSource.ALPACA.value
