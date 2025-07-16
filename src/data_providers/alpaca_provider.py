import os
import datetime as dt
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from src.core.logger import logger
from .base_provider import DataSource
from .base_provider import BaseDataProvider


load_dotenv("secrets.env")

class AlpacaProvider(BaseDataProvider):
    def __init__(self):
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
            raise ValueError(f"Unsupported interval for Alpaca: {interval}")


        logger.debug("Timeframe for Alpaca: %s", tf)
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
            return pd.DataFrame()
        
        df.reset_index(inplace=True) #Bring indexes to columns

        logger.debug("DataFrame columns after cleanup - ALPACA: %s", df.columns)
        return df[["timestamp", "open", "high", "low", "close", "volume"]]
    
    def get_datasource(self):
        return DataSource.ALPACA.value
