import os
import datetime as dt
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from classes.Logger import logger

from .base import BaseDataProvider
load_dotenv("secrets.env")

class AlpacaProvider(BaseDataProvider):
    def __init__(self):
        self.client = StockHistoricalDataClient(
            os.getenv("ALPACA_API_KEY"),
            os.getenv("ALPACA_SECRET_KEY"),
        )

    def get_ohlcv(
        self,
        symbol: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str
    ) -> pd.DataFrame:
        tf = {
            "1m": TimeFrame.Minute,
            "5m": TimeFrame(5, "Minute"),
            "15m": TimeFrame(15, "Minute"),
            "1h": TimeFrame.Hour,
            "1d": TimeFrame.Day
        }.get(interval)

        if tf is None:
            raise ValueError(f"Unsupported interval for Alpaca: {interval}")

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

        # Clean format
        df.reset_index(inplace=True)
        df["ts"] = df["timestamp"]
        df["symbol"] = df["symbol"]

        return df[["symbol", "ts", "open", "high", "low", "close", "volume"]]
