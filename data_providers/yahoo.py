import datetime as dt
import pandas as pd
import yfinance as yf
from classes.Logger import logger


from .base import BaseDataProvider

class YahooFinanceProvider(BaseDataProvider):
    def get_ohlcv(
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

        # Force lowercase column names
        df.columns = [col.lower() for col in df.columns]
        # Rename datetime index to 'ts'
        df.rename(columns={"datetime": "ts"}, inplace=True)

        df["symbol"] = symbol
        logger.debug("DataFrame columns after cleanup: %s", df.columns)

        return df[["symbol", "ts", "open", "high", "low", "close", "volume"]]
