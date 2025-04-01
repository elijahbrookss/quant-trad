import yfinance as yf
import pandas as pd
from typing import Any
from classes.Logger import logger

class StockData:
    """
    A class to fetch historical stock data from Yahoo Finance using yfinance.

    Attributes:
        symbol (str): The stock ticker symbol.
        start (str): The start date for fetching data.
        end (str): The end date for fetching data.
        df (pd.DataFrame): A DataFrame containing the fetched stock data.
    """
    def __init__(self, symbol: str, start: str, end: str) -> None:
        self.symbol = symbol
        self.start = start
        self.end = end
        self.df = self._fetch()

    def _fetch(self) -> pd.DataFrame:
        """
        Fetch historical stock data using yfinance.

        Returns:
            pd.DataFrame: A DataFrame with columns ['Open', 'High', 'Low', 'Close', 'Volume'].

        Raises:
            ValueError: If no data is fetched for the given symbol and date range.
            KeyError: If the expected columns are missing from the downloaded data.
        """
        logger.info(f"Fetching stock data for {self.symbol} from {self.start} to {self.end}...")
        df = yf.download(self.symbol, start=self.start, end=self.end, auto_adjust=True)
        
        if df.empty:
            logger.error(f"No data fetched for symbol {self.symbol}. Please check the symbol and date range.")
            raise ValueError(f"No data fetched for symbol {self.symbol}")

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        # Select required columns and remove timezone localization
        try:
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']].tz_localize(None)
        except KeyError as e:
            logger.error("Required columns missing in fetched data: %s", e)
            raise

        self._check_health(df)
        return df

    def _check_health(self, df: pd.DataFrame) -> None:
        """
        Log health check information about the DataFrame's index.

        Args:
            df (pd.DataFrame): The DataFrame to check.
        """
        logger.info("Index Unique: %s", df.index.is_unique)
        logger.info("Index Type: %s", type(df.index))
        logger.info("Duplicate Index Entries: %d", df.index.duplicated().sum())
