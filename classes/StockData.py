import yfinance as yf
import pandas as pd

class StockData:
    def __init__(self, symbol, start, end):
        self.symbol = symbol
        self.start = start
        self.end = end
        self.df = self._fetch()

    def _fetch(self):
        print(f"Fetching stock data for {self.symbol}...")
        df = yf.download(self.symbol, start=self.start, end=self.end, auto_adjust=True)

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        df = df[['Open', 'High', 'Low', 'Close', 'Volume']].tz_localize(None)
        self._check_health(df)
        return df

    def _check_health(self, df):
        print("[INFO] Index Unique:", df.index.is_unique)
        print("[INFO] Index Type:", type(df.index))
        print("[INFO] Duplicate Index Entries:", df.index.duplicated().sum())