from data_providers.alpaca_provider import AlpacaProvider
from indicators.config import DataContext
import pandas as pd

provider = AlpacaProvider()

def fetch_ohlcv(symbol: str, start: str, end: str, interval: str) -> pd.DataFrame:
    """
    Fetch OHLCV data for a given symbol and time range.
    """
    ctx = DataContext(symbol=symbol, start=start, end=end, interval=interval)
    return provider.get_ohlcv(ctx)