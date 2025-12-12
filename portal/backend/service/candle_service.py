from indicators.config import DataContext
from typing import Optional

import pandas as pd

from data_providers.providers.factory import get_provider

def fetch_ohlcv(
    symbol: str,
    start: str,
    end: str,
    interval: str,
    *,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV data for a given symbol and time range.
    """
    ctx = DataContext(symbol=symbol, start=start, end=end, interval=interval)
    provider = get_provider(datasource, exchange=exchange)
    return provider.get_ohlcv(ctx)
