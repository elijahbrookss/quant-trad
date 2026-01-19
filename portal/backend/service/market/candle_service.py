from indicators.config import DataContext
from typing import Optional

import pandas as pd

from data_providers.providers.factory import get_provider

from ..providers import persistence_bootstrap  # noqa: F401
from . import instrument_service

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
    instrument_id = instrument_service.require_instrument_id(datasource, exchange, symbol)
    ctx = DataContext(
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
        instrument_id=instrument_id,
    )
    provider = get_provider(datasource, exchange=exchange)
    return provider.get_ohlcv(ctx)


def fetch_ohlcv_by_instrument(
    instrument_id: str,
    start: str,
    end: str,
    interval: str,
) -> pd.DataFrame:
    """Fetch OHLCV data for a canonical instrument."""

    try:
        instrument = instrument_service.get_instrument_record(instrument_id)
    except KeyError as exc:
        raise ValueError(str(exc)) from exc
    datasource = instrument.get("datasource")
    exchange = instrument.get("exchange")
    symbol = instrument.get("symbol")
    if not datasource or not symbol:
        raise ValueError(f"Instrument {instrument_id} is missing datasource or symbol.")

    ctx = DataContext(
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
        instrument_id=instrument_id,
    )
    provider = get_provider(datasource, exchange=exchange)
    return provider.get_ohlcv(ctx)
