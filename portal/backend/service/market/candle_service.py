import logging
from typing import Optional

import pandas as pd

from data_providers.providers.factory import get_provider
from data_providers.utils.ohlcv import interval_to_timedelta
from indicators.config import DataContext

from ..providers import persistence_bootstrap  # noqa: F401
from . import instrument_service
from .stats_queue import enqueue_stats_job

logger = logging.getLogger(__name__)


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
    df = provider.get_ohlcv(ctx)
    _schedule_stats_for_context(df, ctx)
    return df


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
    df = provider.get_ohlcv(ctx)
    _schedule_stats_for_context(df, ctx)
    return df


def _schedule_stats_for_context(df: pd.DataFrame, ctx: DataContext) -> None:
    """Enqueue asynchronous stats work for the requested range once candles are available."""

    if ctx.instrument_id is None or not ctx.interval or df is None or df.empty:
        return

    try:
        timeframe_seconds = int(interval_to_timedelta(ctx.interval).total_seconds())
    except Exception as exc:
        logger.warning(
            "stats_job_interval_invalid | instrument_id=%s interval=%s error=%s",
            ctx.instrument_id,
            ctx.interval,
            exc,
        )
        return

    if timeframe_seconds <= 0:
        return

    timestamps = pd.to_datetime(df.index, utc=True)
    if timestamps.empty:
        return

    enqueue_stats_job(
        instrument_id=ctx.instrument_id,
        timeframe_seconds=timeframe_seconds,
        time_min=timestamps.min(),
        time_max=timestamps.max(),
    )
