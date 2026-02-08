import logging
import os
import threading
import time
from typing import Optional

import pandas as pd

from data_providers.providers.factory import get_provider
from data_providers.utils.ohlcv import interval_to_timedelta
from indicators.config import DataContext
from utils.perf_log import get_obs_enabled, get_obs_step_sample_rate, should_sample

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
    # NOTE: NO CACHE – repeated fetch_ohlcv calls with identical windows will re-hit provider/persistence.
    instrument_id = instrument_service.require_instrument_id(datasource, exchange, symbol)
    ctx = DataContext(
        symbol=symbol,
        start=start,
        end=end,
        interval=interval,
        instrument_id=instrument_id,
    )
    provider = get_provider(datasource, exchange=exchange)
    should_log = get_obs_enabled() and should_sample(get_obs_step_sample_rate())
    started = time.perf_counter() if should_log else 0.0
    df = provider.get_ohlcv(ctx)
    if should_log:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        logger.debug(
            "event=cache.absent operation_name=fetch_ohlcv time_taken_ms=%.4f pid=%s thread_name=%s "
            "symbol=%s interval=%s datasource=%s exchange=%s start=%s end=%s",
            elapsed_ms,
            os.getpid(),
            threading.current_thread().name,
            symbol,
            interval,
            datasource,
            exchange,
            start,
            end,
        )
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
    should_log = get_obs_enabled() and should_sample(get_obs_step_sample_rate())
    started = time.perf_counter() if should_log else 0.0
    df = provider.get_ohlcv(ctx)
    if should_log:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        logger.debug(
            "event=cache.absent operation_name=fetch_ohlcv_by_instrument time_taken_ms=%.4f pid=%s thread_name=%s "
            "symbol=%s interval=%s datasource=%s exchange=%s start=%s end=%s instrument_id=%s",
            elapsed_ms,
            os.getpid(),
            threading.current_thread().name,
            symbol,
            interval,
            datasource,
            exchange,
            start,
            end,
            instrument_id,
        )
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
