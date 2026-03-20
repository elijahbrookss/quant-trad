import logging
import os
import threading
import time
from typing import Optional

import pandas as pd

from data_providers.providers.factory import get_provider
from indicators.config import DataContext
from utils.perf_log import get_obs_enabled, get_obs_step_sample_rate, should_sample

from ..providers import persistence_bootstrap  # noqa: F401
from . import instrument_service

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
    return df


def fetch_ohlcv_for_context(
    ctx: DataContext,
    *,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch OHLCV through the canonical candle service using an indicator/runtime data context."""

    if ctx.instrument_id:
        return fetch_ohlcv_by_instrument(
            str(ctx.instrument_id),
            str(ctx.start),
            str(ctx.end),
            str(ctx.interval),
        )
    return fetch_ohlcv(
        str(ctx.symbol),
        str(ctx.start),
        str(ctx.end),
        str(ctx.interval),
        datasource=datasource,
        exchange=exchange,
    )
