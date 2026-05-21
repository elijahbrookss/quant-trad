import logging
import os
import threading
import time
from typing import Any, Dict, Optional

import pandas as pd

from core.candle_continuity import expected_interval_seconds, summarize_candle_continuity
from data_providers.providers.factory import get_provider
from data_providers.utils.ohlcv import interval_to_timedelta
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


def _iso(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        ts = pd.to_datetime(value, utc=True)
    except Exception:
        return str(value)
    if pd.isna(ts):
        return None
    return ts.isoformat().replace("+00:00", "Z")


def _dataframe_times(df: pd.DataFrame) -> pd.Series:
    if "timestamp" in df.columns:
        return pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dropna()
    return pd.to_datetime(df.index, utc=True, errors="coerce").dropna()


def preflight_candle_coverage_by_instrument(
    instrument_id: str,
    start: str,
    end: str,
    interval: str,
) -> Dict[str, Any]:
    """Return compact candle coverage for a canonical instrument/window."""

    try:
        instrument = instrument_service.get_instrument_record(instrument_id)
    except KeyError as exc:
        return {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": instrument_id,
            "timeframe": interval,
            "requested_start": start,
            "requested_end": end,
            "status": "error",
            "severity": "error",
            "message": str(exc),
        }

    try:
        requested_start = pd.to_datetime(start, utc=True)
        requested_end = pd.to_datetime(end, utc=True)
    except Exception as exc:
        return {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": instrument_id,
            "symbol": instrument.get("symbol"),
            "provider": instrument.get("datasource"),
            "exchange": instrument.get("exchange"),
            "timeframe": interval,
            "requested_start": start,
            "requested_end": end,
            "status": "error",
            "severity": "error",
            "message": f"invalid requested window: {exc}",
        }

    try:
        df = fetch_ohlcv_by_instrument(instrument_id, start, end, interval)
    except Exception as exc:  # noqa: BLE001 - preflight reports provider/storage failures as evidence.
        return {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": instrument_id,
            "symbol": instrument.get("symbol"),
            "provider": instrument.get("datasource"),
            "exchange": instrument.get("exchange"),
            "timeframe": interval,
            "requested_start": _iso(requested_start),
            "requested_end": _iso(requested_end),
            "status": "error",
            "severity": "error",
            "message": f"candle fetch failed: {exc}",
        }

    if df is None or df.empty:
        return {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": instrument_id,
            "symbol": instrument.get("symbol"),
            "provider": instrument.get("datasource"),
            "exchange": instrument.get("exchange"),
            "timeframe": interval,
            "requested_start": _iso(requested_start),
            "requested_end": _iso(requested_end),
            "available_start": None,
            "available_end": None,
            "row_count": 0,
            "missing_ranges": [{"start": _iso(requested_start), "end": _iso(requested_end)}],
            "continuity": {"candle_count": 0, "final_status": "missing"},
            "status": "warning",
            "severity": "warning",
            "message": "No candles returned for requested window.",
        }

    times = _dataframe_times(df).sort_values()
    if times.empty:
        return {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": instrument_id,
            "symbol": instrument.get("symbol"),
            "provider": instrument.get("datasource"),
            "exchange": instrument.get("exchange"),
            "timeframe": interval,
            "requested_start": _iso(requested_start),
            "requested_end": _iso(requested_end),
            "row_count": int(len(df)),
            "status": "warning",
            "severity": "warning",
            "message": "Candles were returned but no parseable candle timestamps were found.",
        }

    available_start = times.iloc[0]
    last_candle_start = times.iloc[-1]
    interval_delta = interval_to_timedelta(interval)
    coverage_end = last_candle_start + interval_delta
    gap_classification = getattr(df, "attrs", {}).get("gap_classification") if hasattr(df, "attrs") else None
    continuity = summarize_candle_continuity(
        [{"time": item.isoformat()} for item in times],
        expected_interval_seconds_value=expected_interval_seconds(timeframe=interval),
        gap_classification=gap_classification,
    ).to_dict()
    missing_ranges: list[dict[str, str | None]] = []
    if available_start > requested_start:
        missing_ranges.append({"start": _iso(requested_start), "end": _iso(available_start)})
    if coverage_end < requested_end:
        missing_ranges.append({"start": _iso(coverage_end), "end": _iso(requested_end)})

    final_status = str(continuity.get("final_status") or "unknown")
    warning = bool(missing_ranges) or final_status in {"defect", "unknown", "missing"}
    severity = "warning" if warning else "info" if final_status == "expected_sparse" else "ok"
    return {
        "schema_version": "candle_coverage_preflight.v1",
        "instrument_id": instrument_id,
        "symbol": instrument.get("symbol"),
        "provider": instrument.get("datasource"),
        "exchange": instrument.get("exchange"),
        "timeframe": interval,
        "requested_start": _iso(requested_start),
        "requested_end": _iso(requested_end),
        "available_start": _iso(available_start),
        "available_end": _iso(coverage_end),
        "last_candle_start": _iso(last_candle_start),
        "row_count": int(len(times)),
        "missing_ranges": missing_ranges,
        "continuity": continuity,
        "status": "warning" if warning else "ok",
        "severity": severity,
        "message": (
            "Candle coverage has missing ranges or continuity defects."
            if warning
            else "Candle coverage is available for requested window."
        ),
    }


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
