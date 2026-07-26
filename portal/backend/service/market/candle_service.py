from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional

import pandas as pd

from core.candle_continuity import expected_interval_seconds, summarize_candle_continuity
from data_providers.utils.ohlcv import compute_tr_atr, interval_to_timedelta
from indicators.config import DataContext

from . import instrument_service
from .feed_service import canonical_candle_feed


_DERIVED_CANDLE_FEATURE_VERSION = "runtime_candle_features.wilder_atr_14.v1"


@dataclass(frozen=True)
class MarketDataReadScope:
    as_of_commit_seq: int

    def __post_init__(self) -> None:
        if int(self.as_of_commit_seq) < 0:
            raise ValueError("market_data_read_scope_invalid: commit sequence must be nonnegative")
        object.__setattr__(self, "as_of_commit_seq", int(self.as_of_commit_seq))


_MARKET_DATA_READ_SCOPE: ContextVar[Optional[MarketDataReadScope]] = ContextVar(
    "market_data_read_scope", default=None
)


@contextmanager
def market_data_read_scope(*, as_of_commit_seq: int) -> Iterator[MarketDataReadScope]:
    """Bind nested candle/indicator reads to one immutable commit watermark."""

    scope = MarketDataReadScope(as_of_commit_seq=as_of_commit_seq)
    token = _MARKET_DATA_READ_SCOPE.set(scope)
    try:
        yield scope
    finally:
        _MARKET_DATA_READ_SCOPE.reset(token)


def current_market_data_read_scope() -> Optional[MarketDataReadScope]:
    return _MARKET_DATA_READ_SCOPE.get()


def _with_runtime_candle_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive execution inputs explicitly without storing them as source facts."""

    if frame is None or frame.empty:
        return frame
    enriched = compute_tr_atr(frame.copy(), period=14)
    enriched.attrs.update(getattr(frame, "attrs", {}))
    enriched.attrs["derived_candle_features"] = {
        "schema_version": _DERIVED_CANDLE_FEATURE_VERSION,
        "atr": {
            "method": "wilder_ewm",
            "period": 14,
            "source_fields": ["high", "low", "close"],
        },
    }
    return enriched


def fetch_ohlcv(
    symbol: str,
    start: str,
    end: str,
    interval: str,
    *,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
) -> pd.DataFrame:
    """Read canonical stored candles; missing data requires explicit ingestion."""

    instrument_id = instrument_service.require_instrument_id(
        datasource, exchange, symbol
    )
    return fetch_ohlcv_by_instrument(instrument_id, start, end, interval)


def fetch_ohlcv_by_instrument(
    instrument_id: str,
    start: str,
    end: str,
    interval: str,
) -> pd.DataFrame:
    """Read one canonical instrument series without provider/API fallback."""

    try:
        instrument = instrument_service.get_instrument_record(instrument_id)
    except KeyError as exc:
        raise ValueError(str(exc)) from exc
    scope = current_market_data_read_scope()
    frame = canonical_candle_feed.read_by_instrument(
        instrument,
        start=start,
        end=end,
        interval=interval,
        as_of_commit_seq=(scope.as_of_commit_seq if scope is not None else None),
    )
    enriched = _with_runtime_candle_features(frame)
    if scope is not None:
        enriched.attrs["market_data_read_scope"] = {
            "schema_version": "market_data_read_scope.v1",
            "as_of_commit_seq": scope.as_of_commit_seq,
        }
    return enriched


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
