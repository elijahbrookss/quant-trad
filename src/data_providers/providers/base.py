from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from core.chart_plotter import ChartPlotter
from core.logger import logger
from indicators.config import DataContext
from ..config.runtime import ProviderRuntimeConfig, runtime_config_from_env
from ..services.persistence import DataPersistence, NullPersistence
from ..utils import ohlcv as utils


class DataSource(str, Enum):
    YFINANCE = "YFINANCE"
    ALPACA = "ALPACA"
    IBKR = "IBKR"
    CCXT = "CCXT"
    UNKNOWN = "UNKNOWN"


class InstrumentType(str, Enum):
    SPOT = "SPOT"
    FUTURE = "FUTURE"


@dataclass(frozen=True)
class InstrumentMetadata:
    """Standardized instrument metadata expressed per trading unit."""

    tick_size: Optional[float]
    contract_size: Optional[float]
    tick_value: Optional[float]
    can_short: Optional[bool]
    short_requires_borrow: Optional[bool]
    has_funding: Optional[bool]
    expiry_ts: Optional[dt.datetime]
    base_currency: Optional[str]
    quote_currency: Optional[str]

    def as_dict(self) -> dict:
        return {
            "tick_size": self.tick_size,
            "contract_size": self.contract_size,
            "tick_value": self.tick_value,
            "can_short": self.can_short,
            "short_requires_borrow": self.short_requires_borrow,
            "has_funding": self.has_funding,
            "expiry_ts": self.expiry_ts,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
        }


class ProviderInterface(ABC):
    """Minimal provider contract covering market metadata and fetch operations."""

    @abstractmethod
    def get_datasource(self) -> str:
        pass

    @abstractmethod
    def fetch_from_api(self, symbol: str, start: dt.datetime, end: dt.datetime, interval: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Return a binary instrument classification (spot vs futures/perps)."""

    @abstractmethod
    def validate_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Raise if the instrument type cannot be confirmed."""

    @abstractmethod
    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        """Return tick_size, contract_size, and tick_value for a trading unit."""

    @abstractmethod
    def validate_symbol(self, venue: str, symbol: str) -> None:
        """Raise if the symbol does not exist for the provider/venue."""


class BaseDataProvider(ProviderInterface):
    def __init__(
        self,
        *,
        persistence: Optional[DataPersistence] = None,
        settings: Optional[ProviderRuntimeConfig] = None,
    ) -> None:
        self._settings = settings or runtime_config_from_env()
        self._persistence = persistence or NullPersistence()

    @staticmethod
    def _normalize_metadata(
        *,
        tick_size: Optional[float] = None,
        contract_size: Optional[float] = None,
        tick_value: Optional[float] = None,
        can_short: Optional[bool] = None,
        short_requires_borrow: Optional[bool] = None,
        has_funding: Optional[bool] = None,
        expiry_ts: Optional[dt.datetime] = None,
        base_currency: Optional[str] = None,
        quote_currency: Optional[str] = None,
    ) -> InstrumentMetadata:
        """Derive a consistent metadata triple from the provided inputs."""

        ts = float(tick_size) if tick_size is not None else None
        cs = float(contract_size) if contract_size is not None else None
        tv = float(tick_value) if tick_value is not None else None

        if ts is None and tv is None:
            raise ValueError("At least tick_size or tick_value must be provided")

        if tv is None and ts is not None and cs is not None:
            tv = ts * cs

        if cs is None and ts is not None and tv is not None and ts != 0:
            cs = tv / ts

        if ts is None and cs is not None and tv is not None and cs != 0:
            ts = tv / cs

        missing = []
        if can_short is None:
            missing.append("can_short")
        if short_requires_borrow is None:
            missing.append("short_requires_borrow")
        if has_funding is None:
            missing.append("has_funding")
        if not base_currency:
            missing.append("base_currency")
        if not quote_currency:
            missing.append("quote_currency")
        if missing:
            raise ValueError(f"Instrument metadata missing fields: {', '.join(missing)}")

        return InstrumentMetadata(
            ts,
            cs,
            tv,
            can_short,
            short_requires_borrow,
            has_funding,
            expiry_ts,
            str(base_currency).upper() if base_currency else None,
            str(quote_currency).upper() if quote_currency else None,
        )

    def ensure_schema(self):
        self._persistence.ensure_schema()

    @staticmethod
    def _interval_to_timedelta(interval: str) -> dt.timedelta:
        return utils.interval_to_timedelta(interval)

    @staticmethod
    def _compute_tr_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        return utils.compute_tr_atr(df, period)

    @staticmethod
    def _collect_missing_ranges(
        timestamps: Iterable[pd.Timestamp],
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
        interval: str,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        return utils.collect_missing_ranges(timestamps, requested_start, requested_end, interval)

    @staticmethod
    def _subtract_ranges(
        ranges: List[Tuple[pd.Timestamp, pd.Timestamp]],
        closures: List[Tuple[pd.Timestamp, pd.Timestamp]],
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        return utils.subtract_ranges(ranges, closures)

    def _load_closure_ranges(
        self,
        ctx: DataContext,
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        return self._persistence.load_closure_ranges(
            ctx,
            self.get_datasource(),
            requested_start,
            requested_end,
        )

    def _record_closure_range(
        self,
        ctx: DataContext,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ):
        self._persistence.record_closure_range(
            ctx,
            self.get_datasource(),
            start,
            end,
        )

    def _write_dataframe(self, df: pd.DataFrame, ctx: DataContext) -> int:
        return self._persistence.write_dataframe(df, ctx)

    def _history_segment_target(self) -> int:
        return max(1, int(self._settings.history_segment_points))

    def _split_history_range(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        interval: str,
        *,
        max_points: int = None,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        target = max_points or self._history_segment_target()
        return utils.split_history_range(start, end, interval, max_points=target)

    def ingest_history(self, ctx: DataContext, days: int = 30) -> int:
        start = ctx.start
        end = ctx.end

        if not start or not end:
            end_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            start_dt = end_dt - dt.timedelta(days=days)
            ctx.start = start_dt.isoformat()
            ctx.end = end_dt.isoformat()
            start = ctx.start
            end = ctx.end

        try:
            start_ts = pd.to_datetime(start, utc=True)
            end_ts = pd.to_datetime(end, utc=True)
        except Exception as exc:
            logger.exception("Failed to parse history range for %s: %s", ctx.symbol, exc)
            return 0

        if start_ts >= end_ts:
            logger.warning(
                "History ingest skipped for %s [%s]; start %s is not before end %s.",
                ctx.symbol,
                ctx.interval,
                start_ts,
                end_ts,
            )
            return 0

        segments = self._split_history_range(start_ts, end_ts, ctx.interval)
        total_segments = len(segments)

        frames: List[pd.DataFrame] = []
        for index, (segment_start, segment_end) in enumerate(segments, start=1):
            logger.info(
                "History ingest segment %d/%d for %s [%s]: %s -> %s",
                index,
                total_segments,
                ctx.symbol,
                ctx.interval,
                segment_start.isoformat(),
                segment_end.isoformat(),
            )

            try:
                df = self.fetch_from_api(
                    ctx.symbol,
                    segment_start.to_pydatetime(),
                    segment_end.to_pydatetime(),
                    ctx.interval,
                )
            except Exception as exc:
                logger.exception(
                    "History segment fetch failed for %s [%s] (%s -> %s): %s",
                    ctx.symbol,
                    ctx.interval,
                    segment_start.isoformat(),
                    segment_end.isoformat(),
                    exc,
                )
                continue

            if df is None or df.empty:
                logger.warning(
                    "History segment returned no data for %s [%s] (%s -> %s).",
                    ctx.symbol,
                    ctx.interval,
                    segment_start.isoformat(),
                    segment_end.isoformat(),
                )
                continue

            frames.append(df.copy())

        if not frames:
            logger.warning("No history data fetched for %s [%s].", ctx.symbol, ctx.interval)
            return 0

        combined = pd.concat(frames, ignore_index=True)
        combined.drop_duplicates(subset="timestamp", keep="last", inplace=True)
        combined.sort_values("timestamp", inplace=True)

        combined = self._compute_tr_atr(combined)

        now_ts = dt.datetime.now(dt.timezone.utc)
        combined["data_ingested_ts"] = now_ts
        combined["datasource"] = self.get_datasource()
        combined["interval"] = ctx.interval
        combined["symbol"] = ctx.symbol

        try:
            return self._write_dataframe(combined, ctx)
        except SQLAlchemyError as e:
            logger.exception("DB error during ingest_history for %s: %s", ctx.symbol, e)
            raise

    def get_ohlcv(self, ctx: DataContext) -> pd.DataFrame:
        ctx.validate()
        exchange = getattr(self, "_exchange_id", None) or getattr(self, "exchange", None)
        logger.info(
            "candle_fetch_start | datasource=%s exchange=%s symbol=%s interval=%s start=%s end=%s",
            self.get_datasource(),
            exchange,
            ctx.symbol,
            ctx.interval,
            ctx.start,
            ctx.end,
        )

        if not self._persistence.engine_available:
            logger.warning(
                "Database engine unavailable; fetching OHLCV from API for %s [%s].",
                ctx.symbol,
                ctx.interval,
            )
            return self._fetch_and_format(ctx)

        try:
            df = self._persistence.fetch_ohlcv(ctx, self.get_datasource())
        except SQLAlchemyError as e:
            logger.exception("Database error during OHLCV query: %s. Falling back to API.", e)
            logger.warning("Unable to connect to database, deferred to API.")
            return self._fetch_and_format(ctx)

        if df.empty:
            logger.warning("No rows found for %s [%s] from %s to %s. Attempting auto-ingestion...",
                           ctx.symbol, ctx.interval, ctx.start, ctx.end)
            try:
                logger.info(
                    "Auto-ingestion requesting %s [%s] candles via API from %s to %s.",
                    ctx.symbol,
                    ctx.interval,
                    ctx.start,
                    ctx.end,
                )
                self.ingest_history(ctx)
                df = self._persistence.fetch_ohlcv(ctx, self.get_datasource())
                if df.empty:
                    logger.error("Auto-ingestion attempted but still no data found.")
                    return df
            except Exception as e:
                logger.exception("Auto-ingestion failed: %s", e)
                return df

        if not df.empty:
            requested_start = pd.to_datetime(ctx.start, utc=True)
            requested_end = pd.to_datetime(ctx.end, utc=True)
            timestamps = pd.to_datetime(df["timestamp"], utc=True)
            missing_ranges = self._collect_missing_ranges(
                timestamps,
                requested_start,
                requested_end,
                ctx.interval,
            )

            closures = self._load_closure_ranges(ctx, requested_start, requested_end)
            if closures:
                missing_ranges = self._subtract_ranges(missing_ranges, closures)

            supplemental_frames = []
            for start, end in missing_ranges:
                if end <= start:
                    continue

                logger.info(
                    "Partial cache miss for %s [%s]; fetching %s to %s via API.",
                    ctx.symbol,
                    ctx.interval,
                    start.isoformat(),
                    end.isoformat(),
                )

                try:
                    segment = self.fetch_from_api(ctx.symbol, start, end, ctx.interval)
                except Exception as exc:
                    logger.exception(
                        "Failed to fetch %s [%s] for partial range %s -> %s: %s",
                        ctx.symbol,
                        ctx.interval,
                        start.isoformat(),
                        end.isoformat(),
                        exc,
                    )
                    continue

                if segment is None or segment.empty:
                    logger.info(
                        "Partial fetch for %s [%s] returned no rows for %s to %s; caching closure.",
                        ctx.symbol,
                        ctx.interval,
                        start.isoformat(),
                        end.isoformat(),
                    )
                    self._record_closure_range(ctx, start, end)
                    continue

                segment = segment.copy()
                segment_ts = pd.to_datetime(segment["timestamp"], utc=True)
                segment["timestamp"] = segment_ts
                in_window = segment[(segment_ts >= start) & (segment_ts < end)]
                if in_window.empty:
                    logger.info(
                        "Partial fetch for %s [%s] produced data outside %s -> %s; caching closure.",
                        ctx.symbol,
                        ctx.interval,
                        start.isoformat(),
                        end.isoformat(),
                    )
                    self._record_closure_range(ctx, start, end)
                    continue

                segment.sort_values("timestamp", inplace=True)
                segment = self._compute_tr_atr(segment)
                segment["data_ingested_ts"] = dt.datetime.now(dt.timezone.utc)
                segment["datasource"] = self.get_datasource()
                segment["interval"] = ctx.interval
                segment["symbol"] = ctx.symbol

                try:
                    self._write_dataframe(segment, ctx)
                except SQLAlchemyError:
                    # _write_dataframe already logged the failure.
                    pass

                supplemental_frames.append(
                    segment[
                        [
                            "timestamp",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "tr",
                            "atr_wilder",
                        ]
                    ]
                )

            if supplemental_frames:
                combined = pd.concat([df] + supplemental_frames, ignore_index=True)
                combined.drop_duplicates(subset="timestamp", keep="last", inplace=True)
                combined.sort_values("timestamp", inplace=True)
                df = combined.reset_index(drop=True)

        # Recompute ATR to ensure it's up-to-date with the latest calculation logic
        # This handles cases where cached data has stale or invalid ATR values
        df = self._compute_tr_atr(df)

        return self._format_ohlcv_dataframe(df, ctx)

    def _fetch_and_format(self, ctx: DataContext) -> pd.DataFrame:
        try:
            start_dt = pd.to_datetime(ctx.start, utc=True)
            end_dt = pd.to_datetime(ctx.end, utc=True)
        except Exception as exc:
            logger.exception(
                "Fallback fetch has invalid timestamps for %s [%s]: %s",
                ctx.symbol,
                ctx.interval,
                exc,
            )
            return pd.DataFrame()

        if start_dt is None or end_dt is None:
            logger.error(
                "Fallback fetch missing start/end for %s [%s]; start=%s end=%s",
                ctx.symbol,
                ctx.interval,
                ctx.start,
                ctx.end,
            )
            return pd.DataFrame()

        start_dt = start_dt.to_pydatetime()
        end_dt = end_dt.to_pydatetime()

        if start_dt >= end_dt:
            logger.warning(
                "Fallback fetch has non-increasing window for %s [%s]; start=%s end=%s",
                ctx.symbol,
                ctx.interval,
                start_dt,
                end_dt,
            )
            return pd.DataFrame()

        try:
            df = self.fetch_from_api(ctx.symbol, start_dt, end_dt, ctx.interval)
        except Exception as e:
            logger.exception("Fallback fetch_from_api failed for %s: %s", ctx.symbol, e)
            return pd.DataFrame()

        if df is None or df.empty:
            logger.warning("Fallback fetch returned no data for %s [%s].", ctx.symbol, ctx.interval)
            return pd.DataFrame()

        df = df.sort_values("timestamp")
        df = self._compute_tr_atr(df)
        df["datasource"] = self.get_datasource()
        df["interval"] = ctx.interval
        df["symbol"] = ctx.symbol

        return self._format_ohlcv_dataframe(df, ctx)

    def _format_ohlcv_dataframe(self, df: pd.DataFrame, ctx: DataContext) -> pd.DataFrame:
        if df.empty:
            return df

        if "datasource" not in df.columns:
            df["datasource"] = self.get_datasource()
        if "interval" not in df.columns:
            df["interval"] = ctx.interval
        if "symbol" not in df.columns:
            df["symbol"] = ctx.symbol

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
        df["timestamp"] = df.index
        exchange = getattr(self, "_exchange_id", None) or getattr(self, "exchange", None)
        row_count = len(df.index) if df is not None else 0
        range_start = None
        range_end = None
        if df is not None and not df.empty and "timestamp" in df.columns:
            try:
                ts = pd.to_datetime(df["timestamp"], utc=True)
                range_start = ts.min()
                range_end = ts.max()
            except Exception:
                range_start = None
                range_end = None
        logger.info(
            "candle_fetch_end | datasource=%s exchange=%s symbol=%s interval=%s rows=%s range_start=%s range_end=%s",
            self.get_datasource(),
            exchange,
            ctx.symbol,
            ctx.interval,
            row_count,
            range_start,
            range_end,
        )
        return df

    def plot_ohlcv(self, plot_ctx: DataContext, title: str = None, **kwargs):
        df = self.get_ohlcv(plot_ctx)
        title = title or f"{plot_ctx.symbol} | {plot_ctx.interval}"
        logger.debug("df index sample: %s", df.index)
        ChartPlotter.plot_ohlc(df, title=title, ctx=plot_ctx, datasource=self.get_datasource(), **kwargs)
