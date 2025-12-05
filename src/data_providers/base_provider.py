from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import datetime as dt
import pandas as pd
import os
from typing import Iterable, List, Tuple, Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError

from core.logger import logger
from indicators.config import DataContext
from core.chart_plotter import ChartPlotter

load_dotenv(".env")
load_dotenv("secrets.env")


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

    def as_dict(self) -> dict:
        return {
            "tick_size": self.tick_size,
            "contract_size": self.contract_size,
            "tick_value": self.tick_value,
        }


class BaseDataProvider(ABC):
    _dsn = os.getenv("PG_DSN")
    _table = os.getenv("OHLC_TABLE")
    _closures_table = os.getenv("OHLC_CLOSURES_TABLE", "portal_ohlc_closures")

    _engine = None

    if not _dsn:
        logger.warning("PG_DSN is not defined. Database operations will be skipped.")
    else:
        try:
            _engine = create_engine(_dsn)
        except SQLAlchemyError as e:
            logger.exception("Failed to create database engine: %s", e)
            _engine = None

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

    @staticmethod
    def _normalize_metadata(
        *,
        tick_size: Optional[float] = None,
        contract_size: Optional[float] = None,
        tick_value: Optional[float] = None,
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

        return InstrumentMetadata(ts, cs, tv)

    def ensure_schema(self):
        if not self._engine:
            logger.warning("Database engine unavailable; skipping ensure_schema call for '%s'.", self._table)
            return

        ddl_create = f"""
        CREATE TABLE IF NOT EXISTS {self._table} (
            datasource TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            open  DOUBLE PRECISION,
            high  DOUBLE PRECISION,
            low   DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            tr DOUBLE PRECISION,
            atr_wilder DOUBLE PRECISION,
            data_ingested_ts TIMESTAMPTZ DEFAULT now(),
            interval TEXT NOT NULL,
            PRIMARY KEY (symbol, timestamp, datasource, interval)
        );
        """
        ddl_add_columns = f"""
        ALTER TABLE {self._table}
            ADD COLUMN IF NOT EXISTS tr DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS atr_wilder DOUBLE PRECISION;
        """
        ddl_hypertable = f"""
        SELECT create_hypertable('{self._table}', 'timestamp', if_not_exists => TRUE);
        """

        ddl_closures = f"""
        CREATE TABLE IF NOT EXISTS {self._closures_table} (
            datasource TEXT NOT NULL,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            start_ts TIMESTAMPTZ NOT NULL,
            end_ts TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (datasource, symbol, interval, start_ts, end_ts)
        );
        """

        try:
            with self._engine.begin() as conn:
                conn.execute(text(ddl_create))
                conn.execute(text(ddl_add_columns))
                conn.execute(text(ddl_hypertable))
                conn.execute(text(ddl_closures))
            logger.info("Schema ensured for table '%s'.", self._table)
        except SQLAlchemyError as e:
            logger.exception("Failed to ensure schema for '%s': %s", self._table, e)
            raise

    @staticmethod
    def _interval_to_timedelta(interval: str) -> dt.timedelta:
        unit = interval.lower()

        if unit.endswith("ms"):
            return dt.timedelta(milliseconds=max(1, int(unit[:-2])))
        if unit.endswith("s"):
            return dt.timedelta(seconds=max(1, int(unit[:-1])))
        if unit.endswith("m"):
            return dt.timedelta(minutes=max(1, int(unit[:-1])))
        if unit.endswith("h"):
            return dt.timedelta(hours=max(1, int(unit[:-1])))
        if unit.endswith("d"):
            return dt.timedelta(days=max(1, int(unit[:-1])))
        if unit.endswith("w"):
            return dt.timedelta(weeks=max(1, int(unit[:-1])))
        if unit.endswith("mo"):
            return dt.timedelta(days=max(1, int(unit[:-2])) * 30)
        if unit.endswith("y"):
            return dt.timedelta(days=max(1, int(unit[:-1])) * 365)

        raise ValueError(f"Unsupported interval string: {interval}")

    @staticmethod
    def _compute_tr_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        required_cols = {"high", "low", "close"}
        if df is None or df.empty or not required_cols.issubset(df.columns):
            return df

        hl = df["high"] - df["low"]
        h_cp = (df["high"] - df["close"].shift()).abs()
        l_cp = (df["low"] - df["close"].shift()).abs()

        tr = pd.concat([hl, h_cp, l_cp], axis=1).max(axis=1)
        df["tr"] = tr
        df["atr_wilder"] = tr.ewm(alpha=1 / period, adjust=False).mean()
        return df

    @staticmethod
    def _collect_missing_ranges(
        timestamps: Iterable[pd.Timestamp],
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
        interval: str,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        ordered = sorted(set(pd.to_datetime(list(timestamps), utc=True)))
        if not ordered:
            return []

        try:
            step = BaseDataProvider._interval_to_timedelta(interval)
        except Exception:
            step = None

        if step is None and len(ordered) >= 2:
            deltas = pd.Series(ordered).diff().dropna()
            if not deltas.empty:
                step = deltas.median()

        if step is None:
            step = pd.Timedelta(0)

        has_step = step > pd.Timedelta(0)
        tolerance = step / 2 if has_step else pd.Timedelta(0)
        missing: List[Tuple[pd.Timestamp, pd.Timestamp]] = []

        first = ordered[0]
        if first - requested_start > tolerance:
            missing.append((requested_start, min(first, requested_end)))

        if has_step:
            for previous, current in zip(ordered, ordered[1:]):
                gap = current - previous
                if gap > step * 1.5 and previous + step < current:
                    gap_start = previous + step
                    gap_end = current
                    missing.append((gap_start, gap_end))

        last = ordered[-1]
        effective_end = requested_end
        trailing_start = max(last, requested_start)

        if has_step:
            effective_end = max(requested_start, requested_end - step)
            trailing_start = last + step

        if effective_end - last > tolerance:
            trailing_start = max(trailing_start, requested_start)
            if trailing_start < requested_end:
                missing.append((trailing_start, requested_end))

        filtered: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        for start, end in missing:
            if end <= start:
                continue
            filtered.append((start, end))

        return filtered

    @staticmethod
    def _subtract_ranges(
        ranges: List[Tuple[pd.Timestamp, pd.Timestamp]],
        closures: List[Tuple[pd.Timestamp, pd.Timestamp]],
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        if not closures:
            return ranges

        result: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        for start, end in ranges:
            segments = [(start, end)]
            for closure_start, closure_end in closures:
                updated: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
                for seg_start, seg_end in segments:
                    if closure_end <= seg_start or closure_start >= seg_end:
                        updated.append((seg_start, seg_end))
                        continue

                    if seg_start < closure_start:
                        updated.append((seg_start, closure_start))
                    if closure_end < seg_end:
                        updated.append((closure_end, seg_end))
                segments = updated
                if not segments:
                    break
            result.extend(segments)

        filtered: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        for start, end in result:
            if end <= start:
                continue
            filtered.append((start, end))

        return filtered

    def _load_closure_ranges(
        self,
        ctx: DataContext,
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        if not self._engine:
            return []

        query = text(
            f"""
            SELECT start_ts, end_ts
            FROM {self._closures_table}
            WHERE datasource = :datasource
              AND symbol = :symbol
              AND interval = :interval
              AND NOT (end_ts <= :request_start OR start_ts >= :request_end)
            ORDER BY start_ts
            """
        )

        try:
            with self._engine.begin() as conn:
                rows = conn.execute(
                    query,
                    {
                        "datasource": self.get_datasource(),
                        "symbol": ctx.symbol,
                        "interval": ctx.interval,
                        "request_start": requested_start,
                        "request_end": requested_end,
                    },
                ).fetchall()
        except ProgrammingError as exc:
            message = str(exc).lower()
            if "does not exist" in message:
                logger.warning(
                    "Closure table '%s' missing. Ensuring schema before retry.",
                    self._closures_table,
                )
                self.ensure_schema()
                return []
            logger.exception(
                "Failed to load closure ranges for %s [%s]: %s",
                ctx.symbol,
                ctx.interval,
                exc,
            )
            return []
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to load closure ranges for %s [%s]: %s",
                ctx.symbol,
                ctx.interval,
                exc,
            )
            return []

        closures: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        for row in rows:
            closures.append((pd.to_datetime(row[0], utc=True), pd.to_datetime(row[1], utc=True)))

        return closures

    def _record_closure_range(
        self,
        ctx: DataContext,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ):
        if not self._engine or end <= start:
            return

        start_ts = pd.to_datetime(start, utc=True)
        end_ts = pd.to_datetime(end, utc=True)

        overlap_query = text(
            f"""
            SELECT start_ts, end_ts FROM {self._closures_table}
            WHERE datasource = :datasource
              AND symbol = :symbol
              AND interval = :interval
              AND NOT (end_ts <= :start_ts OR start_ts >= :end_ts)
            """
        )

        delete_query = text(
            f"""
            DELETE FROM {self._closures_table}
            WHERE datasource = :datasource
              AND symbol = :symbol
              AND interval = :interval
              AND NOT (end_ts <= :start_ts OR start_ts >= :end_ts)
            """
        )

        insert_query = text(
            f"""
            INSERT INTO {self._closures_table}
                (datasource, symbol, interval, start_ts, end_ts)
            VALUES (:datasource, :symbol, :interval, :start_ts, :end_ts)
            ON CONFLICT (datasource, symbol, interval, start_ts, end_ts) DO NOTHING
            """
        )

        params = {
            "datasource": self.get_datasource(),
            "symbol": ctx.symbol,
            "interval": ctx.interval,
            "start_ts": start_ts,
            "end_ts": end_ts,
        }

        try:
            with self._engine.begin() as conn:
                rows = conn.execute(overlap_query, params).fetchall()
                if rows:
                    start_ts = min(start_ts, *(pd.to_datetime(row[0], utc=True) for row in rows))
                    end_ts = max(end_ts, *(pd.to_datetime(row[1], utc=True) for row in rows))
                    conn.execute(
                        delete_query,
                        {
                            **params,
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                        },
                    )

                conn.execute(
                    insert_query,
                    {
                        **params,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                    },
                )

                logger.info(
                    "Recorded scheduled closure for %s [%s]: %s -> %s",
                    ctx.symbol,
                    ctx.interval,
                    start_ts.isoformat(),
                    end_ts.isoformat(),
                )
        except ProgrammingError as exc:
            message = str(exc).lower()
            if "does not exist" in message:
                logger.warning(
                    "Closure table '%s' missing during record; ensuring schema and retrying once.",
                    self._closures_table,
                )
                self.ensure_schema()
                try:
                    self._record_closure_range(ctx, start, end)
                except Exception:
                    logger.exception(
                        "Retry failed while recording closure for %s [%s].",
                        ctx.symbol,
                        ctx.interval,
                    )
                return
            logger.exception(
                "Failed to record closure for %s [%s]: %s",
                ctx.symbol,
                ctx.interval,
                exc,
            )
            return
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to record closure for %s [%s]: %s",
                ctx.symbol,
                ctx.interval,
                exc,
            )
            return

    def _write_dataframe(self, df: pd.DataFrame, ctx: DataContext) -> int:
        if df.empty:
            return 0

        if not self._engine:
            logger.warning("Database engine unavailable; skipping ingestion for %s [%s].", ctx.symbol, ctx.interval)
            return 0

        # Ensure the target schema exists (including new columns) before creating temp tables.
        self.ensure_schema()

        try:
            with self._engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"CREATE TEMP TABLE tmp (LIKE {self._table}) ON COMMIT DROP;"))
                    try:
                        df.to_sql("tmp", conn, if_exists="append", index=False, method="multi")
                    except Exception as exc:
                        logger.exception("Failed to write to temp table 'tmp': %s", exc)
                        raise

                    conn.execute(text(f"INSERT INTO {self._table} SELECT * FROM tmp ON CONFLICT DO NOTHING;"))

            logger.info("Ingested %d rows for %s [%s].", len(df), ctx.symbol, ctx.interval)
            return len(df)

        except SQLAlchemyError as exc:
            logger.exception("DB error during ingest for %s: %s", ctx.symbol, exc)
            raise

    @staticmethod
    def _history_segment_target() -> int:
        raw = os.getenv("HISTORY_SEGMENT_POINTS")
        if raw is None:
            return 1000
        try:
            return max(1, int(raw))
        except ValueError:
            return 1000

    @classmethod
    def _split_history_range(
        cls,
        start: pd.Timestamp,
        end: pd.Timestamp,
        interval: str,
        *,
        max_points: int = None,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        if max_points is None:
            max_points = cls._history_segment_target()

        try:
            step = cls._interval_to_timedelta(interval)
        except Exception:
            step = dt.timedelta(minutes=1)

        if step <= dt.timedelta(0):
            step = dt.timedelta(minutes=1)

        span = step * max_points
        if span <= dt.timedelta(0):
            span = dt.timedelta(minutes=1)

        segments: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        cursor = start

        while cursor < end:
            segment_end = min(cursor + span, end)
            if segment_end <= cursor:
                break
            segments.append((cursor, segment_end))
            cursor = segment_end

        if not segments:
            segments.append((start, end))

        return segments

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

        if not self._engine:
            logger.warning("Database engine unavailable; fetching OHLCV from API for %s [%s].", ctx.symbol, ctx.interval)
            return self._fetch_and_format(ctx)

        def query_ohlcv():
            try:
                query = text(f"""
                    SELECT timestamp, open, high, low, close, volume, tr, atr_wilder
                    FROM {self._table}
                    WHERE symbol = :symbol
                    AND datasource = :ds
                    AND interval = :interval
                    AND timestamp BETWEEN :start AND :end
                    ORDER BY timestamp
                """)
                return pd.read_sql(query, self._engine, params={
                    "symbol": ctx.symbol,
                    "ds": self.get_datasource(),
                    "interval": ctx.interval,
                    "start": ctx.start,
                    "end": ctx.end,
                })
            except ProgrammingError as e:
                if "does not exist" in str(e).lower():
                    logger.warning("Table '%s' does not exist. Auto-creating schema.", self._table)
                    self.ensure_schema()
                    return pd.DataFrame()
                else:
                    logger.exception("Query failed for table '%s': %s", self._table, e)
                return pd.DataFrame()
        try:
            df = query_ohlcv()
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
                df = query_ohlcv()
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
        return df

    def plot_ohlcv(self, plot_ctx: DataContext, title: str = None, **kwargs):
        df = self.get_ohlcv(plot_ctx)
        title = title or f"{plot_ctx.symbol} | {plot_ctx.interval}"
        logger.debug("df index sample: %s", df.index)
        ChartPlotter.plot_ohlc(df, title=title, ctx=plot_ctx, datasource=self.get_datasource(), **kwargs)
