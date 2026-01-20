from typing import List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from core.logger import logger
from indicators.config import DataContext
from data_providers.config.runtime import PersistenceConfig
from data_providers.utils.ohlcv import interval_to_timedelta

from ..market.stats_queue import enqueue_stats_job

class DataPersistenceService:
    """Handle storage, schema management, and closure bookkeeping for OHLCV data."""

    def __init__(self, config: PersistenceConfig, *, engine=None):
        self._config = config
        self._engine = engine or (create_engine(config.dsn) if config.dsn else None)

        if not self._engine:
            logger.warning("Database engine unavailable; persistence features disabled.")

    @property
    def config(self) -> PersistenceConfig:
        return self._config

    @property
    def engine_available(self) -> bool:
        return self._engine is not None

    def ensure_schema(self):
        """Create candle, stats, derivatives, and closure tables if they are missing."""

        if not self._engine:
            logger.warning(
                "Database engine unavailable; skipping ensure_schema call for '%s'.",
                self._config.candles_raw_table,
            )
            return

        ddl_create = f"""
        CREATE TABLE IF NOT EXISTS {self._config.candles_raw_table} (
            instrument_id TEXT NOT NULL,
            timeframe_seconds INTEGER NOT NULL,
            candle_time TIMESTAMPTZ NOT NULL,
            close_time TIMESTAMPTZ NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION,
            trade_count BIGINT,
            is_closed BOOLEAN NOT NULL DEFAULT TRUE,
            source_time TIMESTAMPTZ,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (instrument_id, timeframe_seconds, candle_time),
            CHECK (timeframe_seconds > 0),
            CHECK (close_time > candle_time),
            CHECK (high >= low),
            CHECK (low <= open AND open <= high),
            CHECK (low <= close AND close <= high),
            CHECK (volume IS NULL OR volume >= 0),
            CHECK (trade_count IS NULL OR trade_count >= 0)
        );
        """
        ddl_stats = f"""
        CREATE TABLE IF NOT EXISTS {self._config.candle_stats_table} (
            instrument_id TEXT NOT NULL,
            timeframe_seconds INTEGER NOT NULL,
            candle_time TIMESTAMPTZ NOT NULL,
            stats_version TEXT NOT NULL,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            stats JSONB NOT NULL,
            PRIMARY KEY (instrument_id, timeframe_seconds, candle_time, stats_version),
            FOREIGN KEY (instrument_id, timeframe_seconds, candle_time)
                REFERENCES {self._config.candles_raw_table} (instrument_id, timeframe_seconds, candle_time)
                ON DELETE CASCADE,
            CHECK (jsonb_typeof(stats) = 'object')
        );
        """
        ddl_regime = f"""
        CREATE TABLE IF NOT EXISTS {self._config.regime_stats_table} (
            instrument_id TEXT NOT NULL,
            timeframe_seconds INTEGER NOT NULL,
            candle_time TIMESTAMPTZ NOT NULL,
            regime_version TEXT NOT NULL,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            regime JSONB NOT NULL,
            PRIMARY KEY (instrument_id, timeframe_seconds, candle_time, regime_version),
            FOREIGN KEY (instrument_id, timeframe_seconds, candle_time)
                REFERENCES {self._config.candles_raw_table} (instrument_id, timeframe_seconds, candle_time)
                ON DELETE CASCADE,
            CHECK (jsonb_typeof(regime) = 'object')
        );
        """
        ddl_derivatives = f"""
        CREATE TABLE IF NOT EXISTS {self._config.derivatives_state_table} (
            instrument_id TEXT NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            source_time TIMESTAMPTZ,
            open_interest DOUBLE PRECISION,
            open_interest_value DOUBLE PRECISION,
            funding_rate DOUBLE PRECISION,
            funding_time TIMESTAMPTZ,
            mark_price DOUBLE PRECISION,
            index_price DOUBLE PRECISION,
            premium_rate DOUBLE PRECISION,
            premium_index DOUBLE PRECISION,
            next_funding_time TIMESTAMPTZ,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (instrument_id, observed_at),
            CHECK (open_interest IS NULL OR open_interest >= 0),
            CHECK (open_interest_value IS NULL OR open_interest_value >= 0)
        );
        """
        ddl_closures = f"""
        CREATE TABLE IF NOT EXISTS {self._config.closures_table} (
            instrument_id TEXT NOT NULL,
            timeframe_seconds INTEGER NOT NULL,
            start_ts TIMESTAMPTZ NOT NULL,
            end_ts TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (instrument_id, timeframe_seconds, start_ts, end_ts),
            CHECK (timeframe_seconds > 0),
            CHECK (end_ts > start_ts)
        );
        """
        ddl_indexes = [
            f"""
            CREATE INDEX IF NOT EXISTS idx_candles_raw_instrument_tf_time
            ON {self._config.candles_raw_table} (instrument_id, timeframe_seconds, candle_time DESC);
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_candle_stats_instrument_tf_time
            ON {self._config.candle_stats_table} (instrument_id, timeframe_seconds, candle_time DESC);
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_regime_stats_instrument_tf_time
            ON {self._config.regime_stats_table} (instrument_id, timeframe_seconds, candle_time DESC);
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_regime_stats_instrument_tf_version_time
            ON {self._config.regime_stats_table} (instrument_id, timeframe_seconds, regime_version, candle_time DESC);
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_derivatives_state_instrument_time
            ON {self._config.derivatives_state_table} (instrument_id, observed_at DESC);
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_derivatives_state_time
            ON {self._config.derivatives_state_table} (observed_at DESC);
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_candle_closures_lookup
            ON {self._config.closures_table} (instrument_id, timeframe_seconds, start_ts);
            """,
        ]

        try:
            with self._engine.begin() as conn:
                conn.execute(text(ddl_create))
                conn.execute(text(ddl_stats))
                conn.execute(text(ddl_regime))
                conn.execute(text(ddl_derivatives))
                conn.execute(text(ddl_closures))
                for ddl in ddl_indexes:
                    conn.execute(text(ddl))
            logger.info(
                "Schema ensured for tables raw=%s stats=%s regime=%s derivatives=%s closures=%s.",
                self._config.candles_raw_table,
                self._config.candle_stats_table,
                self._config.regime_stats_table,
                self._config.derivatives_state_table,
                self._config.closures_table,
            )
        except SQLAlchemyError as e:
            logger.exception(
                "Failed to ensure schema for raw=%s stats=%s regime=%s derivatives=%s closures=%s: %s",
                self._config.candles_raw_table,
                self._config.candle_stats_table,
                self._config.regime_stats_table,
                self._config.derivatives_state_table,
                self._config.closures_table,
                e,
            )
            raise

    def fetch_ohlcv(self, ctx: DataContext, datasource: str) -> pd.DataFrame:
        """Load OHLCV rows for the requested context."""

        if not self._engine:
            return pd.DataFrame()

        try:
            instrument_id, timeframe_seconds = self._resolve_context(ctx)
            query = text(
                f"""
                SELECT candle_time AS timestamp, open, high, low, close, volume, trade_count
                FROM {self._config.candles_raw_table}
                WHERE instrument_id = :instrument_id
                  AND timeframe_seconds = :timeframe_seconds
                  AND candle_time BETWEEN :start AND :end
                ORDER BY candle_time
                """
            )
            return pd.read_sql(
                query,
                self._engine,
                params={
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "start": ctx.start,
                    "end": ctx.end,
                },
            )
        except ProgrammingError as exc:
            if "does not exist" in str(exc).lower():
                logger.warning("Table '%s' missing. Ensuring schema.", self._config.candles_raw_table)
                self.ensure_schema()
                return pd.DataFrame()
            logger.exception("Query failed for table '%s': %s", self._config.candles_raw_table, exc)
            return pd.DataFrame()
        except SQLAlchemyError as exc:
            logger.exception("Database error during OHLCV query: %s", exc)
            raise

    def load_closure_ranges(
        self,
        ctx: DataContext,
        datasource: str,
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """Retrieve cached closure windows for the requested series."""

        if not self._engine:
            return []

        instrument_id, timeframe_seconds = self._resolve_context(ctx)
        query = text(
            f"""
            SELECT start_ts, end_ts
            FROM {self._config.closures_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND NOT (end_ts <= :request_start OR start_ts >= :request_end)
            ORDER BY start_ts
            """
        )

        try:
            with self._engine.begin() as conn:
                rows = conn.execute(
                    query,
                    {
                        "instrument_id": instrument_id,
                        "timeframe_seconds": timeframe_seconds,
                        "request_start": requested_start,
                        "request_end": requested_end,
                    },
                ).fetchall()
        except ProgrammingError as exc:
            message = str(exc).lower()
            if "does not exist" in message:
                logger.warning(
                    "Closure table '%s' missing. Ensuring schema before retry.",
                    self._config.closures_table,
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

    def record_closure_range(
        self,
        ctx: DataContext,
        datasource: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ):
        """Persist a window indicating upstream returned no data."""

        if not self._engine or end <= start:
            return

        instrument_id, timeframe_seconds = self._resolve_context(ctx)
        start_ts = pd.to_datetime(start, utc=True)
        end_ts = pd.to_datetime(end, utc=True)

        overlap_query = text(
            f"""
            SELECT start_ts, end_ts FROM {self._config.closures_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND NOT (end_ts <= :start_ts OR start_ts >= :end_ts)
            """
        )

        delete_query = text(
            f"""
            DELETE FROM {self._config.closures_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND NOT (end_ts <= :start_ts OR start_ts >= :end_ts)
            """
        )

        insert_query = text(
            f"""
            INSERT INTO {self._config.closures_table}
                (instrument_id, timeframe_seconds, start_ts, end_ts)
            VALUES (:instrument_id, :timeframe_seconds, :start_ts, :end_ts)
            ON CONFLICT (instrument_id, timeframe_seconds, start_ts, end_ts) DO NOTHING
            """
        )

        params = {
            "instrument_id": instrument_id,
            "timeframe_seconds": timeframe_seconds,
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
                    instrument_id,
                    timeframe_seconds,
                    start_ts.isoformat(),
                    end_ts.isoformat(),
                )
        except ProgrammingError as exc:
            message = str(exc).lower()
            if "does not exist" in message:
                logger.warning(
                    "Closure table '%s' missing during record; ensuring schema and retrying once.",
                    self._config.closures_table,
                )
                self.ensure_schema()
                try:
                    self.record_closure_range(ctx, datasource, start, end)
                except Exception:
                    logger.exception(
                        "Retry failed while recording closure for %s [%s].",
                        instrument_id,
                        timeframe_seconds,
                    )
                return
            logger.exception(
                "Failed to record closure for %s [%s]: %s",
                instrument_id,
                timeframe_seconds,
                exc,
            )
            return
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to record closure for %s [%s]: %s",
                instrument_id,
                timeframe_seconds,
                exc,
            )
            return

    def write_dataframe(self, df: pd.DataFrame, ctx: DataContext) -> int:
        """Write a prepared OHLCV dataframe into the persistence layer."""

        if df.empty:
            return 0

        if not self._engine:
            logger.warning(
                "Database engine unavailable; skipping ingestion for %s [%s].",
                ctx.symbol,
                ctx.interval,
            )
            return 0

        self.ensure_schema()

        try:
            instrument_id, timeframe_seconds = self._resolve_context(ctx)
            if "timestamp" not in df.columns:
                raise ValueError("Input dataframe missing 'timestamp' column for candle_time mapping.")
            candle_time = pd.to_datetime(df["timestamp"], utc=True)
            close_time = candle_time + interval_to_timedelta(ctx.interval)
            prepared = pd.DataFrame(
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "candle_time": candle_time,
                    "close_time": close_time,
                    "open": df.get("open"),
                    "high": df.get("high"),
                    "low": df.get("low"),
                    "close": df.get("close"),
                    "volume": df.get("volume"),
                    "trade_count": df.get("trade_count"),
                    "is_closed": True,
                    "source_time": df.get("source_time"),
                }
            )
            with self._engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(
                            f"CREATE TEMP TABLE tmp (LIKE {self._config.candles_raw_table} INCLUDING DEFAULTS) ON COMMIT DROP;"
                        )
                    )
                    try:
                        prepared.to_sql("tmp", conn, if_exists="append", index=False, method="multi")
                    except Exception as exc:
                        logger.exception("Failed to write to temp table 'tmp': %s", exc)
                        raise

                    conn.execute(
                        text(
                            f"INSERT INTO {self._config.candles_raw_table} "
                            f"SELECT * FROM tmp ON CONFLICT DO NOTHING;"
                        )
                    )

            logger.info(
                "Ingested %d rows for %s [%s].",
                len(prepared),
                instrument_id,
                timeframe_seconds,
            )
            logger.debug(
                "candle_ingest_range | instrument_id=%s timeframe_seconds=%s time_min=%s time_max=%s",
                instrument_id,
                timeframe_seconds,
                candle_time.min().isoformat(),
                candle_time.max().isoformat(),
            )
            enqueue_stats_job(
                instrument_id=instrument_id,
                timeframe_seconds=timeframe_seconds,
                time_min=candle_time.min(),
                time_max=candle_time.max(),
            )
            return len(prepared)

        except SQLAlchemyError as exc:
            logger.exception("DB error during ingest for %s: %s", ctx.symbol, exc)
            raise

    def _resolve_context(self, ctx: DataContext) -> Tuple[str, int]:
        if not ctx.instrument_id:
            raise ValueError("instrument_id is required for candle persistence operations.")
        timeframe = interval_to_timedelta(ctx.interval)
        timeframe_seconds = int(timeframe.total_seconds())
        if timeframe_seconds <= 0:
            raise ValueError(f"Invalid timeframe interval: {ctx.interval}")
        return ctx.instrument_id, timeframe_seconds


__all__ = ["DataPersistenceService"]
