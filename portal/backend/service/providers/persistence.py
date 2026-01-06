from typing import List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from core.logger import logger
from indicators.config import DataContext
from data_providers.config.runtime import PersistenceConfig


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
        """Create OHLCV and closure tables if they are missing."""

        if not self._engine:
            logger.warning(
                "Database engine unavailable; skipping ensure_schema call for '%s'.",
                self._config.ohlc_table,
            )
            return

        ddl_create = f"""
        CREATE TABLE IF NOT EXISTS {self._config.ohlc_table} (
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
        ALTER TABLE {self._config.ohlc_table}
            ADD COLUMN IF NOT EXISTS tr DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS atr_wilder DOUBLE PRECISION;
        """
        ddl_hypertable = f"""
        SELECT create_hypertable('{self._config.ohlc_table}', 'timestamp', if_not_exists => TRUE);
        """

        ddl_closures = f"""
        CREATE TABLE IF NOT EXISTS {self._config.closures_table} (
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
            logger.info("Schema ensured for table '%s'.", self._config.ohlc_table)
        except SQLAlchemyError as e:
            logger.exception("Failed to ensure schema for '%s': %s", self._config.ohlc_table, e)
            raise

    def fetch_ohlcv(self, ctx: DataContext, datasource: str) -> pd.DataFrame:
        """Load OHLCV rows for the requested context."""

        if not self._engine:
            return pd.DataFrame()

        try:
            query = text(
                f"""
                SELECT timestamp, open, high, low, close, volume, tr, atr_wilder
                FROM {self._config.ohlc_table}
                WHERE symbol = :symbol
                AND datasource = :ds
                AND interval = :interval
                AND timestamp BETWEEN :start AND :end
                ORDER BY timestamp
                """
            )
            return pd.read_sql(
                query,
                self._engine,
                params={
                    "symbol": ctx.symbol,
                    "ds": datasource,
                    "interval": ctx.interval,
                    "start": ctx.start,
                    "end": ctx.end,
                },
            )
        except ProgrammingError as exc:
            if "does not exist" in str(exc).lower():
                logger.warning("Table '%s' missing. Ensuring schema.", self._config.ohlc_table)
                self.ensure_schema()
                return pd.DataFrame()
            logger.exception("Query failed for table '%s': %s", self._config.ohlc_table, exc)
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

        query = text(
            f"""
            SELECT start_ts, end_ts
            FROM {self._config.closures_table}
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
                        "datasource": datasource,
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

        start_ts = pd.to_datetime(start, utc=True)
        end_ts = pd.to_datetime(end, utc=True)

        overlap_query = text(
            f"""
            SELECT start_ts, end_ts FROM {self._config.closures_table}
            WHERE datasource = :datasource
              AND symbol = :symbol
              AND interval = :interval
              AND NOT (end_ts <= :start_ts OR start_ts >= :end_ts)
            """
        )

        delete_query = text(
            f"""
            DELETE FROM {self._config.closures_table}
            WHERE datasource = :datasource
              AND symbol = :symbol
              AND interval = :interval
              AND NOT (end_ts <= :start_ts OR start_ts >= :end_ts)
            """
        )

        insert_query = text(
            f"""
            INSERT INTO {self._config.closures_table}
                (datasource, symbol, interval, start_ts, end_ts)
            VALUES (:datasource, :symbol, :interval, :start_ts, :end_ts)
            ON CONFLICT (datasource, symbol, interval, start_ts, end_ts) DO NOTHING
            """
        )

        params = {
            "datasource": datasource,
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
                    self._config.closures_table,
                )
                self.ensure_schema()
                try:
                    self.record_closure_range(ctx, datasource, start, end)
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
            with self._engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"CREATE TEMP TABLE tmp (LIKE {self._config.ohlc_table}) ON COMMIT DROP;"))
                    try:
                        df.to_sql("tmp", conn, if_exists="append", index=False, method="multi")
                    except Exception as exc:
                        logger.exception("Failed to write to temp table 'tmp': %s", exc)
                        raise

                    conn.execute(text(f"INSERT INTO {self._config.ohlc_table} SELECT * FROM tmp ON CONFLICT DO NOTHING;"))

            logger.info("Ingested %d rows for %s [%s].", len(df), ctx.symbol, ctx.interval)
            return len(df)

        except SQLAlchemyError as exc:
            logger.exception("DB error during ingest for %s: %s", ctx.symbol, exc)
            raise


__all__ = ["DataPersistenceService"]
