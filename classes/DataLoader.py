import datetime as dt
import os
from typing import Literal

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from classes.Logger import logger

TS_TZ = "UTC"  # store everything in UTC
DEFAULT_DSN = os.environ.get(
    "PG_DSN",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres",
)


class DataLoader:
    """Static helper – all methods are classmethods."""

    _dsn = DEFAULT_DSN
    _table = "ohlc_raw"
    _engine = create_engine(_dsn)

    @classmethod
    def ensure_schema(cls):
        """Create hypertable if it doesn't yet exist."""
        logger.debug("Connected to DB: %s", cls._engine.url)
        ddl_create = f"""
        CREATE TABLE IF NOT EXISTS {cls._table} (
            datasource TEXT NOT NULL,
            symbol TEXT NOT NULL,
            ts TIMESTAMPTZ NOT NULL,
            open  DOUBLE PRECISION,
            high  DOUBLE PRECISION,
            low   DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            data_ingested_ts TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (symbol, ts, datasource)
        );
        """
        ddl_hypertable= f"""
        SELECT create_hypertable('{cls._table}', 'ts', if_not_exists => TRUE);
        """
        try:
            with cls._engine.begin() as conn:
                conn.execute(text(ddl_create))
                conn.execute(text(ddl_hypertable))
            logger.info("Schema ensured for table '%s'.", cls._table)
        except SQLAlchemyError as e:
            logger.exception("Failed to ensure schema for '%s': %s", cls._table, e)
            raise

    @classmethod
    def ingest_history(
        cls,
        symbol: str,
        provider,
        days: int = 365,
        interval: str = "1d"
    ) -> int:
        """
        Ingest OHLCV history using the specified data provider.
        """
        end = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        start = end - dt.timedelta(days=days)

        try:
            df = provider.get_ohlcv(symbol, start, end, interval)
            df["data_ingested_ts"] = dt.datetime.now(dt.timezone.utc)
            df["datasource"] = provider.get_datasource()

        except Exception as e:
            logger.exception("Data fetch failed for %s: %s", symbol, e)
            return 0

        if df is None or df.empty:
            logger.warning("No data returned for %s – check symbol/interval.", symbol)
            return 0

        try:
            with cls._engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(f"CREATE TEMP TABLE tmp (LIKE {cls._table}) ON COMMIT DROP;")
                    )
                    df.to_sql("tmp", conn, if_exists="append", index=False, method="multi")
                    conn.execute(
                        text(f"INSERT INTO {cls._table} SELECT * FROM tmp ON CONFLICT DO NOTHING;")
                    )

            logger.info("Ingested %d rows for %s.", len(df), symbol)
            return len(df)

        except SQLAlchemyError as e:
            logger.exception("Database error during ingest_history for %s: %s", symbol, e)
            raise
        except Exception as e:
            logger.exception("Unexpected error during ingest_history for %s: %s", symbol, e)
            raise

    @classmethod
    def get(
        cls,
        symbol: str,
        tf: Literal["1h", "4h", "30min", "15min", "1d"] = "4h",
        lookback_days: int = 365,
    ) -> pd.DataFrame:
        """Return resampled OHLCV DataFrame indexed by bucket timestamp."""
        bucket = tf
        sql = f"""
            SELECT time_bucket(:bucket, ts) AS bucket,
                   first(open, ts)  AS open,
                   max(high)        AS high,
                   min(low)         AS low,
                   last(close, ts)  AS close,
                   sum(volume)      AS volume
            FROM {cls._table}
            WHERE symbol = :symbol
              AND ts >= NOW() - INTERVAL :interval_param
            GROUP BY bucket
            ORDER BY bucket;
        """
        interval_param = f"{lookback_days} days"

        try:
            with cls._engine.connect() as conn:
                df = pd.read_sql(
                    text(sql),
                    conn,
                    params={
                        "bucket": bucket,
                        "symbol": symbol,
                        "interval_param": interval_param
                    }
                )
        except SQLAlchemyError as e:
            logger.exception("Database error in get() for %s: %s", symbol, e)
            raise

        if df.empty:
            msg = f"No data found for {symbol} in the last {lookback_days} days."
            logger.error(msg)
            raise ValueError(msg)

        df.set_index("bucket", inplace=True)
        df.index = pd.to_datetime(df.index, utc=True)
        df.rename(columns=str.title, inplace=True)  # match previous naming
        logger.info("Retrieved %d rows for %s [%s].", len(df), symbol, tf)
        return df
