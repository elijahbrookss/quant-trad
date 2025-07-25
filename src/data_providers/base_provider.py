from abc import ABC, abstractmethod
from enum import Enum
import datetime as dt
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError
from core.logger import logger
from indicators.config import DataContext
from core.chart_plotter import ChartPlotter

load_dotenv("secrets.env")


class DataSource(str, Enum):
    YFINANCE = "YFINANCE"
    ALPACA = "ALPACA"
    UNKNOWN = "UNKNOWN"


class BaseDataProvider(ABC):
    _dsn = os.getenv("PG_DSN")
    _table = os.getenv("OHLC_TABLE")

    if not _dsn:
        raise ValueError("PG_DSN is not defined. Check your .env or environment variables.")

    _engine = create_engine(_dsn)

    @abstractmethod
    def get_datasource(self) -> str:
        pass

    @abstractmethod
    def fetch_from_api(self, symbol: str, start: dt.datetime, end: dt.datetime, interval: str) -> pd.DataFrame:
        pass

    def ensure_schema(self):
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
            data_ingested_ts TIMESTAMPTZ DEFAULT now(),
            interval TEXT NOT NULL,
            PRIMARY KEY (symbol, timestamp, datasource, interval)
        );
        """
        ddl_hypertable = f"""
        SELECT create_hypertable('{self._table}', 'timestamp', if_not_exists => TRUE);
        """

        try:
            with self._engine.begin() as conn:
                conn.execute(text(ddl_create))
                conn.execute(text(ddl_hypertable))
            logger.info("Schema ensured for table '%s'.", self._table)
        except SQLAlchemyError as e:
            logger.exception("Failed to ensure schema for '%s': %s", self._table, e)
            raise

    def ingest_history(self, ctx: DataContext, days: int = 30) -> int:
        start = ctx.start
        end = ctx.end

        if not start or not end:
            end_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            start_dt = end_dt - dt.timedelta(days=days)
            ctx.start = start_dt.isoformat()
            ctx.end = end_dt.isoformat()

        try:
            logger.debug("Fetching data for %s [%s] from %s to %s", ctx.symbol, ctx.interval, ctx.start, ctx.end)
            df = self.fetch_from_api(ctx.symbol, ctx.start, ctx.end, ctx.interval)

            if df is None or df.empty:
                logger.warning("No data returned for %s (%s).", ctx.symbol, ctx.interval)
                return 0

            df["data_ingested_ts"] = dt.datetime.now(dt.timezone.utc)
            df["datasource"] = self.get_datasource()
            df["interval"] = ctx.interval
            df["symbol"] = ctx.symbol

        except Exception as e:
            logger.exception("Data fetch failed for %s: %s", ctx.symbol, e)
            return 0

        try:
            with self._engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"CREATE TEMP TABLE tmp (LIKE {self._table}) ON COMMIT DROP;"))
                    try:
                        df.to_sql("tmp", conn, if_exists="append", index=False, method="multi")
                    except Exception as e:
                        logger.exception("Failed to write to temp table 'tmp': %s", e)
                        raise

                    conn.execute(text(f"INSERT INTO {self._table} SELECT * FROM tmp ON CONFLICT DO NOTHING;"))

            logger.info("Ingested %d rows for %s [%s].", len(df), ctx.symbol, ctx.interval)
            return len(df)

        except SQLAlchemyError as e:
            logger.exception("DB error during ingest_history for %s: %s", ctx.symbol, e)
            raise

    def get_ohlcv(self, ctx: DataContext) -> pd.DataFrame:
        ctx.validate()

        def query_ohlcv():
            try:
                query = text(f"""
                    SELECT timestamp, open, high, low, close, volume
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
            except SQLAlchemyError as e:
                logger.exception("Database error during OHLCV query: %s", e)
                return pd.DataFrame()

        df = query_ohlcv()

        if df.empty:
            logger.warning("No rows found for %s [%s] from %s to %s. Attempting auto-ingestion...",
                           ctx.symbol, ctx.interval, ctx.start, ctx.end)
            try:
                self.ingest_history(ctx)
                df = query_ohlcv()
                if df.empty:
                    logger.error("Auto-ingestion attempted but still no data found.")
                    return df
            except Exception as e:
                logger.exception("Auto-ingestion failed: %s", e)
                return df

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
        df["timestamp"] = df.index
        return df

    def plot_ohlcv(self, plot_ctx: DataContext, title: str = None, **kwargs):
        df = self.get_ohlcv(plot_ctx)
        title = title or f"{plot_ctx.symbol} | {plot_ctx.interval}"
        logger.debug("df index sample: %s", df.index)
        ChartPlotter.plot_ohlc(df, title=title, ctx=plot_ctx, datasource=self.get_datasource(), **kwargs)
