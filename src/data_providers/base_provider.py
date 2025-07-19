
# BaseDataProvider defines the common interface and database logic for all data providers in quant-trad.
# It handles schema management, ingestion, and querying of OHLCV data from a unified database table.
# Concrete providers (e.g., YahooFinanceProvider, AlpacaProvider) inherit from this class and implement
# the fetch_from_api method to retrieve data from their respective sources. This ensures all market data
# is standardized and accessible for backtesting, analysis, and plotting within the project.

from abc import ABC, abstractmethod
from enum import Enum
import datetime as dt
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from classes.Logger import logger
from classes.indicators.config import DataContext
from classes.ChartPlotter import ChartPlotter

load_dotenv("secrets.env")



# Enum for supported data sources. Used to tag and distinguish data in the database.
class DataSource(str, Enum):
    YFINANCE = "YFINANCE"
    ALPACA = "ALPACA"
    UNKNOWN = "UNKNOWN"



# Abstract base class for all data providers. Handles DB connection and schema logic.
class BaseDataProvider(ABC):
    _dsn = os.getenv("PG_DSN")
    _table = os.getenv("OHLC_TABLE")
    _engine = create_engine(_dsn)


    @abstractmethod
    def get_datasource(self) -> str:
        """
        Return the identifier for the data source (e.g., 'YFINANCE', 'ALPACA').
        Used to tag data in the database and distinguish providers.
        """
        pass

    @abstractmethod
    def fetch_from_api(self, symbol: str, start: dt.datetime, end: dt.datetime, interval: str) -> pd.DataFrame:
        """
        Fetch historical OHLCV data from the provider's API.
        Must be implemented by subclasses for each data source.
        """
        pass


    def ensure_schema(self):
        """
        Ensure the OHLCV table exists in the database and is a TimescaleDB hypertable.
        This enables efficient time-series storage and querying for all providers.
        """
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


    def ingest_history(
        self,
        ctx: DataContext,
        days: int = 30
    ) -> int:
        """
        Ingest historical OHLCV data for a symbol and interval into the database.
        If start/end are missing, defaults to the last N days. Fetches data from the provider's API,
        tags it, and inserts it into the OHLCV table. Handles DB errors and logs ingestion status.
        """
        # Fill in start/end if not defined in DataContext
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

            # Tag data with ingestion timestamp, datasource, interval, and symbol
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
                    # Use a temp table for bulk insert, then merge into main table
                    conn.execute(
                        text(f"CREATE TEMP TABLE tmp (LIKE {self._table}) ON COMMIT DROP;")
                    )
                    df.to_sql("tmp", conn, if_exists="append", index=False, method="multi")
                    conn.execute(
                        text(f"INSERT INTO {self._table} SELECT * FROM tmp ON CONFLICT DO NOTHING;")
                    )

            logger.info("Ingested %d rows for %s [%s].", len(df), ctx.symbol, ctx.interval)
            return len(df)

        except SQLAlchemyError as e:
            logger.exception("DB error during ingest_history for %s: %s", ctx.symbol, e)
            raise



    def get_ohlcv(self, ctx: DataContext) -> pd.DataFrame:
        """
        Query OHLCV data for a symbol/interval from the database.
        If no data is found, automatically triggers ingestion from the provider's API.
        Returns a DataFrame indexed by timestamp for downstream analysis and plotting.
        """
        ctx.validate()

        def query_ohlcv():
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

        df = query_ohlcv()

        if df.empty:
            # If no data is found, try to ingest from API and re-query
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

        # Ensure timestamp is timezone-aware and set as index
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
        df["timestamp"] = df.index
        return df
    

    def plot_ohlcv(self, plot_ctx: DataContext, title: str = None, **kwargs):
        """
        Plot OHLCV data for a symbol/interval using the ChartPlotter utility.
        Fetches data from the database and passes it to the plotting engine for visualization.
        """
        df = self.get_ohlcv(plot_ctx)
        title = title or f"{plot_ctx.symbol} | {plot_ctx.interval}"
        logger.debug("df index sample: %s", df.index)
        ChartPlotter.plot_ohlc(df, title=title, ctx=plot_ctx, datasource=self.get_datasource(), **kwargs)