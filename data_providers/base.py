from abc import ABC, abstractmethod
from enum import Enum
import datetime as dt
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from classes.Logger import logger

load_dotenv("secrets.env")


class DataSource(str, Enum):
    YFINANCE = "YFINANCE"
    ALPACA = "ALPACA"
    UNKNOWN = "UNKNOWN"


class BaseDataProvider(ABC):
    _dsn = os.getenv("PG_DSN")
    _table = os.getenv("OHLC_TABLE")
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

    def ingest_history(
        self,
        symbol: str,
        interval: str = "1d",
        days: int = 30,
        start: dt.datetime = None,
        end: dt.datetime = None
    ) -> int:
        if not start or not end:
            end = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            start = end - dt.timedelta(days=days)

        try:
            logger.debug("Fetching data for %s [%s] from %s to %s", symbol, interval, start, end)
            df = self.fetch_from_api(symbol, start, end, interval)
            if df is None or df.empty:
                logger.warning("No data returned for %s (%s).", symbol, interval)
                return 0

            df["data_ingested_ts"] = dt.datetime.now(dt.timezone.utc)
            df["datasource"] = self.get_datasource()
            df["interval"] = interval
            df["symbol"] = symbol

        except Exception as e:
            logger.exception("Data fetch failed for %s: %s", symbol, e)
            return 0

        try:
            with self._engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(f"CREATE TEMP TABLE tmp (LIKE {self._table}) ON COMMIT DROP;")
                    )
                    df.to_sql("tmp", conn, if_exists="append", index=False, method="multi")
                    conn.execute(
                        text(f"INSERT INTO {self._table} SELECT * FROM tmp ON CONFLICT DO NOTHING;")
                    )

            logger.info("Ingested %d rows for %s [%s].", len(df), symbol, interval)
            return len(df)

        except SQLAlchemyError as e:
            logger.exception("DB error during ingest_history for %s: %s", symbol, e)
            raise

    def get_ohlcv(self, symbol: str, start: str, end: str, interval: str = "1d") -> pd.DataFrame:
        query = text(f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {self._table}
            WHERE symbol = :symbol
              AND datasource = :ds
              AND interval = :interval
              AND timestamp BETWEEN :start AND :end
            ORDER BY timestamp
        """)

        df = pd.read_sql(query, self._engine, params={
            "symbol": symbol,
            "ds": self.get_datasource(),
            "interval": interval,
            "start": start,
            "end": end,
        })

        if df.empty:
            logger.warning("No rows found for %s [%s] from %s to %s", symbol, interval, start, end)
            return df

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df