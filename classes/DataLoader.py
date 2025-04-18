import datetime as dt
import os
from typing import Literal, Optional

import pandas as pd
import psycopg2
import yfinance as yf

TS_TZ = "UTC"  # store everything in UTC
DEFAULT_DSN = os.environ.get(
    "PG_DSN",
    "dbname=postgres user=postgres password=postgres host=localhost port=5432",
)


class DataLoader:
    """Static helper – all methods are classmethods."""

    _dsn = DEFAULT_DSN
    _table = "ohlc_raw"

    # --------------------------------------------------------------
    @classmethod
    def _conn(cls):
        return psycopg2.connect(cls._dsn)

    # --------------------------------------------------------------
    @classmethod
    def ensure_schema(cls):
        """Create hypertable if it doesn't yet exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {cls._table} (
            symbol TEXT NOT NULL,
            ts TIMESTAMPTZ NOT NULL,
            open  DOUBLE PRECISION,
            high  DOUBLE PRECISION,
            low   DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            PRIMARY KEY (symbol, ts)
        );
        SELECT create_hypertable('{cls._table}', 'ts', if_not_exists => TRUE);
        """
        with cls._conn() as conn, conn.cursor() as cur:
            cur.execute(ddl)
            conn.commit()

    # --------------------------------------------------------------
    @classmethod
    def ingest_history(cls, symbol: str, days: int = 365, interval: str = "1m"):
        """Pull yfinance candles and upsert into the hypertable."""
        end = dt.datetime.utcnow().replace(microsecond=0, tzinfo=dt.timezone.utc)
        start = end - dt.timedelta(days=days)
        df = yf.download(
            symbol,
            start=start,
            end=end,
            interval=interval,
            progress=False,
            threads=False,
        ).tz_convert(None)  # make naive UTC
        if df.empty:
            print("No data downloaded – check symbol / interval.")
            return 0
        df.reset_index(inplace=True)
        df.rename(columns={
            "Datetime": "ts",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }, inplace=True)
        df["symbol"] = symbol
        cols = ["symbol", "ts", "open", "high", "low", "close", "volume"]
        # Upsert via COPY to temp table then INSERT ... ON CONFLICT DO NOTHING
        with cls._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE tmp (LIKE " + cls._table + ") ON COMMIT DROP;")
                # psycopg2 fast copy
                from io import StringIO

                buf = StringIO()
                df[cols].to_csv(buf, index=False, header=False)
                buf.seek(0)
                cur.copy_from(buf, "tmp", sep=",")
                cur.execute(
                    f"INSERT INTO {cls._table} SELECT * FROM tmp ON CONFLICT DO NOTHING;"
                )
            conn.commit()
        return len(df)

    # --------------------------------------------------------------
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
            SELECT time_bucket(%s, ts) AS bucket,
                   first(open, ts)  AS open,
                   max(high)        AS high,
                   min(low)         AS low,
                   last(close, ts)  AS close,
                   sum(volume)      AS volume
            FROM {cls._table}
            WHERE symbol = %s
              AND ts >= NOW() - INTERVAL %s
            GROUP BY bucket
            ORDER BY bucket;
        """
        interval_param = f"{lookback_days} days"
        with cls._conn() as conn:
            df = pd.read_sql(sql, conn, params=[bucket, symbol, interval_param])
        if df.empty:
            raise ValueError("No data found – did you ingest the symbol?")
        df.set_index("bucket", inplace=True)
        df.index = pd.to_datetime(df.index, utc=True)
        df.rename(columns=str.title, inplace=True)  # match previous naming
        return df
