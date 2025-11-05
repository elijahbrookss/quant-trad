import os
import math
import datetime as dt
from typing import Optional, Tuple, Union

import pandas as pd
import ccxt

from core.logger import logger
from .base_provider import BaseDataProvider


class CCXTProvider(BaseDataProvider):
    """Data provider that fetches OHLCV candles via CCXT exchanges."""

    def __init__(self, exchange_id: str, *, sandbox: Optional[bool] = None):
        if not exchange_id:
            raise ValueError("exchange_id is required for CCXTProvider")

        self._exchange_id = exchange_id.lower()
        self._sandbox = sandbox if sandbox is not None else self._sandbox_flag()
        self._exchange = self._build_exchange()

    def get_datasource(self) -> str:
        # Store the actual exchange identifier in the datasource column so downstream
        # consumers can differentiate between venues.
        return self._exchange_id.upper()

    def _sandbox_flag(self) -> bool:
        flag = os.getenv("CCXT_SANDBOX_MODE", "false").strip().lower()
        return flag in {"1", "true", "yes", "on"}

    def _resolve_credentials(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        upper = self._exchange_id.upper()
        prefix = f"CCXT_{upper}_"

        api_key = os.getenv(prefix + "API_KEY") or os.getenv("CCXT_API_KEY")
        secret = os.getenv(prefix + "API_SECRET") or os.getenv("CCXT_API_SECRET") or os.getenv("CCXT_SECRET")
        password = os.getenv(prefix + "API_PASSWORD") or os.getenv("CCXT_PASSWORD")

        return api_key, secret, password

    def _build_exchange(self):
        if not hasattr(ccxt, self._exchange_id):
            raise ValueError(f"Unsupported CCXT exchange: {self._exchange_id}")

        exchange_cls = getattr(ccxt, self._exchange_id)
        exchange = exchange_cls({"enableRateLimit": True})

        api_key, secret, password = self._resolve_credentials()
        if api_key:
            exchange.apiKey = api_key
        if secret:
            exchange.secret = secret
        if password:
            exchange.password = password

        if self._sandbox and hasattr(exchange, "set_sandbox_mode"):
            try:
                exchange.set_sandbox_mode(True)
            except Exception as exc:  # pragma: no cover - best effort sandbox flag
                logger.warning("Failed to enable sandbox mode for %s: %s", self._exchange_id, exc)

        return exchange

    @staticmethod
    def _parse_datetime(value: Union[dt.datetime, str]) -> pd.Timestamp:
        ts = pd.to_datetime(value, utc=True)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts

    @staticmethod
    def _timeframe_to_seconds(timeframe: str) -> int:
        unit = timeframe.lower()
        if unit.endswith("ms"):
            return max(1, int(unit[:-2])) / 1000
        if unit.endswith("s"):
            return max(1, int(unit[:-1]))
        if unit.endswith("m"):
            return max(1, int(unit[:-1])) * 60
        if unit.endswith("h"):
            return max(1, int(unit[:-1])) * 60 * 60
        if unit.endswith("d"):
            return max(1, int(unit[:-1])) * 24 * 60 * 60
        if unit.endswith("w"):
            return max(1, int(unit[:-1])) * 7 * 24 * 60 * 60
        if unit.endswith("mo"):
            return max(1, int(unit[:-2])) * 30 * 24 * 60 * 60
        if unit.endswith("y"):
            return max(1, int(unit[:-1])) * 365 * 24 * 60 * 60
        raise ValueError(f"Unsupported CCXT timeframe: {timeframe}")

    def fetch_from_api(
        self,
        symbol: str,
        start: Union[dt.datetime, str],
        end: Union[dt.datetime, str],
        interval: str,
    ) -> pd.DataFrame:
        if not symbol:
            raise ValueError("symbol is required for CCXT fetch")

        start_ts = self._parse_datetime(start)
        end_ts = self._parse_datetime(end)
        if end_ts <= start_ts:
            raise ValueError("end must be after start for CCXT fetch")

        seconds = self._timeframe_to_seconds(interval)
        since_ms = int(start_ts.timestamp() * 1000)
        until_ms = int(end_ts.timestamp() * 1000)
        step_ms = max(int(seconds * 1000), 1)

        rows = []
        next_since = since_ms
        iteration_cap = 200
        max_iterations = iteration_cap

        while next_since < until_ms and max_iterations > 0:
            remaining_ms = until_ms - next_since
            estimated_bars = math.ceil(remaining_ms / step_ms) + 2
            limit = max(1, min(estimated_bars, 1500))

            try:
                batch = self._exchange.fetch_ohlcv(
                    symbol,
                    timeframe=interval,
                    since=next_since,
                    limit=limit,
                )
            except Exception as exc:  # pragma: no cover - network interaction
                raise RuntimeError(f"CCXT fetch failed for {self._exchange_id}:{symbol} -> {exc}") from exc

            batch_count = len(batch)
            batch_start = pd.to_datetime(next_since, unit="ms", utc=True)
            if not batch_count:
                logger.info(
                    "CCXT %s fetch returned no data for %s [%s] starting %s; stopping.",
                    self._exchange_id,
                    symbol,
                    interval,
                    batch_start.isoformat(),
                )
                break

            rows.extend(batch)

            last_ts = batch[-1][0]
            if last_ts is None:
                logger.warning(
                    "CCXT %s fetch produced a batch without timestamps for %s [%s]; stopping.",
                    self._exchange_id,
                    symbol,
                    interval,
                )
                break

            last_dt = pd.to_datetime(last_ts, unit="ms", utc=True)

            # Advance the cursor to avoid duplicate bars; CCXT `since` is inclusive.
            next_since = max(last_ts + step_ms, next_since + step_ms)
            max_iterations -= 1

            reached_end = next_since >= until_ms or last_ts >= until_ms
            logger.info(
                "CCXT %s fetched %d candles for %s [%s] from %s to %s (limit=%d).%s",
                self._exchange_id,
                batch_count,
                symbol,
                interval,
                batch_start.isoformat(),
                last_dt.isoformat(),
                limit,
                " Reached requested end; stopping." if reached_end else " Continuing pagination.",
            )

            if reached_end:
                break

        if max_iterations == 0 and next_since < until_ms:
            logger.warning(
                "CCXT %s pagination stopped after %d iterations before reaching %s for %s [%s].",
                self._exchange_id,
                iteration_cap,
                pd.to_datetime(until_ms, unit="ms", utc=True).isoformat(),
                symbol,
                interval,
            )

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df.drop_duplicates(subset="timestamp", keep="last", inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df[(df["timestamp"] >= start_ts) & (df["timestamp"] <= end_ts)]

        # Align with downstream expectations
        return df[["timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
