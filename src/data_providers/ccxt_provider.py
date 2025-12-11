import inspect
import os
import math
import datetime as dt
from typing import Optional, Tuple, Union, Dict, Any

import pandas as pd
import ccxt

from core.logger import logger
from .base_provider import BaseDataProvider, InstrumentMetadata, InstrumentType


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

    def _load_market(self, symbol: str) -> Dict[str, Any]:
        """Return the CCXT market metadata for *symbol* if available."""

        try:
            if not getattr(self._exchange, "markets", None):
                self._exchange.load_markets()
            market = self._exchange.market(symbol)
            return market or {}
        except Exception as exc:  # pragma: no cover - network interaction
            logger.warning("ccxt_market_load_failed | exchange=%s | symbol=%s | error=%s", self._exchange_id, symbol, exc)
            return {}

    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Identify spot vs. derivatives using CCXT market flags."""

        market = self._load_market(symbol)
        market_type = str(market.get("type", "")).lower()

        if market.get("contract") or market.get("future") or market.get("swap"):
            return InstrumentType.FUTURE

        if market_type in {"future", "swap"}:
            return InstrumentType.FUTURE

        if market.get("spot") is False and market_type:
            return InstrumentType.FUTURE

        return InstrumentType.SPOT

    def validate_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Raise if the market is missing and return the resolved type."""

        market = self._load_market(symbol)
        if not market:
            raise ValueError(f"Symbol '{symbol}' not found on {self._exchange_id}")

        return self.get_instrument_type(venue, symbol)

    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        """Return tick/contract details derived from CCXT market metadata."""

        market = self._load_market(symbol)
        instrument_type = self.get_instrument_type(venue, symbol) if market else InstrumentType.SPOT

        precision = market.get("precision") or {}
        limits = market.get("limits") or {}
        price_limits = limits.get("price") or {}

        tick_size = (
            market.get("tickSize")
            or price_limits.get("min")
            or precision.get("price")
        )
        tick_value = market.get("tickValue")

        contract_size = market.get("contractSize")
        if contract_size is None and instrument_type == InstrumentType.SPOT:
            contract_size = 1.0

        return self._normalize_metadata(
            tick_size=tick_size,
            contract_size=contract_size,
            tick_value=tick_value,
        )

    def validate_symbol(self, venue: str, symbol: str) -> None:
        """Ensure CCXT has metadata for the requested symbol."""

        if not symbol:
            raise ValueError("symbol is required for CCXT validation")

        market = self._load_market(symbol)
        if not market:
            raise ValueError(f"Symbol '{symbol}' not found on {self._exchange_id}")

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

    def _resolve_ohlcv_limit(self) -> int:
        env_value = os.getenv("CCXT_OHLCV_LIMIT")
        if env_value is not None:
            try:
                return max(1, int(env_value))
            except ValueError:
                logger.warning(
                    "Invalid CCXT_OHLCV_LIMIT value '%s'; falling back to default.",
                    env_value,
                )

        candidates = []
        for attr in ("limit", "maxLimit", "defaultLimit"):
            value = getattr(self._exchange, attr, None)
            if isinstance(value, int) and value > 0:
                candidates.append(value)

        options = getattr(self._exchange, "options", {}) or {}
        for attr in ("limit", "maxLimit", "defaultLimit"):
            value = options.get(attr)
            if isinstance(value, int) and value > 0:
                candidates.append(value)

        ohlcv_opts = options.get("OHLCV") or options.get("ohlcv") or {}
        for attr in ("max", "maxLimit", "limit", "defaultLimit"):
            value = ohlcv_opts.get(attr)
            if isinstance(value, int) and value > 0:
                candidates.append(value)

        if candidates:
            return max(1, min(max(candidates), 5000))

        return 1000

    def _resolve_end_param(self) -> Optional[str]:
        mapping = {
            "binance": "endTime",
            "binanceus": "endTime",
            "binanceusdm": "endTime",
            "binancecoinm": "endTime",
        }
        return mapping.get(self._exchange_id)

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

        batches = []
        cursor = since_ms
        previous_last = None
        limit_hint = self._resolve_ohlcv_limit()
        end_param = self._resolve_end_param()
        supports_params = False
        try:
            signature = inspect.signature(self._exchange.fetch_ohlcv)
            supports_params = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD or parameter.name == "params"
                for parameter in signature.parameters.values()
            )
        except (TypeError, ValueError):  # pragma: no cover - best effort
            supports_params = True

        for _ in range(1000):
            remaining_ms = max(until_ms - cursor, 0)
            if remaining_ms <= 0:
                break

            approx_points = max(1, math.ceil(remaining_ms / step_ms))
            request_limit = max(1, min(limit_hint, approx_points))

            fetch_kwargs = {
                "timeframe": interval,
                "since": cursor,
                "limit": request_limit,
            }
            if end_param and supports_params:
                capped_end = min(until_ms, cursor + request_limit * step_ms)
                fetch_kwargs["params"] = {end_param: capped_end}

            try:
                batch = self._exchange.fetch_ohlcv(symbol, **fetch_kwargs)
            except TypeError as exc:
                if "params" in fetch_kwargs and "unexpected keyword" in str(exc):
                    supports_params = False
                    fetch_kwargs.pop("params", None)
                    batch = self._exchange.fetch_ohlcv(symbol, **fetch_kwargs)
                else:
                    raise
            except Exception as exc:  # pragma: no cover - network interaction
                raise RuntimeError(f"CCXT fetch failed for {self._exchange_id}:{symbol} -> {exc}") from exc

            if not batch:
                break

            batches.extend(batch)

            last_ts = int(batch[-1][0])
            if previous_last is not None and last_ts <= previous_last:
                break

            previous_last = last_ts
            if last_ts >= until_ms:
                break

            cursor = last_ts + 1
            if cursor > until_ms:
                break

        else:  # pragma: no cover - defensive guard
            logger.warning(
                "CCXT pagination limit reached for %s:%s between %s and %s",
                self._exchange_id,
                symbol,
                start_ts.isoformat(),
                end_ts.isoformat(),
            )

        if not batches:
            return pd.DataFrame()

        df = pd.DataFrame(batches, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.drop_duplicates(subset="timestamp", keep="last")
        df = df[(df["timestamp"] >= start_ts) & (df["timestamp"] <= end_ts)]

        # Align with downstream expectations
        return df[["timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
