"""Interactive Brokers (IBKR) data provider implementation.

This provider bridges the QuantLab data pipeline with the Interactive
 Brokers Trader Workstation (TWS) / Gateway API using :mod:`ib_insync`.
It reuses the common :class:`~data_providers.base_provider.BaseDataProvider`
contract so downstream services can request OHLCV windows via the existing
factory helpers.

The implementation keeps the connection lazy (only opening once data is
requested) and supports a handful of configuration knobs through
environment variables:

``IB_HOST`` / ``IB_PORT`` / ``IB_CLIENT_ID``
    Connection parameters for the TWS/Gateway instance.

``IB_DEFAULT_SEC_TYPE`` / ``IB_DEFAULT_EXCHANGE`` / ``IB_DEFAULT_CURRENCY``
    Fallback contract hints used when a request does not provide more
    specific metadata.

``IB_SYMBOL_OVERRIDES``
    Optional JSON object mapping symbol strings to explicit contract
    dictionaries (``{"secType": "FUT", "symbol": "CL", "exchange":
    "NYMEX", "lastTradeDateOrContractMonth": "202406"}``). This is the
    recommended way to describe complex instruments such as futures.

The provider attempts a best-effort heuristic for simple symbols (stocks,
forex pairs, crypto tickers). Users can refine behaviour via the override
mapping whenever required.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import threading
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd
from ib_insync import IB, Contract, util

from core.logger import logger
from .base_provider import BaseDataProvider, DataSource


_KNOWN_SEC_TYPES = {
    "STK",
    "FUT",
    "OPT",
    "CASH",
    "CRYPTO",
    "IND",
    "CFD",
}


class InteractiveBrokersProvider(BaseDataProvider):
    """Fetch OHLCV candles from Interactive Brokers via ``ib_insync``."""

    _lock = threading.Lock()

    def __init__(self, *, exchange: Optional[str] = None):
        self._host = os.getenv("IB_HOST", "127.0.0.1")
        self._port = int(os.getenv("IB_PORT", "7497"))
        self._client_id = int(os.getenv("IB_CLIENT_ID", "1"))

        # Resolve default contract hints.
        self._default_currency = os.getenv("IB_DEFAULT_CURRENCY", "USD").upper()
        default_sec = os.getenv("IB_DEFAULT_SEC_TYPE", "STK").upper()
        default_exchange = os.getenv("IB_DEFAULT_EXCHANGE", "SMART").upper()

        sec_hint, parsed_exchange = self._parse_exchange(exchange)
        self._default_sec_type = sec_hint or default_sec
        self._default_exchange = parsed_exchange or default_exchange

        self._ib = IB()
        self._symbol_overrides = self._load_symbol_overrides()

    # ------------------------------------------------------------------
    # BaseDataProvider API
    # ------------------------------------------------------------------
    def get_datasource(self) -> str:
        """Return the datasource identifier stored alongside ingested bars."""

        return DataSource.IBKR.value

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def fetch_from_api(
        self,
        symbol: str,
        start: dt.datetime | str,
        end: dt.datetime | str,
        interval: str,
    ) -> pd.DataFrame:
        """Retrieve OHLCV bars for *symbol* between *start* and *end*.

        Parameters
        ----------
        symbol:
            The instrument identifier. The provider supports a handful of
            modifiers (``SYMBOL:SECTYPE:EXCHANGE[:CURRENCY]`` or
            ``SYMBOL.SEC``) to specify the security type inline. Complex
            instruments should be defined through ``IB_SYMBOL_OVERRIDES``.
        start / end:
            ISO formatted timestamps or :class:`datetime.datetime` objects.
        interval:
            Requested bar size (``1m``, ``5m``, ``1h``, ``1d`` ...).
        """

        if not symbol:
            raise ValueError("symbol is required for Interactive Brokers fetch")

        start_dt = self._coerce_datetime(start)
        end_dt = self._coerce_datetime(end)
        if end_dt <= start_dt:
            raise ValueError("end must be after start for Interactive Brokers fetch")

        bar_size = self._map_interval(interval)
        if not bar_size:
            raise ValueError(f"Unsupported interval for Interactive Brokers: {interval}")

        duration = self._derive_duration(start_dt, end_dt)
        contract = self._build_contract(symbol)

        logger.info(
            "ibkr_fetch_request | symbol=%s | interval=%s | start=%s | end=%s | bar_size=%s | duration=%s",
            symbol,
            interval,
            start_dt.isoformat(),
            end_dt.isoformat(),
            bar_size,
            duration,
        )

        with self._lock:
            self._ensure_connection()
            try:
                bars = self._ib.reqHistoricalData(
                    contract,
                    endDateTime=end_dt,
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow=os.getenv("IB_WHAT_TO_SHOW", "TRADES"),
                    useRTH=False,
                    formatDate=1,
                    keepUpToDate=False,
                )
            except Exception as exc:  # pragma: no cover - network interaction
                logger.exception(
                    "ibkr_fetch_failed | symbol=%s | interval=%s | error=%s",
                    symbol,
                    interval,
                    exc,
                )
                raise

        if not bars:
            logger.info(
                "ibkr_fetch_empty | symbol=%s | interval=%s | start=%s | end=%s",
                symbol,
                interval,
                start_dt.isoformat(),
                end_dt.isoformat(),
            )
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        frame = util.df(bars)
        if frame.empty:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        frame["timestamp"] = pd.to_datetime(frame["date"], utc=True)
        frame = frame[(frame["timestamp"] >= start_dt) & (frame["timestamp"] <= end_dt)]

        return frame[["timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _ensure_connection(self) -> None:
        """Open an IB connection if we are not already connected."""

        if self._ib.isConnected():
            return

        try:
            self._ib.connect(
                self._host,
                self._port,
                clientId=self._client_id,
                readonly=True,
            )
        except Exception as exc:  # pragma: no cover - network interaction
            logger.exception(
                "ibkr_connect_failed | host=%s | port=%s | client_id=%s | error=%s",
                self._host,
                self._port,
                self._client_id,
                exc,
            )
            raise

    def _parse_exchange(self, exchange: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        """Split ``SEC:EXCHANGE`` style hints into components."""

        if not exchange:
            return None, None

        token = str(exchange).strip()
        if not token:
            return None, None

        normalized = token.replace("|", ":")
        parts = [part for part in normalized.split(":") if part]
        if not parts:
            return None, None

        sec = parts[0].upper()
        if sec in _KNOWN_SEC_TYPES and len(parts) >= 2:
            return sec, parts[1].upper()

        return None, parts[0].upper()

    def _load_symbol_overrides(self) -> Dict[str, Dict[str, Any]]:
        """Load optional per-symbol contract overrides from the environment."""

        raw = os.getenv("IB_SYMBOL_OVERRIDES")
        if not raw:
            return {}

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("ibkr_symbol_override_parse_failed | error=%s", exc)
            return {}

        overrides: Dict[str, Dict[str, Any]] = {}
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    overrides[str(key)] = value
        return overrides

    def _coerce_datetime(self, value: dt.datetime | str) -> dt.datetime:
        """Return a timezone-aware timestamp from *value*."""

        ts = pd.to_datetime(value, utc=True)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.to_pydatetime()

    def _map_interval(self, interval: str) -> Optional[str]:
        """Translate internal interval strings into IB bar sizes."""

        mapping = {
            "1m": "1 min",
            "2m": "2 mins",
            "3m": "3 mins",
            "5m": "5 mins",
            "10m": "10 mins",
            "15m": "15 mins",
            "30m": "30 mins",
            "45m": "45 mins",
            "1h": "1 hour",
            "2h": "2 hours",
            "3h": "3 hours",
            "4h": "4 hours",
            "1d": "1 day",
            "1w": "1 week",
            "1mo": "1 month",
        }
        return mapping.get(interval.lower())

    def _derive_duration(self, start: dt.datetime, end: dt.datetime) -> str:
        """Derive an IB-compatible duration string from *start* and *end*."""

        seconds = max(int((end - start).total_seconds()), 60)

        if seconds < 3600:
            minutes = math.ceil(seconds / 60)
            return f"{minutes} M"
        if seconds < 86400:
            hours = math.ceil(seconds / 3600)
            return f"{hours} H"
        if seconds < 604800:
            days = math.ceil(seconds / 86400)
            return f"{days} D"
        if seconds < 2592000:
            weeks = math.ceil(seconds / 604800)
            return f"{weeks} W"
        if seconds < 31536000:
            months = math.ceil(seconds / 2592000)
            return f"{months} M"
        years = math.ceil(seconds / 31536000)
        return f"{years} Y"

    def _apply_override(self, symbol: str) -> Optional[Contract]:
        """Return an override contract if the user supplied one."""

        if symbol in self._symbol_overrides:
            override = self._symbol_overrides[symbol]
        elif symbol.upper() in self._symbol_overrides:
            override = self._symbol_overrides[symbol.upper()]
        else:
            return None

        contract = Contract()
        for key, value in override.items():
            setattr(contract, key, value)
        return contract

    def _extract_symbol_hints(self, symbol: str) -> Tuple[str, str, str, str, Optional[str]]:
        """Infer contract hints from inline symbol tokens."""

        base = symbol.strip()
        sec_type = self._default_sec_type
        exchange = self._default_exchange
        currency = self._default_currency
        expiry = None

        # Support SYMBOL:SECTYPE:EXCHANGE[:CURRENCY[:EXPIRY]] patterns.
        tokens = [token for token in base.replace("|", ":").split(":") if token]
        if len(tokens) >= 2 and tokens[1].upper() in _KNOWN_SEC_TYPES:
            base = tokens[0]
            sec_type = tokens[1].upper()
            if len(tokens) >= 3:
                exchange = tokens[2].upper()
            if len(tokens) >= 4:
                currency = tokens[3].upper()
            if len(tokens) >= 5:
                expiry = tokens[4]
            return base, sec_type, exchange, currency, expiry

        if "." in base:
            parts = base.split(".")
            maybe_sec = parts[-1].upper()
            if maybe_sec in _KNOWN_SEC_TYPES:
                base = ".".join(parts[:-1])
                sec_type = maybe_sec

        if "-" in base and sec_type == "FUT":
            base, expiry = base.split("-", 1)

        return base, sec_type, exchange, currency, expiry

    def _qualify_contract(self, contract: Contract) -> Contract:
        """Qualify a partially filled contract via IB if possible."""

        try:
            qualified: Iterable[Contract] = self._ib.qualifyContracts(contract)
        except Exception as exc:  # pragma: no cover - network interaction
            logger.warning("ibkr_contract_qualify_failed | error=%s", exc)
            return contract

        qualified_list = list(qualified)
        if qualified_list:
            return qualified_list[0]
        return contract

    def _build_contract(self, symbol: str) -> Contract:
        """Construct a best-effort IB contract for *symbol*."""

        override = self._apply_override(symbol)
        if override is not None:
            logger.debug("ibkr_contract_override | symbol=%s", symbol)
            return self._qualify_contract(override)

        base_symbol, sec_type, exchange, currency, expiry = self._extract_symbol_hints(symbol)
        contract = Contract()
        contract.symbol = base_symbol
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency or self._default_currency

        if expiry and sec_type == "FUT":
            contract.lastTradeDateOrContractMonth = expiry

        qualified = self._qualify_contract(contract)
        logger.debug(
            "ibkr_contract_built | symbol=%s | secType=%s | exchange=%s | expiry=%s",
            qualified.symbol,
            qualified.secType,
            qualified.exchange,
            getattr(qualified, "lastTradeDateOrContractMonth", None),
        )
        return qualified

