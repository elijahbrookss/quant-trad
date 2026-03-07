"""Interactive Brokers (IBKR) data provider implementation.

This provider bridges the QuantLab data pipeline with the Interactive
 Brokers Trader Workstation (TWS) / Gateway API using :mod:`ib_insync`.
It reuses the common :class:`~data_providers.providers.base.BaseDataProvider`
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

import asyncio
import datetime as dt
import json
import math
import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd
from ib_insync import IB, Contract, util

from core.logger import logger
from data_providers.registry import _REGISTRY
from .base import BaseDataProvider, DataSource, InstrumentMetadata, InstrumentType


@dataclass(frozen=True)
class _DurationRule:
    """Container describing how to express a duration window."""

    upper_bound: Optional[int]
    unit: str
    unit_seconds: int

    def format(self, seconds: int) -> str:
        """Return the IB duration string for *seconds* within this rule."""

        value = math.ceil(seconds / self.unit_seconds)
        return f"{value} {self.unit}"


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

    def __init__(self, *, exchange: Optional[str] = None, persistence=None, settings=None):
        super().__init__(persistence=persistence, settings=settings)

        self._host = os.getenv("IB_HOST", "ibkr-gateway")
        # The IB Gateway paper-trading endpoint defaults to 4002 while the
        # production endpoint listens on 4001. Users can override the port via
        # ``IB_PORT`` when connecting to a standalone TWS installation.
        self._port = int(os.getenv("IB_PORT", "4002"))
        self._client_id = int(os.getenv("IB_CLIENT_ID", "1"))

        # Resolve default contract hints.
        self._default_currency = os.getenv("IB_DEFAULT_CURRENCY", "USD").upper()
        default_sec = os.getenv("IB_DEFAULT_SEC_TYPE", "STK").upper()
        default_exchange = os.getenv("IB_DEFAULT_EXCHANGE", "SMART").upper()

        sec_hint, parsed_exchange = self._parse_exchange(exchange)
        self._default_sec_type = sec_hint or default_sec
        self._default_exchange = parsed_exchange or default_exchange
        if "IB_DEFAULT_CURRENCY" not in os.environ:
            logger.warning("ibkr_default_currency_fallback | currency=%s", self._default_currency)
        if "IB_DEFAULT_SEC_TYPE" not in os.environ and not sec_hint:
            logger.warning("ibkr_default_sec_type_fallback | sec_type=%s", self._default_sec_type)
        if "IB_DEFAULT_EXCHANGE" not in os.environ and not parsed_exchange:
            logger.warning("ibkr_default_exchange_fallback | exchange=%s", self._default_exchange)

        self._ib = IB()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._symbol_overrides = self._load_symbol_overrides()
        self._duration_rules = self._build_duration_rules()

    # ------------------------------------------------------------------
    # BaseDataProvider API
    # ------------------------------------------------------------------
    def get_datasource(self) -> str:
        """Return the datasource identifier stored alongside ingested bars."""

        return DataSource.IBKR.value

    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Map IBKR security types into a spot vs futures/perps binary."""

        _, details = self._resolve_contract_details(symbol)
        sec_type = ((details[0].contract.secType if details else None) or "").upper()

        if sec_type in {"FUT", "OPT"}:
            return InstrumentType.FUTURE

        return InstrumentType.SPOT

    def validate_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        """Resolve contract details and return the derived instrument type."""

        return self.get_instrument_type(venue, symbol)

    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        """Return tick size, contract multiplier, and derived tick value."""

        contract, details = self._resolve_contract_details(symbol)
        sec_type = ((details[0].contract.secType if details else None) or "").upper()
        instrument_type = InstrumentType.FUTURE if sec_type in {"FUT", "OPT"} else InstrumentType.SPOT
        min_tick: Optional[float] = None
        multiplier: Optional[float] = None

        if details:
            first = details[0]
            try:
                min_tick = float(getattr(first, "minTick", None)) if getattr(first, "minTick", None) is not None else None
            except (TypeError, ValueError):
                min_tick = None

            multiplier_value = getattr(first.contract, "multiplier", None) or getattr(first, "multiplier", None)
            try:
                multiplier = float(multiplier_value) if multiplier_value is not None else None
            except (TypeError, ValueError):
                multiplier = None

        if multiplier is None and instrument_type == InstrumentType.SPOT:
            multiplier = 1.0

        if min_tick is None:
            min_tick = 0.01

        currency = getattr(contract, "currency", None)
        if not currency:
            raise ValueError(f"Interactive Brokers metadata missing currency for '{symbol}'")

        can_short = None
        if instrument_type == InstrumentType.FUTURE:
            can_short = True

        return self._normalize_metadata(
            tick_size=min_tick,
            contract_size=multiplier,
            can_short=can_short,
            short_requires_borrow=bool(can_short) if instrument_type == InstrumentType.SPOT else False,
            has_funding=False,
            expiry_ts=None,
            base_currency=symbol,
            quote_currency=currency,
        )

    def validate_symbol(self, venue: str, symbol: str) -> None:
        """Raise if IBKR cannot resolve contract details for the symbol."""

        self._resolve_contract_details(symbol)

    def fetch_from_api(
        self,
        symbol: str,
        start: dt.datetime | str,
        end: dt.datetime | str,
        interval: str,
    ) -> pd.DataFrame:
        """Retrieve OHLCV bars for *symbol* between *start* and *end*."""

        return _fetch_from_api_impl(self, symbol, start, end, interval)

    def _resolve_contract_details(self, symbol: str) -> Tuple[Contract, list]:
        if not symbol:
            raise ValueError("symbol is required for Interactive Brokers validation")

        contract = self._build_contract(symbol)
        details = self._fetch_contract_details(contract)

        if not details:
            raise ValueError(f"Interactive Brokers could not resolve contract details for '{symbol}'")

        return contract, details

    def _fetch_contract_details(self, contract: Contract) -> list:
        with self._lock:
            self._ensure_connection()
            try:
                return self._ib.reqContractDetails(contract)
            except Exception as exc:  # pragma: no cover - network interaction
                logger.warning(
                    "ibkr_metadata_fetch_failed | symbol=%s | secType=%s | exchange=%s | error=%s",
                    contract.symbol,
                    contract.secType,
                    contract.exchange,
                    exc,
                )
        return []



# ------------------------------------------------------------------
# Public helpers
# ------------------------------------------------------------------
def _fetch_from_api_impl(
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
            logger.debug("ibkr_fetch_before_request | connected=%s", self._ib.isConnected())
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
            logger.debug("ibkr_fetch_after_request | bar_count=%s", len(bars) if bars else 0)
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
    filtered = frame[(frame["timestamp"] >= start_dt) & (frame["timestamp"] <= end_dt)]

    if filtered.empty:
        logger.info(
            "ibkr_fetch_preserve_unfiltered | symbol=%s | interval=%s | start=%s | end=%s | bars=%s",
            symbol,
            interval,
            start_dt.isoformat(),
            end_dt.isoformat(),
            len(frame),
        )
        filtered = frame

    return filtered[["timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)

# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------
def _ensure_connection(self) -> None:
    """Open an IB connection if we are not already connected."""
    if self._ib.isConnected():
        logger.debug("ibkr_already_connected | host=%s | port=%s | client_id=%s",
                    self._host, self._port, self._client_id)
        return

    logger.info("ibkr_connect_start | host=%s | port=%s | client_id=%s",
                self._host, self._port, self._client_id)

    try:
        self._ensure_event_loop()
        logger.debug("ibkr_event_loop_ready | loop=%s | loop_is_closed=%s | thread=%s",
                    self._loop, getattr(self._loop, 'is_closed', lambda: None)(),
                    threading.current_thread().name)

        self._ib.connect(
            self._host,
            self._port,
            clientId=self._client_id,
            readonly=True,
        )

        logger.info(
            "ibkr_connect_success | host=%s | port=%s | client_id=%s | isConnected=%s",
            self._host,
            self._port,
            self._client_id,
            self._ib.isConnected(),
        )
    except Exception as exc:
        logger.exception(
            "ibkr_connect_failed | host=%s | port=%s | client_id=%s | exc_type=%s | exc_repr=%r",
            self._host,
            self._port,
            self._client_id,
            type(exc).__name__,
            exc,
        )
        raise


def _ensure_event_loop(self) -> None:
    """Ensure the current thread has an asyncio event loop."""

    try:
        asyncio.get_running_loop()
        return
    except RuntimeError:
        pass

    loop = self._loop
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        self._loop = loop

    asyncio.set_event_loop(loop)

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

def _build_duration_rules(self) -> Iterable[_DurationRule]:
    """Return duration formatting rules in ascending order of window size."""

    return (
        _DurationRule(upper_bound=3600, unit="S", unit_seconds=1),
        _DurationRule(upper_bound=86400, unit="H", unit_seconds=3600),
        _DurationRule(upper_bound=604800, unit="D", unit_seconds=86400),
        _DurationRule(upper_bound=2592000, unit="W", unit_seconds=604800),
        _DurationRule(upper_bound=31536000, unit="M", unit_seconds=2592000),
        _DurationRule(upper_bound=None, unit="Y", unit_seconds=31536000),
    )

def _derive_duration(self, start: dt.datetime, end: dt.datetime) -> str:
    """Derive an IB-compatible duration string from *start* and *end*."""

    seconds = max(int((end - start).total_seconds()), 60)

    for rule in self._duration_rules:
        if rule.upper_bound is None or seconds < rule.upper_bound:
            return rule.format(seconds)

    # Fallback should never trigger because the last rule has no bound.
    return _DurationRule(upper_bound=None, unit="S", unit_seconds=1).format(seconds)

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


for _method_name in (
    "_ensure_connection",
    "_ensure_event_loop",
    "_parse_exchange",
    "_load_symbol_overrides",
    "_coerce_datetime",
    "_map_interval",
    "_build_duration_rules",
    "_derive_duration",
    "_apply_override",
    "_extract_symbol_hints",
    "_qualify_contract",
    "_build_contract",
):
    setattr(InteractiveBrokersProvider, _method_name, globals()[_method_name])



@_REGISTRY.provider(
    id="INTERACTIVE_BROKERS",
    label="Interactive Brokers",
    supported_venues=["INTERACTIVE_BROKERS"],
    capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["equities", "futures", "options"]},
)
def _register_ibkr_provider():
    return InteractiveBrokersProvider


@_REGISTRY.venue(
    id="INTERACTIVE_BROKERS",
    label="Interactive Brokers",
    provider_id="INTERACTIVE_BROKERS",
    adapter_id=None,
)
def _register_ibkr_venue():
    return "INTERACTIVE_BROKERS"
