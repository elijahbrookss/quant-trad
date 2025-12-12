"""Instrument metadata helpers for tick/fee simulations."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Dict, List, Mapping, Optional, Tuple

from data_providers.providers.factory import get_provider

try:  # pragma: no cover - optional dependency wiring
    import ccxt  # type: ignore
except Exception:  # pragma: no cover - CCXT unavailable at runtime
    ccxt = None  # type: ignore

from .storage import (
    delete_instrument,
    find_instrument,
    get_instrument,
    load_instruments,
    upsert_instrument,
)


logger = logging.getLogger(__name__)


def _normalize_exchange(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip().lower()
    return text or None


def _coerce_float(value: Optional[object]) -> Optional[float]:
    try:
        if value is None:
            return None
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric


def _normalize_symbol(value: Optional[str]) -> str:
    return (value or "").strip().upper()


def instrument_key(datasource: Optional[str], exchange: Optional[str], symbol: Optional[str]) -> str:
    """Return a deterministic key for datasource/exchange/symbol combos."""

    return "::".join(
        [
            (datasource or "").strip().lower(),
            (exchange or "").strip().lower(),
            _normalize_symbol(symbol),
        ]
    )


def list_instruments() -> List[Dict[str, Any]]:
    """Return all stored instrument definitions."""

    return load_instruments()


def get_instrument_record(instrument_id: str) -> Dict[str, Any]:
    """Return a single instrument row or raise if missing."""

    record = get_instrument(instrument_id)
    if not record:
        raise KeyError(f"Instrument {instrument_id} was not found")
    return record


def create_instrument(**payload: object) -> Dict[str, Any]:
    """Persist a new instrument definition."""

    symbol = _normalize_symbol(payload.get("symbol"))
    if not symbol:
        raise ValueError("Instrument symbol is required")
    body = {
        "id": payload.get("id"),
        "symbol": symbol,
        "datasource": (payload.get("datasource") or "").strip() or None,
        "exchange": (payload.get("exchange") or "").strip() or None,
        "instrument_type": payload.get("instrument_type") or None,
        "tick_size": _coerce_float(payload.get("tick_size")),
        "tick_value": _coerce_float(payload.get("tick_value")),
        "contract_size": _coerce_float(payload.get("contract_size")),
        "min_order_size": _coerce_float(payload.get("min_order_size")),
        "quote_currency": (payload.get("quote_currency") or "").upper() or None,
        "maker_fee_rate": _coerce_float(payload.get("maker_fee_rate")),
        "taker_fee_rate": _coerce_float(payload.get("taker_fee_rate")),
        "metadata": payload.get("metadata") or {},
    }
    if body["tick_size"] is None and body["tick_value"] is None:
        raise ValueError("Specify at least a tick size or tick value for the instrument")
    return upsert_instrument(body)


def update_instrument(instrument_id: str, **payload: object) -> Dict[str, Any]:
    """Update mutable instrument fields."""

    record = get_instrument_record(instrument_id)
    updates = dict(record)
    for key in (
        "datasource",
        "exchange",
        "instrument_type",
        "quote_currency",
    ):
        if key in payload and payload[key] is not None:
            value = payload[key]
            if isinstance(value, str):
                updates[key] = value.strip() or None
            else:
                updates[key] = value
    for key in ("tick_size", "tick_value", "contract_size", "min_order_size", "maker_fee_rate", "taker_fee_rate"):
        if key in payload:
            updates[key] = _coerce_float(payload.get(key))
    if "symbol" in payload and payload["symbol"]:
        updates["symbol"] = _normalize_symbol(payload["symbol"])
    if "metadata" in payload and isinstance(payload["metadata"], dict):
        updates["metadata"] = payload["metadata"]
    return upsert_instrument(updates)


def delete_instrument_record(instrument_id: str) -> None:
    """Remove an instrument metadata row."""

    delete_instrument(instrument_id)


def resolve_instrument(datasource: Optional[str], exchange: Optional[str], symbol: str) -> Optional[Dict[str, Any]]:
    """Return the best matching instrument for the provided identifiers."""

    return find_instrument(datasource, exchange, symbol)


def instrument_index() -> Dict[str, Dict[str, Any]]:
    """Return instruments keyed by datasource/exchange/symbol triplets."""

    index: Dict[str, Dict[str, Any]] = {}
    for record in list_instruments():
        key = instrument_key(record.get("datasource"), record.get("exchange"), record.get("symbol"))
        index[key] = record
    return index


def _ccxt_client(exchange_id: str):  # pragma: no cover - exercised in runtime integration
    if ccxt is None:
        raise RuntimeError("ccxt is not installed; cannot auto-load market metadata")
    if not hasattr(ccxt, exchange_id):
        raise ValueError(f"Exchange '{exchange_id}' is not supported by ccxt")
    exchange_cls = getattr(ccxt, exchange_id)
    return exchange_cls({"enableRateLimit": True})


@lru_cache(maxsize=8)
def _load_markets(exchange_id: str) -> Dict[str, Any]:  # pragma: no cover - heavy network call
    client = _ccxt_client(exchange_id)
    markets = client.load_markets()
    return markets or {}


def _match_market_symbol(symbol: str, market: Mapping[str, Any]) -> bool:
    """Return True if *market* matches *symbol* ignoring separators/case."""

    target = symbol.replace("/", "").replace("-", "").upper()
    candidates = []
    for key in ("symbol", "id", "baseId", "quoteId"):
        value = market.get(key)
        if not value:
            continue
        tokens = [
            str(value).upper(),
            str(value).replace("/", "").replace("-", "").upper(),
        ]
        candidates.extend(tokens)
    return target in candidates


def _market_for_symbol(exchange_id: str, symbol: str) -> Dict[str, Any]:
    """Return the CCXT market dict for *symbol* on *exchange_id*."""

    markets = _load_markets(exchange_id)
    if symbol in markets:
        return markets[symbol]
    normalized = symbol.replace("/", "").replace("-", "").upper()
    for market in markets.values():
        if not isinstance(market, dict):
            continue
        if _match_market_symbol(normalized, market):
            return market
    raise ValueError(f"Symbol {symbol} not found on {exchange_id}")


def _tick_from_precision(value: object) -> Optional[float]:
    """Return a tick increment derived from CCXT precision metadata."""

    if value is None:
        return None
    # CCXT reports integers for decimal precision (e.g. 5 -> 0.00001)
    if isinstance(value, (int, float)) and float(value).is_integer():
        integer = int(float(value))
        if integer >= 0:
            return float(10 ** (-integer))
    numeric = _coerce_float(value)
    if numeric in (None, 0):
        return None
    return float(numeric)


def _tick_from_market(market: Mapping[str, Any]) -> Optional[float]:
    precision = market.get("precision") or {}
    limits = market.get("limits") or {}
    price_precision_raw = precision.get("price")
    tick_size = _tick_from_precision(price_precision_raw)
    if tick_size not in (None, 0):
        return tick_size
    price_precision = _coerce_float(price_precision_raw)
    if price_precision not in (None, 0) and price_precision < 1:
        return price_precision
    price_limit = limits.get("price") if isinstance(limits.get("price"), Mapping) else {}
    min_price = _coerce_float(price_limit.get("min"))
    if min_price not in (None, 0):
        return min_price
    return price_precision if price_precision not in (None, 0) else None


def _instrument_payload_from_market(
    *,
    datasource: Optional[str],
    exchange: Optional[str],
    symbol: str,
    market: Mapping[str, Any],
) -> Dict[str, Any]:
    """Translate a CCXT market entry into our instrument schema."""

    tick_size = _tick_from_market(market)
    contract_size = _coerce_float(market.get("contractSize")) or 1.0
    min_amount = _coerce_float(((market.get("limits") or {}).get("amount") or {}).get("min"))
    maker_fee = _coerce_float(market.get("maker"))
    taker_fee = _coerce_float(market.get("taker"))
    instrument_type = market.get("type")
    if not instrument_type:
        if market.get("spot"):
            instrument_type = "spot"
        elif market.get("swap"):
            instrument_type = "swap"
        elif market.get("future"):
            instrument_type = "future"
    tick_value = None
    if tick_size is not None and contract_size is not None:
        tick_value = tick_size * contract_size
    metadata = {
        "ccxt_symbol": market.get("symbol"),
        "ccxt_id": market.get("id"),
        "precision": market.get("precision"),
        "limits": market.get("limits"),
        "info": market.get("info"),
    }
    payload = {
        "symbol": _normalize_symbol(symbol),
        "datasource": datasource,
        "exchange": exchange,
        "instrument_type": instrument_type,
        "tick_size": tick_size,
        "tick_value": tick_value,
        "contract_size": contract_size,
        "min_order_size": min_amount,
        "quote_currency": market.get("quote") or market.get("settle"),
        "maker_fee_rate": maker_fee,
        "taker_fee_rate": taker_fee,
        "metadata": metadata,
    }
    return payload


def validate_instrument(
    datasource: Optional[str],
    exchange: Optional[str],
    symbol: Optional[str],
    *,
    provider_id: Optional[str] = None,
    venue_id: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Validate that an instrument exists for the provider/venue and persist metadata."""

    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        return None, "Symbol is required for instrument validation"

    datasource_id = (datasource or provider_id or "").strip()
    exchange_id = (exchange or venue_id or "").strip()

    if datasource_id.upper() == "CCXT":
        record, error = auto_sync_instrument(datasource_id, exchange_id, normalized_symbol)
        if record or error:
            return record, error

    try:
        provider = get_provider(datasource_id, venue=exchange_id or venue_id, exchange=exchange_id)
    except Exception as exc:  # pragma: no cover - runtime resolution
        logger.warning(
            "instrument_provider_lookup_failed | provider=%s venue=%s symbol=%s error=%s",
            datasource_id,
            exchange_id or venue_id,
            normalized_symbol,
            exc,
        )
        return None, f"Provider lookup failed: {exc}"

    venue_arg = exchange_id or venue_id or ""
    try:
        provider.validate_symbol(venue_arg, normalized_symbol)
        instrument_type = provider.validate_instrument_type(venue_arg, normalized_symbol)
        metadata = provider.get_instrument_metadata(venue_arg, normalized_symbol)
    except Exception as exc:  # pragma: no cover - provider integration
        logger.warning(
            "instrument_validation_failed | provider=%s venue=%s symbol=%s error=%s",
            datasource_id,
            venue_arg,
            normalized_symbol,
            exc,
        )
        return None, f"Instrument validation failed: {exc}"

    if metadata is None or (metadata.tick_size is None and metadata.tick_value is None):
        return None, "Provider did not return tick metadata for this symbol"

    payload = {
        "symbol": normalized_symbol,
        "datasource": getattr(provider, "get_datasource", lambda: datasource_id)(),
        "exchange": exchange_id or venue_id,
        "instrument_type": getattr(instrument_type, "value", instrument_type),
        "tick_size": metadata.tick_size,
        "tick_value": metadata.tick_value,
        "contract_size": metadata.contract_size,
    }

    try:
        record = upsert_instrument(payload)
    except Exception as exc:  # pragma: no cover - storage failure
        logger.warning(
            "instrument_persist_failed | provider=%s venue=%s symbol=%s error=%s",
            datasource_id,
            venue_arg,
            normalized_symbol,
            exc,
        )
        return None, f"Unable to persist instrument metadata: {exc}"

    return record, None


def auto_sync_instrument(
    datasource: Optional[str], exchange: Optional[str], symbol: Optional[str]
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Ensure an instrument exists for the datasource/exchange/symbol combo."""

    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        return None, "Symbol is required for instrument metadata"

    existing = resolve_instrument(datasource, exchange, normalized_symbol)
    if existing:
        return existing, None

    exchange_id = _normalize_exchange(exchange)
    datasource_id = (datasource or "").strip().upper()
    if datasource_id != "CCXT":
        return None, "Automatic metadata is currently available only for CCXT datasources"
    if not exchange_id:
        return None, "Specify an exchange to auto-fetch CCXT market metadata"

    try:
        market = _market_for_symbol(exchange_id, normalized_symbol)
    except Exception as exc:  # pragma: no cover - network edge cases
        logger.warning(
            "instrument_market_lookup_failed | exchange=%s symbol=%s error=%s",
            exchange_id,
            normalized_symbol,
            exc,
        )
        return None, f"Unable to load CCXT market metadata: {exc}"

    payload = _instrument_payload_from_market(
        datasource=datasource_id,
        exchange=exchange_id,
        symbol=normalized_symbol,
        market=market,
    )
    record = upsert_instrument(payload)
    logger.info(
        "instrument_autosynced | datasource=%s exchange=%s symbol=%s",
        datasource_id,
        exchange_id,
        normalized_symbol,
    )
    return record, None
