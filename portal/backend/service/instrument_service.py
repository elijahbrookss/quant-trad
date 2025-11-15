"""Instrument metadata helpers for tick/fee simulations."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .storage import (
    delete_instrument,
    find_instrument,
    get_instrument,
    load_instruments,
    upsert_instrument,
)


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
