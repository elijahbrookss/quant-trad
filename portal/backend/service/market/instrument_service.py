"""Instrument metadata helpers for tick/fee simulations."""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from data_providers.providers.factory import get_provider
from data_providers.registry import normalize_provider_id, normalize_venue_id
from engines.bot_runtime.core.execution_profile import (
    compile_series_execution_profile,
    normalize_execution_semantics,
    normalize_runtime_instrument_type,
)

from ..providers import persistence_bootstrap  # noqa: F401

from ..storage.storage import (
    delete_instrument,
    find_instrument,
    get_instrument,
    load_instruments,
    upsert_instrument,
)


logger = logging.getLogger(__name__)

def _coerce_float(value: Optional[object]) -> Optional[float]:
    try:
        if value is None:
            return None
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric


def _coerce_bool(value: Optional[object]) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _coerce_datetime(value: Optional[object]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _missing_behavior_fields(payload: Mapping[str, Any]) -> List[str]:
    missing: List[str] = []
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    instrument_fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
    if instrument_fields.get("can_short") is None:
        missing.append("can_short")
    if instrument_fields.get("short_requires_borrow") is None:
        missing.append("short_requires_borrow")
    if instrument_fields.get("has_funding") is None:
        missing.append("has_funding")
    if not instrument_fields.get("quote_currency"):
        missing.append("quote_currency")
    if not instrument_fields.get("base_currency"):
        missing.append("base_currency")
    return missing


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


def _instrument_fields(record: Mapping[str, Any]) -> Dict[str, Any]:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else {}
    fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
    return dict(fields)


def _has_margin_rates(value: Any) -> bool:
    if not isinstance(value, Mapping) or not value:
        return False
    for session in ("intraday", "overnight"):
        rates = value.get(session)
        if isinstance(rates, Mapping):
            for key in ("long_margin_rate", "short_margin_rate"):
                if rates.get(key) not in (None, ""):
                    return True
    return False


_PROXY_DERIVATIVE_EXECUTION_FIELD_KEYS = (
    "tick_size",
    "contract_size",
    "tick_value",
    "min_order_size",
    "min_qty",
    "max_order_size",
    "max_qty",
    "qty_step",
    "step_size",
    "min_notional",
    "min_cost",
    "precision",
    "maker_fee_rate",
    "taker_fee_rate",
    "can_short",
    "short_requires_borrow",
    "has_funding",
    "expiry_ts",
)


def _proxy_derivative_execution_fields(candidate: Mapping[str, Any], fields: Mapping[str, Any]) -> Dict[str, Any]:
    execution_fields: Dict[str, Any] = {}
    for key in _PROXY_DERIVATIVE_EXECUTION_FIELD_KEYS:
        if key in fields:
            execution_fields[key] = deepcopy(fields.get(key))
            continue
        if key in candidate:
            execution_fields[key] = deepcopy(candidate.get(key))
    return execution_fields


def _proxy_derivative_reference(record: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    fields = _instrument_fields(record)
    base = str(fields.get("base_currency") or record.get("base_currency") or "").strip().upper()
    quote = str(fields.get("quote_currency") or record.get("quote_currency") or "").strip().upper()
    if not base or not quote:
        return None

    current_id = str(record.get("id") or "").strip()
    candidates: List[Tuple[int, Dict[str, Any], Mapping[str, Any], Dict[str, Any]]] = []
    for candidate in load_instruments():
        if current_id and str(candidate.get("id") or "").strip() == current_id:
            continue
        candidate_type = normalize_runtime_instrument_type(candidate.get("instrument_type"))
        if candidate_type not in {"future", "perp", "swap"}:
            continue
        candidate_fields = _instrument_fields(candidate)
        candidate_base = str(candidate_fields.get("base_currency") or candidate.get("base_currency") or "").strip().upper()
        candidate_quote = str(candidate_fields.get("quote_currency") or candidate.get("quote_currency") or "").strip().upper()
        if candidate_base != base:
            continue
        if candidate_quote != quote:
            continue
        margin_rates = candidate_fields.get("margin_rates") or candidate.get("margin_rates")
        if not _has_margin_rates(margin_rates):
            continue

        score = 0
        if str(candidate.get("datasource") or "").strip().upper() == "COINBASE":
            score += 10
        if str(candidate.get("exchange") or "").strip().lower() == "coinbase_direct":
            score += 5
        if "P-" in str(candidate.get("symbol") or "").upper():
            score += 1
        candidates.append((score, candidate, margin_rates, candidate_fields))

    if not candidates:
        return None
    _score, candidate, margin_rates, candidate_fields = sorted(
        candidates,
        key=lambda item: (item[0], str(item[1].get("symbol") or "")),
        reverse=True,
    )[0]
    return {
        "instrument_id": candidate.get("id"),
        "symbol": candidate.get("symbol"),
        "datasource": candidate.get("datasource"),
        "exchange": candidate.get("exchange"),
        "instrument_type": normalize_runtime_instrument_type(candidate.get("instrument_type")) or candidate.get("instrument_type"),
        "margin_rates": deepcopy(margin_rates),
        "maker_fee_rate": candidate_fields.get("maker_fee_rate"),
        "taker_fee_rate": candidate_fields.get("taker_fee_rate"),
        "execution_fields": _proxy_derivative_execution_fields(candidate, candidate_fields),
    }


def _with_proxy_derivative_reference(record: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(record)
    if normalize_runtime_instrument_type(payload.get("instrument_type")) != "spot":
        return payload

    metadata = dict(payload.get("metadata") or {})
    fields = dict(metadata.get("instrument_fields") or {})
    if _has_margin_rates(fields.get("proxy_derivative_margin_rates")):
        return payload

    reference = _proxy_derivative_reference(payload)
    if not reference:
        return payload

    fields["proxy_derivative_margin_rates"] = deepcopy(reference["margin_rates"])
    if isinstance(reference.get("execution_fields"), Mapping) and reference.get("execution_fields"):
        fields["proxy_derivative_instrument_fields"] = deepcopy(reference["execution_fields"])
    if reference.get("maker_fee_rate") is not None:
        fields["proxy_derivative_maker_fee_rate"] = reference.get("maker_fee_rate")
    if reference.get("taker_fee_rate") is not None:
        fields["proxy_derivative_taker_fee_rate"] = reference.get("taker_fee_rate")
    metadata["instrument_fields"] = fields
    metadata["proxy_derivative_reference"] = {
        key: value
        for key, value in reference.items()
        if key not in {"margin_rates", "maker_fee_rate", "taker_fee_rate", "execution_fields"}
    }
    payload["metadata"] = metadata
    return payload


def _has_proxy_derivative_reference(record: Mapping[str, Any]) -> bool:
    fields = _instrument_fields(record)
    margin_rates = record.get("proxy_derivative_margin_rates") or fields.get("proxy_derivative_margin_rates")
    execution_fields = record.get("proxy_derivative_instrument_fields") or fields.get("proxy_derivative_instrument_fields")
    return isinstance(margin_rates, Mapping) and bool(margin_rates) and isinstance(execution_fields, Mapping) and bool(execution_fields)


def _execution_semantics_for_record(
    record: Mapping[str, Any],
    *,
    requested: Optional[object] = None,
) -> str:
    instrument_type = normalize_runtime_instrument_type(record.get("instrument_type"))
    if requested not in (None, ""):
        return normalize_execution_semantics(requested, instrument_type=instrument_type)
    if instrument_type == "spot" and _has_proxy_derivative_reference(record):
        return "proxy_derivative"
    return normalize_execution_semantics(None, instrument_type=instrument_type)


def _runtime_policy_from_execution_semantics(execution_semantics: str) -> str:
    if execution_semantics == "proxy_derivative":
        return "proxy_derivative"
    if execution_semantics == "derivative":
        return "derivative"
    if execution_semantics == "spot":
        return "spot"
    return "unknown"


def instrument_health_report(
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a spot metadata health report for stored instruments."""

    report: List[Dict[str, Any]] = []
    for record in list_instruments():
        if datasource and str(record.get("datasource") or "").upper() != str(datasource).upper():
            continue
        if exchange and str(record.get("exchange") or "").upper() != str(exchange).upper():
            continue
        instrument_type = normalize_runtime_instrument_type(record.get("instrument_type"))
        if instrument_type != "spot":
            continue
        issues = _spot_issues_from_record(record)
        report.append(
            {
                "id": record.get("id"),
                "symbol": record.get("symbol"),
                "datasource": record.get("datasource"),
                "exchange": record.get("exchange"),
                "issues": issues,
            }
        )
    incomplete = [entry for entry in report if entry["issues"]]
    return {
        "total_spot": len(report),
        "incomplete": len(incomplete),
        "complete": len(report) - len(incomplete),
        "details": report,
    }


def get_instrument_record(instrument_id: str) -> Dict[str, Any]:
    """Return a single instrument row or raise if missing."""

    record = get_instrument(instrument_id)
    if not record:
        raise KeyError(f"Instrument {instrument_id} was not found")
    return _with_proxy_derivative_reference(record)


def create_instrument(**payload: object) -> Dict[str, Any]:
    """Persist a new instrument definition."""

    symbol = _normalize_symbol(payload.get("symbol"))
    if not symbol:
        raise ValueError("Instrument symbol is required")
    metadata_input = payload.get("metadata") or {}
    if isinstance(metadata_input, Mapping) and (
        "instrument_fields" in metadata_input or "provider_metadata" in metadata_input
    ):
        metadata = dict(metadata_input)
        instrument_fields = dict(metadata.get("instrument_fields") or {})
        provider_metadata = dict(metadata.get("provider_metadata") or {})
    else:
        instrument_fields = {}
        provider_metadata = dict(metadata_input) if isinstance(metadata_input, Mapping) else {}
    for key, coercer in (
        ("tick_size", _coerce_float),
        ("tick_value", _coerce_float),
        ("contract_size", _coerce_float),
        ("min_order_size", _coerce_float),
        ("qty_step", _coerce_float),
        ("max_qty", _coerce_float),
        ("min_notional", _coerce_float),
        ("maker_fee_rate", _coerce_float),
        ("taker_fee_rate", _coerce_float),
    ):
        if key in payload:
            instrument_fields[key] = coercer(payload.get(key))
    if "margin_rates" in payload:
        instrument_fields["margin_rates"] = payload.get("margin_rates")
    if "quote_currency" in payload:
        instrument_fields["quote_currency"] = (payload.get("quote_currency") or "").upper() or None
    if "base_currency" in payload:
        instrument_fields["base_currency"] = (payload.get("base_currency") or "").upper() or None
    if "can_short" in payload:
        instrument_fields["can_short"] = _coerce_bool(payload.get("can_short"))
    if "short_requires_borrow" in payload:
        instrument_fields["short_requires_borrow"] = _coerce_bool(payload.get("short_requires_borrow"))
    if "has_funding" in payload:
        instrument_fields["has_funding"] = _coerce_bool(payload.get("has_funding"))
    if "expiry_ts" in payload:
        expiry_ts = _coerce_datetime(payload.get("expiry_ts"))
        instrument_fields["expiry_ts"] = expiry_ts.isoformat() if expiry_ts else None
    metadata = {"instrument_fields": instrument_fields, "provider_metadata": provider_metadata}

    body = {
        "id": payload.get("id"),
        "symbol": symbol,
        "datasource": (payload.get("datasource") or "").strip() or None,
        "exchange": (payload.get("exchange") or "").strip() or None,
        "instrument_type": payload.get("instrument_type") or None,
        "metadata": metadata,
    }
    if instrument_fields.get("tick_size") is None and instrument_fields.get("tick_value") is None:
        raise ValueError("Specify at least a tick size or tick value for the instrument")
    missing = _missing_behavior_fields(body)
    if missing:
        raise ValueError(f"Instrument metadata missing fields: {', '.join(missing)}")
    return upsert_instrument(body)


def update_instrument(instrument_id: str, **payload: object) -> Dict[str, Any]:
    """Update mutable instrument fields."""

    record = get_instrument_record(instrument_id)
    updates = dict(record)
    for key in (
        "datasource",
        "exchange",
        "instrument_type",
    ):
        if key in payload and payload[key] is not None:
            value = payload[key]
            if isinstance(value, str):
                updates[key] = value.strip() or None
            else:
                updates[key] = value
    metadata = dict(record.get("metadata") or {})
    instrument_fields = dict(metadata.get("instrument_fields") or {})
    provider_metadata = dict(metadata.get("provider_metadata") or {})
    for key in (
        "tick_size",
        "tick_value",
        "contract_size",
        "min_order_size",
        "qty_step",
        "max_qty",
        "min_notional",
        "maker_fee_rate",
        "taker_fee_rate",
    ):
        if key in payload:
            instrument_fields[key] = _coerce_float(payload.get(key))
    if "margin_rates" in payload:
        instrument_fields["margin_rates"] = payload.get("margin_rates")
    if "symbol" in payload and payload["symbol"]:
        updates["symbol"] = _normalize_symbol(payload["symbol"])
    if "metadata" in payload and isinstance(payload["metadata"], dict):
        incoming = payload["metadata"]
        if "instrument_fields" in incoming or "provider_metadata" in incoming:
            instrument_fields.update(incoming.get("instrument_fields") or {})
            provider_metadata.update(incoming.get("provider_metadata") or {})
        else:
            provider_metadata.update(incoming)
    if "base_currency" in payload:
        instrument_fields["base_currency"] = (payload.get("base_currency") or "").upper() or None
    if "quote_currency" in payload:
        instrument_fields["quote_currency"] = (payload.get("quote_currency") or "").upper() or None
    if "expiry_ts" in payload:
        expiry_ts = _coerce_datetime(payload.get("expiry_ts"))
        instrument_fields["expiry_ts"] = expiry_ts.isoformat() if expiry_ts else None
    if "can_short" in payload:
        coerced = _coerce_bool(payload.get("can_short"))
        if coerced is None:
            raise ValueError("can_short must be a boolean value")
        instrument_fields["can_short"] = coerced
    if "short_requires_borrow" in payload:
        coerced = _coerce_bool(payload.get("short_requires_borrow"))
        if coerced is None:
            raise ValueError("short_requires_borrow must be a boolean value")
        instrument_fields["short_requires_borrow"] = coerced
    if "has_funding" in payload:
        coerced = _coerce_bool(payload.get("has_funding"))
        if coerced is None:
            raise ValueError("has_funding must be a boolean value")
        instrument_fields["has_funding"] = coerced
    updates["metadata"] = {"instrument_fields": instrument_fields, "provider_metadata": provider_metadata}
    missing = _missing_behavior_fields(updates)
    if missing:
        raise ValueError(f"Instrument metadata missing fields: {', '.join(missing)}")
    return upsert_instrument(updates)


def delete_instrument_record(instrument_id: str) -> None:
    """Remove an instrument metadata row."""

    delete_instrument(instrument_id)


def resolve_instrument(datasource: Optional[str], exchange: Optional[str], symbol: str) -> Optional[Dict[str, Any]]:
    """Return the best matching instrument for the provided identifiers."""

    record = find_instrument(datasource, exchange, symbol)
    return _with_proxy_derivative_reference(record) if record else None


def require_instrument_id(datasource: Optional[str], exchange: Optional[str], symbol: Optional[str]) -> str:
    """Return the instrument_id for the provided identifiers or raise if missing."""

    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        raise ValueError("Instrument symbol is required to resolve instrument_id")

    record = resolve_instrument(datasource, exchange, normalized_symbol)
    instrument_id = record.get("id") if isinstance(record, dict) else None
    if not instrument_id:
        logger.error(
            "instrument_lookup_failed | symbol=%s datasource=%s exchange=%s",
            normalized_symbol,
            datasource,
            exchange,
        )
        raise ValueError(
            f"Instrument not found for symbol={normalized_symbol} datasource={datasource} exchange={exchange}"
        )
    return instrument_id


def instrument_index() -> Dict[str, Dict[str, Any]]:
    """Return instruments keyed by datasource/exchange/symbol triplets."""

    index: Dict[str, Dict[str, Any]] = {}
    for record in list_instruments():
        key = instrument_key(record.get("datasource"), record.get("exchange"), record.get("symbol"))
        index[key] = record
    return index


def _step_from_precision(value: object) -> Optional[float]:
    """Return a quantity step derived from CCXT amount precision metadata."""

    if value is None:
        return None
    if isinstance(value, (int, float)) and float(value).is_integer():
        integer = int(float(value))
        if integer >= 0:
            return float(10 ** (-integer))
    numeric = _coerce_float(value)
    if numeric in (None, 0):
        return None
    if numeric < 1:
        return float(numeric)
    return None


def _tick_from_market(market: Mapping[str, Any]) -> Optional[float]:
    """Return a tick size derived from CCXT-style market metadata."""

    precision = market.get("precision") if isinstance(market.get("precision"), Mapping) else {}
    limits = market.get("limits") if isinstance(market.get("limits"), Mapping) else {}
    price_limits = limits.get("price") if isinstance(limits.get("price"), Mapping) else {}

    precision_price = precision.get("price")
    precision_tick: Optional[float] = None
    if precision_price is not None:
        if isinstance(precision_price, int):
            precision_tick = float(10 ** (-precision_price)) if precision_price >= 0 else None
        else:
            numeric = _coerce_float(precision_price)
            if numeric not in (None, 0):
                if float(numeric).is_integer() and numeric >= 1:
                    precision_tick = float(10 ** (-int(numeric)))
                elif numeric < 1:
                    precision_tick = float(numeric)

    tick = market.get("tickSize")
    if tick is None:
        tick = price_limits.get("min")
    if tick is None:
        tick = precision_tick
    return _coerce_float(tick)


def _spot_issues_from_record(record: Mapping[str, Any]) -> List[str]:
    issues: List[str] = []
    fields = _instrument_fields(record)
    if not (record.get("tick_size") or fields.get("tick_size")):
        issues.append("missing_tick_size")
    if not (record.get("min_order_size") or fields.get("min_order_size") or fields.get("min_qty")):
        issues.append("missing_min_qty")
    if not (record.get("quote_currency") or fields.get("quote_currency")):
        issues.append("missing_quote_currency")
    if record.get("maker_fee_rate") is None and fields.get("maker_fee_rate") is None:
        issues.append("missing_maker_fee")
    if record.get("taker_fee_rate") is None and fields.get("taker_fee_rate") is None:
        issues.append("missing_taker_fee")

    metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else {}
    provider_metadata = (
        metadata.get("provider_metadata")
        if isinstance(metadata.get("provider_metadata"), Mapping)
        else metadata
    )
    qty_step = provider_metadata.get("qty_step") if isinstance(provider_metadata, Mapping) else None
    if not qty_step:
        precision = (
            provider_metadata.get("precision")
            if isinstance(provider_metadata, Mapping) and isinstance(provider_metadata.get("precision"), Mapping)
            else {}
        )
        qty_step = _step_from_precision(precision.get("amount"))
    if not qty_step:
        issues.append("missing_qty_step")

    min_notional = provider_metadata.get("min_notional") if isinstance(provider_metadata, Mapping) else None
    if not min_notional:
        limits = (
            provider_metadata.get("limits")
            if isinstance(provider_metadata, Mapping) and isinstance(provider_metadata.get("limits"), Mapping)
            else {}
        )
        cost_limits = limits.get("cost") if isinstance(limits.get("cost"), Mapping) else {}
        min_notional = _coerce_float(cost_limits.get("min"))
    if not min_notional:
        issues.append("missing_min_notional")

    return issues


def validate_instrument(
    datasource: Optional[str],
    exchange: Optional[str],
    symbol: Optional[str],
    *,
    provider_id: Optional[str] = None,
    venue_id: Optional[str] = None,
    force_refresh: bool = False,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Validate that an instrument exists for the provider/venue and persist metadata."""

    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        return None, "Symbol is required for instrument validation"

    datasource_id = (datasource or provider_id or "").strip()
    exchange_id = (exchange or venue_id or "").strip()
    canonical_provider_id = normalize_provider_id(provider_id or datasource)
    canonical_venue_id = normalize_venue_id(venue_id or exchange)

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
    def _sanitize_provider_error(message: str) -> str:
        if not message:
            return "Instrument validation failed"
        # Strip verbose provider payloads before returning to the frontend.
        for token in (" | payload=", " | mapped="):
            if token in message:
                message = message.split(token, 1)[0]
        return message

    try:
        provider.validate_symbol(venue_arg, normalized_symbol)
        instrument_type = provider.validate_instrument_type(venue_arg, normalized_symbol)
        metadata = provider.get_instrument_metadata(venue_arg, normalized_symbol)
    except Exception as exc:  # pragma: no cover - provider integration
        error_type = type(exc).__name__
        status_code = getattr(exc, "status_code", None)
        logger.warning(
            "instrument_validation_failed | provider=%s venue=%s symbol=%s error=%s | error_type=%s | status_code=%s",
            datasource_id,
            venue_arg,
            normalized_symbol,
            exc,
            error_type,
            status_code,
        )
        sanitized = _sanitize_provider_error(str(exc))
        return None, f"Instrument validation failed: {sanitized}"

    if metadata is None or (metadata.tick_size is None and metadata.tick_value is None):
        return None, "Provider did not return tick metadata for this symbol"

    resolved_type = getattr(instrument_type, "value", instrument_type)
    metadata_payload = metadata.as_dict()
    payload = {
        "symbol": normalized_symbol,
        "datasource": canonical_provider_id or getattr(provider, "get_datasource", lambda: datasource_id)(),
        "exchange": canonical_venue_id or exchange_id or venue_id,
        "instrument_type": resolved_type,
        "metadata": metadata_payload,
    }
    payload = _with_proxy_derivative_reference(payload)

    missing = _missing_behavior_fields(payload)
    if missing:
        return None, f"Instrument metadata missing fields: {', '.join(missing)}"

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


def resolve_or_create_instrument(
    datasource: Optional[str],
    exchange: Optional[str],
    symbol: Optional[str],
    *,
    provider_id: Optional[str] = None,
    venue_id: Optional[str] = None,
    force_refresh: bool = False,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Resolve or create a canonical instrument after provider validation."""

    record, error = validate_instrument(
        datasource,
        exchange,
        symbol,
        provider_id=provider_id,
        venue_id=venue_id,
        force_refresh=force_refresh,
    )
    if error:
        return None, error
    if not record:
        return None, "Instrument validation returned no record."
    return record, None


def instrument_runtime_profile(
    record: Mapping[str, Any],
    *,
    execution_semantics: Optional[object] = None,
) -> Dict[str, Any]:
    """Compile and return the canonical runtime execution profile for an instrument."""

    payload = _with_proxy_derivative_reference(record)
    resolved_execution_semantics = _execution_semantics_for_record(payload, requested=execution_semantics)
    profile = compile_series_execution_profile(
        payload,
        require_margin_accounting=resolved_execution_semantics in {"derivative", "proxy_derivative"},
        execution_semantics=resolved_execution_semantics,
    )
    return {
        "schema_version": "instrument_runtime_profile.v1",
        "instrument_id": payload.get("id"),
        "symbol": payload.get("symbol"),
        "runtime_policy": _runtime_policy_from_execution_semantics(profile.instrument.execution_semantics),
        "runtime_policy_version": "instrument_runtime_policy.v1",
        "profile": profile.to_dict(),
    }


def instrument_runtime_status(record: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Return research/runtime readiness derived from canonical instrument metadata."""

    if not isinstance(record, Mapping) or not record:
        return {
            "research_ready": False,
            "runtime_ready": False,
            "runtime_message": "Instrument metadata is unavailable.",
            "runtime_policy": "unknown",
            "runtime_policy_version": "instrument_runtime_policy.v1",
        }

    payload = _with_proxy_derivative_reference(record)
    execution_semantics = _execution_semantics_for_record(payload)
    try:
        profile_payload = instrument_runtime_profile(payload, execution_semantics=execution_semantics)
        profile = profile_payload["profile"]
    except ValueError as exc:
        return {
            "research_ready": True,
            "runtime_ready": False,
            "runtime_message": str(exc),
            "runtime_policy": _runtime_policy_from_execution_semantics(execution_semantics),
            "runtime_policy_version": "instrument_runtime_policy.v1",
            "execution_semantics": execution_semantics,
        }

    return {
        "research_ready": True,
        "runtime_ready": True,
        "runtime_message": None,
        "runtime_policy": _runtime_policy_from_execution_semantics(
            str(profile.get("instrument", {}).get("execution_semantics") or execution_semantics)
        ),
        "runtime_policy_version": "instrument_runtime_policy.v1",
        "execution_semantics": profile.get("instrument", {}).get("execution_semantics") or execution_semantics,
    }


def instrument_api_payload(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Return an API-safe instrument payload annotated with research/runtime status."""

    payload = _with_proxy_derivative_reference(record)
    payload.update(instrument_runtime_status(payload))
    return payload
