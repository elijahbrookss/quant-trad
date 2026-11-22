"""Bounded, acquisition-only Coinalyze liquidation history; no implicit network I/O."""
from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable

import requests

from data_providers.numeric_facts import (
    NumericAcquisitionBudget, NumericFactBinding, NumericFactProviderError,
    ProviderNumericBatch, ProviderNumericGap, ProviderNumericObservation,
)
from data_providers.services.credential_store import load_credentials

ADAPTER_ID = "coinalyze_liquidation_history.v1"
ENDPOINT_REF = "coinalyze.v1"
_API_URL = "https://api.coinalyze.net/v1"
_INTERVALS = {
    "1min": 60, "5min": 300, "15min": 900, "30min": 1800,
    "1hour": 3600, "2hour": 7200, "4hour": 14400, "6hour": 21600,
    "12hour": 43200, "daily": 86400,
}
_PRODUCT_FIELDS = {
    "symbol", "exchange", "symbol_on_exchange", "base_asset", "quote_asset",
    "is_perpetual", "expire_at", "oi_lq_vol_denominated_in",
}
# Calls made in this process share pacing. Multiple processes still require the
# existing serialized acquisition window; this is not a distributed rate limiter.
_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_AT = 0.0


class CoinalyzeError(NumericFactProviderError):
    """Sanitized upstream/contract failure (never include response body or key)."""

    def __init__(self, message: str, *, retry_after_seconds: int | None = None):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


def _invalid(reason: str) -> CoinalyzeError:
    return CoinalyzeError(f"coinalyze_invalid: {reason}")


def _product(value: object) -> dict:
    if not isinstance(value, dict) or not (_PRODUCT_FIELDS - {"expire_at"}) <= value.keys():
        raise _invalid("catalogue product identity is incomplete")
    product = {key: value.get(key) for key in _PRODUCT_FIELDS}
    for key in ("symbol", "exchange", "symbol_on_exchange", "base_asset", "quote_asset"):
        text = product[key]
        if not isinstance(text, str) or not text or text != text.strip() or len(text) > 128:
            raise _invalid(f"catalogue field {key}")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:/-" for character in product["symbol"]):
        raise _invalid("exactly one catalogue symbol is required")
    if product["base_asset"] != "BTC":
        raise _invalid("this adapter slice admits only an explicitly bound BTC product")
    if type(product["is_perpetual"]) is not bool:
        raise _invalid("is_perpetual must be boolean")
    expiry = product["expire_at"]
    if product["is_perpetual"]:
        if expiry not in (None, 0) or isinstance(expiry, bool):
            raise _invalid("perpetual expiry must be absent or zero")
        product["expire_at"] = None
    elif type(expiry) is not int or expiry <= 0:
        raise _invalid("dated future requires expiry in milliseconds")
    denomination = product["oi_lq_vol_denominated_in"]
    if not isinstance(denomination, str) or denomination not in {"BASE_ASSET", "QUOTE_ASSET", "CONTRACTS"}:
        raise _invalid("unknown liquidation denomination")
    return product


class _JsonDecimal(str):
    """Preserve the original JSON numeric token independently of its Decimal value."""


def _decode(raw: bytes) -> object:
    def reject_constant(_value):
        raise ValueError("nonfinite JSON")
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result
    try:
        return json.loads(raw, parse_float=_JsonDecimal, parse_constant=reject_constant, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError):
        raise _invalid("response must be finite JSON") from None


def _amount(value: object) -> Decimal:
    if isinstance(value, (bool, float)) or not isinstance(value, (int, Decimal, _JsonDecimal)):
        raise _invalid("liquidation total must be a JSON number parsed exactly")
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError):
        raise _invalid("invalid liquidation total") from None
    if not result.is_finite() or result < 0:
        raise _invalid("liquidation total must be finite and nonnegative")
    return result


class CoinalyzeProvider:
    """NumericFactProvider constructed by the existing provider factory.

    Transport injection is the existing acquisition test seam. Construction and
    binding validation never load credentials or contact a provider.
    """

    adapter_id = ADAPTER_ID

    def __init__(self, *, transport: Callable | None = None):
        self._transport = transport or self._http_get

    @staticmethod
    def get_datasource() -> str:
        return "COINALYZE"

    @staticmethod
    def validate_binding(binding: NumericFactBinding) -> dict:
        if binding.adapter != ADAPTER_ID or binding.endpoint_ref != ENDPOINT_REF:
            raise _invalid("adapter/endpoint binding mismatch")
        if binding.fact_type != "derivatives.liquidation_volume":
            raise _invalid("only liquidation volume is supported")
        if dict(binding.source) != {
            "provider": "COINALYZE", "venue": "COINALYZE_AGGREGATE",
            "source_kind": "rest_history", "adapter_version": ADAPTER_ID,
        }:
            raise _invalid("source identity mismatch")
        config = dict(binding.config)
        if set(config) != {"product", "interval", "credential_ref", "confirmations"}:
            raise _invalid("binding config fields do not match adapter contract")
        if type(config["confirmations"]) is not int or config["confirmations"] != 0:
            raise _invalid("REST source requires confirmation_depth=0")
        ref = config["credential_ref"]
        if not isinstance(ref, str) or not ref.strip():
            raise _invalid("opaque credential_ref is required")
        product = _product(config["product"])
        interval = config["interval"]
        seconds = _INTERVALS.get(interval) if isinstance(interval, str) else None
        if seconds is None:
            raise _invalid("unsupported interval")
        denomination = product["oi_lq_vol_denominated_in"]
        unit = {"BASE_ASSET": product["base_asset"],
                "QUOTE_ASSET": product["quote_asset"], "CONTRACTS": "CONTRACTS"}[denomination]
        dimensions = dict(binding.dimensions)
        if (dimensions["exchange"] != product["exchange"]
                or dimensions["product"] != product["symbol_on_exchange"]
                or dimensions["bucket_seconds"] != str(seconds)
                or binding.unit != unit):
            raise _invalid("series identity/unit disagrees with catalogue binding")
        return product

    @staticmethod
    def _http_get(path: str, params: dict, credential_ref: str) -> tuple[bytes, datetime]:
        global _LAST_REQUEST_AT
        if path not in {"future-markets", "liquidation-history"}:
            raise _invalid("unsupported endpoint")
        credentials = load_credentials(
            "COINALYZE", "COINALYZE_AGGREGATE", credential_ref=credential_ref,
        ) or {}
        key = credentials.get("COINALYZE_API_KEY")
        if not isinstance(key, str) or not key.strip():
            raise CoinalyzeError("coinalyze_credentials_missing: configure the encrypted provider reference")
        limit = 16 * 1024 * 1024 if path == "future-markets" else 1024 * 1024
        # No automatic retries. A 429 returns its delay for an explicit later
        # acquisition, without consuming an unbounded retry/sleep window.
        with _REQUEST_LOCK:
            delay = max(0.0, 1.6 - (time.monotonic() - _LAST_REQUEST_AT))
            if delay:
                time.sleep(delay)
            _LAST_REQUEST_AT = time.monotonic()
            try:
                with requests.get(
                    f"{_API_URL}/{path}", params=params, headers={"api_key": key},
                    timeout=(5, 15), stream=True, allow_redirects=False,
                ) as response:
                    if response.status_code != 200:
                        raw_delay = response.headers.get("Retry-After", "")
                        retry_after = int(raw_delay) if raw_delay.isascii() and raw_delay.isdecimal() and len(raw_delay) <= 8 else None
                        raise CoinalyzeError(
                            f"coinalyze_http_failed: endpoint={path} status={response.status_code} retry_after_seconds={retry_after}",
                            retry_after_seconds=retry_after,
                        )
                    body = bytearray()
                    deadline = time.monotonic() + 20
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        body.extend(chunk)
                        if len(body) > limit or time.monotonic() > deadline:
                            raise CoinalyzeError(f"coinalyze_response_budget_exceeded: endpoint={path}")
                    return bytes(body), datetime.now(timezone.utc)
            except requests.RequestException:
                raise CoinalyzeError(f"coinalyze_transport_failed: endpoint={path}") from None

    def fetch_current(self, binding, *, budget):
        raise CoinalyzeError("coinalyze_current_unsupported: request an explicit closed historical range")

    def fetch_history(
        self, binding: NumericFactBinding, *, start: datetime, end: datetime,
        budget: NumericAcquisitionBudget,
    ) -> ProviderNumericBatch:
        product = self.validate_binding(binding)
        interval = binding.config["interval"]
        seconds = _INTERVALS[interval]
        if start.tzinfo is None or end.tzinfo is None or start.microsecond or end.microsecond:
            raise _invalid("range requires timezone-aware whole seconds")
        lower, upper = int(start.timestamp()), int(end.timestamp())
        count = (upper - lower) // seconds
        if lower < 0 or upper <= lower or lower % seconds or upper % seconds:
            raise _invalid("range must align to closed bucket boundaries")
        if count > 1500 or count > budget.max_logs or budget.max_requests < 2:
            raise CoinalyzeError("coinalyze_acquisition_budget_exceeded: at most 1500 buckets and two requests required")
        if end > datetime.now(timezone.utc):
            raise _invalid("open/future buckets are not supported")
        ref = binding.config["credential_ref"]
        catalogue_raw, _ = self._transport("future-markets", {}, ref)
        if len(catalogue_raw) > 16 * 1024 * 1024:
            raise _invalid("catalogue response exceeds byte bound")
        catalogue = _decode(catalogue_raw)
        if not isinstance(catalogue, list):
            raise _invalid("catalogue must be an array")
        matches = [row for row in catalogue if isinstance(row, dict) and row.get("symbol") == product["symbol"]]
        if len(matches) != 1 or _product(matches[0]) != product:
            raise _invalid("catalogue product missing, duplicated or changed; review binding")
        params = {"symbols": product["symbol"], "interval": interval,
                  "from": lower, "to": upper - 1, "convert_to_usd": "false"}
        raw, observed_at = self._transport("liquidation-history", params, ref)
        if len(raw) > 1024 * 1024:
            raise _invalid("history response exceeds byte bound")
        if observed_at.tzinfo is None or observed_at < end:
            raise _invalid("receipt must follow the requested closed range")
        payload = _decode(raw)
        if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
            raise _invalid("expected exactly one history product")
        history = payload[0].get("history")
        if payload[0].get("symbol") != product["symbol"] or not isinstance(history, list) or len(history) > count:
            raise _invalid("history product/count mismatch")
        observations, seen = [], set()
        side = binding.dimensions["liquidated_side"]
        field = {"long": "l", "short": "s"}[side]
        response_hash = hashlib.sha256(raw).hexdigest()
        catalogue_hash = hashlib.sha256(catalogue_raw).hexdigest()
        for row in history:
            if not isinstance(row, dict) or not {"t", "l", "s"} <= row.keys():
                raise _invalid("history bucket fields missing")
            stamp = row["t"]
            if type(stamp) is not int or stamp in seen or not lower <= stamp < upper or stamp % seconds:
                raise _invalid("duplicate, out-of-range or unaligned bucket")
            seen.add(stamp)
            amounts = {name: _amount(row[name]) for name in ("l", "s")}
            event = f"{product['symbol']}:{seconds}:{stamp}"
            material = {"product": product, "interval": interval, "t": stamp,
                        "side": side, "value": str(amounts[field]), "convert_to_usd": False}
            observations.append(ProviderNumericObservation(
                value=amounts[field], raw_value=str(row[field]),
                effective_at=datetime.fromtimestamp(stamp, tz=timezone.utc),
                effective_at_method="provider_bucket_start", source_published_at=None,
                known_at=observed_at, known_at_method="platform_receipt",
                source_event_key=f"{event}:{side}", source_event_group_key=event,
                source_event_component_key=side, source_event_material=material,
                provenance={**material, "bucket_end": stamp + seconds,
                            "received_at": observed_at.isoformat(), "response_hash": response_hash,
                            "catalogue_response_hash": catalogue_hash,
                            "source_path": "/v1/liquidation-history",
                            "publication_time_available": False,
                            "source_completeness": "unverified"},
            ))
        gaps = [ProviderNumericGap(
            classification="provider_completeness_unverified", start=start, end=end,
            evidence={"expected_bucket_count": count, "observed_bucket_count": len(history),
                      "missing_is_zero": False, "response_hash": response_hash},
        )]
        # Coalesce missing buckets; never synthesize zero-valued observations.
        gap_start = None
        for stamp in range(lower, upper + seconds, seconds):
            if stamp < upper and stamp not in seen:
                gap_start = stamp if gap_start is None else gap_start
            elif gap_start is not None:
                gaps.append(ProviderNumericGap(
                    classification="provider_bucket_missing",
                    start=datetime.fromtimestamp(gap_start, tz=timezone.utc),
                    end=datetime.fromtimestamp(stamp, tz=timezone.utc),
                ))
                gap_start = None
        return ProviderNumericBatch(
            observations=tuple(sorted(observations, key=lambda item: item.effective_at)),
            gaps=tuple(gaps), range_start=start, range_end=end,
            source_position_start=str(lower), source_position_end=str(upper),
            source_position_head=None, status="partial",
            capabilities={"publication_time_available": False, "source_completeness": "unverified",
                          "closed_buckets_only": True, "exact_numeric": True},
            request={"mode": "historical", **params, "product": product},
            budget_requests_used=2, budget_logs_used=len(history), budget_blocks_scanned=0,
        )
