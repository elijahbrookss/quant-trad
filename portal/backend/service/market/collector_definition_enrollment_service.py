"""Explicit product enrollment through deployed collector adapter packs."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

from market_data.stream_enrollment import (
    StreamEnrollment,
    StreamEnrollmentManifest,
    load_stream_enrollment_manifest,
)
from market_data.structure import ProductContract

from . import instrument_service
from .collector_service import market_data_collector
from .market_structure_service import market_structure_service


COINBASE_PRODUCT_COLLECTORS = (
    "open_interest",
    "funding_rate",
    "market_trades",
    "level2",
)
_REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
_COINBASE_TRADE_TEMPLATE = (
    _REPOSITORY_ROOT
    / "config"
    / "market_data"
    / "coinbase_perpetual_trade_fleet.v1.json"
)
_COINBASE_L2_TEMPLATE = (
    _REPOSITORY_ROOT
    / "config"
    / "market_data"
    / "coinbase_perpetual_l2_fleet.v1.json"
)
_COINBASE_PRODUCT_PATTERN = re.compile(r"[A-Z0-9][A-Z0-9._-]{1,127}")


def _required(value: Any, *, field: str, max_length: int = 500) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"collector_product_enrollment_invalid: {field} is required")
    if len(normalized) > max_length:
        raise ValueError(
            f"collector_product_enrollment_invalid: {field} exceeds {max_length} characters"
        )
    return normalized


def _positive_decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"collector_product_enrollment_invalid: {field} must be decimal"
        ) from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(
            f"collector_product_enrollment_invalid: {field} must be positive"
        )
    return result


def product_enrollment_confirmation(
    *, provider: str, venue: str, product_id: str
) -> str:
    return f"{provider.upper()}:{venue.upper()}:{product_id.upper()}:enroll"


class ProductCollectorPack(Protocol):
    provider: str
    venue: str
    collector_types: tuple[str, ...]

    def enroll(
        self,
        *,
        product_id: str,
        collector_types: tuple[str, ...],
        poll_interval_seconds: int,
    ) -> dict[str, Any]: ...


class CoinbaseFuturesCollectorPack:
    """Enroll any validated Coinbase future through the reviewed V1 adapters."""

    provider = "COINBASE"
    venue = "COINBASE_DIRECT"
    collector_types = COINBASE_PRODUCT_COLLECTORS

    def __init__(
        self,
        *,
        instrument_resolver: Callable[..., tuple[dict[str, Any] | None, str | None]] = (
            instrument_service.resolve_or_create_instrument
        ),
        stream_service: Any = market_structure_service,
        scheduled_collector: Any = market_data_collector,
        trade_template_path: Path | str = _COINBASE_TRADE_TEMPLATE,
        l2_template_path: Path | str = _COINBASE_L2_TEMPLATE,
    ) -> None:
        self.instrument_resolver = instrument_resolver
        self.stream_service = stream_service
        self.scheduled_collector = scheduled_collector
        self.trade_template_path = Path(trade_template_path)
        self.l2_template_path = Path(l2_template_path)

    @staticmethod
    def _instrument_contract(
        instrument: Mapping[str, Any], *, product_id: str
    ) -> ProductContract:
        symbol = str(instrument.get("symbol") or "").strip().upper()
        instrument_type = str(instrument.get("instrument_type") or "").strip().lower()
        if symbol != product_id:
            raise ValueError(
                "collector_product_enrollment_invalid: provider product and instrument symbol disagree"
            )
        if instrument_type != "future":
            raise ValueError(
                "collector_product_enrollment_invalid: Coinbase product must be a future"
            )
        metadata = (
            instrument.get("metadata")
            if isinstance(instrument.get("metadata"), Mapping)
            else {}
        )
        fields = (
            metadata.get("instrument_fields")
            if isinstance(metadata.get("instrument_fields"), Mapping)
            else {}
        )
        provider_metadata = (
            metadata.get("provider_metadata")
            if isinstance(metadata.get("provider_metadata"), Mapping)
            else {}
        )
        product = (
            provider_metadata.get("product")
            if isinstance(provider_metadata.get("product"), Mapping)
            else {}
        )
        future_details = (
            product.get("future_product_details")
            if isinstance(product.get("future_product_details"), Mapping)
            else {}
        )
        contract_size = _positive_decimal(
            future_details.get("contract_size", fields.get("contract_size")),
            field="contract_size",
        )
        _positive_decimal(fields.get("tick_size"), field="tick_size")
        _positive_decimal(fields.get("qty_step"), field="qty_step")
        base_currency = _required(
            fields.get("base_currency"), field="base_currency", max_length=32
        ).upper()
        quote_currency = _required(
            fields.get("quote_currency"), field="quote_currency", max_length=32
        ).upper()
        return ProductContract(
            provider_product_id=product_id,
            provider_size_unit="contracts",
            base_currency=base_currency,
            quote_currency=quote_currency,
            product_definition_version_id=(
                f"coinbase.{product_id}.product_contract.v1"
            ),
            contract_size=contract_size,
        )

    @staticmethod
    def _single_product_manifest(
        *,
        template_path: Path,
        instrument_id: str,
        product_id: str,
        product_contract: ProductContract,
    ) -> StreamEnrollmentManifest:
        template_manifest = load_stream_enrollment_manifest(template_path)
        if len(template_manifest.enrollments) < 1:
            raise RuntimeError(
                "collector_product_enrollment_invalid: stream template is empty"
            )
        template = template_manifest.enrollments[0]
        primary_channel = template.channels[0]
        enrollment = StreamEnrollment(
            enrollment_id=f"coinbase.{product_id}.{primary_channel}.v1",
            fleet_id=template_manifest.fleet_id,
            instrument_id=instrument_id,
            product_type="future",
            provider=template.provider,
            venue=template.venue,
            channels=template.channels,
            auth_mode=template.auth_mode,
            adapter_version=template.adapter_version,
            contract_version=template.contract_version,
            max_spool_bytes=template.max_spool_bytes,
            max_segment_bytes=template.max_segment_bytes,
            continuous=True,
            product_contract=product_contract,
        )
        return StreamEnrollmentManifest(
            schema_version=template_manifest.schema_version,
            fleet_id=template_manifest.fleet_id,
            safety_policy=template_manifest.safety_policy,
            enrollments=(enrollment,),
        )

    def enroll(
        self,
        *,
        product_id: str,
        collector_types: tuple[str, ...],
        poll_interval_seconds: int,
    ) -> dict[str, Any]:
        normalized_product = str(product_id or "").strip().upper()
        if not _COINBASE_PRODUCT_PATTERN.fullmatch(normalized_product):
            raise ValueError(
                "collector_product_enrollment_invalid: Coinbase product ID has invalid characters"
            )
        instrument, error = self.instrument_resolver(
            self.provider,
            self.venue,
            normalized_product,
            provider_id=self.provider,
            venue_id=self.venue,
            force_refresh=True,
        )
        if error or not instrument:
            raise ValueError(
                "collector_product_enrollment_provider_validation_failed: "
                f"product_id={normalized_product} error={error or 'no instrument returned'}"
            )
        product_contract = self._instrument_contract(
            instrument, product_id=normalized_product
        )
        instrument_id = _required(
            instrument.get("id"), field="instrument_id", max_length=128
        )
        fields = dict(
            (instrument.get("metadata") or {}).get("instrument_fields") or {}
        )
        if "funding_rate" in collector_types and not bool(fields.get("has_funding")):
            raise ValueError(
                "collector_product_enrollment_invalid: funding_rate requires a funding-enabled product"
            )

        definitions: dict[str, Any] = {}
        if "market_trades" in collector_types:
            manifest = self._single_product_manifest(
                template_path=self.trade_template_path,
                instrument_id=instrument_id,
                product_id=normalized_product,
                product_contract=product_contract,
            )
            definitions["market_trades"] = (
                self.stream_service.apply_stream_enrollment_manifest(
                    manifest=manifest
                )
            )
        if "level2" in collector_types:
            manifest = self._single_product_manifest(
                template_path=self.l2_template_path,
                instrument_id=instrument_id,
                product_id=normalized_product,
                product_contract=product_contract,
            )
            definitions["level2"] = (
                self.stream_service.apply_stream_enrollment_manifest(
                    manifest=manifest
                )
            )
        common = {
            "instrument_id": instrument_id,
            "provider_product_id": normalized_product,
            "poll_interval_seconds": poll_interval_seconds,
            "max_attempts": 3,
            "minimum_spacing_seconds": 1.0,
            "enabled": True,
        }
        if "open_interest" in collector_types:
            definitions["open_interest"] = (
                self.scheduled_collector.create_coinbase_open_interest_definition(
                    **common
                )
            )
        if "funding_rate" in collector_types:
            definitions["funding_rate"] = (
                self.scheduled_collector.create_coinbase_funding_rate_definition(
                    **common
                )
            )
        return {
            "provider": self.provider,
            "venue": self.venue,
            "product_id": normalized_product,
            "instrument": dict(instrument),
            "product_contract": {
                "provider_size_unit": "contracts",
                "contract_size": str(product_contract.contract_size),
                "base_currency": product_contract.base_currency,
                "quote_currency": product_contract.quote_currency,
                "product_definition_version_id": (
                    product_contract.product_definition_version_id
                ),
            },
            "definitions": definitions,
        }


class CollectorDefinitionEnrollmentService:
    """Route product enrollment only through deployed, registered adapter packs."""

    def __init__(self, packs: Iterable[ProductCollectorPack]) -> None:
        self._packs: dict[tuple[str, str], ProductCollectorPack] = {}
        for pack in packs:
            key = (pack.provider.upper(), pack.venue.upper())
            if key in self._packs:
                raise ValueError(
                    "collector_product_enrollment_invalid: duplicate adapter pack"
                )
            self._packs[key] = pack

    def enroll_product(
        self,
        *,
        provider: str,
        venue: str,
        product_id: str,
        collector_types: Iterable[str] = COINBASE_PRODUCT_COLLECTORS,
        poll_interval_seconds: int = 60,
        request_id: str,
        actor_id: str,
        reason: str,
        confirmation: str,
    ) -> dict[str, Any]:
        normalized_provider = _required(
            provider, field="provider", max_length=64
        ).upper()
        normalized_venue = _required(venue, field="venue", max_length=64).upper()
        normalized_product = _required(
            product_id, field="product_id", max_length=128
        ).upper()
        request = _required(request_id, field="request_id", max_length=128)
        actor = _required(actor_id, field="actor_id", max_length=128)
        explanation = _required(reason, field="reason", max_length=500)
        expected_confirmation = product_enrollment_confirmation(
            provider=normalized_provider,
            venue=normalized_venue,
            product_id=normalized_product,
        )
        if str(confirmation or "").strip() != expected_confirmation:
            raise ValueError(
                "collector_product_enrollment_confirmation_required: "
                f"expected={expected_confirmation}"
            )
        pack = self._packs.get((normalized_provider, normalized_venue))
        if pack is None:
            raise ValueError(
                "collector_product_enrollment_adapter_unsupported: "
                f"provider={normalized_provider} venue={normalized_venue}"
            )
        requested_collectors = tuple(
            dict.fromkeys(
                str(value or "").strip().lower() for value in collector_types
            )
        )
        if not requested_collectors:
            raise ValueError(
                "collector_product_enrollment_invalid: collector_types is empty"
            )
        unsupported = sorted(set(requested_collectors) - set(pack.collector_types))
        if unsupported:
            raise ValueError(
                "collector_product_enrollment_collector_unsupported: "
                + ",".join(unsupported)
            )
        interval = int(poll_interval_seconds)
        if interval < 10 or interval > 86400:
            raise ValueError(
                "collector_product_enrollment_invalid: poll interval must be between 10 and 86400 seconds"
            )
        result = pack.enroll(
            product_id=normalized_product,
            collector_types=requested_collectors,
            poll_interval_seconds=interval,
        )
        return {
            "schema_version": "market.collector_product_enrollment.v1",
            "status": "enrolled",
            "request_id": request,
            "actor_id": actor,
            "reason": explanation,
            "enrolled_at": datetime.now(UTC).isoformat(),
            "collector_types": list(requested_collectors),
            "initial_desired_state": "running",
            **result,
        }


collector_definition_enrollment_service = CollectorDefinitionEnrollmentService(
    (CoinbaseFuturesCollectorPack(),)
)


__all__ = [
    "COINBASE_PRODUCT_COLLECTORS",
    "CoinbaseFuturesCollectorPack",
    "CollectorDefinitionEnrollmentService",
    "collector_definition_enrollment_service",
    "product_enrollment_confirmation",
]
