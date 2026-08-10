"""Provider-neutral envelope for schema-registered canonical market Facts."""

from __future__ import annotations

import hashlib
import json
import math
import struct
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from .contracts import SourceIdentity
from .fact_registry import get_fact_payload_schema


FACT_MATERIAL_HASH_VERSION = "market.fact_material.v1"
FACT_PROVENANCE_HASH_VERSION = "market.fact_provenance_hash.v1"
FACT_QUALITY_HASH_VERSION = "market.fact_quality_hash.v1"
FACT_ROW_HASH_VERSION = "market.fact_row.v1"
FACT_VERSION_ID_VERSION = "market.fact_version_id.v1"

_RECEIPT_KNOWN_AT_METHODS = frozenset(
    {"platform_acceptance", "platform_receipt", "stream_receipt"}
)


class FactState(str, Enum):
    ACTIVE = "active"
    INVALIDATED = "invalidated"


def _utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"canonical_fact_invalid: {field_name} must be datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _json_value(value: Any, *, field_name: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                f"canonical_fact_invalid: {field_name} contains non-finite float"
            )
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(
                f"canonical_fact_invalid: {field_name} contains non-finite decimal"
            )
        if value.is_zero():
            return "0"
        normalized = format(value.normalize(), "f")
        return (
            normalized.rstrip("0").rstrip(".")
            if "." in normalized
            else normalized
        )
    if isinstance(value, datetime):
        return _time(_utc(value, field_name=field_name))
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item, field_name=f"{field_name}.{key}")
            for key, item in sorted(value.items(), key=lambda row: str(row[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _json_value(item, field_name=f"{field_name}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ValueError(
        f"canonical_fact_invalid: {field_name} is not canonical JSON material"
    )


def _hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _optional_key(value: str | None, *, field_name: str) -> str | None:
    normalized = str(value or "").strip() or None
    if normalized is not None and len(normalized) > 512:
        raise ValueError(
            f"canonical_fact_invalid: {field_name} exceeds 512 characters"
        )
    return normalized


@dataclass(frozen=True)
class CanonicalFact:
    """One atomic, typed provider-neutral observation before storage identity."""

    fact_type: str
    payload_schema_id: str
    observation_key: str
    observation_time: datetime
    observation_time_method: str
    accepted_at: datetime
    known_at: datetime
    known_at_method: str
    source: SourceIdentity
    transformation_id: str
    payload: Mapping[str, Any]
    source_published_at: datetime | None = None
    received_at: datetime | None = None
    external_event_key: str | None = None
    external_event_group_key: str | None = None
    external_event_component_key: str | None = None
    state: FactState | str = FactState.ACTIVE
    provenance_schema_id: str = "market.fact_provenance.v1"
    provenance: Mapping[str, Any] = field(default_factory=dict)
    quality_schema_id: str = "market.fact_quality.v1"
    quality: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        fact_type = str(self.fact_type or "").strip().lower()
        schema = get_fact_payload_schema(self.payload_schema_id)
        if fact_type != schema.fact_type:
            raise ValueError(
                "canonical_fact_invalid: fact type and payload schema disagree "
                f"fact_type={fact_type or '<missing>'} schema_id={schema.schema_id}"
            )
        observation_key = str(self.observation_key or "").strip()
        if not observation_key or len(observation_key) > 512:
            raise ValueError(
                "canonical_fact_invalid: observation_key is required and bounded"
            )
        observation_time_method = str(self.observation_time_method or "").strip()
        known_at_method = str(self.known_at_method or "").strip()
        transformation_id = str(self.transformation_id or "").strip()
        provenance_schema_id = str(self.provenance_schema_id or "").strip()
        quality_schema_id = str(self.quality_schema_id or "").strip()
        if not all(
            (
                observation_time_method,
                known_at_method,
                transformation_id,
                provenance_schema_id,
                quality_schema_id,
            )
        ):
            raise ValueError(
                "canonical_fact_invalid: clock, transformation, provenance, and quality identities are required"
            )
        observation_time = _utc(self.observation_time, field_name="observation_time")
        accepted_at = _utc(self.accepted_at, field_name="accepted_at")
        known_at = _utc(self.known_at, field_name="known_at")
        source_published_at = (
            _utc(self.source_published_at, field_name="source_published_at")
            if self.source_published_at is not None
            else None
        )
        received_at = (
            _utc(self.received_at, field_name="received_at")
            if self.received_at is not None
            else None
        )
        if received_at is not None and accepted_at < received_at:
            raise ValueError(
                "canonical_fact_invalid: accepted_at precedes received_at"
            )
        if known_at_method in _RECEIPT_KNOWN_AT_METHODS and known_at < accepted_at:
            raise ValueError(
                "canonical_fact_invalid: receipt-based known_at precedes acceptance"
            )
        try:
            state = FactState(str(getattr(self.state, "value", self.state)).lower())
        except ValueError as exc:
            raise ValueError("canonical_fact_invalid: unsupported state") from exc
        normalized_payload = schema.normalize_payload(self.payload)
        provenance = _json_value(self.provenance, field_name="provenance")
        quality = _json_value(self.quality, field_name="quality")
        assert isinstance(provenance, dict) and isinstance(quality, dict)
        object.__setattr__(self, "fact_type", fact_type)
        object.__setattr__(self, "payload_schema_id", schema.schema_id)
        object.__setattr__(self, "observation_key", observation_key)
        object.__setattr__(self, "observation_time", observation_time)
        object.__setattr__(self, "observation_time_method", observation_time_method)
        object.__setattr__(self, "accepted_at", accepted_at)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "known_at_method", known_at_method)
        object.__setattr__(self, "transformation_id", transformation_id)
        object.__setattr__(self, "source_published_at", source_published_at)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(
            self,
            "external_event_key",
            _optional_key(self.external_event_key, field_name="external_event_key"),
        )
        object.__setattr__(
            self,
            "external_event_group_key",
            _optional_key(
                self.external_event_group_key,
                field_name="external_event_group_key",
            ),
        )
        object.__setattr__(
            self,
            "external_event_component_key",
            _optional_key(
                self.external_event_component_key,
                field_name="external_event_component_key",
            ),
        )
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "provenance_schema_id", provenance_schema_id)
        object.__setattr__(self, "provenance", provenance)
        object.__setattr__(self, "quality_schema_id", quality_schema_id)
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "payload", normalized_payload)

    @property
    def payload_contract_hash(self) -> str:
        return get_fact_payload_schema(self.payload_schema_id).contract_hash

    @property
    def payload_hash(self) -> str:
        return _hash(
            {
                "schema_version": "market.fact_payload_hash.v1",
                "payload_schema_id": self.payload_schema_id,
                "payload_contract_hash": self.payload_contract_hash,
                "payload": dict(self.payload),
            }
        )

    def _schema_owned_material_hash(self) -> str | None:
        """Reproduce retained structured-v1 material identities."""

        if self.payload_schema_id == "market.trade.v1":
            evidence = self.provenance.get("_qt_trade_evidence")
            if not isinstance(evidence, Mapping):
                return None
            required = (
                "provider_product_id",
                "provider_trade_id",
                "aggressor_transform_version",
                "product_definition_version_id",
            )
            if any(name not in evidence for name in required):
                return None
            return _hash(
                {
                    "schema_version": "market.trade_material_hash.v1",
                    "provider_product_id": evidence["provider_product_id"],
                    "provider_trade_id": evidence["provider_trade_id"],
                    "price": self.payload["price"],
                    "provider_size": self.payload["reported_quantity"],
                    "provider_size_unit": self.payload["reported_quantity_unit"],
                    "maker_side": self.payload["maker_side"],
                    "aggressor_side": self.payload["aggressor_side"],
                    "aggressor_transform_version": evidence[
                        "aggressor_transform_version"
                    ],
                    "contract_quantity": self.payload["contract_quantity"],
                    "base_quantity": self.payload["base_quantity"],
                    "quote_notional": self.payload["quote_notional"],
                    "base_currency": self.payload["base_currency"],
                    "quote_currency": self.payload["quote_currency"],
                    "product_definition_version_id": evidence[
                        "product_definition_version_id"
                    ],
                    "provider_event_time": _time(self.observation_time),
                }
            )
        if self.payload_schema_id == "market.trade_flow.v1":
            evidence = self.provenance.get("_qt_trade_flow_evidence")
            quality = self.quality.get("_qt_trade_flow_quality")
            if not isinstance(evidence, Mapping) or not isinstance(quality, Mapping):
                return None
            required_evidence = (
                "interval_seconds",
                "first_trade_id",
                "last_trade_id",
                "first_receive_ordinal",
                "last_receive_ordinal",
                "coverage_interval_id",
                "coverage_revision",
                "input_fingerprint",
            )
            required_quality = (
                "aggregate_complete",
                "archive_complete",
                "canonicalization_complete",
                "late_trade_count",
            )
            if any(name not in evidence for name in required_evidence) or any(
                name not in quality for name in required_quality
            ):
                return None
            return _hash(
                {
                    "schema_version": "market.trade_flow_material_hash.v1",
                    "interval_seconds": evidence["interval_seconds"],
                    "bucket_start": _time(self.observation_time),
                    "bucket_end": self.payload["bucket_end"],
                    "trade_count": self.payload["trade_count"],
                    "maker_buy_count": self.payload["maker_buy_count"],
                    "maker_sell_count": self.payload["maker_sell_count"],
                    "aggressor_buy_count": self.payload["aggressor_buy_count"],
                    "aggressor_sell_count": self.payload["aggressor_sell_count"],
                    "contract_volume": self.payload["contract_volume"],
                    "base_volume": self.payload["base_volume"],
                    "quote_notional": self.payload["quote_notional"],
                    "maker_buy_base_volume": self.payload[
                        "maker_buy_base_volume"
                    ],
                    "maker_sell_base_volume": self.payload[
                        "maker_sell_base_volume"
                    ],
                    "aggressor_buy_base_volume": self.payload[
                        "aggressor_buy_base_volume"
                    ],
                    "aggressor_sell_base_volume": self.payload[
                        "aggressor_sell_base_volume"
                    ],
                    "cvd_delta": self.payload["cvd_delta"],
                    "cvd_unit": self.payload["cvd_unit"],
                    "open_price": self.payload["open_price"],
                    "high_price": self.payload["high_price"],
                    "low_price": self.payload["low_price"],
                    "close_price": self.payload["close_price"],
                    "first_trade_id": evidence["first_trade_id"],
                    "last_trade_id": evidence["last_trade_id"],
                    "first_receive_ordinal": evidence["first_receive_ordinal"],
                    "last_receive_ordinal": evidence["last_receive_ordinal"],
                    "coverage_interval_id": evidence["coverage_interval_id"],
                    "coverage_revision": evidence["coverage_revision"],
                    "aggregate_complete": quality["aggregate_complete"],
                    "archive_complete": quality["archive_complete"],
                    "canonicalization_complete": quality[
                        "canonicalization_complete"
                    ],
                    "late_trade_count": quality["late_trade_count"],
                    "input_fingerprint": evidence["input_fingerprint"],
                }
            )
        return None

    @property
    def material_hash(self) -> str:
        schema_owned = self._schema_owned_material_hash()
        if schema_owned is not None:
            return schema_owned
        schema = get_fact_payload_schema(self.payload_schema_id)
        return _hash(
            {
                "schema_version": schema.material_hash_version,
                "fact_type": self.fact_type,
                "payload_schema_id": self.payload_schema_id,
                "observation_key": self.observation_key,
                "observation_time": _time(self.observation_time),
                "state": self.state.value,
                "payload_hash": self.payload_hash,
            }
        )

    @property
    def provenance_hash(self) -> str:
        return _hash(
            {
                "schema_version": FACT_PROVENANCE_HASH_VERSION,
                "source_identity_key": self.source.identity_key,
                "transformation_id": self.transformation_id,
                "source_published_at": _time(self.source_published_at),
                "received_at": _time(self.received_at),
                "accepted_at": _time(self.accepted_at),
                "known_at": _time(self.known_at),
                "known_at_method": self.known_at_method,
                "external_event_key": self.external_event_key,
                "external_event_group_key": self.external_event_group_key,
                "external_event_component_key": self.external_event_component_key,
                "provenance_schema_id": self.provenance_schema_id,
                "provenance": dict(self.provenance),
            }
        )

    @property
    def quality_hash(self) -> str:
        return _hash(
            {
                "schema_version": FACT_QUALITY_HASH_VERSION,
                "quality_schema_id": self.quality_schema_id,
                "quality": dict(self.quality),
            }
        )

    def _historical_row_hash(self) -> str | None:
        """Reproduce retained v1 evidence identities from canonical fields."""

        def float64(value: Any) -> str | None:
            return None if value is None else struct.pack("!d", float(value)).hex()

        if self.payload_schema_id == "candle.ohlcv.v1":
            return _hash(
                {
                    "schema_version": self.payload_schema_id,
                    "open_time": _time(self.observation_time),
                    "close_time": self.payload["close_time"],
                    "open": float64(self.payload["open"]),
                    "high": float64(self.payload["high"]),
                    "low": float64(self.payload["low"]),
                    "close": float64(self.payload["close"]),
                    "volume": float64(self.payload.get("volume")),
                    "trade_count": self.payload.get("trade_count"),
                    "source_published_at": _time(self.source_published_at),
                    "received_at": _time(self.received_at),
                    "known_at": _time(self.known_at),
                    "known_at_method": self.known_at_method,
                }
            )
        if self.payload_schema_id == "derivatives.open_interest.v1":
            return _hash(
                {
                    "schema_version": self.payload_schema_id,
                    "sample_time": _time(self.observation_time),
                    "sample_time_method": self.observation_time_method,
                    "value": float64(self.payload["value"]),
                    "unit": self.payload["unit"],
                    "source_published_at": _time(self.source_published_at),
                    "received_at": _time(self.received_at),
                    "known_at": _time(self.known_at),
                    "known_at_method": self.known_at_method,
                }
            )
        if self.payload_schema_id == "derivatives.funding_rate.v1":
            return _hash(
                {
                    "schema_version": self.payload_schema_id,
                    "sample_time": _time(self.observation_time),
                    "sample_time_method": self.observation_time_method,
                    "rate": float64(self.payload["rate"]),
                    "funding_time": self.payload["funding_time"],
                    "interval_seconds": self.payload["interval_seconds"],
                    "unit": self.payload["unit"],
                    "source_published_at": _time(self.source_published_at),
                    "received_at": _time(self.received_at),
                    "known_at": _time(self.known_at),
                    "known_at_method": self.known_at_method,
                }
            )
        if self.payload_schema_id in {
            "market.reference_price.v1",
            "market.reserve_balance.v1",
        }:
            evidence = self.provenance.get("_qt_numeric_evidence")
            if not isinstance(evidence, Mapping):
                migration = self.provenance.get("_qt_migration")
                evidence = migration if isinstance(migration, Mapping) else None
            if evidence is None:
                return None
            dimensions = evidence.get("series_dimensions")
            source_material_hash = evidence.get("source_event_material_hash")
            if not isinstance(dimensions, Mapping) or not source_material_hash:
                return None
            return _hash(
                {
                    "schema_version": self.payload_schema_id,
                    "fact_type": self.fact_type,
                    "value": self.payload["value"],
                    "raw_value": self.payload["raw_value"],
                    "unit": self.payload["unit"],
                    "dimensions": dict(dimensions),
                    "effective_at": _time(self.observation_time),
                    "effective_at_method": self.observation_time_method,
                    "source_published_at": _time(self.source_published_at),
                    "received_at": _time(self.received_at),
                    "known_at": _time(self.known_at),
                    "known_at_method": self.known_at_method,
                    "source_event_key": self.external_event_key
                    or self.observation_key,
                    "source_event_group_key": self.external_event_group_key,
                    "source_event_component_key": self.external_event_component_key,
                    "source_event_material_hash": str(source_material_hash),
                    "state": self.state.value,
                }
            )
        if self.payload_schema_id == "market.trade.v1":
            evidence = self.provenance.get("_qt_trade_evidence")
            if not isinstance(evidence, Mapping):
                return None
            required = (
                "delivery_kind",
                "provider_message_time",
                "provider_sequence_num",
                "connection_epoch",
                "receive_ordinal",
                "event_ordinal",
                "trade_ordinal",
                "raw_record_id",
                "coverage_interval_id",
            )
            if any(name not in evidence for name in required):
                return None
            return _hash(
                {
                    "schema_version": "market.trade.v1",
                    "material_hash": self.material_hash,
                    "delivery_kind": evidence["delivery_kind"],
                    "provider_message_time": evidence["provider_message_time"],
                    "received_at": _time(self.received_at),
                    "accepted_at": _time(self.accepted_at),
                    "known_at": _time(self.known_at),
                    "provider_sequence_num": evidence["provider_sequence_num"],
                    "connection_epoch": evidence["connection_epoch"],
                    "receive_ordinal": evidence["receive_ordinal"],
                    "event_ordinal": evidence["event_ordinal"],
                    "trade_ordinal": evidence["trade_ordinal"],
                    "raw_record_id": evidence["raw_record_id"],
                    "coverage_interval_id": evidence["coverage_interval_id"],
                }
            )
        return None

    @property
    def row_hash(self) -> str:
        historical = self._historical_row_hash()
        if historical is not None:
            return historical
        schema = get_fact_payload_schema(self.payload_schema_id)
        return _hash(
            {
                "schema_version": schema.row_hash_version,
                "material_hash": self.material_hash,
                "provenance_hash": self.provenance_hash,
                "quality_hash": self.quality_hash,
                "observation_time_method": self.observation_time_method,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "fact_type": self.fact_type,
            "payload_schema_id": self.payload_schema_id,
            "payload_contract_hash": self.payload_contract_hash,
            "observation_key": self.observation_key,
            "observation_time": _time(self.observation_time),
            "observation_time_method": self.observation_time_method,
            "source_published_at": _time(self.source_published_at),
            "received_at": _time(self.received_at),
            "accepted_at": _time(self.accepted_at),
            "known_at": _time(self.known_at),
            "known_at_method": self.known_at_method,
            "source": {
                "identity_key": self.source.identity_key,
                "provider": self.source.provider,
                "venue": self.source.venue,
                "source_kind": self.source.source_kind,
                "adapter_version": self.source.adapter_version,
            },
            "transformation_id": self.transformation_id,
            "external_event_key": self.external_event_key,
            "external_event_group_key": self.external_event_group_key,
            "external_event_component_key": self.external_event_component_key,
            "state": self.state.value,
            "payload": dict(self.payload),
            "payload_hash": self.payload_hash,
            "material_hash": self.material_hash,
            "provenance_schema_id": self.provenance_schema_id,
            "provenance": dict(self.provenance),
            "provenance_hash": self.provenance_hash,
            "quality_schema_id": self.quality_schema_id,
            "quality": dict(self.quality),
            "quality_hash": self.quality_hash,
            "row_hash": self.row_hash,
        }


def build_fact_version_id(
    *, series_id: int, observation_key: str, revision: int, row_hash: str
) -> str:
    if int(series_id) <= 0 or int(revision) <= 0:
        raise ValueError("canonical_fact_record_invalid: identity must be positive")
    if len(str(row_hash or "")) != 64:
        raise ValueError("canonical_fact_record_invalid: row_hash is invalid")
    digest = _hash(
        {
            "schema_version": FACT_VERSION_ID_VERSION,
            "series_id": int(series_id),
            "observation_key": str(observation_key),
            "revision": int(revision),
            "row_hash": str(row_hash),
        }
    )
    return f"mfv_{digest[:40]}"


@dataclass(frozen=True)
class CanonicalFactRecord:
    """A canonical Fact plus immutable database revision identity."""

    series_id: int
    source_id: int
    revision: int
    market_commit_seq: int
    fact: CanonicalFact
    ingestion_run_id: str | None = None
    row_hash: str | None = None
    fact_version_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("series_id", "source_id", "revision", "market_commit_seq"):
            value = int(getattr(self, name))
            if value <= 0:
                raise ValueError(
                    f"canonical_fact_record_invalid: {name} must be positive"
                )
            object.__setattr__(self, name, value)
        ingestion_run_id = str(self.ingestion_run_id or "").strip() or None
        row_hash = str(self.row_hash or self.fact.row_hash).strip().lower()
        if len(row_hash) != 64 or any(
            character not in "0123456789abcdef" for character in row_hash
        ):
            raise ValueError(
                "canonical_fact_record_invalid: row_hash must be sha256"
            )
        fact_version_id = str(self.fact_version_id or "").strip() or build_fact_version_id(
            series_id=self.series_id,
            observation_key=self.fact.observation_key,
            revision=self.revision,
            row_hash=row_hash,
        )
        object.__setattr__(self, "ingestion_run_id", ingestion_run_id)
        object.__setattr__(self, "row_hash", row_hash)
        object.__setattr__(self, "fact_version_id", fact_version_id)

    @property
    def source_identity_key(self) -> str:
        return self.fact.source.identity_key

    @property
    def source(self) -> SourceIdentity:
        return self.fact.source

    @property
    def provenance(self) -> Mapping[str, Any]:
        return self.fact.provenance


def build_canonical_fact_series_material_hash(
    *,
    series_identity: Mapping[str, Any],
    records: Sequence[CanonicalFactRecord],
) -> str:
    """Hash exact canonical Fact revisions independently of storage watermarks."""

    rows = sorted(
        records,
        key=lambda record: (
            record.fact.observation_time,
            record.fact.observation_key,
            record.revision,
        ),
    )
    if not rows:
        raise ValueError("canonical_fact_series_material_hash_invalid: records required")
    return _hash(
        {
            "schema_version": "market.canonical_fact_series_material.v1",
            "series": dict(series_identity),
            "rows": [
                {
                    "observation_time": _time(record.fact.observation_time),
                    "observation_key": record.fact.observation_key,
                    "revision": record.revision,
                    "payload_schema_id": record.fact.payload_schema_id,
                    "payload_contract_hash": record.fact.payload_contract_hash,
                    "row_hash": record.row_hash,
                }
                for record in rows
            ],
        }
    )


def build_canonical_fact_provenance_hash(
    records: Sequence[CanonicalFactRecord],
) -> str:
    """Hash the immutable acquisition lineage of canonical Fact revisions."""

    rows = sorted(
        records,
        key=lambda record: (
            record.fact.observation_time,
            record.fact.observation_key,
            record.revision,
        ),
    )
    if not rows:
        raise ValueError("canonical_fact_provenance_hash_invalid: records required")
    return _hash(
        {
            "schema_version": "market.canonical_fact_provenance.v1",
            "records": [
                {
                    "fact_version_id": record.fact_version_id,
                    "source_identity_key": record.source_identity_key,
                    "ingestion_run_id": record.ingestion_run_id,
                    "provenance_hash": record.fact.provenance_hash,
                }
                for record in rows
            ],
        }
    )


__all__ = [
    "CanonicalFact",
    "CanonicalFactRecord",
    "FactState",
    "build_canonical_fact_provenance_hash",
    "build_canonical_fact_series_material_hash",
    "build_fact_version_id",
]
