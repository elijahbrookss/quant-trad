"""Strict code-owned instrument identities for reproducible fleet enrollment."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


INSTRUMENT_ENROLLMENT_MANIFEST_VERSION = (
    "market.instrument_enrollment_manifest.v1"
)
_ROOT_FIELDS = frozenset({"schema_version", "fleet_id", "instruments", "manifest_hash"})
_INSTRUMENT_FIELDS = frozenset(
    {"id", "symbol", "datasource", "exchange", "instrument_type", "metadata"}
)
_METADATA_FIELDS = frozenset(
    {"instrument_fields", "provider_metadata", "provenance"}
)
_REQUIRED_CONTRACT_FIELDS = frozenset(
    {
        "tick_size",
        "qty_step",
        "contract_size",
        "base_currency",
        "quote_currency",
        "can_short",
        "short_requires_borrow",
        "has_funding",
    }
)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _required(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"instrument_enrollment_invalid: {field} is required")
    return normalized


def _positive_decimal(value: Any, *, field: str) -> str:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(
            f"instrument_enrollment_invalid: {field} must be decimal"
        ) from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(
            f"instrument_enrollment_invalid: {field} must be positive"
        )
    return format(result, "f")


@dataclass(frozen=True)
class InstrumentEnrollment:
    id: str
    symbol: str
    datasource: str
    exchange: str
    instrument_type: str
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        instrument_id = _required(self.id, field="id")
        try:
            uuid.UUID(instrument_id)
        except ValueError as exc:
            raise ValueError(
                "instrument_enrollment_invalid: id must be UUID"
            ) from exc
        object.__setattr__(self, "id", instrument_id)
        object.__setattr__(self, "symbol", _required(self.symbol, field="symbol").upper())
        object.__setattr__(
            self, "datasource", _required(self.datasource, field="datasource").upper()
        )
        object.__setattr__(
            self, "exchange", _required(self.exchange, field="exchange").upper()
        )
        instrument_type = _required(
            self.instrument_type, field="instrument_type"
        ).lower()
        if instrument_type not in {"future", "perp", "spot"}:
            raise ValueError(
                "instrument_enrollment_invalid: unsupported instrument_type"
            )
        object.__setattr__(self, "instrument_type", instrument_type)
        metadata = dict(self.metadata)
        if set(metadata) != _METADATA_FIELDS:
            raise ValueError(
                "instrument_enrollment_invalid: metadata fields must be exact"
            )
        fields = dict(metadata["instrument_fields"])
        missing = sorted(_REQUIRED_CONTRACT_FIELDS - set(fields))
        if missing:
            raise ValueError(
                "instrument_enrollment_invalid: missing instrument fields "
                + ",".join(missing)
            )
        for key in ("tick_size", "qty_step", "contract_size"):
            fields[key] = _positive_decimal(fields[key], field=key)
        fields["base_currency"] = _required(
            fields["base_currency"], field="base_currency"
        ).upper()
        fields["quote_currency"] = _required(
            fields["quote_currency"], field="quote_currency"
        ).upper()
        for key in ("can_short", "short_requires_borrow", "has_funding"):
            if not isinstance(fields[key], bool):
                raise ValueError(
                    f"instrument_enrollment_invalid: {key} must be boolean"
                )
        provider_metadata = dict(metadata["provider_metadata"])
        if _required(
            provider_metadata.get("provider_product_id"),
            field="provider_product_id",
        ).upper() != self.symbol:
            raise ValueError(
                "instrument_enrollment_invalid: provider product/symbol mismatch"
            )
        provenance = dict(metadata["provenance"])
        if provenance.get("contract") != "market.instrument_enrollment.v1":
            raise ValueError(
                "instrument_enrollment_invalid: provenance contract mismatch"
            )
        object.__setattr__(
            self,
            "metadata",
            {
                "instrument_fields": fields,
                "provider_metadata": provider_metadata,
                "provenance": provenance,
            },
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "instrument_type": self.instrument_type,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class InstrumentEnrollmentManifest:
    schema_version: str
    fleet_id: str
    instruments: tuple[InstrumentEnrollment, ...]
    manifest_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != INSTRUMENT_ENROLLMENT_MANIFEST_VERSION:
            raise ValueError("unsupported instrument enrollment manifest schema")
        object.__setattr__(self, "fleet_id", _required(self.fleet_id, field="fleet_id"))
        rows = tuple(self.instruments)
        if not rows:
            raise ValueError("instrument enrollment manifest must not be empty")
        if len({row.id for row in rows}) != len(rows):
            raise ValueError("instrument enrollment IDs must be unique")
        identities = {
            (row.datasource, row.exchange, row.symbol) for row in rows
        }
        if len(identities) != len(rows):
            raise ValueError("instrument enrollment identities must be unique")
        object.__setattr__(self, "instruments", rows)
        expected = _stable_hash(self._material())
        if self.manifest_hash and self.manifest_hash != expected:
            raise ValueError("instrument_enrollment_manifest_hash_mismatch")
        object.__setattr__(self, "manifest_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "fleet_id": self.fleet_id,
            "instruments": [row.to_dict() for row in self.instruments],
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "manifest_hash": self.manifest_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "InstrumentEnrollmentManifest":
        unknown = set(raw) - _ROOT_FIELDS
        missing = {"schema_version", "fleet_id", "instruments"} - set(raw)
        if unknown or missing:
            raise ValueError(
                "instrument_enrollment_manifest_fields_invalid: "
                f"missing={sorted(missing)} unknown={sorted(unknown)}"
            )
        rows = []
        for raw_row in raw.get("instruments") or ():
            values = dict(raw_row)
            if set(values) != _INSTRUMENT_FIELDS:
                raise ValueError(
                    "instrument_enrollment_fields_invalid: fields must be exact"
                )
            rows.append(InstrumentEnrollment(**values))
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            fleet_id=str(raw.get("fleet_id") or ""),
            instruments=tuple(rows),
            manifest_hash=str(raw.get("manifest_hash") or ""),
        )


def load_instrument_enrollment_manifest(
    path: Path | str,
) -> InstrumentEnrollmentManifest:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("instrument enrollment manifest root must be an object")
    return InstrumentEnrollmentManifest.from_dict(raw)


__all__ = [
    "INSTRUMENT_ENROLLMENT_MANIFEST_VERSION",
    "InstrumentEnrollment",
    "InstrumentEnrollmentManifest",
    "load_instrument_enrollment_manifest",
]
