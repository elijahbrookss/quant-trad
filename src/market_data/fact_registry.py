"""Typed fact-contract registry shared by planning, freezing, and runtime."""

from __future__ import annotations

import re
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping, Sequence


NORMALIZED_FACT_PREFIX = "market.normalized."
NORMALIZED_FACT_VERSION = "market.normalized_feature.v1"


class FactPayloadKind(str, Enum):
    """Primitive encodings admitted by the canonical payload registry."""

    DECIMAL = "decimal"
    FLOAT64 = "float64"
    INTEGER = "integer"
    STRING = "string"
    TIMESTAMP = "timestamp"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"


def _canonical_decimal(value: Any, *, field: str) -> str:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError(
            "market_fact_payload_invalid: "
            f"field={field} exact decimal forbids binary floating point"
        )
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"market_fact_payload_invalid: field={field} must be an exact decimal"
        ) from exc
    if not parsed.is_finite():
        raise ValueError(
            f"market_fact_payload_invalid: field={field} must be finite"
        )
    if parsed.is_zero():
        return "0"
    normalized = format(parsed.normalize(), "f")
    return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized


def _canonical_float64(value: Any, *, field: str) -> str:
    if isinstance(value, bool):
        raise ValueError(
            f"market_fact_payload_invalid: field={field} must be finite float64"
        )
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f"market_fact_payload_invalid: field={field} must be finite float64"
        ) from exc
    if not math.isfinite(parsed):
        raise ValueError(
            f"market_fact_payload_invalid: field={field} must be finite float64"
        )
    return repr(0.0 if parsed == 0.0 else parsed)


def _canonical_timestamp(value: Any, *, field: str) -> str:
    if not isinstance(value, datetime):
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(
                f"market_fact_payload_invalid: field={field} timestamp is required"
            )
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            value = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"market_fact_payload_invalid: field={field} must be ISO-8601"
            ) from exc
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _canonical_json_value(value: Any, *, field: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        return _canonical_decimal(value, field=field)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                f"market_fact_payload_invalid: field={field} contains non-finite float"
            )
        return value
    if isinstance(value, datetime):
        return _canonical_timestamp(value, field=field)
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_json_value(item, field=f"{field}.{key}")
            for key, item in sorted(value.items(), key=lambda row: str(row[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _canonical_json_value(item, field=f"{field}[{index}]")
            for index, item in enumerate(value)
        ]
    raise ValueError(
        f"market_fact_payload_invalid: field={field} is not canonical JSON material"
    )


@dataclass(frozen=True)
class FactPayloadField:
    """One strict field in a versioned canonical Fact payload."""

    name: str
    kind: FactPayloadKind | str
    required: bool = True
    nullable: bool = False
    enum: tuple[str, ...] = ()
    minimum: Decimal | None = None
    minimum_inclusive: bool = True

    def __post_init__(self) -> None:
        name = str(self.name or "").strip()
        if not name:
            raise ValueError("market_fact_payload_field_invalid: name is required")
        try:
            kind = FactPayloadKind(str(getattr(self.kind, "value", self.kind)))
        except ValueError as exc:
            raise ValueError(
                f"market_fact_payload_field_invalid: field={name} kind is unsupported"
            ) from exc
        enum = tuple(str(item) for item in self.enum)
        if len(enum) != len(set(enum)) or any(not item for item in enum):
            raise ValueError(
                f"market_fact_payload_field_invalid: field={name} enum is invalid"
            )
        if enum and kind is not FactPayloadKind.STRING:
            raise ValueError(
                f"market_fact_payload_field_invalid: field={name} enum requires string kind"
            )
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "enum", enum)

    def contract_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind.value,
            "required": bool(self.required),
            "nullable": bool(self.nullable),
            "enum": list(self.enum),
            "minimum": (
                _canonical_decimal(self.minimum, field=f"{self.name}.minimum")
                if self.minimum is not None
                else None
            ),
            "minimum_inclusive": bool(self.minimum_inclusive),
        }

    def normalize(self, value: Any) -> Any:
        if value is None:
            if self.nullable:
                return None
            raise ValueError(
                f"market_fact_payload_invalid: field={self.name} cannot be null"
            )
        if self.kind is FactPayloadKind.DECIMAL:
            normalized: Any = _canonical_decimal(value, field=self.name)
            comparable = Decimal(normalized)
        elif self.kind is FactPayloadKind.FLOAT64:
            normalized = _canonical_float64(value, field=self.name)
            comparable = Decimal(normalized)
        elif self.kind is FactPayloadKind.INTEGER:
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(
                    f"market_fact_payload_invalid: field={self.name} must be an integer"
                )
            normalized = int(value)
            comparable = Decimal(normalized)
        elif self.kind is FactPayloadKind.STRING:
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"market_fact_payload_invalid: field={self.name} must be a nonempty string"
                )
            if self.enum and value not in self.enum:
                raise ValueError(
                    "market_fact_payload_invalid: "
                    f"field={self.name} value={value} is outside the schema enum"
                )
            normalized = value
            comparable = None
        elif self.kind is FactPayloadKind.TIMESTAMP:
            normalized = _canonical_timestamp(value, field=self.name)
            comparable = None
        elif self.kind is FactPayloadKind.BOOLEAN:
            if not isinstance(value, bool):
                raise ValueError(
                    f"market_fact_payload_invalid: field={self.name} must be boolean"
                )
            normalized = value
            comparable = None
        elif self.kind is FactPayloadKind.OBJECT:
            if not isinstance(value, Mapping):
                raise ValueError(
                    f"market_fact_payload_invalid: field={self.name} must be an object"
                )
            normalized = _canonical_json_value(value, field=self.name)
            comparable = None
        elif self.kind is FactPayloadKind.ARRAY:
            if not isinstance(value, Sequence) or isinstance(
                value, (str, bytes, bytearray)
            ):
                raise ValueError(
                    f"market_fact_payload_invalid: field={self.name} must be an array"
                )
            normalized = _canonical_json_value(value, field=self.name)
            comparable = None
        else:  # pragma: no cover - Enum construction makes this unreachable.
            raise AssertionError(self.kind)
        if self.minimum is not None and comparable is not None:
            below = comparable < self.minimum
            excluded_equal = not self.minimum_inclusive and comparable == self.minimum
            if below or excluded_equal:
                operator = ">=" if self.minimum_inclusive else ">"
                raise ValueError(
                    "market_fact_payload_invalid: "
                    f"field={self.name} must be {operator} {self.minimum}"
                )
        return normalized


@dataclass(frozen=True)
class FactPayloadSchema:
    """Immutable schema for one atomic canonical Fact payload."""

    schema_id: str
    fact_type: str
    fields: tuple[FactPayloadField, ...]
    observation_time_field: str
    material_hash_version: str
    row_hash_version: str
    query_fields: tuple[str, ...] = ()
    dataset_eligible: bool = True

    def __post_init__(self) -> None:
        schema_id = str(self.schema_id or "").strip().lower()
        fact_type = str(self.fact_type or "").strip().lower()
        if not schema_id or not fact_type or not self.fields:
            raise ValueError("market_fact_payload_schema_invalid: identity and fields are required")
        names = tuple(field.name for field in self.fields)
        if len(names) != len(set(names)):
            raise ValueError(
                f"market_fact_payload_schema_invalid: schema_id={schema_id} has duplicate fields"
            )
        query_fields = tuple(str(item) for item in self.query_fields)
        unknown_queries = sorted(set(query_fields) - set(names))
        if unknown_queries:
            raise ValueError(
                "market_fact_payload_schema_invalid: "
                f"schema_id={schema_id} unknown_query_fields={','.join(unknown_queries)}"
            )
        for name, value in (
            ("observation_time_field", self.observation_time_field),
            ("material_hash_version", self.material_hash_version),
            ("row_hash_version", self.row_hash_version),
        ):
            if not str(value or "").strip():
                raise ValueError(
                    f"market_fact_payload_schema_invalid: schema_id={schema_id} {name} is required"
                )
        object.__setattr__(self, "schema_id", schema_id)
        object.__setattr__(self, "fact_type", fact_type)
        object.__setattr__(self, "query_fields", query_fields)

    @property
    def contract(self) -> dict[str, Any]:
        return {
            "schema_version": "market.fact_payload_contract.v1",
            "schema_id": self.schema_id,
            "fact_type": self.fact_type,
            "additional_properties": False,
            "fields": [field.contract_dict() for field in self.fields],
            "observation_time_field": self.observation_time_field,
            "material_hash_version": self.material_hash_version,
            "row_hash_version": self.row_hash_version,
            "query_fields": list(self.query_fields),
            "dataset_eligible": bool(self.dataset_eligible),
        }

    @property
    def contract_hash(self) -> str:
        encoded = json.dumps(
            self.contract, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def normalize_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            raise ValueError(
                f"market_fact_payload_invalid: schema_id={self.schema_id} payload must be an object"
            )
        raw = dict(payload)
        allowed = {field.name for field in self.fields}
        unexpected = sorted(str(key) for key in raw if str(key) not in allowed)
        if unexpected:
            raise ValueError(
                "market_fact_payload_invalid: "
                f"schema_id={self.schema_id} unexpected={','.join(unexpected)}"
            )
        missing = sorted(
            field.name for field in self.fields if field.required and field.name not in raw
        )
        if missing:
            raise ValueError(
                "market_fact_payload_invalid: "
                f"schema_id={self.schema_id} missing={','.join(missing)}"
            )
        normalized: dict[str, Any] = {}
        for field in self.fields:
            if field.name in raw:
                normalized[field.name] = field.normalize(raw[field.name])
        return normalized


@dataclass(frozen=True)
class FactContract:
    fact_type: str
    contract_version: str
    timeframe_mode: str
    archive_policy: str
    record_time_field: str
    dataset_eligible: bool = True
    storage_shape: str = "specialized"
    numeric_type: str | None = None
    subject_type: str = "instrument"
    required_dimensions: tuple[str, ...] = ()
    optional_dimensions: tuple[str, ...] = ()
    series_identity_dimensions: tuple[str, ...] = ()
    uppercase_dimensions: tuple[str, ...] = ()
    allowed_units: tuple[str, ...] = ()
    unit_dimension: str | None = None
    minimum_value: Decimal | None = None
    minimum_inclusive: bool = True

    def validate(
        self, *, contract_version: str, timeframe_seconds: int | None
    ) -> None:
        actual_version = str(contract_version or "").strip()
        version_valid = actual_version == self.contract_version
        if self.fact_type.startswith(NORMALIZED_FACT_PREFIX):
            pattern = (
                rf"{re.escape(NORMALIZED_FACT_VERSION)}/nsp_[0-9a-f]{{31}}"
            )
            version_valid = bool(re.fullmatch(pattern, actual_version))
        if not version_valid:
            raise ValueError(
                "market_fact_contract_mismatch: "
                f"fact_type={self.fact_type} expected={self.contract_version} "
                f"actual={contract_version or '<missing>'}"
            )
        if self.timeframe_mode == "required" and (
            timeframe_seconds is None or int(timeframe_seconds) <= 0
        ):
            raise ValueError(
                f"market_fact_contract_invalid: fact_type={self.fact_type} facts "
                "require timeframe_seconds"
            )
        if self.timeframe_mode == "forbidden" and timeframe_seconds is not None:
            raise ValueError(
                f"market_fact_contract_invalid: fact_type={self.fact_type} "
                "facts do not have a timeframe"
            )

    @property
    def default_alignment(self) -> str:
        return (
            "exact_interval"
            if self.timeframe_mode == "required"
            else "latest_known"
        )

    @property
    def uses_exact_numeric_storage(self) -> bool:
        return self.storage_shape == "exact_numeric"

    def normalize_dimensions(self, dimensions: Mapping[str, Any] | None) -> dict[str, str]:
        """Validate the contract-enumerated dimensions used in series identity."""

        raw = dict(dimensions or {})
        allowed = set(self.required_dimensions) | set(self.optional_dimensions)
        unexpected = sorted(str(key) for key in raw if str(key) not in allowed)
        if unexpected:
            raise ValueError(
                "market_fact_dimensions_invalid: "
                f"fact_type={self.fact_type} unexpected={','.join(unexpected)}"
            )
        missing = sorted(key for key in self.required_dimensions if key not in raw)
        if missing:
            raise ValueError(
                "market_fact_dimensions_invalid: "
                f"fact_type={self.fact_type} missing={','.join(missing)}"
            )
        normalized: dict[str, str] = {}
        for key in sorted(raw):
            if isinstance(raw[key], bool):
                raise ValueError(
                    "market_fact_dimensions_invalid: "
                    f"fact_type={self.fact_type} dimension={key} must be a string or integer"
                )
            value = str(raw[key]).strip()
            if not value:
                raise ValueError(
                    "market_fact_dimensions_invalid: "
                    f"fact_type={self.fact_type} dimension={key} is empty"
                )
            if key in self.uppercase_dimensions:
                value = value.upper()
            normalized[str(key)] = value
        return normalized

    def validate_numeric_value(
        self,
        *,
        value: Decimal,
        unit: str,
        dimensions: Mapping[str, Any] | None,
    ) -> tuple[str, dict[str, str]]:
        """Validate an exact numeric value without admitting provider-specific shape."""

        if not self.uses_exact_numeric_storage or self.numeric_type != "decimal":
            raise ValueError(
                f"market_fact_numeric_contract_invalid: fact_type={self.fact_type}"
            )
        normalized_dimensions = self.normalize_dimensions(dimensions)
        normalized_unit = str(unit or "").strip().upper()
        if not normalized_unit:
            raise ValueError(
                f"market_fact_numeric_contract_invalid: fact_type={self.fact_type} unit is required"
            )
        allowed_units = {item.upper() for item in self.allowed_units}
        if allowed_units and normalized_unit not in allowed_units:
            raise ValueError(
                "market_fact_numeric_contract_invalid: "
                f"fact_type={self.fact_type} unit={normalized_unit}"
            )
        if self.unit_dimension is not None:
            expected_unit = normalized_dimensions[self.unit_dimension].upper()
            if normalized_unit != expected_unit:
                raise ValueError(
                    "market_fact_numeric_contract_invalid: "
                    f"fact_type={self.fact_type} unit={normalized_unit} "
                    f"expected_from_dimension={expected_unit}"
                )
        if self.minimum_value is not None:
            below = value < self.minimum_value
            excluded_equal = not self.minimum_inclusive and value == self.minimum_value
            if below or excluded_equal:
                operator = ">=" if self.minimum_inclusive else ">"
                raise ValueError(
                    "market_fact_numeric_contract_invalid: "
                    f"fact_type={self.fact_type} value must be {operator} {self.minimum_value}"
                )
        return normalized_unit, normalized_dimensions


_CONTRACTS = {
    "candle.ohlcv": FactContract(
        "candle.ohlcv", "candle.ohlcv.v1", "required", "none", "open_time"
    ),
    "derivatives.open_interest": FactContract(
        "derivatives.open_interest",
        "derivatives.open_interest.v1",
        "forbidden",
        "none",
        "sample_time",
    ),
    "derivatives.funding_rate": FactContract(
        "derivatives.funding_rate",
        "derivatives.funding_rate.v1",
        "forbidden",
        "none",
        "sample_time",
    ),
    "market.l2_book": FactContract(
        "market.l2_book",
        "market.l2_book.v1",
        "forbidden",
        "raw_required",
        "effective_at",
        False,
    ),
    "market.trade": FactContract(
        "market.trade", "market.trade.v1", "forbidden", "raw_required", "provider_event_time"
    ),
    "market.trade_flow": FactContract(
        "market.trade_flow", "market.trade_flow.v1", "required", "raw_required", "bucket_start"
    ),
    "market.bbo": FactContract(
        "market.bbo", "market.bbo.v1", "required", "raw_required", "bucket_start"
    ),
    "market.depth_observation": FactContract(
        "market.depth_observation",
        "market.depth_band.v1",
        "required",
        "raw_required",
        "bucket_start",
    ),
    "market.trade_flow_feature": FactContract(
        "market.trade_flow_feature",
        "market.trade_flow_feature.v1",
        "required",
        "raw_required",
        "bucket_start",
    ),
    "market.futures_spot_relationship": FactContract(
        "market.futures_spot_relationship",
        "market.futures_spot_basis.v1",
        "required",
        "raw_required",
        "effective_at",
    ),
    "market.derivative_state": FactContract(
        "market.derivative_state",
        "market.derivative_state.v1",
        "forbidden",
        "none",
        "effective_at",
    ),
    "market.reference_price": FactContract(
        "market.reference_price",
        "market.reference_price.v1",
        "forbidden",
        "none",
        "effective_at",
        storage_shape="exact_numeric",
        numeric_type="decimal",
        subject_type="instrument",
        required_dimensions=("quote_currency",),
        series_identity_dimensions=("quote_currency",),
        uppercase_dimensions=("quote_currency",),
        unit_dimension="quote_currency",
        minimum_value=Decimal("0"),
        minimum_inclusive=False,
    ),
    "market.reserve_balance": FactContract(
        "market.reserve_balance",
        "market.reserve_balance.v1",
        "forbidden",
        "none",
        "effective_at",
        storage_shape="exact_numeric",
        numeric_type="decimal",
        subject_type="instrument",
        required_dimensions=("reserve_unit",),
        series_identity_dimensions=("reserve_unit",),
        uppercase_dimensions=("reserve_unit",),
        unit_dimension="reserve_unit",
        minimum_value=Decimal("0"),
        minimum_inclusive=True,
    ),
    "market.market_response": FactContract(
        "market.market_response",
        "market.market_response.v1",
        "required",
        "raw_required",
        "effective_at",
    ),
}


_PAYLOAD_SCHEMAS = {
    schema.schema_id: schema
    for schema in (
        FactPayloadSchema(
            schema_id="candle.ohlcv.v1",
            fact_type="candle.ohlcv",
            fields=(
                FactPayloadField("close_time", FactPayloadKind.TIMESTAMP),
                FactPayloadField("open", FactPayloadKind.FLOAT64),
                FactPayloadField("high", FactPayloadKind.FLOAT64),
                FactPayloadField("low", FactPayloadKind.FLOAT64),
                FactPayloadField("close", FactPayloadKind.FLOAT64),
                FactPayloadField(
                    "volume",
                    FactPayloadKind.FLOAT64,
                    nullable=True,
                    minimum=Decimal("0"),
                ),
                FactPayloadField(
                    "trade_count",
                    FactPayloadKind.INTEGER,
                    nullable=True,
                    minimum=Decimal("0"),
                ),
            ),
            observation_time_field="open_time",
            material_hash_version="candle_material_hash.v1",
            row_hash_version="candle.ohlcv.v1",
            query_fields=("close_time", "open", "high", "low", "close"),
        ),
        FactPayloadSchema(
            schema_id="derivatives.open_interest.v1",
            fact_type="derivatives.open_interest",
            fields=(
                FactPayloadField(
                    "value",
                    FactPayloadKind.FLOAT64,
                    minimum=Decimal("0"),
                ),
                FactPayloadField(
                    "unit", FactPayloadKind.STRING, enum=("contracts",)
                ),
            ),
            observation_time_field="sample_time",
            material_hash_version="open_interest_material_hash.v1",
            row_hash_version="derivatives.open_interest.v1",
            query_fields=("value",),
        ),
        FactPayloadSchema(
            schema_id="derivatives.open_interest.v2",
            fact_type="derivatives.open_interest",
            fields=(
                FactPayloadField(
                    "value",
                    FactPayloadKind.DECIMAL,
                    minimum=Decimal("0"),
                ),
                FactPayloadField("raw_value", FactPayloadKind.STRING),
                FactPayloadField(
                    "unit", FactPayloadKind.STRING, enum=("contracts",)
                ),
            ),
            observation_time_field="sample_time",
            material_hash_version="market.fact_material.v1",
            row_hash_version="market.fact_row.v1",
            query_fields=("value",),
        ),
        FactPayloadSchema(
            schema_id="derivatives.funding_rate.v1",
            fact_type="derivatives.funding_rate",
            fields=(
                FactPayloadField("rate", FactPayloadKind.FLOAT64),
                FactPayloadField("funding_time", FactPayloadKind.TIMESTAMP),
                FactPayloadField(
                    "interval_seconds",
                    FactPayloadKind.INTEGER,
                    minimum=Decimal("0"),
                    minimum_inclusive=False,
                ),
                FactPayloadField(
                    "unit", FactPayloadKind.STRING, enum=("fraction",)
                ),
            ),
            observation_time_field="sample_time",
            material_hash_version="funding_rate_material_hash.v1",
            row_hash_version="derivatives.funding_rate.v1",
            query_fields=("rate", "funding_time", "interval_seconds"),
        ),
        FactPayloadSchema(
            schema_id="derivatives.funding_rate.v2",
            fact_type="derivatives.funding_rate",
            fields=(
                FactPayloadField("rate", FactPayloadKind.DECIMAL),
                FactPayloadField("raw_rate", FactPayloadKind.STRING),
                FactPayloadField("funding_time", FactPayloadKind.TIMESTAMP),
                FactPayloadField(
                    "interval_seconds",
                    FactPayloadKind.INTEGER,
                    minimum=Decimal("0"),
                    minimum_inclusive=False,
                ),
                FactPayloadField(
                    "unit", FactPayloadKind.STRING, enum=("fraction",)
                ),
            ),
            observation_time_field="sample_time",
            material_hash_version="market.fact_material.v1",
            row_hash_version="market.fact_row.v1",
            query_fields=("rate", "funding_time", "interval_seconds"),
        ),
        FactPayloadSchema(
            schema_id="market.reference_price.v1",
            fact_type="market.reference_price",
            fields=(
                FactPayloadField(
                    "value",
                    FactPayloadKind.DECIMAL,
                    minimum=Decimal("0"),
                    minimum_inclusive=False,
                ),
                FactPayloadField("raw_value", FactPayloadKind.STRING),
                FactPayloadField("unit", FactPayloadKind.STRING),
            ),
            observation_time_field="effective_at",
            material_hash_version="numeric_fact_material_hash.v1",
            row_hash_version="market.reference_price.v1",
            query_fields=("value",),
        ),
        FactPayloadSchema(
            schema_id="market.reserve_balance.v1",
            fact_type="market.reserve_balance",
            fields=(
                FactPayloadField(
                    "value",
                    FactPayloadKind.DECIMAL,
                    minimum=Decimal("0"),
                ),
                FactPayloadField("raw_value", FactPayloadKind.STRING),
                FactPayloadField("unit", FactPayloadKind.STRING),
            ),
            observation_time_field="effective_at",
            material_hash_version="numeric_fact_material_hash.v1",
            row_hash_version="market.reserve_balance.v1",
            query_fields=("value",),
        ),
    )
}


def get_fact_contract(fact_type: str) -> FactContract:
    normalized = str(fact_type or "").strip().lower()
    if normalized.startswith(NORMALIZED_FACT_PREFIX) and len(normalized) > len(
        NORMALIZED_FACT_PREFIX
    ):
        return FactContract(
            normalized,
            NORMALIZED_FACT_VERSION,
            "optional",
            "source_dependent",
            "effective_at",
        )
    contract = _CONTRACTS.get(normalized)
    if contract is None:
        raise ValueError(
            f"market_fact_contract_unsupported: fact_type={normalized or '<missing>'}"
        )
    return contract


def supported_fact_contracts() -> tuple[FactContract, ...]:
    return tuple(_CONTRACTS[key] for key in sorted(_CONTRACTS))


def get_fact_payload_schema(schema_id: str) -> FactPayloadSchema:
    normalized = str(schema_id or "").strip().lower()
    schema = _PAYLOAD_SCHEMAS.get(normalized)
    if schema is None:
        raise ValueError(
            "market_fact_payload_schema_unsupported: "
            f"schema_id={normalized or '<missing>'}"
        )
    return schema


def supported_fact_payload_schemas() -> tuple[FactPayloadSchema, ...]:
    return tuple(_PAYLOAD_SCHEMAS[key] for key in sorted(_PAYLOAD_SCHEMAS))


__all__ = [
    "FactContract",
    "FactPayloadField",
    "FactPayloadKind",
    "FactPayloadSchema",
    "NORMALIZED_FACT_PREFIX",
    "NORMALIZED_FACT_VERSION",
    "get_fact_contract",
    "get_fact_payload_schema",
    "supported_fact_contracts",
    "supported_fact_payload_schemas",
]
