"""Typed market-structure source facts and deterministic trade aggregation.

The contracts in this module preserve provider meaning and causal visibility.
They do not perform I/O and are shared by live acquisition, archive replay,
dataset freezing, and provider-free consumers.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Optional

from data_providers.streams.contracts import CanonicalMarketEvent, ProviderRawMessage


MARKET_TRADE_FACT_TYPE = "market.trade"
MARKET_TRADE_FACT_VERSION = "market.trade.v1"
TRADE_FLOW_FACT_TYPE = "market.trade_flow"
TRADE_FLOW_FACT_VERSION = "market.trade_flow.v1"
COINBASE_AGGRESSOR_TRANSFORM_VERSION = "coinbase_maker_to_aggressor.v1"
RAW_RECORD_ID_VERSION = "market.raw_record_id.v1"
TRADE_MATERIAL_HASH_VERSION = "market.trade_material_hash.v1"
TRADE_FLOW_MATERIAL_HASH_VERSION = "market.trade_flow_material_hash.v1"
TRADE_SERIES_MATERIAL_HASH_VERSION = "market.trade_series_material_hash.v1"
TRADE_FLOW_SERIES_MATERIAL_HASH_VERSION = "market.trade_flow_series_material_hash.v1"


class MarketSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class UnsupportedMarketTradeSideError(ValueError):
    """A provider trade cannot enter the canonical BUY/SELL contract."""

    def __init__(self, provider_side: object) -> None:
        self.provider_side = str(provider_side or "").strip()
        super().__init__(
            "market_trade_translation_invalid: maker side is outside the "
            f"supported BUY/SELL contract: {self.provider_side!r}"
        )


class ProviderSizeUnit(str, Enum):
    CONTRACTS = "contracts"
    BASE = "base"


class TradeDeliveryKind(str, Enum):
    SNAPSHOT = "snapshot"
    UPDATE = "update"


class CoverageStatus(str, Enum):
    OPEN_VALID = "open_valid"
    CLOSED_VALID = "closed_valid"
    INVALID = "invalid"


class OrderingAssurance(str, Enum):
    PROVIDER_SEQUENCE_CONTIGUOUS = "provider_sequence_contiguous"
    PROVIDER_DELIVERY_GUARANTEED = "provider_delivery_guaranteed"
    RECEIPT_CONTIGUOUS = "receipt_contiguous"
    CONNECTION_HEALTH_ONLY = "connection_health_only"


class ArchiveStatus(str, Enum):
    PENDING = "pending"
    COMPLETE = "complete"
    LOSS = "loss"


def _utc(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"market_structure_invalid: {field_name} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"market_structure_invalid: {field_name} must be ISO-8601"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_utc(value: Any, *, field_name: str) -> Optional[datetime]:
    if value in (None, ""):
        return None
    return _utc(value, field_name=field_name)


def _decimal(
    value: Any,
    *,
    field_name: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"market_structure_invalid: {field_name} must be decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"market_structure_invalid: {field_name} must be decimal"
        ) from exc
    if not result.is_finite():
        raise ValueError(f"market_structure_invalid: {field_name} must be finite")
    if positive and result <= 0:
        raise ValueError(f"market_structure_invalid: {field_name} must be positive")
    if nonnegative and result < 0:
        raise ValueError(
            f"market_structure_invalid: {field_name} must be nonnegative"
        )
    return Decimal(0) if result == 0 else result


def _optional_decimal(
    value: Any,
    *,
    field_name: str,
    positive: bool = False,
    nonnegative: bool = False,
) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    return _decimal(
        value,
        field_name=field_name,
        positive=positive,
        nonnegative=nonnegative,
    )


def _canonical_decimal(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def _canonical_time(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def build_spool_segment_id(
    *, definition_id: str, session_id: str, connection_epoch: int, segment_ordinal: int
) -> str:
    """Return a stable segment identity before any object upload exists."""

    material = {
        "schema_version": "market.spool_segment_id.v1",
        "definition_id": str(definition_id or "").strip(),
        "session_id": str(session_id or "").strip(),
        "connection_epoch": int(connection_epoch),
        "segment_ordinal": int(segment_ordinal),
    }
    if not material["definition_id"] or not material["session_id"]:
        raise ValueError(
            "market_raw_identity_invalid: definition_id and session_id are required"
        )
    if material["connection_epoch"] < 0 or material["segment_ordinal"] < 0:
        raise ValueError(
            "market_raw_identity_invalid: epoch and segment ordinal must be nonnegative"
        )
    return f"spool_{_stable_hash(material)}"


def build_raw_record_id(
    *,
    definition_id: str,
    session_id: str,
    connection_epoch: int,
    receive_ordinal: int,
    raw_frame_sha256: str,
) -> str:
    """Return the immutable pre-upload identity of one exact received frame."""

    digest = str(raw_frame_sha256 or "").strip().lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError("market_raw_identity_invalid: raw_frame_sha256 is invalid")
    material = {
        "schema_version": RAW_RECORD_ID_VERSION,
        "definition_id": str(definition_id or "").strip(),
        "session_id": str(session_id or "").strip(),
        "connection_epoch": int(connection_epoch),
        "receive_ordinal": int(receive_ordinal),
        "raw_frame_sha256": digest,
    }
    if not material["definition_id"] or not material["session_id"]:
        raise ValueError(
            "market_raw_identity_invalid: definition_id and session_id are required"
        )
    if material["connection_epoch"] < 0 or material["receive_ordinal"] <= 0:
        raise ValueError(
            "market_raw_identity_invalid: epoch must be nonnegative and receive ordinal positive"
        )
    return f"raw_{_stable_hash(material)}"


@dataclass(frozen=True)
class RawStreamRecord:
    """One fsync-able exact provider frame with identity assigned before parsing."""

    raw_record_id: str
    spool_segment_id: str
    definition_id: str
    session_id: str
    connection_epoch: int
    receive_ordinal: int
    received_at: datetime
    provider: str
    venue: str
    provider_product_id: str
    requested_channel: str
    observed_channel: str
    raw_frame: bytes
    raw_frame_sha256: str

    @classmethod
    def from_provider_message(
        cls,
        message: ProviderRawMessage,
        *,
        definition_id: str,
        spool_segment_id: str,
        provider_product_id: str,
        requested_channel: str,
        observed_channel: str,
    ) -> "RawStreamRecord":
        raw_record_id = build_raw_record_id(
            definition_id=definition_id,
            session_id=message.stream_session_id,
            connection_epoch=message.connection_epoch,
            receive_ordinal=message.receive_ordinal,
            raw_frame_sha256=message.raw_frame_sha256,
        )
        return cls(
            raw_record_id=raw_record_id,
            spool_segment_id=spool_segment_id,
            definition_id=definition_id,
            session_id=message.stream_session_id,
            connection_epoch=message.connection_epoch,
            receive_ordinal=message.receive_ordinal,
            received_at=_utc(message.received_at, field_name="received_at"),
            provider=str(message.provider or "").strip().upper(),
            venue=str(message.venue or "").strip().upper(),
            provider_product_id=str(provider_product_id or "").strip(),
            requested_channel=str(requested_channel or "").strip().lower(),
            observed_channel=str(observed_channel or "").strip().lower(),
            raw_frame=bytes(message.raw_frame),
            raw_frame_sha256=str(message.raw_frame_sha256).lower(),
        )

    def __post_init__(self) -> None:
        required = (
            "raw_record_id",
            "spool_segment_id",
            "definition_id",
            "session_id",
            "provider",
            "venue",
            "provider_product_id",
            "requested_channel",
            "observed_channel",
        )
        for name in required:
            normalized = str(getattr(self, name) or "").strip()
            if not normalized:
                raise ValueError(f"market_raw_record_invalid: {name} is required")
            object.__setattr__(self, name, normalized)
        if int(self.connection_epoch) < 0 or int(self.receive_ordinal) <= 0:
            raise ValueError("market_raw_record_invalid: invalid source position")
        received_at = _utc(self.received_at, field_name="received_at")
        frame = bytes(self.raw_frame)
        digest = hashlib.sha256(frame).hexdigest()
        if digest != str(self.raw_frame_sha256).lower():
            raise ValueError("market_raw_record_invalid: frame checksum mismatch")
        expected = build_raw_record_id(
            definition_id=self.definition_id,
            session_id=self.session_id,
            connection_epoch=self.connection_epoch,
            receive_ordinal=self.receive_ordinal,
            raw_frame_sha256=digest,
        )
        if expected != self.raw_record_id:
            raise ValueError("market_raw_record_invalid: raw_record_id mismatch")
        object.__setattr__(self, "connection_epoch", int(self.connection_epoch))
        object.__setattr__(self, "receive_ordinal", int(self.receive_ordinal))
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "raw_frame", frame)
        object.__setattr__(self, "raw_frame_sha256", digest)


@dataclass(frozen=True)
class ProductContract:
    """Versioned translation contract for one provider product."""

    provider_product_id: str
    provider_size_unit: ProviderSizeUnit | str
    base_currency: str
    quote_currency: str
    product_definition_version_id: str
    contract_size: Optional[Decimal] = None
    linear_quote_notional: bool = True
    maker_side_semantics_proven: bool = True
    provider_sequence_scope: str = "connection_epoch"

    def __post_init__(self) -> None:
        product_id = str(self.provider_product_id or "").strip()
        base = str(self.base_currency or "").strip().upper()
        quote = str(self.quote_currency or "").strip().upper()
        definition_id = str(self.product_definition_version_id or "").strip()
        if not product_id or not base or not quote or not definition_id:
            raise ValueError("market_trade_contract_invalid: identity fields are required")
        try:
            unit = ProviderSizeUnit(str(getattr(self.provider_size_unit, "value", self.provider_size_unit)).lower())
        except ValueError as exc:
            raise ValueError("market_trade_contract_invalid: unsupported size unit") from exc
        contract_size = _optional_decimal(
            self.contract_size,
            field_name="contract_size",
            positive=True,
        )
        if unit is ProviderSizeUnit.CONTRACTS and contract_size is None:
            raise ValueError(
                "market_trade_contract_invalid: contract unit requires contract_size"
            )
        if unit is ProviderSizeUnit.BASE and contract_size is not None:
            raise ValueError(
                "market_trade_contract_invalid: base unit cannot declare contract_size"
            )
        object.__setattr__(self, "provider_product_id", product_id)
        object.__setattr__(self, "provider_size_unit", unit)
        object.__setattr__(self, "base_currency", base)
        object.__setattr__(self, "quote_currency", quote)
        object.__setattr__(self, "product_definition_version_id", definition_id)
        object.__setattr__(self, "contract_size", contract_size)
        object.__setattr__(self, "linear_quote_notional", bool(self.linear_quote_notional))
        object.__setattr__(self, "maker_side_semantics_proven", bool(self.maker_side_semantics_proven))
        if str(self.provider_sequence_scope) != "connection_epoch":
            raise ValueError(
                "market_trade_contract_invalid: only proven connection_epoch sequence scope is supported"
            )


@dataclass(frozen=True)
class MarketTradeFact:
    """One Coinbase trade preserving provider identity and maker-side meaning."""

    provider_product_id: str
    provider_trade_id: str
    delivery_kind: TradeDeliveryKind | str
    price: Decimal
    provider_size: Decimal
    provider_size_unit: ProviderSizeUnit | str
    maker_side: MarketSide | str
    aggressor_side: Optional[MarketSide | str]
    aggressor_transform_version: Optional[str]
    contract_quantity: Optional[Decimal]
    base_quantity: Optional[Decimal]
    quote_notional: Optional[Decimal]
    base_currency: str
    quote_currency: str
    product_definition_version_id: str
    provider_event_time: datetime
    provider_message_time: Optional[datetime]
    received_at: datetime
    accepted_at: datetime
    known_at: datetime
    provider_sequence_num: Optional[int]
    connection_epoch: int
    receive_ordinal: int
    event_ordinal: int
    trade_ordinal: int
    raw_record_id: str
    coverage_interval_id: Optional[str] = None

    def __post_init__(self) -> None:
        for name in (
            "provider_product_id",
            "provider_trade_id",
            "base_currency",
            "quote_currency",
            "product_definition_version_id",
            "raw_record_id",
        ):
            value = str(getattr(self, name) or "").strip()
            if not value:
                raise ValueError(f"market_trade_invalid: {name} is required")
            object.__setattr__(self, name, value)
        try:
            delivery_kind = TradeDeliveryKind(
                str(getattr(self.delivery_kind, "value", self.delivery_kind)).lower()
            )
            size_unit = ProviderSizeUnit(
                str(getattr(self.provider_size_unit, "value", self.provider_size_unit)).lower()
            )
            maker_side = MarketSide(
                str(getattr(self.maker_side, "value", self.maker_side)).upper()
            )
            aggressor_side = (
                MarketSide(str(getattr(self.aggressor_side, "value", self.aggressor_side)).upper())
                if self.aggressor_side is not None
                else None
            )
        except ValueError as exc:
            raise ValueError("market_trade_invalid: unsupported enum value") from exc
        price = _decimal(self.price, field_name="price", positive=True)
        provider_size = _decimal(
            self.provider_size, field_name="provider_size", positive=True
        )
        contract_quantity = _optional_decimal(
            self.contract_quantity,
            field_name="contract_quantity",
            positive=True,
        )
        base_quantity = _optional_decimal(
            self.base_quantity, field_name="base_quantity", positive=True
        )
        quote_notional = _optional_decimal(
            self.quote_notional, field_name="quote_notional", positive=True
        )
        if size_unit is ProviderSizeUnit.CONTRACTS:
            if provider_size != provider_size.to_integral_value():
                raise ValueError(
                    "market_trade_invalid: proven futures provider size must be integral contracts"
                )
            if contract_quantity != provider_size:
                raise ValueError(
                    "market_trade_invalid: contract_quantity must equal provider_size"
                )
        elif contract_quantity is not None:
            raise ValueError(
                "market_trade_invalid: base-unit trade cannot have contract_quantity"
            )
        if aggressor_side is None and self.aggressor_transform_version is not None:
            raise ValueError(
                "market_trade_invalid: transform version requires aggressor_side"
            )
        if aggressor_side is not None:
            if str(self.aggressor_transform_version or "") != COINBASE_AGGRESSOR_TRANSFORM_VERSION:
                raise ValueError(
                    "market_trade_invalid: aggressor side requires the proven Coinbase transform"
                )
            expected = MarketSide.SELL if maker_side is MarketSide.BUY else MarketSide.BUY
            if aggressor_side is not expected:
                raise ValueError(
                    "market_trade_invalid: aggressor side contradicts maker-side transform"
                )
        provider_event_time = _utc(
            self.provider_event_time, field_name="provider_event_time"
        )
        provider_message_time = _optional_utc(
            self.provider_message_time, field_name="provider_message_time"
        )
        received_at = _utc(self.received_at, field_name="received_at")
        accepted_at = _utc(self.accepted_at, field_name="accepted_at")
        known_at = _utc(self.known_at, field_name="known_at")
        if accepted_at < received_at or known_at < accepted_at:
            raise ValueError(
                "market_trade_invalid: received_at <= accepted_at <= known_at is required"
            )
        if self.provider_sequence_num is not None and int(self.provider_sequence_num) < 0:
            raise ValueError("market_trade_invalid: provider sequence must be nonnegative")
        for name in ("connection_epoch", "event_ordinal", "trade_ordinal"):
            if int(getattr(self, name)) < 0:
                raise ValueError(f"market_trade_invalid: {name} must be nonnegative")
        if int(self.receive_ordinal) <= 0:
            raise ValueError("market_trade_invalid: receive_ordinal must be positive")
        object.__setattr__(self, "delivery_kind", delivery_kind)
        object.__setattr__(self, "provider_size_unit", size_unit)
        object.__setattr__(self, "maker_side", maker_side)
        object.__setattr__(self, "aggressor_side", aggressor_side)
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "provider_size", provider_size)
        object.__setattr__(self, "contract_quantity", contract_quantity)
        object.__setattr__(self, "base_quantity", base_quantity)
        object.__setattr__(self, "quote_notional", quote_notional)
        object.__setattr__(self, "base_currency", self.base_currency.upper())
        object.__setattr__(self, "quote_currency", self.quote_currency.upper())
        object.__setattr__(self, "provider_event_time", provider_event_time)
        object.__setattr__(self, "provider_message_time", provider_message_time)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "accepted_at", accepted_at)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "connection_epoch", int(self.connection_epoch))
        object.__setattr__(self, "receive_ordinal", int(self.receive_ordinal))
        object.__setattr__(self, "event_ordinal", int(self.event_ordinal))
        object.__setattr__(self, "trade_ordinal", int(self.trade_ordinal))
        if self.provider_sequence_num is not None:
            object.__setattr__(self, "provider_sequence_num", int(self.provider_sequence_num))
        coverage = str(self.coverage_interval_id or "").strip() or None
        object.__setattr__(self, "coverage_interval_id", coverage)

    @property
    def material_hash(self) -> str:
        """Hash provider semantics and justified unit translation, not delivery."""

        return _stable_hash(
            {
                "schema_version": TRADE_MATERIAL_HASH_VERSION,
                "provider_product_id": self.provider_product_id,
                "provider_trade_id": self.provider_trade_id,
                "price": _canonical_decimal(self.price),
                "provider_size": _canonical_decimal(self.provider_size),
                "provider_size_unit": self.provider_size_unit.value,
                "maker_side": self.maker_side.value,
                "aggressor_side": self.aggressor_side.value if self.aggressor_side else None,
                "aggressor_transform_version": self.aggressor_transform_version,
                "contract_quantity": _canonical_decimal(self.contract_quantity),
                "base_quantity": _canonical_decimal(self.base_quantity),
                "quote_notional": _canonical_decimal(self.quote_notional),
                "base_currency": self.base_currency,
                "quote_currency": self.quote_currency,
                "product_definition_version_id": self.product_definition_version_id,
                "provider_event_time": _canonical_time(self.provider_event_time),
            }
        )

    @property
    def row_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": MARKET_TRADE_FACT_VERSION,
                "material_hash": self.material_hash,
                "delivery_kind": self.delivery_kind.value,
                "provider_message_time": _canonical_time(self.provider_message_time),
                "received_at": _canonical_time(self.received_at),
                "accepted_at": _canonical_time(self.accepted_at),
                "known_at": _canonical_time(self.known_at),
                "provider_sequence_num": self.provider_sequence_num,
                "connection_epoch": self.connection_epoch,
                "receive_ordinal": self.receive_ordinal,
                "event_ordinal": self.event_ordinal,
                "trade_ordinal": self.trade_ordinal,
                "raw_record_id": self.raw_record_id,
                "coverage_interval_id": self.coverage_interval_id,
            }
        )


@dataclass(frozen=True)
class MarketTradeRecord:
    """A canonical trade fact plus immutable database revision identity."""

    version_id: str
    series_id: int
    source_id: int
    revision: int
    market_commit_seq: int
    provenance_hash: str
    quality: Mapping[str, Any]
    fact: MarketTradeFact

    def __post_init__(self) -> None:
        if not str(self.version_id or "").strip():
            raise ValueError("market_trade_record_invalid: version_id is required")
        for name in ("series_id", "source_id", "revision", "market_commit_seq"):
            if int(getattr(self, name)) <= 0:
                raise ValueError(f"market_trade_record_invalid: {name} must be positive")
            object.__setattr__(self, name, int(getattr(self, name)))
        if len(str(self.provenance_hash or "")) != 64:
            raise ValueError("market_trade_record_invalid: provenance_hash is invalid")
        object.__setattr__(self, "quality", dict(self.quality or {}))


def translate_coinbase_market_trade(
    event: CanonicalMarketEvent,
    *,
    contract: ProductContract,
    raw_record_id: str,
    connection_epoch: int,
    receive_ordinal: int,
    accepted_at: Optional[datetime] = None,
    coverage_interval_id: Optional[str] = None,
) -> MarketTradeFact:
    """Translate one parsed Coinbase event without losing maker-side meaning."""

    if event.event_kind != "market_trade":
        raise ValueError("market_trade_translation_invalid: event_kind must be market_trade")
    if str(event.provider).upper() != "COINBASE":
        raise ValueError("market_trade_translation_invalid: provider must be Coinbase")
    if str(event.product_id or "") != contract.provider_product_id:
        raise ValueError("market_trade_translation_invalid: product contract mismatch")
    payload = dict(event.payload or {})
    provider_size = _decimal(payload.get("size"), field_name="size", positive=True)
    price = _decimal(payload.get("price"), field_name="price", positive=True)
    if contract.provider_size_unit is ProviderSizeUnit.CONTRACTS:
        if provider_size != provider_size.to_integral_value():
            raise ValueError(
                "market_trade_translation_invalid: futures size is not integral contracts"
            )
        contract_quantity = provider_size
        assert contract.contract_size is not None
        base_quantity = contract_quantity * contract.contract_size
    else:
        contract_quantity = None
        base_quantity = provider_size
    quote_notional = price * base_quantity if contract.linear_quote_notional else None
    provider_side = str(payload.get("side") or "").strip()
    try:
        maker_side = MarketSide(provider_side.upper())
    except ValueError as exc:
        raise UnsupportedMarketTradeSideError(provider_side) from exc
    if contract.maker_side_semantics_proven:
        aggressor_side: Optional[MarketSide] = (
            MarketSide.SELL if maker_side is MarketSide.BUY else MarketSide.BUY
        )
        transform_version: Optional[str] = COINBASE_AGGRESSOR_TRANSFORM_VERSION
    else:
        aggressor_side = None
        transform_version = None
    received = _utc(event.received_at, field_name="received_at")
    accepted = _utc(accepted_at or datetime.now(UTC), field_name="accepted_at")
    if accepted < received:
        accepted = received
    return MarketTradeFact(
        provider_product_id=contract.provider_product_id,
        provider_trade_id=str(payload.get("trade_id") or "").strip(),
        delivery_kind=str(payload.get("type") or "").strip().lower(),
        price=price,
        provider_size=provider_size,
        provider_size_unit=contract.provider_size_unit,
        maker_side=maker_side,
        aggressor_side=aggressor_side,
        aggressor_transform_version=transform_version,
        contract_quantity=contract_quantity,
        base_quantity=base_quantity,
        quote_notional=quote_notional,
        base_currency=contract.base_currency,
        quote_currency=contract.quote_currency,
        product_definition_version_id=contract.product_definition_version_id,
        provider_event_time=_utc(event.provider_event_time, field_name="provider_event_time"),
        provider_message_time=_optional_utc(
            event.provider_message_time, field_name="provider_message_time"
        ),
        received_at=received,
        accepted_at=accepted,
        known_at=accepted,
        provider_sequence_num=event.provider_sequence_num,
        connection_epoch=connection_epoch,
        receive_ordinal=receive_ordinal,
        event_ordinal=int(payload.get("event_ordinal") or 0),
        trade_ordinal=int(payload.get("trade_ordinal") or 0),
        raw_record_id=raw_record_id,
        coverage_interval_id=coverage_interval_id,
    )


@dataclass(frozen=True)
class TradeCoverageIntervalVersion:
    """One immutable view of trade delivery validity for a connection epoch."""

    interval_id: str
    revision: int
    definition_id: str
    session_id: str
    connection_epoch: int
    provider_product_id: str
    channel: str
    status: CoverageStatus | str
    ordering_assurance: OrderingAssurance | str
    archive_status: ArchiveStatus | str
    opening_raw_record_id: str
    opening_receive_ordinal: int
    opening_effective_at: datetime
    last_raw_record_id: str
    last_receive_ordinal: int
    last_effective_at: datetime
    canonicalization_watermark_ordinal: int
    archive_complete_through_ordinal: int
    known_at: datetime
    closing_raw_record_id: Optional[str] = None
    closing_receive_ordinal: Optional[int] = None
    closing_effective_at: Optional[datetime] = None
    first_provider_sequence_num: Optional[int] = None
    last_provider_sequence_num: Optional[int] = None
    gap_quality_event_ids: tuple[str, ...] = ()
    opening_evidence: Mapping[str, Any] = field(default_factory=dict)
    closing_evidence: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "interval_id",
            "definition_id",
            "session_id",
            "provider_product_id",
            "channel",
            "opening_raw_record_id",
            "last_raw_record_id",
        ):
            value = str(getattr(self, name) or "").strip()
            if not value:
                raise ValueError(f"trade_coverage_invalid: {name} is required")
            object.__setattr__(self, name, value)
        if int(self.revision) <= 0 or int(self.connection_epoch) < 0:
            raise ValueError("trade_coverage_invalid: invalid revision or epoch")
        try:
            status = CoverageStatus(str(getattr(self.status, "value", self.status)).lower())
            assurance = OrderingAssurance(
                str(getattr(self.ordering_assurance, "value", self.ordering_assurance)).lower()
            )
            archive_status = ArchiveStatus(
                str(getattr(self.archive_status, "value", self.archive_status)).lower()
            )
        except ValueError as exc:
            raise ValueError("trade_coverage_invalid: unsupported status") from exc
        opening_effective = _utc(
            self.opening_effective_at, field_name="opening_effective_at"
        )
        last_effective = _utc(self.last_effective_at, field_name="last_effective_at")
        closing_effective = _optional_utc(
            self.closing_effective_at, field_name="closing_effective_at"
        )
        known_at = _utc(self.known_at, field_name="known_at")
        if last_effective < opening_effective:
            raise ValueError("trade_coverage_invalid: last evidence precedes opening")
        if closing_effective is not None and closing_effective < opening_effective:
            raise ValueError("trade_coverage_invalid: closure precedes opening")
        if status is CoverageStatus.OPEN_VALID and closing_effective is not None:
            raise ValueError("trade_coverage_invalid: open interval cannot have closure")
        if status is not CoverageStatus.OPEN_VALID and closing_effective is None:
            raise ValueError("trade_coverage_invalid: closed/invalid interval needs closure")
        for name in (
            "opening_receive_ordinal",
            "last_receive_ordinal",
            "canonicalization_watermark_ordinal",
            "archive_complete_through_ordinal",
        ):
            value = int(getattr(self, name))
            if value <= 0:
                raise ValueError(f"trade_coverage_invalid: {name} must be positive")
            object.__setattr__(self, name, value)
        if self.last_receive_ordinal < self.opening_receive_ordinal:
            raise ValueError("trade_coverage_invalid: ordinal range is reversed")
        if self.closing_receive_ordinal is not None:
            closing_ordinal = int(self.closing_receive_ordinal)
            if closing_ordinal < self.opening_receive_ordinal:
                raise ValueError("trade_coverage_invalid: closing ordinal is reversed")
            object.__setattr__(self, "closing_receive_ordinal", closing_ordinal)
        object.__setattr__(self, "revision", int(self.revision))
        object.__setattr__(self, "connection_epoch", int(self.connection_epoch))
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "ordering_assurance", assurance)
        object.__setattr__(self, "archive_status", archive_status)
        object.__setattr__(self, "opening_effective_at", opening_effective)
        object.__setattr__(self, "last_effective_at", last_effective)
        object.__setattr__(self, "closing_effective_at", closing_effective)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "gap_quality_event_ids", tuple(self.gap_quality_event_ids))
        object.__setattr__(self, "opening_evidence", dict(self.opening_evidence or {}))
        object.__setattr__(self, "closing_evidence", dict(self.closing_evidence or {}))

    def complete_for_bucket(self, *, bucket_start: datetime, bucket_end: datetime) -> bool:
        """Apply the v1 zero/complete rule to one half-open UTC bucket."""

        start = _utc(bucket_start, field_name="bucket_start")
        end = _utc(bucket_end, field_name="bucket_end")
        if end <= start:
            raise ValueError("trade_coverage_invalid: bucket end must follow start")
        if self.status is CoverageStatus.INVALID:
            return False
        if self.ordering_assurance not in {
            OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS,
            OrderingAssurance.PROVIDER_DELIVERY_GUARANTEED,
        }:
            return False
        if self.archive_status is not ArchiveStatus.COMPLETE:
            return False
        if self.gap_quality_event_ids:
            return False
        close_time = self.closing_effective_at or self.last_effective_at
        close_ordinal = self.closing_receive_ordinal or self.last_receive_ordinal
        if self.opening_effective_at > start or close_time < end:
            return False
        if self.archive_complete_through_ordinal < close_ordinal:
            return False
        if self.canonicalization_watermark_ordinal < close_ordinal:
            return False
        return True

    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": "market.trade_coverage.v1",
                "interval_id": self.interval_id,
                "revision": self.revision,
                "definition_id": self.definition_id,
                "session_id": self.session_id,
                "connection_epoch": self.connection_epoch,
                "provider_product_id": self.provider_product_id,
                "channel": self.channel,
                "status": self.status.value,
                "ordering_assurance": self.ordering_assurance.value,
                "archive_status": self.archive_status.value,
                "opening_raw_record_id": self.opening_raw_record_id,
                "opening_receive_ordinal": self.opening_receive_ordinal,
                "opening_effective_at": _canonical_time(self.opening_effective_at),
                "last_raw_record_id": self.last_raw_record_id,
                "last_receive_ordinal": self.last_receive_ordinal,
                "last_effective_at": _canonical_time(self.last_effective_at),
                "closing_raw_record_id": self.closing_raw_record_id,
                "closing_receive_ordinal": self.closing_receive_ordinal,
                "closing_effective_at": _canonical_time(self.closing_effective_at),
                "canonicalization_watermark_ordinal": self.canonicalization_watermark_ordinal,
                "archive_complete_through_ordinal": self.archive_complete_through_ordinal,
                "first_provider_sequence_num": self.first_provider_sequence_num,
                "last_provider_sequence_num": self.last_provider_sequence_num,
                "gap_quality_event_ids": sorted(self.gap_quality_event_ids),
            }
        )


@dataclass(frozen=True)
class TradeFlowAggregateFact:
    """One deterministic causal trade-flow bucket revision."""

    interval_seconds: int
    bucket_start: datetime
    bucket_end: datetime
    trade_count: int
    maker_buy_count: int
    maker_sell_count: int
    aggressor_buy_count: Optional[int]
    aggressor_sell_count: Optional[int]
    contract_volume: Optional[Decimal]
    base_volume: Optional[Decimal]
    quote_notional: Optional[Decimal]
    maker_buy_base_volume: Optional[Decimal]
    maker_sell_base_volume: Optional[Decimal]
    aggressor_buy_base_volume: Optional[Decimal]
    aggressor_sell_base_volume: Optional[Decimal]
    cvd_delta: Optional[Decimal]
    cvd_unit: Optional[str]
    open_price: Optional[Decimal]
    high_price: Optional[Decimal]
    low_price: Optional[Decimal]
    close_price: Optional[Decimal]
    first_trade_id: Optional[str]
    last_trade_id: Optional[str]
    first_receive_ordinal: Optional[int]
    last_receive_ordinal: Optional[int]
    coverage_interval_id: Optional[str]
    coverage_revision: Optional[int]
    aggregate_complete: bool
    archive_complete: bool
    canonicalization_complete: bool
    late_trade_count: int
    known_at: datetime
    input_fingerprint: str

    def __post_init__(self) -> None:
        interval = int(self.interval_seconds)
        if interval not in {1, 60}:
            raise ValueError("trade_flow_fact_invalid: interval must be 1 or 60 seconds")
        start = _utc(self.bucket_start, field_name="bucket_start")
        end = _utc(self.bucket_end, field_name="bucket_end")
        known_at = _utc(self.known_at, field_name="known_at")
        if end != start + timedelta(seconds=interval):
            raise ValueError("trade_flow_fact_invalid: bucket bounds do not match interval")
        if known_at < end:
            raise ValueError("trade_flow_fact_invalid: known_at precedes bucket end")

        counts: dict[str, int] = {}
        for name in (
            "trade_count",
            "maker_buy_count",
            "maker_sell_count",
            "late_trade_count",
        ):
            value = int(getattr(self, name))
            if value < 0:
                raise ValueError(f"trade_flow_fact_invalid: {name} must be nonnegative")
            counts[name] = value
            object.__setattr__(self, name, value)
        if counts["maker_buy_count"] + counts["maker_sell_count"] != counts["trade_count"]:
            raise ValueError("trade_flow_fact_invalid: maker counts do not reconcile")
        if counts["late_trade_count"] > counts["trade_count"]:
            raise ValueError("trade_flow_fact_invalid: late count exceeds trade count")

        aggressor_counts = (self.aggressor_buy_count, self.aggressor_sell_count)
        if (aggressor_counts[0] is None) != (aggressor_counts[1] is None):
            raise ValueError("trade_flow_fact_invalid: aggressor counts must be jointly known")
        if aggressor_counts[0] is not None:
            aggressor_buy = int(aggressor_counts[0])
            aggressor_sell = int(aggressor_counts[1])
            if aggressor_buy < 0 or aggressor_sell < 0:
                raise ValueError("trade_flow_fact_invalid: aggressor counts must be nonnegative")
            if aggressor_buy + aggressor_sell != counts["trade_count"]:
                raise ValueError("trade_flow_fact_invalid: aggressor counts do not reconcile")
            object.__setattr__(self, "aggressor_buy_count", aggressor_buy)
            object.__setattr__(self, "aggressor_sell_count", aggressor_sell)

        for name in (
            "contract_volume",
            "base_volume",
            "quote_notional",
            "maker_buy_base_volume",
            "maker_sell_base_volume",
            "aggressor_buy_base_volume",
            "aggressor_sell_base_volume",
        ):
            value = _optional_decimal(
                getattr(self, name), field_name=name, nonnegative=True
            )
            object.__setattr__(self, name, value)
        cvd = _optional_decimal(self.cvd_delta, field_name="cvd_delta")
        cvd_unit = str(self.cvd_unit or "").strip().lower() or None
        if (cvd is None) != (cvd_unit is None):
            raise ValueError("trade_flow_fact_invalid: cvd value and unit must be jointly known")
        if cvd_unit not in {None, "base"}:
            raise ValueError("trade_flow_fact_invalid: unsupported cvd unit")
        object.__setattr__(self, "cvd_delta", cvd)
        object.__setattr__(self, "cvd_unit", cvd_unit)

        price_names = ("open_price", "high_price", "low_price", "close_price")
        prices = {
            name: _optional_decimal(getattr(self, name), field_name=name, positive=True)
            for name in price_names
        }
        for name, value in prices.items():
            object.__setattr__(self, name, value)
        if counts["trade_count"] == 0:
            if any(value is not None for value in prices.values()):
                raise ValueError("trade_flow_fact_invalid: zero bucket cannot have prices")
            if any(
                value is not None
                for value in (
                    self.first_trade_id,
                    self.last_trade_id,
                    self.first_receive_ordinal,
                    self.last_receive_ordinal,
                )
            ):
                raise ValueError("trade_flow_fact_invalid: zero bucket cannot have trade bounds")
        else:
            if any(value is None for value in prices.values()):
                raise ValueError("trade_flow_fact_invalid: populated bucket requires OHLC")
            assert prices["high_price"] is not None and prices["low_price"] is not None
            assert prices["open_price"] is not None and prices["close_price"] is not None
            if prices["high_price"] < max(prices["open_price"], prices["close_price"]):
                raise ValueError("trade_flow_fact_invalid: high price is inconsistent")
            if prices["low_price"] > min(prices["open_price"], prices["close_price"]):
                raise ValueError("trade_flow_fact_invalid: low price is inconsistent")
            if not str(self.first_trade_id or "").strip() or not str(self.last_trade_id or "").strip():
                raise ValueError("trade_flow_fact_invalid: populated bucket requires trade ids")
            for name in ("first_receive_ordinal", "last_receive_ordinal"):
                value = int(getattr(self, name) or 0)
                if value <= 0:
                    raise ValueError(f"trade_flow_fact_invalid: {name} must be positive")
                object.__setattr__(self, name, value)

        coverage_id = str(self.coverage_interval_id or "").strip() or None
        coverage_revision = (
            int(self.coverage_revision) if self.coverage_revision is not None else None
        )
        if (coverage_id is None) != (coverage_revision is None):
            raise ValueError("trade_flow_fact_invalid: coverage identity is incomplete")
        if coverage_revision is not None and coverage_revision <= 0:
            raise ValueError("trade_flow_fact_invalid: coverage revision must be positive")
        aggregate_complete = bool(self.aggregate_complete)
        archive_complete = bool(self.archive_complete)
        canonicalization_complete = bool(self.canonicalization_complete)
        if aggregate_complete and not (
            archive_complete and canonicalization_complete and coverage_id is not None
        ):
            raise ValueError(
                "trade_flow_fact_invalid: complete aggregate requires complete archive, canonicalization, and coverage"
            )

        fingerprint = str(self.input_fingerprint or "").strip().lower()
        if len(fingerprint) != 64 or any(
            character not in "0123456789abcdef" for character in fingerprint
        ):
            raise ValueError("trade_flow_fact_invalid: input fingerprint is invalid")
        object.__setattr__(self, "interval_seconds", interval)
        object.__setattr__(self, "bucket_start", start)
        object.__setattr__(self, "bucket_end", end)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "coverage_interval_id", coverage_id)
        object.__setattr__(self, "coverage_revision", coverage_revision)
        object.__setattr__(self, "aggregate_complete", aggregate_complete)
        object.__setattr__(self, "archive_complete", archive_complete)
        object.__setattr__(self, "canonicalization_complete", canonicalization_complete)
        object.__setattr__(self, "input_fingerprint", fingerprint)

    @property
    def material_hash(self) -> str:
        payload: dict[str, Any] = {
            "schema_version": TRADE_FLOW_MATERIAL_HASH_VERSION,
            "interval_seconds": self.interval_seconds,
            "bucket_start": _canonical_time(self.bucket_start),
            "bucket_end": _canonical_time(self.bucket_end),
            "trade_count": self.trade_count,
            "maker_buy_count": self.maker_buy_count,
            "maker_sell_count": self.maker_sell_count,
            "aggressor_buy_count": self.aggressor_buy_count,
            "aggressor_sell_count": self.aggressor_sell_count,
            "contract_volume": _canonical_decimal(self.contract_volume),
            "base_volume": _canonical_decimal(self.base_volume),
            "quote_notional": _canonical_decimal(self.quote_notional),
            "maker_buy_base_volume": _canonical_decimal(self.maker_buy_base_volume),
            "maker_sell_base_volume": _canonical_decimal(self.maker_sell_base_volume),
            "aggressor_buy_base_volume": _canonical_decimal(self.aggressor_buy_base_volume),
            "aggressor_sell_base_volume": _canonical_decimal(self.aggressor_sell_base_volume),
            "cvd_delta": _canonical_decimal(self.cvd_delta),
            "cvd_unit": self.cvd_unit,
            "open_price": _canonical_decimal(self.open_price),
            "high_price": _canonical_decimal(self.high_price),
            "low_price": _canonical_decimal(self.low_price),
            "close_price": _canonical_decimal(self.close_price),
            "first_trade_id": self.first_trade_id,
            "last_trade_id": self.last_trade_id,
            "first_receive_ordinal": self.first_receive_ordinal,
            "last_receive_ordinal": self.last_receive_ordinal,
            "coverage_interval_id": self.coverage_interval_id,
            "coverage_revision": self.coverage_revision,
            "aggregate_complete": self.aggregate_complete,
            "archive_complete": self.archive_complete,
            "canonicalization_complete": self.canonicalization_complete,
            "late_trade_count": self.late_trade_count,
            "input_fingerprint": self.input_fingerprint,
        }
        return _stable_hash(payload)


@dataclass(frozen=True)
class TradeFlowAggregateRecord:
    """A trade-flow fact plus immutable database revision identity."""

    version_id: str
    series_id: int
    revision: int
    market_commit_seq: int
    aggregation_version: str
    provenance_hash: str
    quality: Mapping[str, Any]
    fact: TradeFlowAggregateFact

    def __post_init__(self) -> None:
        if not str(self.version_id or "").strip():
            raise ValueError("trade_flow_record_invalid: version_id is required")
        for name in ("series_id", "revision", "market_commit_seq"):
            if int(getattr(self, name)) <= 0:
                raise ValueError(f"trade_flow_record_invalid: {name} must be positive")
            object.__setattr__(self, name, int(getattr(self, name)))
        if not str(self.aggregation_version or "").strip():
            raise ValueError("trade_flow_record_invalid: aggregation_version is required")
        if len(str(self.provenance_hash or "")) != 64:
            raise ValueError("trade_flow_record_invalid: provenance_hash is invalid")
        object.__setattr__(self, "quality", dict(self.quality or {}))


def build_market_trade_material_hash(
    *,
    series_identity: Mapping[str, Any],
    records: Iterable[MarketTradeRecord],
) -> str:
    """Hash one exact visible trade series independently of DB revisions."""

    rows = sorted(
        records,
        key=lambda record: (
            record.fact.provider_event_time,
            record.fact.provider_product_id,
            record.fact.provider_trade_id,
        ),
    )
    seen: set[tuple[str, str]] = set()
    material: list[dict[str, Any]] = []
    for record in rows:
        identity = (
            record.fact.provider_product_id,
            record.fact.provider_trade_id,
        )
        if identity in seen:
            raise ValueError(
                "market_trade_material_hash_invalid: duplicate provider trade identity"
            )
        seen.add(identity)
        material.append(
            {
                "provider_event_time": _canonical_time(
                    record.fact.provider_event_time
                ),
                "provider_product_id": record.fact.provider_product_id,
                "provider_trade_id": record.fact.provider_trade_id,
                "row_hash": record.fact.row_hash,
            }
        )
    return _stable_hash(
        {
            "schema_version": TRADE_SERIES_MATERIAL_HASH_VERSION,
            "series": dict(series_identity),
            "rows": material,
        }
    )


def build_trade_flow_material_hash(
    *,
    series_identity: Mapping[str, Any],
    records: Iterable[TradeFlowAggregateRecord],
) -> str:
    """Hash one exact visible aggregate series independently of revisions."""

    rows = sorted(
        records,
        key=lambda record: (
            record.fact.bucket_start,
            record.fact.interval_seconds,
            record.aggregation_version,
        ),
    )
    seen: set[tuple[datetime, int, str]] = set()
    material: list[dict[str, Any]] = []
    for record in rows:
        identity = (
            record.fact.bucket_start,
            record.fact.interval_seconds,
            record.aggregation_version,
        )
        if identity in seen:
            raise ValueError(
                "trade_flow_material_hash_invalid: duplicate aggregate identity"
            )
        seen.add(identity)
        material.append(
            {
                "bucket_start": _canonical_time(record.fact.bucket_start),
                "interval_seconds": record.fact.interval_seconds,
                "aggregation_version": record.aggregation_version,
                "material_hash": record.fact.material_hash,
            }
        )
    return _stable_hash(
        {
            "schema_version": TRADE_FLOW_SERIES_MATERIAL_HASH_VERSION,
            "series": dict(series_identity),
            "rows": material,
        }
    )


def bucket_start_for(value: datetime, *, interval_seconds: int) -> datetime:
    interval = int(interval_seconds)
    if interval not in {1, 60}:
        raise ValueError("trade_flow_invalid: v1 interval must be 1 or 60 seconds")
    timestamp = _utc(value, field_name="event_time")
    epoch_seconds = int(timestamp.timestamp())
    return datetime.fromtimestamp(epoch_seconds - (epoch_seconds % interval), tz=UTC)


def aggregate_trade_bucket(
    trades: Iterable[MarketTradeFact],
    *,
    interval_seconds: int,
    bucket_start: datetime,
    coverage: Optional[TradeCoverageIntervalVersion],
    computed_at: datetime,
    late_known_after: Optional[datetime] = None,
) -> TradeFlowAggregateFact:
    """Aggregate a known-at prefix with stable ordering and exact Decimal sums."""

    start = _utc(bucket_start, field_name="bucket_start")
    interval = int(interval_seconds)
    if interval not in {1, 60}:
        raise ValueError("trade_flow_invalid: v1 interval must be 1 or 60 seconds")
    end = start + timedelta(seconds=interval)
    computed = _utc(computed_at, field_name="computed_at")
    if computed < end:
        raise ValueError("trade_flow_invalid: bucket cannot emit before bucket end")
    candidates = [
        trade
        for trade in trades
        if start <= trade.provider_event_time < end
    ]
    identity_material: dict[tuple[str, str], MarketTradeFact] = {}

    def delivery_order(trade: MarketTradeFact) -> tuple[Any, ...]:
        return (
            trade.known_at,
            trade.received_at,
            trade.connection_epoch,
            trade.receive_ordinal,
            trade.event_ordinal,
            trade.trade_ordinal,
            trade.raw_record_id,
        )

    for trade in candidates:
        identity = (trade.provider_product_id, trade.provider_trade_id)
        prior = identity_material.get(identity)
        if prior is None:
            identity_material[identity] = trade
            continue
        if prior.material_hash != trade.material_hash:
            raise ValueError(
                "trade_flow_invalid: conflicting provider trade identity in aggregate"
            )
        if delivery_order(trade) < delivery_order(prior):
            identity_material[identity] = trade
    rows = sorted(
        identity_material.values(),
        key=lambda trade: (
            trade.provider_event_time,
            trade.provider_sequence_num if trade.provider_sequence_num is not None else 2**63,
            trade.connection_epoch,
            trade.receive_ordinal,
            trade.event_ordinal,
            trade.trade_ordinal,
            trade.provider_trade_id,
        ),
    )
    complete = bool(
        coverage and coverage.complete_for_bucket(bucket_start=start, bucket_end=end)
    )
    if not rows and not complete:
        raise ValueError(
            "trade_flow_incomplete_zero_forbidden: zero rows require proven complete coverage"
        )
    maker_buy = [trade for trade in rows if trade.maker_side is MarketSide.BUY]
    maker_sell = [trade for trade in rows if trade.maker_side is MarketSide.SELL]
    aggressor_available = all(trade.aggressor_side is not None for trade in rows)
    if not rows:
        aggressor_available = True
    aggressor_buy = [trade for trade in rows if trade.aggressor_side is MarketSide.BUY]
    aggressor_sell = [trade for trade in rows if trade.aggressor_side is MarketSide.SELL]

    def sum_optional(field_name: str) -> Optional[Decimal]:
        values = [getattr(trade, field_name) for trade in rows]
        if any(value is None for value in values):
            return None
        return sum((value for value in values if value is not None), Decimal(0))

    def sum_side(side_rows: list[MarketTradeFact]) -> Optional[Decimal]:
        values = [trade.base_quantity for trade in side_rows]
        if any(value is None for value in values):
            return None
        return sum((value for value in values if value is not None), Decimal(0))

    # A zero-trade bucket cannot infer whether the series is contract-sized from
    # its rows. Keep contract volume absent for that case; the series contract
    # still makes base and quote zeroes unambiguous.
    contract_volume = sum_optional("contract_quantity") if rows else None
    base_volume = sum_optional("base_quantity")
    quote_notional = sum_optional("quote_notional")
    maker_buy_base = sum_side(maker_buy)
    maker_sell_base = sum_side(maker_sell)
    aggressor_buy_base = sum_side(aggressor_buy) if aggressor_available else None
    aggressor_sell_base = sum_side(aggressor_sell) if aggressor_available else None
    cvd = (
        aggressor_buy_base - aggressor_sell_base
        if aggressor_buy_base is not None and aggressor_sell_base is not None
        else None
    )
    late_threshold = (
        _utc(late_known_after, field_name="late_known_after")
        if late_known_after is not None
        else None
    )
    late_count = sum(
        1 for trade in rows if late_threshold is not None and trade.known_at > late_threshold
    )
    input_fingerprint = _stable_hash(
        {
            "schema_version": "market.trade_flow_inputs.v1",
            "rows": [
                {
                    "provider_product_id": trade.provider_product_id,
                    "provider_trade_id": trade.provider_trade_id,
                    "material_hash": trade.material_hash,
                }
                for trade in rows
            ],
            "coverage_hash": coverage.material_hash if coverage else None,
        }
    )
    prices = [trade.price for trade in rows]
    known_at = max(
        [computed]
        + [trade.known_at for trade in rows]
        + ([coverage.known_at] if coverage is not None else [])
    )
    return TradeFlowAggregateFact(
        interval_seconds=interval,
        bucket_start=start,
        bucket_end=end,
        trade_count=len(rows),
        maker_buy_count=len(maker_buy),
        maker_sell_count=len(maker_sell),
        aggressor_buy_count=len(aggressor_buy) if aggressor_available else None,
        aggressor_sell_count=len(aggressor_sell) if aggressor_available else None,
        contract_volume=contract_volume,
        base_volume=base_volume,
        quote_notional=quote_notional,
        maker_buy_base_volume=maker_buy_base,
        maker_sell_base_volume=maker_sell_base,
        aggressor_buy_base_volume=aggressor_buy_base,
        aggressor_sell_base_volume=aggressor_sell_base,
        cvd_delta=cvd,
        cvd_unit="base" if cvd is not None else None,
        open_price=prices[0] if prices else None,
        high_price=max(prices) if prices else None,
        low_price=min(prices) if prices else None,
        close_price=prices[-1] if prices else None,
        first_trade_id=rows[0].provider_trade_id if rows else None,
        last_trade_id=rows[-1].provider_trade_id if rows else None,
        first_receive_ordinal=rows[0].receive_ordinal if rows else None,
        last_receive_ordinal=rows[-1].receive_ordinal if rows else None,
        coverage_interval_id=coverage.interval_id if coverage else None,
        coverage_revision=coverage.revision if coverage else None,
        aggregate_complete=complete,
        archive_complete=bool(coverage and coverage.archive_status is ArchiveStatus.COMPLETE),
        canonicalization_complete=bool(
            coverage
            and coverage.canonicalization_watermark_ordinal
            >= (coverage.closing_receive_ordinal or coverage.last_receive_ordinal)
        ),
        late_trade_count=late_count,
        known_at=known_at,
        input_fingerprint=input_fingerprint,
    )


__all__ = [
    "ArchiveStatus",
    "COINBASE_AGGRESSOR_TRANSFORM_VERSION",
    "CoverageStatus",
    "MARKET_TRADE_FACT_TYPE",
    "MARKET_TRADE_FACT_VERSION",
    "MarketSide",
    "MarketTradeFact",
    "MarketTradeRecord",
    "OrderingAssurance",
    "ProductContract",
    "ProviderSizeUnit",
    "RawStreamRecord",
    "TRADE_FLOW_FACT_TYPE",
    "TRADE_FLOW_FACT_VERSION",
    "TradeCoverageIntervalVersion",
    "TradeDeliveryKind",
    "TradeFlowAggregateFact",
    "TradeFlowAggregateRecord",
    "UnsupportedMarketTradeSideError",
    "build_market_trade_material_hash",
    "build_trade_flow_material_hash",
    "aggregate_trade_bucket",
    "bucket_start_for",
    "build_raw_record_id",
    "build_spool_segment_id",
    "translate_coinbase_market_trade",
]
