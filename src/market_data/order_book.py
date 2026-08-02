"""Deterministic aggregated Level 2 reconstruction contracts.

Coinbase Advanced Trade publishes absolute price-level quantities.  This module
keeps provider evidence typed, applies one provider event atomically, and makes
validity an explicit state rather than an inference from the presence of rows.
It performs no database I/O and is shared by live capture and archive replay.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

from data_providers.streams.contracts import CanonicalMarketEvent

from .structure import OrderingAssurance, ProviderSizeUnit, RawStreamRecord


L2_BOOK_FACT_TYPE = "market.l2_book"
L2_BOOK_FACT_VERSION = "market.l2_book.v1"
L2_SNAPSHOT_FACT_TYPE = "market.l2_snapshot"
L2_SNAPSHOT_FACT_VERSION = "market.l2_snapshot.v1"
L2_MUTATION_FACT_TYPE = "market.l2_mutation"
L2_MUTATION_FACT_VERSION = "market.l2_mutation.v1"
BOOK_CHECKPOINT_VERSION = "market.book_checkpoint.v1"
BOOK_RECONSTRUCTION_VERSION = "coinbase_advanced_trade_l2_absolute.v1"
BOOK_STATE_HASH_VERSION = "market.book_state_hash.v1"
BOOK_CHECKPOINT_SCHEMA_VERSION = "market.book_checkpoint_levels.v1"
CHECKPOINT_MAX_AGE = timedelta(minutes=5)
CHECKPOINT_MAX_MUTATIONS = 100_000


class BookSide(str, Enum):
    BID = "bid"
    ASK = "ask"


class L2EventType(str, Enum):
    SNAPSHOT = "snapshot"
    UPDATE = "update"


class BookLifecycle(str, Enum):
    AWAITING_SNAPSHOT = "awaiting_snapshot"
    VALID = "valid"
    INVALID = "invalid"


class BookValidityStatus(str, Enum):
    OPEN_VALID = "open_valid"
    CLOSED_VALID = "closed_valid"
    CLOSED_INVALIDATED = "closed_invalidated"


def _utc(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"market_l2_invalid: {field_name} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"market_l2_invalid: {field_name} must be ISO-8601"
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
        raise ValueError(f"market_l2_invalid: {field_name} must be decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"market_l2_invalid: {field_name} must be decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"market_l2_invalid: {field_name} must be finite")
    if positive and parsed <= 0:
        raise ValueError(f"market_l2_invalid: {field_name} must be positive")
    if nonnegative and parsed < 0:
        raise ValueError(f"market_l2_invalid: {field_name} must be nonnegative")
    return Decimal(0) if parsed == 0 else parsed


def _canonical_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


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


def _id(prefix: str, payload: Mapping[str, Any]) -> str:
    return f"{prefix}_{_stable_hash(payload)}"


@dataclass(frozen=True)
class L2ProductContract:
    provider_product_id: str
    product_definition_version_id: str
    provider_size_unit: ProviderSizeUnit | str
    price_increment: Optional[Decimal] = None
    quantity_increment: Optional[Decimal] = None

    def __post_init__(self) -> None:
        product = str(self.provider_product_id or "").strip()
        definition = str(self.product_definition_version_id or "").strip()
        if not product or not definition:
            raise ValueError("market_l2_contract_invalid: identity is required")
        try:
            unit = ProviderSizeUnit(
                str(getattr(self.provider_size_unit, "value", self.provider_size_unit))
            )
        except ValueError as exc:
            raise ValueError("market_l2_contract_invalid: size unit is unknown") from exc
        price_increment = (
            _decimal(self.price_increment, field_name="price_increment", positive=True)
            if self.price_increment is not None
            else None
        )
        quantity_increment = (
            _decimal(
                self.quantity_increment,
                field_name="quantity_increment",
                positive=True,
            )
            if self.quantity_increment is not None
            else None
        )
        object.__setattr__(self, "provider_product_id", product)
        object.__setattr__(self, "product_definition_version_id", definition)
        object.__setattr__(self, "provider_size_unit", unit)
        object.__setattr__(self, "price_increment", price_increment)
        object.__setattr__(self, "quantity_increment", quantity_increment)

    def validate_level(self, *, price: Decimal, quantity: Decimal) -> None:
        if self.price_increment is not None and price % self.price_increment != 0:
            raise ValueError("market_l2_invalid: price violates product increment")
        if self.quantity_increment is not None and quantity % self.quantity_increment != 0:
            raise ValueError("market_l2_invalid: quantity violates product increment")
        if (
            self.provider_size_unit is ProviderSizeUnit.CONTRACTS
            and quantity != quantity.to_integral_value()
        ):
            raise ValueError("market_l2_invalid: futures quantity is not integral contracts")


@dataclass(frozen=True)
class BookSourcePosition:
    definition_id: str
    session_id: str
    connection_epoch: int
    provider_product_id: str
    provider_sequence_num: Optional[int]
    receive_ordinal: int
    event_ordinal: int

    def __post_init__(self) -> None:
        for name in ("definition_id", "session_id", "provider_product_id"):
            value = str(getattr(self, name) or "").strip()
            if not value:
                raise ValueError(f"market_l2_position_invalid: {name} is required")
            object.__setattr__(self, name, value)
        if int(self.connection_epoch) < 0 or int(self.receive_ordinal) <= 0:
            raise ValueError("market_l2_position_invalid: source ordinal is invalid")
        if int(self.event_ordinal) < 0:
            raise ValueError("market_l2_position_invalid: event ordinal is invalid")
        object.__setattr__(self, "connection_epoch", int(self.connection_epoch))
        object.__setattr__(self, "receive_ordinal", int(self.receive_ordinal))
        object.__setattr__(self, "event_ordinal", int(self.event_ordinal))
        if self.provider_sequence_num is not None:
            object.__setattr__(
                self, "provider_sequence_num", int(self.provider_sequence_num)
            )

    def material(self) -> Mapping[str, Any]:
        return {
            "definition_id": self.definition_id,
            "session_id": self.session_id,
            "connection_epoch": self.connection_epoch,
            "provider_product_id": self.provider_product_id,
            "provider_sequence_num": self.provider_sequence_num,
            "receive_ordinal": self.receive_ordinal,
            "event_ordinal": self.event_ordinal,
        }


@dataclass(frozen=True)
class L2Mutation:
    mutation_ordinal: int
    side: BookSide | str
    price: Decimal
    new_quantity: Decimal
    provider_event_time: datetime
    provider_size_unit: ProviderSizeUnit | str

    def __post_init__(self) -> None:
        ordinal = int(self.mutation_ordinal)
        if ordinal < 0:
            raise ValueError("market_l2_invalid: mutation ordinal is negative")
        raw_side = str(getattr(self.side, "value", self.side)).strip().lower()
        if raw_side == "offer":
            raw_side = "ask"
        try:
            side = BookSide(raw_side)
            unit = ProviderSizeUnit(
                str(getattr(self.provider_size_unit, "value", self.provider_size_unit))
            )
        except ValueError as exc:
            raise ValueError("market_l2_invalid: side or size unit is unknown") from exc
        object.__setattr__(self, "mutation_ordinal", ordinal)
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "price", _decimal(self.price, field_name="price", positive=True))
        object.__setattr__(
            self,
            "new_quantity",
            _decimal(self.new_quantity, field_name="new_quantity", nonnegative=True),
        )
        object.__setattr__(
            self,
            "provider_event_time",
            _utc(self.provider_event_time, field_name="provider_event_time"),
        )
        object.__setattr__(self, "provider_size_unit", unit)

    def material(self) -> Mapping[str, Any]:
        return {
            "mutation_ordinal": self.mutation_ordinal,
            "side": self.side.value,
            "price": _canonical_decimal(self.price),
            "new_quantity": _canonical_decimal(self.new_quantity),
            "provider_event_time": _canonical_time(self.provider_event_time),
            "provider_size_unit": self.provider_size_unit.value,
        }


@dataclass(frozen=True)
class L2EventFact:
    event_type: L2EventType | str
    position: BookSourcePosition
    product_definition_version_id: str
    mutations: tuple[L2Mutation, ...]
    provider_message_time: Optional[datetime]
    received_at: datetime
    accepted_at: datetime
    known_at: datetime
    raw_record_id: str

    def __post_init__(self) -> None:
        try:
            event_type = L2EventType(
                str(getattr(self.event_type, "value", self.event_type)).lower()
            )
        except ValueError as exc:
            raise ValueError("market_l2_invalid: unsupported event type") from exc
        definition = str(self.product_definition_version_id or "").strip()
        raw_record = str(self.raw_record_id or "").strip()
        if not definition or not raw_record:
            raise ValueError("market_l2_invalid: definition and raw identity are required")
        mutations = tuple(self.mutations)
        if not mutations:
            raise ValueError("market_l2_invalid: provider event has no mutations")
        if [row.mutation_ordinal for row in mutations] != list(range(len(mutations))):
            raise ValueError("market_l2_invalid: mutation ordinals are not contiguous")
        received = _utc(self.received_at, field_name="received_at")
        accepted = _utc(self.accepted_at, field_name="accepted_at")
        known = _utc(self.known_at, field_name="known_at")
        if accepted < received or known < accepted:
            raise ValueError("market_l2_invalid: causal times are reversed")
        object.__setattr__(self, "event_type", event_type)
        object.__setattr__(self, "product_definition_version_id", definition)
        object.__setattr__(self, "mutations", mutations)
        object.__setattr__(
            self,
            "provider_message_time",
            _optional_utc(self.provider_message_time, field_name="provider_message_time"),
        )
        object.__setattr__(self, "received_at", received)
        object.__setattr__(self, "accepted_at", accepted)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "raw_record_id", raw_record)

    @property
    def effective_at(self) -> datetime:
        return max(row.provider_event_time for row in self.mutations)

    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": "market.l2_event_material.v1",
                "event_type": self.event_type.value,
                "position": self.position.material(),
                "product_definition_version_id": self.product_definition_version_id,
                "mutations": [row.material() for row in self.mutations],
            }
        )

    @property
    def provider_material_hash(self) -> str:
        """Hash provider semantics independently of local redelivery position."""

        return _stable_hash(
            {
                "schema_version": "market.l2_provider_event_material.v1",
                "event_type": self.event_type.value,
                "session_id": self.position.session_id,
                "connection_epoch": self.position.connection_epoch,
                "provider_product_id": self.position.provider_product_id,
                "provider_sequence_num": self.position.provider_sequence_num,
                "event_ordinal": self.position.event_ordinal,
                "product_definition_version_id": self.product_definition_version_id,
                "mutations": [row.material() for row in self.mutations],
            }
        )


def translate_coinbase_l2_event(
    event: CanonicalMarketEvent,
    *,
    raw_record: RawStreamRecord,
    contract: L2ProductContract,
    accepted_at: Optional[datetime] = None,
) -> L2EventFact:
    if event.event_kind not in {"market_l2_snapshot", "market_l2_update"}:
        raise ValueError("market_l2_translation_invalid: event is not Level 2")
    if str(event.provider).upper() != "COINBASE":
        raise ValueError("market_l2_translation_invalid: provider must be Coinbase")
    if str(event.product_id or "") != contract.provider_product_id:
        raise ValueError("market_l2_translation_invalid: product contract mismatch")
    payload = dict(event.payload or {})
    raw_updates = payload.get("updates")
    if not isinstance(raw_updates, Sequence) or isinstance(raw_updates, (str, bytes)):
        raise ValueError("market_l2_translation_invalid: updates must be an array")
    mutations: list[L2Mutation] = []
    for expected_ordinal, raw in enumerate(raw_updates):
        if not isinstance(raw, Mapping):
            raise ValueError("market_l2_translation_invalid: update must be an object")
        ordinal = int(raw.get("mutation_ordinal", expected_ordinal))
        mutation = L2Mutation(
            mutation_ordinal=ordinal,
            side=str(raw.get("side") or ""),
            price=raw.get("price_level"),
            new_quantity=raw.get("new_quantity"),
            provider_event_time=raw.get("event_time"),
            provider_size_unit=contract.provider_size_unit,
        )
        contract.validate_level(price=mutation.price, quantity=mutation.new_quantity)
        mutations.append(mutation)
    accepted = _utc(accepted_at or datetime.now(UTC), field_name="accepted_at")
    if accepted < raw_record.received_at:
        accepted = raw_record.received_at
    return L2EventFact(
        event_type=(
            L2EventType.SNAPSHOT
            if event.event_kind == "market_l2_snapshot"
            else L2EventType.UPDATE
        ),
        position=BookSourcePosition(
            definition_id=raw_record.definition_id,
            session_id=raw_record.session_id,
            connection_epoch=raw_record.connection_epoch,
            provider_product_id=contract.provider_product_id,
            provider_sequence_num=event.provider_sequence_num,
            receive_ordinal=raw_record.receive_ordinal,
            event_ordinal=int(payload.get("event_ordinal") or 0),
        ),
        product_definition_version_id=contract.product_definition_version_id,
        mutations=tuple(mutations),
        provider_message_time=event.provider_message_time,
        received_at=raw_record.received_at,
        accepted_at=accepted,
        known_at=accepted,
        raw_record_id=raw_record.raw_record_id,
    )


@dataclass(frozen=True)
class BookQualityEvidence:
    classification: str
    reason: str
    position: BookSourcePosition
    known_at: datetime
    raw_record_id: str
    invalidating: bool
    evidence: Mapping[str, Any] = field(default_factory=dict)

    @property
    def evidence_hash(self) -> str:
        return _stable_hash(
            {
                "classification": self.classification,
                "reason": self.reason,
                "position": self.position.material(),
                "raw_record_id": self.raw_record_id,
                "invalidating": self.invalidating,
                "evidence": dict(self.evidence),
            }
        )


@dataclass(frozen=True)
class L2SnapshotFact:
    snapshot_id: str
    series_id: int
    event: L2EventFact
    validity_interval_id: str
    state_hash: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]


@dataclass(frozen=True)
class L2MutationBatchFact:
    batch_id: str
    series_id: int
    event: L2EventFact
    validity_interval_id: str
    before_state_hash: str
    after_state_hash: str
    unknown_zero_delete_count: int


@dataclass(frozen=True)
class BookValidityIntervalVersion:
    version_id: str
    interval_id: str
    revision: int
    series_id: int
    status: BookValidityStatus
    ordering_assurance: OrderingAssurance
    opening_snapshot_id: str
    opening_position: BookSourcePosition
    opening_effective_at: datetime
    opening_known_at: datetime
    last_valid_position: BookSourcePosition
    last_valid_effective_at: datetime
    last_state_hash: str
    known_at: datetime
    closing_position: Optional[BookSourcePosition] = None
    closing_effective_at: Optional[datetime] = None
    closing_quality_hash: Optional[str] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class BookCheckpointFact:
    checkpoint_id: str
    series_id: int
    validity_interval_id: str
    source_position: BookSourcePosition
    product_definition_version_id: str
    provider_size_unit: ProviderSizeUnit
    ordering_assurance: OrderingAssurance
    effective_at: datetime
    known_at: datetime
    state_hash: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    mutation_count_since_prior: int

    @property
    def content_fingerprint(self) -> str:
        return _stable_hash(
            {
                "schema_version": BOOK_CHECKPOINT_SCHEMA_VERSION,
                "checkpoint_id": self.checkpoint_id,
                "state_hash": self.state_hash,
                "levels": [
                    ("bid", _canonical_decimal(price), _canonical_decimal(quantity))
                    for price, quantity in self.bids
                ]
                + [
                    ("ask", _canonical_decimal(price), _canonical_decimal(quantity))
                    for price, quantity in self.asks
                ],
            }
        )



@dataclass(frozen=True)
class BookStateView:
    """One immutable valid post-event book state exposed to derived consumers."""

    series_id: int
    validity_interval_id: str
    source_position: BookSourcePosition
    product_definition_version_id: str
    provider_size_unit: ProviderSizeUnit
    effective_at: datetime
    known_at: datetime
    state_hash: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]


@dataclass(frozen=True)
class BookApplyResult:
    accepted: bool
    snapshot: Optional[L2SnapshotFact] = None
    batch: Optional[L2MutationBatchFact] = None
    validity_versions: tuple[BookValidityIntervalVersion, ...] = ()
    checkpoints: tuple[BookCheckpointFact, ...] = ()
    quality: tuple[BookQualityEvidence, ...] = ()
    state: Optional[BookStateView] = None


def book_state_hash(
    *,
    contract: L2ProductContract,
    position: BookSourcePosition,
    ordering_assurance: OrderingAssurance,
    bids: Mapping[Decimal, Decimal],
    asks: Mapping[Decimal, Decimal],
) -> str:
    return _stable_hash(
        {
            "schema_version": BOOK_STATE_HASH_VERSION,
            "reconstruction_version": BOOK_RECONSTRUCTION_VERSION,
            "product_definition_version_id": contract.product_definition_version_id,
            "source_position": position.material(),
            "ordering_assurance": ordering_assurance.value,
            "bids": [
                [_canonical_decimal(price), _canonical_decimal(bids[price])]
                for price in sorted(bids)
            ],
            "asks": [
                [_canonical_decimal(price), _canonical_decimal(asks[price])]
                for price in sorted(asks)
            ],
        }
    )


class Level2BookReconstructor:
    """Stateful deterministic reducer for one product and connection epoch."""

    def __init__(
        self,
        *,
        series_id: int,
        contract: L2ProductContract,
        ordering_assurance: OrderingAssurance = OrderingAssurance.PROVIDER_DELIVERY_GUARANTEED,
    ) -> None:
        if int(series_id) <= 0:
            raise ValueError("market_l2_reconstructor_invalid: series_id must be positive")
        self.series_id = int(series_id)
        self.contract = contract
        self.ordering_assurance = ordering_assurance
        self.lifecycle = BookLifecycle.AWAITING_SNAPSHOT
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.current_state_hash: Optional[str] = None
        self.current_interval: Optional[BookValidityIntervalVersion] = None
        self._seen_events: dict[tuple[Any, ...], str] = {}
        self._last_checkpoint_at: Optional[datetime] = None
        self._mutations_since_checkpoint = 0

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: BookCheckpointFact,
        *,
        contract: L2ProductContract,
        validity: BookValidityIntervalVersion,
    ) -> "Level2BookReconstructor":
        """Resume from verified typed levels without weakening validity."""

        if checkpoint.validity_interval_id != validity.interval_id:
            raise ValueError("market_l2_checkpoint_invalid: validity mismatch")
        if (
            checkpoint.product_definition_version_id
            != contract.product_definition_version_id
        ):
            raise ValueError("market_l2_checkpoint_invalid: product definition mismatch")
        if checkpoint.provider_size_unit is not contract.provider_size_unit:
            raise ValueError("market_l2_checkpoint_invalid: quantity unit mismatch")
        instance = cls(
            series_id=checkpoint.series_id,
            contract=contract,
            ordering_assurance=checkpoint.ordering_assurance,
        )
        instance.bids = dict(checkpoint.bids)
        instance.asks = dict(checkpoint.asks)
        instance._validate_state(instance.bids, instance.asks)
        computed_hash = book_state_hash(
            contract=contract,
            position=checkpoint.source_position,
            ordering_assurance=checkpoint.ordering_assurance,
            bids=instance.bids,
            asks=instance.asks,
        )
        if computed_hash != checkpoint.state_hash:
            raise ValueError("market_l2_checkpoint_invalid: state hash mismatch")
        instance.lifecycle = BookLifecycle.VALID
        instance.current_state_hash = computed_hash
        instance.current_interval = replace(
            validity,
            last_valid_position=checkpoint.source_position,
            last_valid_effective_at=checkpoint.effective_at,
            last_state_hash=checkpoint.state_hash,
            known_at=max(validity.known_at, checkpoint.known_at),
        )
        instance._last_checkpoint_at = checkpoint.effective_at
        return instance

    def _event_key(self, event: L2EventFact) -> tuple[Any, ...]:
        position = event.position
        local_fallback = (
            position.receive_ordinal
            if position.provider_sequence_num is None
            else None
        )
        return (
            position.session_id,
            position.connection_epoch,
            position.provider_product_id,
            position.provider_sequence_num,
            local_fallback,
            position.event_ordinal,
        )

    def _quality(
        self,
        event: L2EventFact,
        *,
        classification: str,
        reason: str,
        invalidating: bool,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> BookQualityEvidence:
        return BookQualityEvidence(
            classification=classification,
            reason=reason,
            position=event.position,
            known_at=event.known_at,
            raw_record_id=event.raw_record_id,
            invalidating=invalidating,
            evidence=dict(evidence or {}),
        )

    def _sorted_levels(
        self,
    ) -> tuple[tuple[tuple[Decimal, Decimal], ...], tuple[tuple[Decimal, Decimal], ...]]:
        return (
            tuple((price, self.bids[price]) for price in sorted(self.bids)),
            tuple((price, self.asks[price]) for price in sorted(self.asks)),
        )

    def _validate_state(
        self, bids: Mapping[Decimal, Decimal], asks: Mapping[Decimal, Decimal]
    ) -> None:
        if not bids or not asks:
            raise ValueError("book state must contain both bid and ask levels")
        if max(bids) >= min(asks):
            raise ValueError("book state is crossed or locked")
        for levels in (bids, asks):
            for price, quantity in levels.items():
                self.contract.validate_level(price=price, quantity=quantity)
                if quantity <= 0:
                    raise ValueError("book state contains nonpositive quantity")

    def _close_current(
        self,
        *,
        event: L2EventFact,
        status: BookValidityStatus,
        reason: str,
        quality_hash: Optional[str],
    ) -> Optional[BookValidityIntervalVersion]:
        return self._close_current_at(
            position=event.position,
            effective_at=event.effective_at,
            known_at=event.known_at,
            status=status,
            reason=reason,
            quality_hash=quality_hash,
        )

    def _close_current_at(
        self,
        *,
        position: BookSourcePosition,
        effective_at: datetime,
        known_at: datetime,
        status: BookValidityStatus,
        reason: str,
        quality_hash: Optional[str],
    ) -> Optional[BookValidityIntervalVersion]:
        current = self.current_interval
        if current is None:
            return None
        closed = BookValidityIntervalVersion(
            version_id=_id(
                "bviv",
                {
                    "interval_id": current.interval_id,
                    "revision": current.revision + 1,
                    "status": status.value,
                    "closing_position": position.material(),
                    "quality_hash": quality_hash,
                },
            ),
            interval_id=current.interval_id,
            revision=current.revision + 1,
            series_id=current.series_id,
            status=status,
            ordering_assurance=current.ordering_assurance,
            opening_snapshot_id=current.opening_snapshot_id,
            opening_position=current.opening_position,
            opening_effective_at=current.opening_effective_at,
            opening_known_at=current.opening_known_at,
            last_valid_position=current.last_valid_position,
            last_valid_effective_at=current.last_valid_effective_at,
            last_state_hash=current.last_state_hash,
            known_at=_utc(known_at, field_name="known_at"),
            closing_position=position,
            closing_effective_at=_utc(effective_at, field_name="effective_at"),
            closing_quality_hash=quality_hash,
            reason=reason,
        )
        self.current_interval = None
        return closed

    def invalidate_transport(
        self,
        *,
        position: BookSourcePosition,
        effective_at: datetime,
        known_at: datetime,
        raw_record_id: str,
        classification: str,
        reason: str,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> BookApplyResult:
        """Invalidate at an exact non-L2 transport position without fabrication."""

        quality = BookQualityEvidence(
            classification=str(classification),
            reason=str(reason),
            position=position,
            known_at=_utc(known_at, field_name="known_at"),
            raw_record_id=str(raw_record_id),
            invalidating=True,
            evidence=dict(evidence or {}),
        )
        closed = self._close_current_at(
            position=position,
            effective_at=effective_at,
            known_at=known_at,
            status=BookValidityStatus.CLOSED_INVALIDATED,
            reason=reason,
            quality_hash=quality.evidence_hash,
        )
        self.lifecycle = BookLifecycle.INVALID
        self.bids = {}
        self.asks = {}
        self.current_state_hash = None
        return BookApplyResult(
            accepted=False,
            validity_versions=(closed,) if closed else (),
            quality=(quality,),
        )

    def invalidate(
        self,
        event: L2EventFact,
        *,
        classification: str,
        reason: str,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> BookApplyResult:
        quality = self._quality(
            event,
            classification=classification,
            reason=reason,
            invalidating=True,
            evidence=evidence,
        )
        closed = self._close_current(
            event=event,
            status=BookValidityStatus.CLOSED_INVALIDATED,
            reason=reason,
            quality_hash=quality.evidence_hash,
        )
        self.lifecycle = BookLifecycle.INVALID
        self.bids = {}
        self.asks = {}
        self.current_state_hash = None
        return BookApplyResult(
            accepted=False,
            validity_versions=(closed,) if closed else (),
            quality=(quality,),
        )

    def process(self, event: L2EventFact) -> BookApplyResult:
        if event.position.provider_product_id != self.contract.provider_product_id:
            raise ValueError("market_l2_reconstructor_invalid: product mismatch")
        if (
            event.product_definition_version_id
            != self.contract.product_definition_version_id
        ):
            raise ValueError("market_l2_reconstructor_invalid: definition mismatch")
        key = self._event_key(event)
        prior_hash = self._seen_events.get(key)
        if prior_hash is not None:
            if prior_hash == event.provider_material_hash:
                quality = self._quality(
                    event,
                    classification="duplicate",
                    reason="exact Level 2 event duplicate",
                    invalidating=False,
                    evidence={"provider_material_hash": event.provider_material_hash},
                )
                return BookApplyResult(accepted=False, quality=(quality,))
            return self.invalidate(
                event,
                classification="divergent_duplicate",
                reason="same Level 2 source identity carried different material",
                evidence={
                    "prior_hash": prior_hash,
                    "new_hash": event.provider_material_hash,
                },
            )
        self._seen_events[key] = event.provider_material_hash
        if event.event_type is L2EventType.SNAPSHOT:
            return self._accept_snapshot(event)
        return self._apply_update(event)

    def _accept_snapshot(self, event: L2EventFact) -> BookApplyResult:
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        try:
            for mutation in event.mutations:
                levels = bids if mutation.side is BookSide.BID else asks
                if mutation.price in levels:
                    raise ValueError("snapshot contains duplicate side/price")
                if mutation.new_quantity <= 0:
                    raise ValueError("snapshot contains zero quantity")
                levels[mutation.price] = mutation.new_quantity
            self._validate_state(bids, asks)
        except ValueError as exc:
            return self.invalidate(
                event,
                classification="book_invalid",
                reason=f"snapshot rejected: {exc}",
            )
        prior_close = self._close_current(
            event=event,
            status=BookValidityStatus.CLOSED_VALID,
            reason="fresh snapshot replaced prior valid state",
            quality_hash=None,
        )
        self.bids = bids
        self.asks = asks
        self.lifecycle = BookLifecycle.VALID
        state_hash = book_state_hash(
            contract=self.contract,
            position=event.position,
            ordering_assurance=self.ordering_assurance,
            bids=bids,
            asks=asks,
        )
        self.current_state_hash = state_hash
        interval_id = _id(
            "bvi",
            {
                "schema_version": "market.book_validity_interval_id.v1",
                "series_id": self.series_id,
                "opening_position": event.position.material(),
                "snapshot_hash": event.material_hash,
            },
        )
        snapshot_id = _id(
            "l2s",
            {
                "schema_version": L2_SNAPSHOT_FACT_VERSION,
                "series_id": self.series_id,
                "event_hash": event.material_hash,
                "state_hash": state_hash,
            },
        )
        validity = BookValidityIntervalVersion(
            version_id=_id(
                "bviv", {"interval_id": interval_id, "revision": 1, "status": "open"}
            ),
            interval_id=interval_id,
            revision=1,
            series_id=self.series_id,
            status=BookValidityStatus.OPEN_VALID,
            ordering_assurance=self.ordering_assurance,
            opening_snapshot_id=snapshot_id,
            opening_position=event.position,
            opening_effective_at=event.effective_at,
            opening_known_at=event.known_at,
            last_valid_position=event.position,
            last_valid_effective_at=event.effective_at,
            last_state_hash=state_hash,
            known_at=event.known_at,
        )
        self.current_interval = validity
        levels = self._sorted_levels()
        snapshot = L2SnapshotFact(
            snapshot_id=snapshot_id,
            series_id=self.series_id,
            event=event,
            validity_interval_id=interval_id,
            state_hash=state_hash,
            bids=levels[0],
            asks=levels[1],
        )
        checkpoint = self._checkpoint(event, mutation_count=0)
        versions = ((prior_close,) if prior_close else ()) + (validity,)
        state = self._state_view(event)
        return BookApplyResult(
            accepted=True,
            snapshot=snapshot,
            validity_versions=versions,
            checkpoints=(checkpoint,),
            state=state,
        )

    def _apply_update(self, event: L2EventFact) -> BookApplyResult:
        if self.lifecycle is not BookLifecycle.VALID or self.current_interval is None:
            quality = self._quality(
                event,
                classification="update_before_snapshot",
                reason="Level 2 update suppressed until a fresh snapshot is accepted",
                invalidating=False,
            )
            return BookApplyResult(accepted=False, quality=(quality,))
        working_bids = dict(self.bids)
        working_asks = dict(self.asks)
        unknown_deletes = 0
        try:
            for mutation in event.mutations:
                levels = working_bids if mutation.side is BookSide.BID else working_asks
                if mutation.new_quantity == 0:
                    if mutation.price not in levels:
                        unknown_deletes += 1
                    levels.pop(mutation.price, None)
                else:
                    levels[mutation.price] = mutation.new_quantity
            self._validate_state(working_bids, working_asks)
        except ValueError as exc:
            return self.invalidate(
                event,
                classification="book_invalid",
                reason=f"atomic update rejected: {exc}",
            )
        before_hash = str(self.current_state_hash)
        after_hash = book_state_hash(
            contract=self.contract,
            position=event.position,
            ordering_assurance=self.ordering_assurance,
            bids=working_bids,
            asks=working_asks,
        )
        self.bids = working_bids
        self.asks = working_asks
        self.current_state_hash = after_hash
        current = self.current_interval
        assert current is not None
        # Keep the latest position in the disposable reducer projection. The
        # append-only validity table receives only opening and closing
        # revisions; every accepted batch carries its own exact state hash.
        self.current_interval = replace(
            current,
            last_valid_position=event.position,
            last_valid_effective_at=event.effective_at,
            last_state_hash=after_hash,
            known_at=event.known_at,
        )
        batch = L2MutationBatchFact(
            batch_id=_id(
                "l2b",
                {
                    "schema_version": L2_MUTATION_FACT_VERSION,
                    "series_id": self.series_id,
                    "event_hash": event.material_hash,
                    "before_state_hash": before_hash,
                    "after_state_hash": after_hash,
                },
            ),
            series_id=self.series_id,
            event=event,
            validity_interval_id=current.interval_id,
            before_state_hash=before_hash,
            after_state_hash=after_hash,
            unknown_zero_delete_count=unknown_deletes,
        )
        self._mutations_since_checkpoint += len(event.mutations)
        checkpoints: tuple[BookCheckpointFact, ...] = ()
        if (
            self._last_checkpoint_at is None
            or event.effective_at - self._last_checkpoint_at >= CHECKPOINT_MAX_AGE
            or self._mutations_since_checkpoint >= CHECKPOINT_MAX_MUTATIONS
        ):
            checkpoints = (
                self._checkpoint(
                    event, mutation_count=self._mutations_since_checkpoint
                ),
            )
        quality: tuple[BookQualityEvidence, ...] = ()
        if unknown_deletes:
            quality = (
                self._quality(
                    event,
                    classification="unknown_zero_delete",
                    reason="absolute zero update targeted an absent price level",
                    invalidating=False,
                    evidence={"count": unknown_deletes},
                ),
            )
        state = self._state_view(event)
        return BookApplyResult(
            accepted=True,
            batch=batch,
            validity_versions=(),
            checkpoints=checkpoints,
            quality=quality,
            state=state,
        )


    def _state_view(self, event: L2EventFact) -> BookStateView:
        if self.current_interval is None or self.current_state_hash is None:
            raise RuntimeError("market_l2_state_view_invalid: no valid book state")
        bids, asks = self._sorted_levels()
        return BookStateView(
            series_id=self.series_id,
            validity_interval_id=self.current_interval.interval_id,
            source_position=event.position,
            product_definition_version_id=self.contract.product_definition_version_id,
            provider_size_unit=self.contract.provider_size_unit,
            effective_at=event.effective_at,
            known_at=event.known_at,
            state_hash=self.current_state_hash,
            bids=bids,
            asks=asks,
        )

    def _checkpoint(
        self, event: L2EventFact, *, mutation_count: int
    ) -> BookCheckpointFact:
        if self.current_interval is None or self.current_state_hash is None:
            raise RuntimeError("market_l2_checkpoint_invalid: no valid book state")
        bids, asks = self._sorted_levels()
        checkpoint_id = _id(
            "bcp",
            {
                "schema_version": BOOK_CHECKPOINT_VERSION,
                "series_id": self.series_id,
                "reconstruction_version": BOOK_RECONSTRUCTION_VERSION,
                "source_position": event.position.material(),
                "state_hash": self.current_state_hash,
            },
        )
        result = BookCheckpointFact(
            checkpoint_id=checkpoint_id,
            series_id=self.series_id,
            validity_interval_id=self.current_interval.interval_id,
            source_position=event.position,
            product_definition_version_id=self.contract.product_definition_version_id,
            provider_size_unit=self.contract.provider_size_unit,
            ordering_assurance=self.ordering_assurance,
            effective_at=event.effective_at,
            known_at=event.known_at,
            state_hash=self.current_state_hash,
            bids=bids,
            asks=asks,
            mutation_count_since_prior=int(mutation_count),
        )
        self._last_checkpoint_at = event.effective_at
        self._mutations_since_checkpoint = 0
        return result

    def close_bounded(self, *, at_event: L2EventFact) -> tuple[BookValidityIntervalVersion, ...]:
        closed = self._close_current(
            event=at_event,
            status=BookValidityStatus.CLOSED_VALID,
            reason="bounded capture ended cleanly",
            quality_hash=None,
        )
        self.lifecycle = BookLifecycle.AWAITING_SNAPSHOT
        self.bids = {}
        self.asks = {}
        self.current_state_hash = None
        return (closed,) if closed else ()


def checkpoint_canonical_rows(
    checkpoint: BookCheckpointFact,
) -> tuple[Mapping[str, Any], ...]:
    rows: list[Mapping[str, Any]] = []
    for side, levels in ((BookSide.BID, checkpoint.bids), (BookSide.ASK, checkpoint.asks)):
        for ordinal, (price, quantity) in enumerate(levels):
            rows.append(
                {
                    "schema_version": BOOK_CHECKPOINT_SCHEMA_VERSION,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "side": side.value,
                    "level_ordinal": ordinal,
                    "price": _canonical_decimal(price),
                    "quantity": _canonical_decimal(quantity),
                    "provider_size_unit": checkpoint.provider_size_unit.value,
                }
            )
    return tuple(rows)


__all__ = [
    "BOOK_CHECKPOINT_SCHEMA_VERSION",
    "BOOK_CHECKPOINT_VERSION",
    "BOOK_RECONSTRUCTION_VERSION",
    "BOOK_STATE_HASH_VERSION",
    "BookApplyResult",
    "BookCheckpointFact",
    "BookLifecycle",
    "BookQualityEvidence",
    "BookStateView",
    "BookSide",
    "BookSourcePosition",
    "BookValidityIntervalVersion",
    "BookValidityStatus",
    "L2EventFact",
    "L2EventType",
    "L2Mutation",
    "L2MutationBatchFact",
    "L2ProductContract",
    "L2SnapshotFact",
    "L2_BOOK_FACT_TYPE",
    "L2_BOOK_FACT_VERSION",
    "L2_MUTATION_FACT_TYPE",
    "L2_MUTATION_FACT_VERSION",
    "L2_SNAPSHOT_FACT_TYPE",
    "L2_SNAPSHOT_FACT_VERSION",
    "Level2BookReconstructor",
    "book_state_hash",
    "checkpoint_canonical_rows",
    "translate_coinbase_l2_event",
]
