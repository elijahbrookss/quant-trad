"""Venue-neutral deterministic spread and aggregated-L2 execution.

The market-data plane owns provider translation and book reconstruction.  This
module consumes only immutable, replay-certified post-event book states and
turns aggressive canonical orders into exact price-level fills.  It performs no
provider I/O and contains no venue-name branches.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional, Sequence

from market_data.order_book import BOOK_RECONSTRUCTION_VERSION, BookStateView

from .execution import FillRejection, FillResult
from .execution_intent import ExecutionIntent, ExecutionOutcome
from .fees import FeeResolver


EXECUTION_BOOK_SNAPSHOT_SCHEMA_VERSION = "execution_book_snapshot.v1"
EXECUTION_BOOK_TAPE_SCHEMA_VERSION = "execution_book_tape.v1"
EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION = "execution_book_tape_bundle.v1"
BOOK_EXECUTION_EVIDENCE_SCHEMA_VERSION = "book_execution_evidence.v1"
SPREAD_AWARE_MODEL_VERSION = "spread_aware_top.v1"
AGGREGATED_L2_MODEL_VERSION = "aggregated_l2_walk.v1"

_CAPABILITY_RANK = {"l1": 1, "l2": 2, "l3": 3}
_EPSILON = Decimal("1e-12")


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


def _decimal(value: Any, *, field_name: str, positive: bool = True) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError(f"{field_name} must be positive and finite")
    return Decimal(0) if parsed == 0 else parsed


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _utc(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"{field_name} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _time_text(value: Any, *, field_name: str) -> str:
    return _utc(value, field_name=field_name).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _required_text(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


@dataclass(frozen=True)
class ExecutionBookLevel:
    """One normalized visible price level."""

    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "price", _decimal(self.price, field_name="book_level.price"))
        object.__setattr__(self, "quantity", _decimal(self.quantity, field_name="book_level.quantity"))

    def to_dict(self) -> dict[str, str]:
        return {
            "price": _decimal_text(self.price),
            "quantity": _decimal_text(self.quantity),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionBookLevel":
        return cls(price=raw.get("price"), quantity=raw.get("quantity"))


@dataclass(frozen=True)
class ExecutionBookSourceReference:
    """Provider-agnostic replay position retained only as audit evidence."""

    definition_id: str
    session_id: str
    connection_epoch: int
    source_product_id: str
    source_sequence: Optional[int]
    receive_ordinal: int
    event_ordinal: int

    def __post_init__(self) -> None:
        for name in ("definition_id", "session_id", "source_product_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name), field_name=f"source_reference.{name}"))
        if isinstance(self.connection_epoch, bool) or int(self.connection_epoch) < 0:
            raise ValueError("source_reference.connection_epoch must be non-negative")
        if isinstance(self.receive_ordinal, bool) or int(self.receive_ordinal) <= 0:
            raise ValueError("source_reference.receive_ordinal must be positive")
        if isinstance(self.event_ordinal, bool) or int(self.event_ordinal) < 0:
            raise ValueError("source_reference.event_ordinal must be non-negative")
        object.__setattr__(self, "connection_epoch", int(self.connection_epoch))
        object.__setattr__(self, "receive_ordinal", int(self.receive_ordinal))
        object.__setattr__(self, "event_ordinal", int(self.event_ordinal))
        if self.source_sequence is not None:
            object.__setattr__(self, "source_sequence", int(self.source_sequence))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionBookSourceReference":
        return cls(
            definition_id=str(raw.get("definition_id") or ""),
            session_id=str(raw.get("session_id") or ""),
            connection_epoch=raw.get("connection_epoch"),
            source_product_id=str(raw.get("source_product_id") or raw.get("provider_product_id") or ""),
            source_sequence=raw.get("source_sequence", raw.get("provider_sequence_num")),
            receive_ordinal=raw.get("receive_ordinal"),
            event_ordinal=raw.get("event_ordinal"),
        )


@dataclass(frozen=True)
class ExecutionBookSnapshot:
    """Immutable post-event book state eligible for order-arrival selection."""

    schema_version: str
    instrument_id: str
    series_id: int
    validity_interval_id: str
    source_reference: ExecutionBookSourceReference
    product_definition_version_id: str
    quantity_unit: str
    effective_at: str
    known_at: str
    reconstruction_state_hash: str
    bids: tuple[ExecutionBookLevel, ...]
    asks: tuple[ExecutionBookLevel, ...]
    snapshot_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != EXECUTION_BOOK_SNAPSHOT_SCHEMA_VERSION:
            raise ValueError(f"unsupported execution book snapshot schema: {self.schema_version}")
        object.__setattr__(self, "instrument_id", _required_text(self.instrument_id, field_name="execution_book_snapshot.instrument_id"))
        if isinstance(self.series_id, bool) or int(self.series_id) <= 0:
            raise ValueError("execution_book_snapshot.series_id must be positive")
        object.__setattr__(self, "series_id", int(self.series_id))
        for name in ("validity_interval_id", "product_definition_version_id", "quantity_unit", "reconstruction_state_hash"):
            object.__setattr__(self, name, _required_text(getattr(self, name), field_name=f"execution_book_snapshot.{name}"))
        object.__setattr__(self, "effective_at", _time_text(self.effective_at, field_name="execution_book_snapshot.effective_at"))
        object.__setattr__(self, "known_at", _time_text(self.known_at, field_name="execution_book_snapshot.known_at"))
        bids = tuple(self.bids)
        asks = tuple(self.asks)
        if not bids or not asks:
            raise ValueError("execution_book_snapshot requires both bid and ask levels")
        if tuple(sorted(bids, key=lambda row: row.price)) != bids:
            raise ValueError("execution_book_snapshot bids must be ascending")
        if tuple(sorted(asks, key=lambda row: row.price)) != asks:
            raise ValueError("execution_book_snapshot asks must be ascending")
        if len({row.price for row in bids}) != len(bids) or len({row.price for row in asks}) != len(asks):
            raise ValueError("execution_book_snapshot contains duplicate prices")
        if bids[-1].price >= asks[0].price:
            raise ValueError("execution_book_snapshot is crossed or locked")
        object.__setattr__(self, "bids", bids)
        object.__setattr__(self, "asks", asks)
        expected = _stable_hash(self._material())
        if self.snapshot_hash and self.snapshot_hash != expected:
            raise ValueError("execution_book_snapshot_hash_mismatch")
        object.__setattr__(self, "snapshot_hash", expected)

    @property
    def known_at_time(self) -> datetime:
        return _utc(self.known_at, field_name="execution_book_snapshot.known_at")

    @property
    def best_bid(self) -> ExecutionBookLevel:
        return self.bids[-1]

    @property
    def best_ask(self) -> ExecutionBookLevel:
        return self.asks[0]

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "instrument_id": self.instrument_id,
            "series_id": self.series_id,
            "validity_interval_id": self.validity_interval_id,
            "source_reference": self.source_reference.to_dict(),
            "product_definition_version_id": self.product_definition_version_id,
            "quantity_unit": self.quantity_unit,
            "effective_at": self.effective_at,
            "known_at": self.known_at,
            "reconstruction_state_hash": self.reconstruction_state_hash,
            "bids": [row.to_dict() for row in self.bids],
            "asks": [row.to_dict() for row in self.asks],
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "snapshot_hash": self.snapshot_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionBookSnapshot":
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            instrument_id=str(raw.get("instrument_id") or ""),
            series_id=raw.get("series_id"),
            validity_interval_id=str(raw.get("validity_interval_id") or ""),
            source_reference=ExecutionBookSourceReference.from_dict(raw.get("source_reference") or {}),
            product_definition_version_id=str(raw.get("product_definition_version_id") or ""),
            quantity_unit=str(raw.get("quantity_unit") or ""),
            effective_at=raw.get("effective_at"),
            known_at=raw.get("known_at"),
            reconstruction_state_hash=str(raw.get("reconstruction_state_hash") or ""),
            bids=tuple(ExecutionBookLevel.from_dict(row) for row in raw.get("bids") or ()),
            asks=tuple(ExecutionBookLevel.from_dict(row) for row in raw.get("asks") or ()),
            snapshot_hash=str(raw.get("snapshot_hash") or ""),
        )

    @classmethod
    def from_book_state(cls, state: BookStateView, *, instrument_id: str) -> "ExecutionBookSnapshot":
        position = state.source_position
        return cls(
            schema_version=EXECUTION_BOOK_SNAPSHOT_SCHEMA_VERSION,
            instrument_id=instrument_id,
            series_id=state.series_id,
            validity_interval_id=state.validity_interval_id,
            source_reference=ExecutionBookSourceReference(
                definition_id=position.definition_id,
                session_id=position.session_id,
                connection_epoch=position.connection_epoch,
                source_product_id=position.provider_product_id,
                source_sequence=position.provider_sequence_num,
                receive_ordinal=position.receive_ordinal,
                event_ordinal=position.event_ordinal,
            ),
            product_definition_version_id=state.product_definition_version_id,
            quantity_unit=str(getattr(state.provider_size_unit, "value", state.provider_size_unit)),
            effective_at=state.effective_at,
            known_at=state.known_at,
            reconstruction_state_hash=state.state_hash,
            bids=tuple(ExecutionBookLevel(price, quantity) for price, quantity in state.bids),
            asks=tuple(ExecutionBookLevel(price, quantity) for price, quantity in state.asks),
        )


@dataclass(frozen=True)
class ExecutionBookValidityClosure:
    """Known-at boundary after which one reconstructed interval is unusable."""

    validity_interval_id: str
    status: str
    known_at: str
    reason: str
    source_reference: ExecutionBookSourceReference
    evidence_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "validity_interval_id", _required_text(self.validity_interval_id, field_name="book_closure.validity_interval_id"))
        status = str(self.status or "").strip().lower()
        if status not in {"closed_valid", "closed_invalidated"}:
            raise ValueError("book_closure.status must be closed_valid or closed_invalidated")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "known_at", _time_text(self.known_at, field_name="book_closure.known_at"))
        object.__setattr__(self, "reason", _required_text(self.reason, field_name="book_closure.reason"))
        object.__setattr__(self, "evidence_hash", _required_text(self.evidence_hash, field_name="book_closure.evidence_hash"))

    @property
    def known_at_time(self) -> datetime:
        return _utc(self.known_at, field_name="book_closure.known_at")

    def to_dict(self) -> dict[str, Any]:
        return {
            "validity_interval_id": self.validity_interval_id,
            "status": self.status,
            "known_at": self.known_at,
            "reason": self.reason,
            "source_reference": self.source_reference.to_dict(),
            "evidence_hash": self.evidence_hash,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionBookValidityClosure":
        return cls(
            validity_interval_id=str(raw.get("validity_interval_id") or ""),
            status=str(raw.get("status") or ""),
            known_at=raw.get("known_at"),
            reason=str(raw.get("reason") or ""),
            source_reference=ExecutionBookSourceReference.from_dict(raw.get("source_reference") or {}),
            evidence_hash=str(raw.get("evidence_hash") or ""),
        )


@dataclass(frozen=True)
class ExecutionBookTape:
    """Hash-verified causal sequence of replay-certified book states."""

    schema_version: str
    tape_id: str
    instrument_id: str
    source_capability: str
    reconstruction_version: str
    replay_fingerprint: str
    replay_certified: bool
    snapshots: tuple[ExecutionBookSnapshot, ...]
    validity_closures: tuple[ExecutionBookValidityClosure, ...] = ()
    limitations: tuple[str, ...] = ()
    tape_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != EXECUTION_BOOK_TAPE_SCHEMA_VERSION:
            raise ValueError(f"unsupported execution book tape schema: {self.schema_version}")
        object.__setattr__(self, "instrument_id", _required_text(self.instrument_id, field_name="execution_book_tape.instrument_id"))
        capability = str(self.source_capability or "").strip().lower()
        if capability not in _CAPABILITY_RANK:
            raise ValueError("execution_book_tape.source_capability must be l1, l2, or l3")
        object.__setattr__(self, "source_capability", capability)
        for name in ("reconstruction_version", "replay_fingerprint"):
            object.__setattr__(self, name, _required_text(getattr(self, name), field_name=f"execution_book_tape.{name}"))
        if not isinstance(self.replay_certified, bool):
            raise ValueError("execution_book_tape.replay_certified must be boolean")
        snapshots = tuple(self.snapshots)
        if not snapshots:
            raise ValueError("execution_book_tape.snapshots must not be empty")
        if any(row.instrument_id != self.instrument_id for row in snapshots):
            raise ValueError("execution_book_tape instrument mismatch")
        ordered = tuple(sorted(snapshots, key=self._selection_key))
        if ordered != snapshots:
            raise ValueError("execution_book_tape snapshots are not causally ordered")
        if len({row.snapshot_hash for row in snapshots}) != len(snapshots):
            raise ValueError("execution_book_tape contains duplicate snapshots")
        object.__setattr__(self, "snapshots", snapshots)
        closures = tuple(
            sorted(
                self.validity_closures,
                key=lambda row: (
                    row.known_at_time,
                    row.source_reference.connection_epoch,
                    row.source_reference.receive_ordinal,
                    row.source_reference.event_ordinal,
                ),
            )
        )
        if len(
            {
                (row.validity_interval_id, row.known_at, row.evidence_hash)
                for row in closures
            }
        ) != len(closures):
            raise ValueError("execution_book_tape contains duplicate validity closures")
        object.__setattr__(self, "validity_closures", closures)
        object.__setattr__(self, "limitations", tuple(sorted({_required_text(item, field_name="execution_book_tape.limitations") for item in self.limitations})))
        expected = _stable_hash(self._material(include_tape_id=False))
        expected_id = f"ebt_{expected[:32]}"
        if self.tape_id and self.tape_id != expected_id:
            raise ValueError("execution_book_tape_id_mismatch")
        if self.tape_hash and self.tape_hash != expected:
            raise ValueError("execution_book_tape_hash_mismatch")
        object.__setattr__(self, "tape_id", expected_id)
        object.__setattr__(self, "tape_hash", expected)

    @staticmethod
    def _selection_key(snapshot: ExecutionBookSnapshot) -> tuple[Any, ...]:
        source = snapshot.source_reference
        return (
            snapshot.known_at_time,
            source.connection_epoch,
            source.receive_ordinal,
            source.event_ordinal,
            snapshot.snapshot_hash,
        )

    def _material(self, *, include_tape_id: bool = True) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "instrument_id": self.instrument_id,
            "source_capability": self.source_capability,
            "reconstruction_version": self.reconstruction_version,
            "replay_fingerprint": self.replay_fingerprint,
            "replay_certified": self.replay_certified,
            "snapshot_hashes": [row.snapshot_hash for row in self.snapshots],
            "snapshots": [row.to_dict() for row in self.snapshots],
            "validity_closures": [row.to_dict() for row in self.validity_closures],
            "limitations": list(self.limitations),
        }
        if include_tape_id:
            payload["tape_id"] = self.tape_id
        return payload

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "tape_hash": self.tape_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionBookTape":
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            tape_id=str(raw.get("tape_id") or ""),
            instrument_id=str(raw.get("instrument_id") or ""),
            source_capability=str(raw.get("source_capability") or ""),
            reconstruction_version=str(raw.get("reconstruction_version") or ""),
            replay_fingerprint=str(raw.get("replay_fingerprint") or ""),
            replay_certified=raw.get("replay_certified"),
            snapshots=tuple(ExecutionBookSnapshot.from_dict(row) for row in raw.get("snapshots") or ()),
            validity_closures=tuple(
                ExecutionBookValidityClosure.from_dict(row)
                for row in raw.get("validity_closures") or ()
            ),
            limitations=tuple(raw.get("limitations") or ()),
            tape_hash=str(raw.get("tape_hash") or ""),
        )

    @classmethod
    def from_book_states(
        cls,
        states: Iterable[BookStateView],
        *,
        instrument_id: str,
        replay_fingerprint: str,
        source_capability: str = "l2",
        replay_certified: bool = True,
        limitations: Sequence[str] = (),
        validity_closures: Sequence[ExecutionBookValidityClosure] = (),
    ) -> "ExecutionBookTape":
        snapshots = tuple(
            sorted(
                (ExecutionBookSnapshot.from_book_state(state, instrument_id=instrument_id) for state in states),
                key=cls._selection_key,
            )
        )
        return cls(
            schema_version=EXECUTION_BOOK_TAPE_SCHEMA_VERSION,
            tape_id="",
            instrument_id=instrument_id,
            source_capability=source_capability,
            reconstruction_version=BOOK_RECONSTRUCTION_VERSION,
            replay_fingerprint=replay_fingerprint,
            replay_certified=replay_certified,
            snapshots=snapshots,
            validity_closures=tuple(validity_closures),
            limitations=tuple(limitations),
        )

    def select_at(self, arrival_at: Any) -> ExecutionBookSnapshot:
        """Select only a state known by deterministic order arrival."""

        arrival = _utc(arrival_at, field_name="order_arrival_at")
        selected: Optional[ExecutionBookSnapshot] = None
        for snapshot in self.snapshots:
            if snapshot.known_at_time > arrival:
                break
            selected = snapshot
        if selected is None:
            raise LookupError("execution_book_snapshot_unavailable_at_arrival")
        closed = [
            row
            for row in self.validity_closures
            if row.validity_interval_id == selected.validity_interval_id
            and row.known_at_time <= arrival
            and row.known_at_time >= selected.known_at_time
        ]
        if closed:
            latest = closed[-1]
            raise LookupError(
                "execution_book_snapshot_invalid_at_arrival "
                f"status={latest.status} reason={latest.reason}"
            )
        return selected


@dataclass(frozen=True)
class ExecutionBookTapeBundle:
    schema_version: str
    tapes: tuple[ExecutionBookTape, ...]
    bundle_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION:
            raise ValueError(f"unsupported execution book tape bundle schema: {self.schema_version}")
        tapes = tuple(sorted(self.tapes, key=lambda row: row.instrument_id))
        if not tapes or len({row.instrument_id for row in tapes}) != len(tapes):
            raise ValueError("execution_book_tape_bundle requires unique instruments")
        object.__setattr__(self, "tapes", tapes)
        expected = _stable_hash(
            {
                "schema_version": self.schema_version,
                "tape_hashes": [row.tape_hash for row in tapes],
            }
        )
        if self.bundle_hash and self.bundle_hash != expected:
            raise ValueError("execution_book_tape_bundle_hash_mismatch")
        object.__setattr__(self, "bundle_hash", expected)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "tapes": [row.to_dict() for row in self.tapes],
            "bundle_hash": self.bundle_hash,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionBookTapeBundle":
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            tapes=tuple(ExecutionBookTape.from_dict(row) for row in raw.get("tapes") or ()),
            bundle_hash=str(raw.get("bundle_hash") or ""),
        )

    def tape_for(self, instrument_id: Any) -> ExecutionBookTape:
        normalized = str(instrument_id or "").strip()
        matches = [row for row in self.tapes if row.instrument_id == normalized]
        if len(matches) != 1:
            raise ValueError(
                "execution_book_tape_not_unique "
                f"instrument_id={normalized or '<missing>'} matches={len(matches)}"
            )
        return matches[0]


@dataclass(frozen=True)
class BookOrderExecutionBatch:
    """Aggregate compatibility fill plus exact lifecycle-level fills."""

    fill: Optional[FillResult]
    rejection: Optional[FillRejection]
    level_fills: tuple[FillResult, ...]
    status: str
    residual_disposition: str
    remaining_qty: float
    evidence: Mapping[str, Any] = field(default_factory=dict)


class BookExecutionModel:
    """Deterministically execute aggressive orders against a frozen book tape."""

    def __init__(self, *, execution_context: Any, tape: ExecutionBookTape) -> None:
        self.execution_context = execution_context
        self.tape = tape
        self._fees = FeeResolver(execution_context.fee_schedule)
        instrument_id = str(execution_context.instrument.instrument_id or "").strip()
        if tape.instrument_id != instrument_id:
            raise ValueError("execution_book_tape_context_instrument_mismatch")
        model_capability = str(execution_context.model.input_capability or "").lower()
        if model_capability not in _CAPABILITY_RANK:
            raise ValueError("execution_book_model_requires_l1_l2_or_l3")
        if _CAPABILITY_RANK[tape.source_capability] < _CAPABILITY_RANK[model_capability]:
            raise ValueError("execution_book_tape_capability_below_model_requirement")
        if execution_context.model.execution_quality_ceiling == "X4" and not tape.replay_certified:
            raise ValueError("x4_execution_requires_replay_certified_book_tape")

    @staticmethod
    def _arrival(metadata: Mapping[str, Any]) -> str:
        for key in ("arrival_at", "known_at", "bar_time", "event_time"):
            if metadata.get(key) not in (None, ""):
                return _time_text(metadata[key], field_name=f"order.{key}")
        raise ValueError("book execution requires deterministic arrival_at or known_at")

    @staticmethod
    def _is_buy(side: Any) -> bool:
        normalized = str(side or "").strip().lower()
        if normalized in {"buy", "long"}:
            return True
        if normalized in {"sell", "short"}:
            return False
        raise ValueError(f"unsupported execution side: {normalized or '<missing>'}")

    def _eligible_levels(
        self,
        snapshot: ExecutionBookSnapshot,
        *,
        side: Any,
        order_type: str,
        limit_price: Decimal,
    ) -> tuple[tuple[str, ExecutionBookLevel], ...]:
        is_buy = self._is_buy(side)
        opposing = tuple(("ask", row) for row in snapshot.asks) if is_buy else tuple(
            ("bid", row) for row in reversed(snapshot.bids)
        )
        if str(self.execution_context.model.input_capability).lower() == "l1":
            opposing = opposing[:1]
        if order_type in {"market", "stop_market"}:
            return opposing
        if order_type != "limit_aggressive":
            return ()
        if is_buy:
            return tuple((name, row) for name, row in opposing if row.price <= limit_price)
        return tuple((name, row) for name, row in opposing if row.price >= limit_price)

    def _base_evidence(
        self,
        *,
        snapshot: ExecutionBookSnapshot,
        arrival_at: str,
        order_type: str,
        side: str,
        requested_qty: Decimal,
        reference_price: Decimal,
        time_in_force: str,
    ) -> dict[str, Any]:
        spread = snapshot.best_ask.price - snapshot.best_bid.price
        return {
            "schema_version": BOOK_EXECUTION_EVIDENCE_SCHEMA_VERSION,
            "execution_model_version": self.execution_context.model.version,
            "execution_model_artifact_hash": self.execution_context.model.artifact_hash,
            "execution_quality_ceiling": self.execution_context.model.execution_quality_ceiling,
            "execution_book_tape_id": self.tape.tape_id,
            "execution_book_tape_hash": self.tape.tape_hash,
            "execution_book_replay_fingerprint": self.tape.replay_fingerprint,
            "execution_book_replay_certified": self.tape.replay_certified,
            "execution_book_source_capability": self.tape.source_capability,
            "execution_book_snapshot_hash": snapshot.snapshot_hash,
            "execution_book_state_hash": snapshot.reconstruction_state_hash,
            "execution_book_validity_interval_id": snapshot.validity_interval_id,
            "execution_book_source_reference": snapshot.source_reference.to_dict(),
            "execution_book_product_definition_version_id": snapshot.product_definition_version_id,
            "execution_book_quantity_unit": snapshot.quantity_unit,
            "execution_book_effective_at": snapshot.effective_at,
            "execution_book_known_at": snapshot.known_at,
            "order_arrival_at": arrival_at,
            "arrival_latency_ms": 0,
            "order_type": order_type,
            "side": side,
            "time_in_force": time_in_force,
            "requested_qty": float(requested_qty),
            "reference_price": float(reference_price),
            "best_bid": float(snapshot.best_bid.price),
            "best_ask": float(snapshot.best_ask.price),
            "spread": float(spread),
            "limitations": list(self.tape.limitations)
            + ["deterministic_zero_latency", "passive_queue_not_modeled"],
        }

    def execute_order_batch(self, order: Any) -> BookOrderExecutionBatch:
        metadata = dict(getattr(order, "metadata", None) or {})
        arrival_at = self._arrival(metadata)
        try:
            snapshot = self.tape.select_at(arrival_at)
        except LookupError as exc:
            failure = str(exc)
            reason = (
                "BOOK_STATE_INVALID_AT_ARRIVAL"
                if "invalid_at_arrival" in failure
                else "BOOK_SNAPSHOT_UNAVAILABLE_AT_ARRIVAL"
            )
            rejection = FillRejection(
                reason=reason,
                metadata={
                    "execution_book_tape_id": self.tape.tape_id,
                    "execution_book_tape_hash": self.tape.tape_hash,
                    "order_arrival_at": arrival_at,
                    "book_selection_failure": failure,
                },
            )
            return BookOrderExecutionBatch(None, rejection, (), "rejected", "rejected", float(order.requested_qty), rejection.metadata)

        order_type = str(order.order_type or "").strip().lower()
        time_in_force = str(getattr(order, "time_in_force", "gtc") or "gtc").strip().lower()
        side = str(order.side or "").strip().lower()
        requested_qty = _decimal(order.requested_qty, field_name="order.requested_qty")
        reference_price = _decimal(order.price, field_name="order.price")
        evidence = self._base_evidence(
            snapshot=snapshot,
            arrival_at=arrival_at,
            order_type=order_type,
            side=side,
            requested_qty=requested_qty,
            reference_price=reference_price,
            time_in_force=time_in_force,
        )
        if order_type in {"limit_maker", "limit_resting"}:
            evidence["residual_disposition"] = "open"
            evidence["limitations"] = [
                *evidence["limitations"],
                "resting_order_execution_not_admitted",
            ]
            return BookOrderExecutionBatch(None, None, (), "open", "open", float(requested_qty), evidence)
        if order_type not in {"market", "stop_market", "limit_aggressive"}:
            rejection = FillRejection("UNSUPPORTED_ORDER_TYPE", {**evidence, "unsupported_order_type": order_type})
            return BookOrderExecutionBatch(None, rejection, (), "rejected", "rejected", float(requested_qty), rejection.metadata)
        if time_in_force not in {"gtc", "ioc", "fok"}:
            rejection = FillRejection("UNSUPPORTED_TIME_IN_FORCE", {**evidence, "unsupported_time_in_force": time_in_force})
            return BookOrderExecutionBatch(None, rejection, (), "rejected", "rejected", float(requested_qty), rejection.metadata)

        eligible = self._eligible_levels(
            snapshot,
            side=side,
            order_type=order_type,
            limit_price=reference_price,
        )
        visible_qty = sum((row.quantity for _name, row in eligible), Decimal(0))
        evidence["eligible_visible_depth"] = float(visible_qty)
        evidence["eligible_level_count"] = len(eligible)
        if time_in_force == "fok" and visible_qty + _EPSILON < requested_qty:
            evidence.update(
                {
                    "consumed_levels": [],
                    "consumed_qty": 0.0,
                    "remaining_qty": float(requested_qty),
                    "residual_disposition": "canceled",
                    "block_reason": "FOK_VISIBLE_DEPTH_INSUFFICIENT",
                }
            )
            return BookOrderExecutionBatch(None, None, (), "canceled", "canceled", float(requested_qty), evidence)

        remaining = requested_qty
        level_fills: list[FillResult] = []
        role = self.execution_context.venue.liquidity_role(order_type)
        for index, (book_side, level) in enumerate(eligible, start=1):
            if remaining <= _EPSILON:
                break
            quantity = min(remaining, level.quantity)
            if quantity <= 0:
                continue
            fee = self._fees.resolve(
                role=role,
                price=float(level.price),
                quantity=float(quantity),
                contract_size=float(self.execution_context.instrument.contract_size),
            )
            fill_material = {
                "order_id": str(metadata.get("order_request_id") or metadata.get("order_id") or "unbound"),
                "snapshot_hash": snapshot.snapshot_hash,
                "level_index": index,
                "book_side": book_side,
                "price": _decimal_text(level.price),
                "quantity": _decimal_text(quantity),
            }
            fill_id = f"book_fill_{_stable_hash(fill_material)}"
            level_metadata = {
                **evidence,
                "fill_id": fill_id,
                "book_level_index": index,
                "book_side": book_side,
                "visible_level_qty": float(level.quantity),
                "consumed_level_qty": float(quantity),
                "price_improvement": (
                    float(reference_price - level.price)
                    if self._is_buy(side)
                    else float(level.price - reference_price)
                ),
            }
            level_fills.append(
                FillResult(
                    filled_qty=float(quantity),
                    fill_price=float(level.price),
                    notional=fee.notional,
                    fee=fee.fee_paid,
                    fee_rate=fee.fee_rate,
                    side=side,
                    metadata=level_metadata,
                    fee_role=fee.role,
                    fee_source=fee.source,
                    fee_version=fee.version,
                )
            )
            remaining -= quantity

        filled_qty = requested_qty - remaining
        if filled_qty > visible_qty + _EPSILON:
            raise RuntimeError("book_execution_consumed_quantity_exceeds_visible_depth")
        if remaining <= _EPSILON:
            remaining = Decimal(0)
            status = "filled"
            residual_disposition = "none"
        elif time_in_force == "ioc" or order_type in {"market", "stop_market"}:
            status = "partially_filled" if level_fills else "canceled"
            residual_disposition = "canceled"
        else:
            status = "partially_filled" if level_fills else "open"
            residual_disposition = "open"

        total_notional = sum(row.notional for row in level_fills)
        total_fee = sum(row.fee for row in level_fills)
        average = (
            sum(row.fill_price * row.filled_qty for row in level_fills) / float(filled_qty)
            if filled_qty > 0
            else None
        )
        consumed = [
            {
                "fill_id": str(row.metadata.get("fill_id")),
                "level_index": int(row.metadata.get("book_level_index") or 0),
                "book_side": str(row.metadata.get("book_side") or ""),
                "price": row.fill_price,
                "quantity": row.filled_qty,
                "notional": row.notional,
                "fee": row.fee,
            }
            for row in level_fills
        ]
        evidence.update(
            {
                "consumed_levels": consumed,
                "consumed_qty": float(filled_qty),
                "remaining_qty": float(remaining),
                "residual_disposition": residual_disposition,
                "vwap": average,
                "slippage_price": (average - float(reference_price)) if average is not None else None,
                "slippage_bps": (
                    ((average - float(reference_price)) / float(reference_price) * 10_000.0)
                    if average is not None and self._is_buy(side)
                    else ((float(reference_price) - average) / float(reference_price) * 10_000.0)
                    if average is not None
                    else None
                ),
            }
        )
        aggregate = None
        if level_fills:
            aggregate = FillResult(
                filled_qty=float(filled_qty),
                fill_price=float(average),
                notional=float(total_notional),
                fee=float(total_fee),
                fee_rate=float(level_fills[0].fee_rate),
                side=side,
                metadata={
                    **evidence,
                    "price_level_fills": [
                        {
                            "filled_qty": row.filled_qty,
                            "fill_price": row.fill_price,
                            "notional": row.notional,
                            "fee": row.fee,
                            "fee_rate": row.fee_rate,
                            "fee_role": row.fee_role,
                            "fee_source": row.fee_source,
                            "fee_version": row.fee_version,
                            "metadata": dict(row.metadata),
                        }
                        for row in level_fills
                    ],
                },
                fee_role=level_fills[0].fee_role,
                fee_source=level_fills[0].fee_source,
                fee_version=level_fills[0].fee_version,
            )
        return BookOrderExecutionBatch(
            aggregate,
            None,
            tuple(level_fills),
            status,
            residual_disposition,
            float(remaining),
            evidence,
        )

    def execute_order(self, order: Any) -> tuple[Optional[FillResult], Optional[FillRejection]]:
        batch = self.execute_order_batch(order)
        return batch.fill, batch.rejection

    def submit(self, intent: ExecutionIntent) -> ExecutionOutcome:
        arrival = self._arrival(intent.metadata)
        return ExecutionOutcome(
            order_id=intent.order_id,
            status="submitted",
            filled_qty=0.0,
            avg_fill_price=None,
            fee_paid=0.0,
            fee_role="unknown",
            fee_rate=0.0,
            fee_source=self.execution_context.fee_schedule.source,
            fee_version=self.execution_context.fee_schedule.version,
            created_at=arrival,
            updated_at=arrival,
            filled_at=None,
            remaining_qty=float(intent.qty),
            limit_price=intent.limit_params.limit_price if intent.limit_params else None,
            validity_window=intent.limit_params.validity_window if intent.limit_params else None,
            metadata={
                **dict(intent.metadata),
                **self.execution_context.evidence_metadata(),
                "order_arrival_at": arrival,
            },
        )

    def evaluate(
        self,
        intent: ExecutionIntent,
        *,
        candle_high: float,
        candle_low: float,
        candle_close: float,
        candle_open: float,
    ) -> tuple[ExecutionOutcome, Optional[FillRejection]]:
        del candle_high, candle_low, candle_close, candle_open
        from .execution_order import FillOrder

        role = self.execution_context.venue.liquidity_role(intent.order_type)
        price = (
            float(intent.limit_params.limit_price)
            if intent.limit_params is not None and intent.limit_params.limit_price is not None
            else float(intent.requested_price)
        )
        order = FillOrder(
            side=intent.side,
            requested_qty=float(intent.qty),
            price=price,
            order_type=intent.order_type,
            liquidity_role=role,
            price_source="limit_price" if intent.limit_params is not None else "requested_price",
            fee_rate=(
                self.execution_context.fee_schedule.maker_rate
                if role == "maker"
                else self.execution_context.fee_schedule.taker_rate
            ),
            fee_source=self.execution_context.fee_schedule.source,
            fee_version=self.execution_context.fee_schedule.version,
            time_in_force=intent.time_in_force,
            post_only=intent.post_only,
            execution_context=self.execution_context,
            metadata={**dict(intent.metadata), "order_id": intent.order_id},
        )
        batch = self.execute_order_batch(order)
        base = self.submit(intent)
        if batch.rejection is not None:
            return (
                ExecutionOutcome(
                    **{
                        **asdict(base),
                        "status": "rejected",
                        "updated_at": self._arrival(intent.metadata),
                        "metadata": {**dict(base.metadata), **dict(batch.rejection.metadata)},
                    }
                ),
                batch.rejection,
            )
        fill = batch.fill
        arrival = self._arrival(intent.metadata)
        return (
            ExecutionOutcome(
                order_id=intent.order_id,
                status=batch.status,
                filled_qty=float(fill.filled_qty if fill is not None else 0.0),
                avg_fill_price=float(fill.fill_price) if fill is not None else None,
                fee_paid=float(fill.fee if fill is not None else 0.0),
                fee_role=str(fill.fee_role if fill is not None else role),
                fee_rate=float(fill.fee_rate if fill is not None else 0.0),
                fee_source=str(fill.fee_source if fill is not None else self.execution_context.fee_schedule.source),
                fee_version=fill.fee_version if fill is not None else self.execution_context.fee_schedule.version,
                created_at=arrival,
                updated_at=arrival,
                filled_at=arrival if fill is not None else None,
                remaining_qty=batch.remaining_qty,
                fallback_applied=False,
                fallback_reason=None,
                limit_price=intent.limit_params.limit_price if intent.limit_params else None,
                validity_window=intent.limit_params.validity_window if intent.limit_params else None,
                metadata={
                    **dict(intent.metadata),
                    **dict(fill.metadata if fill is not None else batch.evidence),
                },
            ),
            None,
        )


__all__ = [
    "AGGREGATED_L2_MODEL_VERSION",
    "BOOK_EXECUTION_EVIDENCE_SCHEMA_VERSION",
    "BookExecutionModel",
    "BookOrderExecutionBatch",
    "EXECUTION_BOOK_SNAPSHOT_SCHEMA_VERSION",
    "EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION",
    "EXECUTION_BOOK_TAPE_SCHEMA_VERSION",
    "ExecutionBookLevel",
    "ExecutionBookSnapshot",
    "ExecutionBookSourceReference",
    "ExecutionBookTape",
    "ExecutionBookTapeBundle",
    "ExecutionBookValidityClosure",
    "SPREAD_AWARE_MODEL_VERSION",
]
