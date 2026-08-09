"""Venue-neutral, append-only canonical order lifecycle authority.

The lifecycle owns order state and lineage.  It deliberately does not own fill
generation, wallet settlement, positions, fees, or P&L.  A fill recorded here
must still flow through the existing canonical accounting owners exactly once.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Sequence

from core.events import parse_optional_datetime, serialize_datetime, serialize_value


ORDER_LIFECYCLE_SCHEMA_VERSION = "canonical_order_lifecycle.v1"
ORDER_LIFECYCLE_REPLAY_SCHEMA_VERSION = "canonical_order_lifecycle_replay.v1"
_EPSILON = 1e-12


class CanonicalOrderState(str, Enum):
    """Generic order states independent of venue terminology."""

    REQUESTED = "requested"
    VALIDATED = "validated"
    ACCEPTED = "accepted"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELED = "canceled"
    REPLACED = "replaced"


TERMINAL_ORDER_STATES = frozenset(
    {
        CanonicalOrderState.FILLED,
        CanonicalOrderState.REJECTED,
        CanonicalOrderState.EXPIRED,
        CanonicalOrderState.CANCELED,
        CanonicalOrderState.REPLACED,
    }
)


_ALLOWED_TRANSITIONS: dict[Optional[CanonicalOrderState], frozenset[CanonicalOrderState]] = {
    None: frozenset({CanonicalOrderState.REQUESTED}),
    CanonicalOrderState.REQUESTED: frozenset(
        {
            CanonicalOrderState.VALIDATED,
            CanonicalOrderState.REJECTED,
        }
    ),
    CanonicalOrderState.VALIDATED: frozenset(
        {
            CanonicalOrderState.ACCEPTED,
            CanonicalOrderState.REJECTED,
        }
    ),
    CanonicalOrderState.ACCEPTED: frozenset(
        {
            CanonicalOrderState.OPEN,
            CanonicalOrderState.PARTIALLY_FILLED,
            CanonicalOrderState.FILLED,
            CanonicalOrderState.REJECTED,
            CanonicalOrderState.CANCELED,
            CanonicalOrderState.EXPIRED,
            CanonicalOrderState.REPLACED,
        }
    ),
    CanonicalOrderState.OPEN: frozenset(
        {
            CanonicalOrderState.PARTIALLY_FILLED,
            CanonicalOrderState.FILLED,
            CanonicalOrderState.REJECTED,
            CanonicalOrderState.CANCELED,
            CanonicalOrderState.EXPIRED,
            CanonicalOrderState.REPLACED,
        }
    ),
    CanonicalOrderState.PARTIALLY_FILLED: frozenset(
        {
            CanonicalOrderState.PARTIALLY_FILLED,
            CanonicalOrderState.FILLED,
            CanonicalOrderState.REJECTED,
            CanonicalOrderState.CANCELED,
            CanonicalOrderState.EXPIRED,
            CanonicalOrderState.REPLACED,
        }
    ),
}


def _required_text(value: Any, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def _optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _finite(value: Any, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite number") from exc
    if not math.isfinite(numeric):
        raise ValueError(f"{field_name} must be a finite number")
    return numeric


def _positive(value: Any, *, field_name: str) -> float:
    numeric = _finite(value, field_name=field_name)
    if numeric <= 0.0:
        raise ValueError(f"{field_name} must be > 0")
    return numeric


def _non_negative(value: Any, *, field_name: str) -> float:
    numeric = _finite(value, field_name=field_name)
    if numeric < 0.0:
        raise ValueError(f"{field_name} must be >= 0")
    return numeric


def _canonical_time(value: Any, *, field_name: str) -> str:
    parsed = parse_optional_datetime(value)
    if parsed is None:
        raise ValueError(f"{field_name} is required")
    return str(serialize_datetime(parsed))


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        serialize_value(value),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def stable_order_identity(prefix: str, material: Mapping[str, Any]) -> str:
    """Build a deterministic identifier from immutable semantic material."""

    return f"{_required_text(prefix, field_name='prefix')}:{_stable_hash(material)[:32]}"


def execution_policy_hash(
    *,
    order_type: str,
    time_in_force: str,
    post_only: bool,
    liquidity_role: str,
    price_source: str,
) -> str:
    """Hash the order policy independently from venue and fill-model facts."""

    return _stable_hash(
        {
            "order_type": _required_text(order_type, field_name="order_type").lower(),
            "time_in_force": _required_text(time_in_force, field_name="time_in_force").lower(),
            "post_only": bool(post_only),
            "liquidity_role": _required_text(liquidity_role, field_name="liquidity_role").lower(),
            "price_source": _required_text(price_source, field_name="price_source"),
        }
    )


def venue_lifecycle_event_name(execution_context: Any, state: CanonicalOrderState) -> str:
    """Resolve venue terminology without putting venue checks in generic code."""

    if not isinstance(state, CanonicalOrderState):
        state = CanonicalOrderState(str(state))
    venue = getattr(execution_context, "venue", None)
    mapping = dict(getattr(venue, "lifecycle_event_mapping", ()) or ())
    aliases = {
        CanonicalOrderState.REQUESTED: "submitted",
        CanonicalOrderState.VALIDATED: "submitted",
        CanonicalOrderState.ACCEPTED: "submitted",
        CanonicalOrderState.PARTIALLY_FILLED: "open",
    }
    return str(mapping.get(state.value) or mapping.get(aliases.get(state, "")) or state.value)


@dataclass(frozen=True)
class CanonicalOrderRequest:
    """Immutable strategy-to-execution request with complete authority binding."""

    request_id: str
    run_id: str
    bot_id: str
    strategy_id: str
    instrument_id: str
    symbol: str
    side: str
    requested_qty: float
    requested_price: float
    order_type: str
    time_in_force: str
    post_only: bool
    liquidity_role: str
    price_source: str
    execution_context_hash: str
    execution_policy_hash: str
    known_at: str
    schema_version: str = ORDER_LIFECYCLE_SCHEMA_VERSION
    signal_id: Optional[str] = None
    decision_id: Optional[str] = None
    trade_id: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != ORDER_LIFECYCLE_SCHEMA_VERSION:
            raise ValueError(f"unsupported canonical order request schema: {self.schema_version}")
        for field_name in (
            "request_id",
            "run_id",
            "bot_id",
            "strategy_id",
            "instrument_id",
            "symbol",
            "side",
            "order_type",
            "time_in_force",
            "liquidity_role",
            "price_source",
            "execution_context_hash",
            "execution_policy_hash",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=f"order_request.{field_name}"),
            )
        object.__setattr__(self, "side", self.side.lower())
        if self.side not in {"buy", "sell", "long", "short"}:
            raise ValueError("order_request.side must be buy, sell, long, or short")
        object.__setattr__(self, "order_type", self.order_type.lower())
        object.__setattr__(self, "time_in_force", self.time_in_force.lower())
        object.__setattr__(self, "liquidity_role", self.liquidity_role.lower())
        if self.liquidity_role not in {"maker", "taker"}:
            raise ValueError("order_request.liquidity_role must be maker or taker")
        object.__setattr__(self, "requested_qty", _positive(self.requested_qty, field_name="order_request.requested_qty"))
        object.__setattr__(self, "requested_price", _positive(self.requested_price, field_name="order_request.requested_price"))
        if not isinstance(self.post_only, bool):
            raise ValueError("order_request.post_only must be boolean")
        object.__setattr__(self, "known_at", _canonical_time(self.known_at, field_name="order_request.known_at"))
        object.__setattr__(self, "signal_id", _optional_text(self.signal_id))
        object.__setattr__(self, "decision_id", _optional_text(self.decision_id))
        object.__setattr__(self, "trade_id", _optional_text(self.trade_id))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))
        expected_policy_hash = execution_policy_hash(
            order_type=self.order_type,
            time_in_force=self.time_in_force,
            post_only=self.post_only,
            liquidity_role=self.liquidity_role,
            price_source=self.price_source,
        )
        if self.execution_policy_hash != expected_policy_hash:
            raise ValueError("order_request_execution_policy_hash_mismatch")

    def to_dict(self) -> dict[str, Any]:
        return dict(serialize_value(self))

    @property
    def manifest_hash(self) -> str:
        return _stable_hash(self.to_dict())


@dataclass(frozen=True)
class CanonicalOrderAttempt:
    """Immutable executable attempt for one canonical request."""

    attempt_id: str
    request_id: str
    attempt_number: int
    requested_qty: float
    requested_price: float
    order_type: str
    time_in_force: str
    post_only: bool
    liquidity_role: str
    execution_context_hash: str
    execution_policy_hash: str
    known_at: str
    schema_version: str = ORDER_LIFECYCLE_SCHEMA_VERSION
    replaces_attempt_id: Optional[str] = None
    replacement_reason: Optional[str] = None
    venue_order_id: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != ORDER_LIFECYCLE_SCHEMA_VERSION:
            raise ValueError(f"unsupported canonical order attempt schema: {self.schema_version}")
        for field_name in (
            "attempt_id",
            "request_id",
            "order_type",
            "time_in_force",
            "liquidity_role",
            "execution_context_hash",
            "execution_policy_hash",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=f"order_attempt.{field_name}"),
            )
        if isinstance(self.attempt_number, bool) or int(self.attempt_number) <= 0:
            raise ValueError("order_attempt.attempt_number must be a positive integer")
        object.__setattr__(self, "attempt_number", int(self.attempt_number))
        object.__setattr__(self, "requested_qty", _positive(self.requested_qty, field_name="order_attempt.requested_qty"))
        object.__setattr__(self, "requested_price", _positive(self.requested_price, field_name="order_attempt.requested_price"))
        object.__setattr__(self, "order_type", self.order_type.lower())
        object.__setattr__(self, "time_in_force", self.time_in_force.lower())
        object.__setattr__(self, "liquidity_role", self.liquidity_role.lower())
        if not isinstance(self.post_only, bool):
            raise ValueError("order_attempt.post_only must be boolean")
        object.__setattr__(self, "known_at", _canonical_time(self.known_at, field_name="order_attempt.known_at"))
        object.__setattr__(self, "replaces_attempt_id", _optional_text(self.replaces_attempt_id))
        object.__setattr__(self, "replacement_reason", _optional_text(self.replacement_reason))
        object.__setattr__(self, "venue_order_id", _optional_text(self.venue_order_id))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))

    def to_dict(self) -> dict[str, Any]:
        return dict(serialize_value(self))

    @property
    def manifest_hash(self) -> str:
        return _stable_hash(self.to_dict())


@dataclass(frozen=True)
class CanonicalOrderLifecycleEvent:
    """One immutable, order-scoped transition in append order."""

    event_id: str
    order_event_seq: int
    request_id: str
    attempt_id: str
    previous_state: Optional[CanonicalOrderState]
    state: CanonicalOrderState
    known_at: str
    execution_context_hash: str
    execution_policy_hash: str
    attempt_cumulative_filled_qty: float
    attempt_remaining_qty: float
    order_cumulative_filled_qty: float
    order_remaining_qty: float
    schema_version: str = ORDER_LIFECYCLE_SCHEMA_VERSION
    source_sequence: Optional[int] = None
    fill_id: Optional[str] = None
    fill_qty: Optional[float] = None
    fill_price: Optional[float] = None
    fill_fee: Optional[float] = None
    reason: Optional[str] = None
    replacement_attempt_id: Optional[str] = None
    venue_event_name: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != ORDER_LIFECYCLE_SCHEMA_VERSION:
            raise ValueError(f"unsupported canonical order lifecycle event schema: {self.schema_version}")
        for field_name in (
            "event_id",
            "request_id",
            "attempt_id",
            "execution_context_hash",
            "execution_policy_hash",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=f"order_event.{field_name}"),
            )
        if isinstance(self.order_event_seq, bool) or int(self.order_event_seq) <= 0:
            raise ValueError("order_event.order_event_seq must be a positive integer")
        object.__setattr__(self, "order_event_seq", int(self.order_event_seq))
        if self.source_sequence is not None:
            if isinstance(self.source_sequence, bool) or int(self.source_sequence) < 0:
                raise ValueError("order_event.source_sequence must be a non-negative integer")
            object.__setattr__(self, "source_sequence", int(self.source_sequence))
        if self.previous_state is not None and not isinstance(self.previous_state, CanonicalOrderState):
            object.__setattr__(self, "previous_state", CanonicalOrderState(str(self.previous_state)))
        if not isinstance(self.state, CanonicalOrderState):
            object.__setattr__(self, "state", CanonicalOrderState(str(self.state)))
        object.__setattr__(self, "known_at", _canonical_time(self.known_at, field_name="order_event.known_at"))
        for field_name in (
            "attempt_cumulative_filled_qty",
            "attempt_remaining_qty",
            "order_cumulative_filled_qty",
            "order_remaining_qty",
        ):
            object.__setattr__(self, field_name, _non_negative(getattr(self, field_name), field_name=f"order_event.{field_name}"))
        object.__setattr__(self, "fill_id", _optional_text(self.fill_id))
        object.__setattr__(self, "reason", _optional_text(self.reason))
        object.__setattr__(self, "replacement_attempt_id", _optional_text(self.replacement_attempt_id))
        object.__setattr__(self, "venue_event_name", _optional_text(self.venue_event_name))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))
        if self.fill_qty is not None:
            object.__setattr__(self, "fill_qty", _positive(self.fill_qty, field_name="order_event.fill_qty"))
        if self.fill_price is not None:
            object.__setattr__(self, "fill_price", _positive(self.fill_price, field_name="order_event.fill_price"))
        if self.fill_fee is not None:
            object.__setattr__(self, "fill_fee", _non_negative(self.fill_fee, field_name="order_event.fill_fee"))

    def to_dict(self) -> dict[str, Any]:
        return dict(serialize_value(self))

    @property
    def material_hash(self) -> str:
        return _stable_hash(self.to_dict())


@dataclass(frozen=True)
class CanonicalOrderAttemptSnapshot:
    attempt_id: str
    state: CanonicalOrderState
    cumulative_filled_qty: float
    remaining_qty: float
    fill_count: int
    average_fill_price: Optional[float]
    fees_paid: float
    last_event_seq: int

    def to_dict(self) -> dict[str, Any]:
        return dict(serialize_value(self))


@dataclass(frozen=True)
class CanonicalOrderSnapshot:
    request_id: str
    state: CanonicalOrderState
    active_attempt_id: Optional[str]
    cumulative_filled_qty: float
    remaining_qty: float
    fill_count: int
    average_fill_price: Optional[float]
    fees_paid: float
    attempts: tuple[CanonicalOrderAttemptSnapshot, ...]
    last_event_seq: int
    replay_hash: str

    def to_dict(self) -> dict[str, Any]:
        return dict(serialize_value(self))


@dataclass(frozen=True)
class OrderLifecycleCommand:
    """Unsequenced competing fact resolved by deterministic lifecycle priority."""

    state: CanonicalOrderState
    known_at: str
    source_sequence: int = 0
    fill_id: Optional[str] = None
    fill_qty: Optional[float] = None
    fill_price: Optional[float] = None
    fill_fee: Optional[float] = None
    reason: Optional[str] = None
    replacement_attempt: Optional[CanonicalOrderAttempt] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.state, CanonicalOrderState):
            object.__setattr__(self, "state", CanonicalOrderState(str(self.state)))
        object.__setattr__(self, "known_at", _canonical_time(self.known_at, field_name="order_command.known_at"))
        if isinstance(self.source_sequence, bool) or int(self.source_sequence) < 0:
            raise ValueError("order_command.source_sequence must be a non-negative integer")
        object.__setattr__(self, "source_sequence", int(self.source_sequence))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))


@dataclass(frozen=True)
class OrderLifecycleRaceResolution:
    applied_event_ids: tuple[str, ...]
    suppressed: tuple[Mapping[str, Any], ...]


def _race_priority(command: OrderLifecycleCommand) -> tuple[int, str]:
    # At an identical causal point a venue may fill before a cancel/replace ack.
    # Choosing fill first is deterministic and economically conservative.
    priority = {
        CanonicalOrderState.PARTIALLY_FILLED: 10,
        CanonicalOrderState.FILLED: 10,
        CanonicalOrderState.REJECTED: 20,
        CanonicalOrderState.EXPIRED: 30,
        CanonicalOrderState.CANCELED: 40,
        CanonicalOrderState.REPLACED: 50,
    }.get(command.state, 100)
    return priority, _stable_hash(serialize_value(command))


class CanonicalOrderLifecycle:
    """Sole fail-closed transition authority for one canonical order request."""

    def __init__(
        self,
        request: CanonicalOrderRequest,
        attempts: Sequence[CanonicalOrderAttempt],
        events: Sequence[CanonicalOrderLifecycleEvent] = (),
    ) -> None:
        if not isinstance(request, CanonicalOrderRequest):
            raise ValueError("canonical order lifecycle requires CanonicalOrderRequest")
        if not attempts:
            raise ValueError("canonical order lifecycle requires at least one attempt")
        self.request = request
        self._attempts: dict[str, CanonicalOrderAttempt] = {}
        self._events: list[CanonicalOrderLifecycleEvent] = []
        self._event_material_by_id: dict[str, str] = {}
        self._fill_material_by_id: dict[str, str] = {}
        for attempt in sorted(attempts, key=lambda item: item.attempt_number):
            self._register_attempt(attempt)
        for event in events:
            self.append(event)

    @classmethod
    def create(
        cls,
        request: CanonicalOrderRequest,
        attempt: CanonicalOrderAttempt,
        *,
        venue_event_name: Optional[str] = None,
    ) -> "CanonicalOrderLifecycle":
        authority = cls(request, [attempt])
        authority.transition(
            attempt_id=attempt.attempt_id,
            state=CanonicalOrderState.REQUESTED,
            known_at=attempt.known_at,
            venue_event_name=venue_event_name,
        )
        return authority

    @classmethod
    def replay(
        cls,
        *,
        request: CanonicalOrderRequest,
        attempts: Sequence[CanonicalOrderAttempt],
        events: Sequence[CanonicalOrderLifecycleEvent],
    ) -> "CanonicalOrderLifecycle":
        """Rebuild authoritative state from immutable manifests and events."""

        return cls(request=request, attempts=attempts, events=events)

    @property
    def attempts(self) -> tuple[CanonicalOrderAttempt, ...]:
        return tuple(sorted(self._attempts.values(), key=lambda item: item.attempt_number))

    @property
    def events(self) -> tuple[CanonicalOrderLifecycleEvent, ...]:
        return tuple(self._events)

    def events_after(self, order_event_seq: int) -> tuple[CanonicalOrderLifecycleEvent, ...]:
        return tuple(event for event in self._events if event.order_event_seq > int(order_event_seq))

    def _register_attempt(self, attempt: CanonicalOrderAttempt) -> None:
        if not isinstance(attempt, CanonicalOrderAttempt):
            raise ValueError("order lifecycle attempts must be CanonicalOrderAttempt")
        if attempt.request_id != self.request.request_id:
            raise ValueError("order_attempt_request_id_mismatch")
        if attempt.execution_context_hash != self.request.execution_context_hash:
            raise ValueError("order_attempt_execution_context_hash_mismatch")
        if attempt.execution_policy_hash != execution_policy_hash(
            order_type=attempt.order_type,
            time_in_force=attempt.time_in_force,
            post_only=attempt.post_only,
            liquidity_role=attempt.liquidity_role,
            price_source=self.request.price_source,
        ):
            raise ValueError("order_attempt_execution_policy_hash_mismatch")
        if attempt.attempt_id in self._attempts:
            existing = self._attempts[attempt.attempt_id]
            if existing.to_dict() != attempt.to_dict():
                raise ValueError("order_attempt_identity_reused_with_different_material")
            return
        expected_number = len(self._attempts) + 1
        if attempt.attempt_number != expected_number:
            raise ValueError(
                "order_attempt_number_not_contiguous "
                f"expected={expected_number} actual={attempt.attempt_number}"
            )
        if expected_number == 1:
            if attempt.replaces_attempt_id is not None:
                raise ValueError("first_order_attempt_cannot_replace_another_attempt")
            if abs(attempt.requested_qty - self.request.requested_qty) > _EPSILON:
                raise ValueError("first_order_attempt_quantity_must_equal_request")
        else:
            predecessor = self._attempts.get(str(attempt.replaces_attempt_id or ""))
            if predecessor is None:
                raise ValueError("replacement_attempt_predecessor_missing")
        self._attempts[attempt.attempt_id] = attempt

    def _attempt_events(self, attempt_id: str) -> list[CanonicalOrderLifecycleEvent]:
        return [event for event in self._events if event.attempt_id == attempt_id]

    def attempt_snapshot(self, attempt_id: str) -> Optional[CanonicalOrderAttemptSnapshot]:
        attempt = self._attempts.get(str(attempt_id))
        if attempt is None:
            return None
        events = self._attempt_events(attempt.attempt_id)
        if not events:
            return None
        fills = [event for event in events if event.fill_id is not None]
        filled_qty = sum(float(event.fill_qty or 0.0) for event in fills)
        filled_notional = sum(float(event.fill_qty or 0.0) * float(event.fill_price or 0.0) for event in fills)
        return CanonicalOrderAttemptSnapshot(
            attempt_id=attempt.attempt_id,
            state=events[-1].state,
            cumulative_filled_qty=filled_qty,
            remaining_qty=max(attempt.requested_qty - filled_qty, 0.0),
            fill_count=len(fills),
            average_fill_price=(filled_notional / filled_qty if filled_qty > 0.0 else None),
            fees_paid=sum(float(event.fill_fee or 0.0) for event in fills),
            last_event_seq=events[-1].order_event_seq,
        )

    def snapshot(self) -> CanonicalOrderSnapshot:
        snapshots = tuple(
            snapshot
            for attempt in self.attempts
            if (snapshot := self.attempt_snapshot(attempt.attempt_id)) is not None
        )
        if not snapshots:
            raise RuntimeError("order_lifecycle_has_no_requested_attempt")
        fills = [event for event in self._events if event.fill_id is not None]
        total_qty = sum(float(event.fill_qty or 0.0) for event in fills)
        total_notional = sum(float(event.fill_qty or 0.0) * float(event.fill_price or 0.0) for event in fills)
        active = next((item for item in reversed(snapshots) if item.state not in TERMINAL_ORDER_STATES), None)
        latest = snapshots[-1]
        return CanonicalOrderSnapshot(
            request_id=self.request.request_id,
            state=latest.state,
            active_attempt_id=active.attempt_id if active is not None else None,
            cumulative_filled_qty=total_qty,
            remaining_qty=max(self.request.requested_qty - total_qty, 0.0),
            fill_count=len(fills),
            average_fill_price=(total_notional / total_qty if total_qty > 0.0 else None),
            fees_paid=sum(float(event.fill_fee or 0.0) for event in fills),
            attempts=snapshots,
            last_event_seq=self._events[-1].order_event_seq,
            replay_hash=self.replay_hash,
        )

    @property
    def replay_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": ORDER_LIFECYCLE_REPLAY_SCHEMA_VERSION,
                "request": self.request.to_dict(),
                "attempts": [attempt.to_dict() for attempt in self.attempts],
                "events": [event.to_dict() for event in self.events],
            }
        )

    def replay_hash_at(self, order_event_seq: int) -> str:
        """Hash the immutable replay prefix ending at one lifecycle event."""

        seq = int(order_event_seq)
        if seq <= 0 or seq > len(self._events):
            raise ValueError("order_lifecycle_replay_prefix_sequence_out_of_range")
        prefix_events = tuple(event for event in self._events if event.order_event_seq <= seq)
        referenced_attempt_ids = {
            value
            for event in prefix_events
            for value in (event.attempt_id, event.replacement_attempt_id)
            if value is not None
        }
        prefix_attempts = tuple(
            attempt for attempt in self.attempts if attempt.attempt_id in referenced_attempt_ids
        )
        return _stable_hash(
            {
                "schema_version": ORDER_LIFECYCLE_REPLAY_SCHEMA_VERSION,
                "request": self.request.to_dict(),
                "attempts": [attempt.to_dict() for attempt in prefix_attempts],
                "events": [event.to_dict() for event in prefix_events],
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": ORDER_LIFECYCLE_REPLAY_SCHEMA_VERSION,
            "request": self.request.to_dict(),
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "events": [event.to_dict() for event in self.events],
            "snapshot": self.snapshot().to_dict(),
            "replay_hash": self.replay_hash,
        }

    def _order_fill_totals(self) -> tuple[float, float]:
        qty = sum(float(event.fill_qty or 0.0) for event in self._events if event.fill_id is not None)
        return qty, max(self.request.requested_qty - qty, 0.0)

    def append(self, event: CanonicalOrderLifecycleEvent) -> bool:
        """Append one event, returning False only for an identical duplicate."""

        if not isinstance(event, CanonicalOrderLifecycleEvent):
            raise ValueError("order lifecycle accepts CanonicalOrderLifecycleEvent only")
        material_hash = event.material_hash
        existing_material = self._event_material_by_id.get(event.event_id)
        if existing_material is not None:
            if existing_material != material_hash:
                raise ValueError("order_event_identity_reused_with_different_material")
            return False
        if event.fill_id is not None:
            fill_material = _stable_hash(
                {
                    "request_id": event.request_id,
                    "attempt_id": event.attempt_id,
                    "fill_id": event.fill_id,
                    "fill_qty": event.fill_qty,
                    "fill_price": event.fill_price,
                    "fill_fee": event.fill_fee,
                }
            )
            existing_fill = self._fill_material_by_id.get(event.fill_id)
            if existing_fill is not None:
                if existing_fill != fill_material:
                    raise ValueError("order_fill_identity_reused_with_different_material")
                return False
        expected_seq = len(self._events) + 1
        if event.order_event_seq != expected_seq:
            raise ValueError(
                "order_event_sequence_not_contiguous "
                f"expected={expected_seq} actual={event.order_event_seq}"
            )
        if event.request_id != self.request.request_id:
            raise ValueError("order_event_request_id_mismatch")
        attempt = self._attempts.get(event.attempt_id)
        if attempt is None:
            raise ValueError("order_event_attempt_missing")
        if event.execution_context_hash != attempt.execution_context_hash:
            raise ValueError("order_event_execution_context_hash_mismatch")
        if event.execution_policy_hash != attempt.execution_policy_hash:
            raise ValueError("order_event_execution_policy_hash_mismatch")
        snapshot = self.attempt_snapshot(attempt.attempt_id)
        current_state = snapshot.state if snapshot is not None else None
        if event.previous_state != current_state:
            raise ValueError(
                "order_event_previous_state_mismatch "
                f"expected={current_state.value if current_state else None} "
                f"actual={event.previous_state.value if event.previous_state else None}"
            )
        allowed = _ALLOWED_TRANSITIONS.get(current_state, frozenset())
        if event.state not in allowed:
            raise ValueError(
                "illegal_order_transition "
                f"from={current_state.value if current_state else None} to={event.state.value}"
            )
        attempt_filled = snapshot.cumulative_filled_qty if snapshot is not None else 0.0
        order_filled, _order_remaining = self._order_fill_totals()
        is_fill = event.state in {
            CanonicalOrderState.PARTIALLY_FILLED,
            CanonicalOrderState.FILLED,
        }
        if is_fill:
            if event.fill_id is None or event.fill_qty is None or event.fill_price is None or event.fill_fee is None:
                raise ValueError("fill transition requires fill_id, fill_qty, fill_price, and fill_fee")
            attempt_filled += float(event.fill_qty)
            order_filled += float(event.fill_qty)
        elif any(value is not None for value in (event.fill_id, event.fill_qty, event.fill_price, event.fill_fee)):
            raise ValueError("non-fill transition cannot carry fill material")
        if attempt_filled > attempt.requested_qty + _EPSILON:
            raise ValueError("order_attempt_overfill")
        if order_filled > self.request.requested_qty + _EPSILON:
            raise ValueError("canonical_order_overfill")
        attempt_remaining = max(attempt.requested_qty - attempt_filled, 0.0)
        order_remaining = max(self.request.requested_qty - order_filled, 0.0)
        if abs(event.attempt_cumulative_filled_qty - attempt_filled) > _EPSILON:
            raise ValueError("order_event_attempt_cumulative_quantity_mismatch")
        if abs(event.attempt_remaining_qty - attempt_remaining) > _EPSILON:
            raise ValueError("order_event_attempt_remaining_quantity_mismatch")
        if abs(event.order_cumulative_filled_qty - order_filled) > _EPSILON:
            raise ValueError("order_event_cumulative_quantity_mismatch")
        if abs(event.order_remaining_qty - order_remaining) > _EPSILON:
            raise ValueError("order_event_remaining_quantity_mismatch")
        if event.state == CanonicalOrderState.PARTIALLY_FILLED and attempt_remaining <= _EPSILON:
            raise ValueError("partially_filled_transition_requires_positive_residual")
        if event.state == CanonicalOrderState.FILLED and attempt_remaining > _EPSILON:
            raise ValueError("filled_transition_requires_zero_attempt_residual")
        if event.state == CanonicalOrderState.REPLACED:
            replacement = self._attempts.get(str(event.replacement_attempt_id or ""))
            if replacement is None:
                raise ValueError("replaced_transition_requires_registered_replacement_attempt")
            if replacement.replaces_attempt_id != attempt.attempt_id:
                raise ValueError("replacement_attempt_lineage_mismatch")
            if abs(replacement.requested_qty - attempt_remaining) > _EPSILON:
                raise ValueError("replacement_attempt_quantity_must_equal_predecessor_residual")
        elif event.replacement_attempt_id is not None:
            raise ValueError("replacement_attempt_id_is_valid_only_for_replaced_transition")
        self._events.append(event)
        self._event_material_by_id[event.event_id] = material_hash
        if event.fill_id is not None:
            self._fill_material_by_id[event.fill_id] = _stable_hash(
                {
                    "request_id": event.request_id,
                    "attempt_id": event.attempt_id,
                    "fill_id": event.fill_id,
                    "fill_qty": event.fill_qty,
                    "fill_price": event.fill_price,
                    "fill_fee": event.fill_fee,
                }
            )
        return True

    def transition(
        self,
        *,
        attempt_id: str,
        state: CanonicalOrderState,
        known_at: Any,
        source_sequence: Optional[int] = None,
        fill_id: Optional[str] = None,
        fill_qty: Optional[float] = None,
        fill_price: Optional[float] = None,
        fill_fee: Optional[float] = None,
        reason: Optional[str] = None,
        replacement_attempt_id: Optional[str] = None,
        venue_event_name: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        event_id: Optional[str] = None,
    ) -> CanonicalOrderLifecycleEvent:
        if not isinstance(state, CanonicalOrderState):
            state = CanonicalOrderState(str(state))
        attempt = self._attempts.get(str(attempt_id))
        if attempt is None:
            raise ValueError("order_transition_attempt_missing")
        snapshot = self.attempt_snapshot(attempt.attempt_id)
        previous_state = snapshot.state if snapshot is not None else None
        attempt_filled = snapshot.cumulative_filled_qty if snapshot is not None else 0.0
        order_filled, _ = self._order_fill_totals()
        if fill_qty is not None:
            attempt_filled += float(fill_qty)
            order_filled += float(fill_qty)
        canonical_known_at = _canonical_time(known_at, field_name="order_transition.known_at")
        seq = len(self._events) + 1
        material = {
            "schema_version": ORDER_LIFECYCLE_SCHEMA_VERSION,
            "request_id": self.request.request_id,
            "attempt_id": attempt.attempt_id,
            "order_event_seq": seq,
            "previous_state": previous_state.value if previous_state else None,
            "state": state.value,
            "known_at": canonical_known_at,
            "source_sequence": source_sequence,
            "fill_id": fill_id,
            "fill_qty": fill_qty,
            "fill_price": fill_price,
            "fill_fee": fill_fee,
            "reason": reason,
            "replacement_attempt_id": replacement_attempt_id,
            "execution_context_hash": attempt.execution_context_hash,
            "execution_policy_hash": attempt.execution_policy_hash,
        }
        resolved_event_id = str(event_id or stable_order_identity("order_event", material))
        event = CanonicalOrderLifecycleEvent(
            event_id=resolved_event_id,
            order_event_seq=seq,
            request_id=self.request.request_id,
            attempt_id=attempt.attempt_id,
            previous_state=previous_state,
            state=state,
            known_at=canonical_known_at,
            source_sequence=source_sequence,
            fill_id=fill_id,
            fill_qty=fill_qty,
            fill_price=fill_price,
            fill_fee=fill_fee,
            reason=reason,
            replacement_attempt_id=replacement_attempt_id,
            venue_event_name=venue_event_name,
            execution_context_hash=attempt.execution_context_hash,
            execution_policy_hash=attempt.execution_policy_hash,
            attempt_cumulative_filled_qty=attempt_filled,
            attempt_remaining_qty=max(attempt.requested_qty - attempt_filled, 0.0),
            order_cumulative_filled_qty=order_filled,
            order_remaining_qty=max(self.request.requested_qty - order_filled, 0.0),
            metadata=dict(metadata or {}),
        )
        self.append(event)
        return event

    def record_fill(
        self,
        *,
        attempt_id: str,
        fill_id: str,
        fill_qty: float,
        fill_price: float,
        fill_fee: float,
        known_at: Any,
        source_sequence: Optional[int] = None,
        venue_event_name: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        event_id: Optional[str] = None,
    ) -> CanonicalOrderLifecycleEvent:
        attempt = self._attempts.get(str(attempt_id))
        if attempt is None:
            raise ValueError("order_fill_attempt_missing")
        snapshot = self.attempt_snapshot(attempt.attempt_id)
        already_filled = snapshot.cumulative_filled_qty if snapshot is not None else 0.0
        quantity = _positive(fill_qty, field_name="fill_qty")
        remaining = attempt.requested_qty - already_filled - quantity
        state = CanonicalOrderState.FILLED if remaining <= _EPSILON else CanonicalOrderState.PARTIALLY_FILLED
        return self.transition(
            attempt_id=attempt.attempt_id,
            state=state,
            known_at=known_at,
            source_sequence=source_sequence,
            fill_id=fill_id,
            fill_qty=quantity,
            fill_price=fill_price,
            fill_fee=fill_fee,
            venue_event_name=venue_event_name,
            metadata=metadata,
            event_id=event_id,
        )

    def replace(
        self,
        *,
        attempt_id: str,
        replacement_attempt: CanonicalOrderAttempt,
        known_at: Any,
        reason: str,
        source_sequence: Optional[int] = None,
        venue_event_name: Optional[str] = None,
    ) -> tuple[CanonicalOrderLifecycleEvent, CanonicalOrderLifecycleEvent]:
        predecessor = self._attempts.get(str(attempt_id))
        if predecessor is None:
            raise ValueError("replacement_predecessor_attempt_missing")
        predecessor_snapshot = self.attempt_snapshot(predecessor.attempt_id)
        if predecessor_snapshot is None:
            raise ValueError("replacement_predecessor_not_requested")
        if predecessor_snapshot.state in TERMINAL_ORDER_STATES:
            raise ValueError("replacement_predecessor_already_terminal")
        if replacement_attempt.replaces_attempt_id != predecessor.attempt_id:
            raise ValueError("replacement_attempt_lineage_mismatch")
        if abs(replacement_attempt.requested_qty - predecessor_snapshot.remaining_qty) > _EPSILON:
            raise ValueError("replacement_attempt_quantity_must_equal_predecessor_residual")
        self._register_attempt(replacement_attempt)
        replaced_event = self.transition(
            attempt_id=predecessor.attempt_id,
            state=CanonicalOrderState.REPLACED,
            known_at=known_at,
            source_sequence=source_sequence,
            reason=reason,
            replacement_attempt_id=replacement_attempt.attempt_id,
            venue_event_name=venue_event_name,
        )
        requested_event = self.transition(
            attempt_id=replacement_attempt.attempt_id,
            state=CanonicalOrderState.REQUESTED,
            known_at=replacement_attempt.known_at,
            source_sequence=source_sequence,
            venue_event_name=venue_event_name,
            metadata={"replaces_attempt_id": predecessor.attempt_id},
        )
        return replaced_event, requested_event

    def apply_competing(
        self,
        *,
        attempt_id: str,
        commands: Iterable[OrderLifecycleCommand],
    ) -> OrderLifecycleRaceResolution:
        ordered = sorted(
            list(commands),
            key=lambda command: (
                command.known_at,
                command.source_sequence,
                *_race_priority(command),
            ),
        )
        applied: list[str] = []
        suppressed: list[Mapping[str, Any]] = []
        for command in ordered:
            snapshot = self.attempt_snapshot(attempt_id)
            if snapshot is not None and snapshot.state in TERMINAL_ORDER_STATES:
                suppressed.append(
                    {
                        "state": command.state.value,
                        "known_at": command.known_at,
                        "source_sequence": command.source_sequence,
                        "reason": "order_attempt_already_terminal",
                    }
                )
                continue
            if command.state in {CanonicalOrderState.PARTIALLY_FILLED, CanonicalOrderState.FILLED}:
                event = self.record_fill(
                    attempt_id=attempt_id,
                    fill_id=_required_text(command.fill_id, field_name="order_command.fill_id"),
                    fill_qty=command.fill_qty,
                    fill_price=command.fill_price,
                    fill_fee=command.fill_fee,
                    known_at=command.known_at,
                    source_sequence=command.source_sequence,
                    metadata=command.metadata,
                )
            elif command.state == CanonicalOrderState.REPLACED:
                replacement = command.replacement_attempt
                if replacement is None:
                    raise ValueError("replace command requires replacement_attempt")
                replaced, requested = self.replace(
                    attempt_id=attempt_id,
                    replacement_attempt=replacement,
                    known_at=command.known_at,
                    reason=_required_text(command.reason, field_name="order_command.reason"),
                    source_sequence=command.source_sequence,
                )
                applied.extend([replaced.event_id, requested.event_id])
                continue
            else:
                event = self.transition(
                    attempt_id=attempt_id,
                    state=command.state,
                    known_at=command.known_at,
                    source_sequence=command.source_sequence,
                    reason=command.reason,
                    metadata=command.metadata,
                )
            applied.append(event.event_id)
        return OrderLifecycleRaceResolution(tuple(applied), tuple(suppressed))


def build_initial_order_attempt(
    request: CanonicalOrderRequest,
    *,
    attempt_id: Optional[str] = None,
    venue_order_id: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> CanonicalOrderAttempt:
    """Create the deterministic first executable attempt for a request."""

    resolved_attempt_id = attempt_id or stable_order_identity(
        "order_attempt",
        {"request_id": request.request_id, "attempt_number": 1},
    )
    return CanonicalOrderAttempt(
        attempt_id=resolved_attempt_id,
        request_id=request.request_id,
        attempt_number=1,
        requested_qty=request.requested_qty,
        requested_price=request.requested_price,
        order_type=request.order_type,
        time_in_force=request.time_in_force,
        post_only=request.post_only,
        liquidity_role=request.liquidity_role,
        execution_context_hash=request.execution_context_hash,
        execution_policy_hash=request.execution_policy_hash,
        known_at=request.known_at,
        venue_order_id=venue_order_id,
        metadata=dict(metadata or {}),
    )


def build_replacement_order_attempt(
    lifecycle: CanonicalOrderLifecycle,
    *,
    predecessor_attempt_id: str,
    requested_price: float,
    known_at: Any,
    order_type: Optional[str] = None,
    time_in_force: Optional[str] = None,
    post_only: Optional[bool] = None,
    liquidity_role: Optional[str] = None,
    reason: str,
    attempt_id: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> CanonicalOrderAttempt:
    """Build a replacement attempt pinned to the predecessor's exact residual."""

    predecessor = next(
        (item for item in lifecycle.attempts if item.attempt_id == str(predecessor_attempt_id)),
        None,
    )
    if predecessor is None:
        raise ValueError("replacement_predecessor_attempt_missing")
    snapshot = lifecycle.attempt_snapshot(predecessor.attempt_id)
    if snapshot is None or snapshot.remaining_qty <= _EPSILON:
        raise ValueError("replacement_predecessor_has_no_residual")
    resolved_order_type = str(order_type or predecessor.order_type).strip().lower()
    resolved_tif = str(time_in_force or predecessor.time_in_force).strip().lower()
    resolved_post_only = predecessor.post_only if post_only is None else post_only
    resolved_role = str(liquidity_role or predecessor.liquidity_role).strip().lower()
    policy_hash = execution_policy_hash(
        order_type=resolved_order_type,
        time_in_force=resolved_tif,
        post_only=bool(resolved_post_only),
        liquidity_role=resolved_role,
        price_source=lifecycle.request.price_source,
    )
    attempt_number = len(lifecycle.attempts) + 1
    resolved_attempt_id = attempt_id or stable_order_identity(
        "order_attempt",
        {
            "request_id": lifecycle.request.request_id,
            "attempt_number": attempt_number,
            "replaces_attempt_id": predecessor.attempt_id,
            "requested_price": requested_price,
            "order_type": resolved_order_type,
            "time_in_force": resolved_tif,
            "post_only": resolved_post_only,
            "liquidity_role": resolved_role,
        },
    )
    return CanonicalOrderAttempt(
        attempt_id=resolved_attempt_id,
        request_id=lifecycle.request.request_id,
        attempt_number=attempt_number,
        requested_qty=snapshot.remaining_qty,
        requested_price=requested_price,
        order_type=resolved_order_type,
        time_in_force=resolved_tif,
        post_only=bool(resolved_post_only),
        liquidity_role=resolved_role,
        execution_context_hash=lifecycle.request.execution_context_hash,
        execution_policy_hash=policy_hash,
        known_at=_canonical_time(known_at, field_name="replacement_attempt.known_at"),
        replaces_attempt_id=predecessor.attempt_id,
        replacement_reason=_required_text(reason, field_name="replacement_attempt.reason"),
        metadata=dict(metadata or {}),
    )


def lifecycle_from_dict(payload: Mapping[str, Any]) -> CanonicalOrderLifecycle:
    """Deserialize and verify an order trace for restart/replay recovery."""

    if not isinstance(payload, Mapping):
        raise ValueError("order lifecycle payload must be a mapping")
    request_payload = payload.get("request")
    attempts_payload = payload.get("attempts")
    events_payload = payload.get("events")
    if not isinstance(request_payload, Mapping):
        raise ValueError("order lifecycle request manifest is required")
    if not isinstance(attempts_payload, Sequence) or isinstance(attempts_payload, (str, bytes)):
        raise ValueError("order lifecycle attempt manifests are required")
    if not isinstance(events_payload, Sequence) or isinstance(events_payload, (str, bytes)):
        raise ValueError("order lifecycle events are required")

    def request_from_dict(raw: Mapping[str, Any]) -> CanonicalOrderRequest:
        return CanonicalOrderRequest(**dict(raw))

    def attempt_from_dict(raw: Mapping[str, Any]) -> CanonicalOrderAttempt:
        return CanonicalOrderAttempt(**dict(raw))

    def event_from_dict(raw: Mapping[str, Any]) -> CanonicalOrderLifecycleEvent:
        values = dict(raw)
        previous = values.get("previous_state")
        values["previous_state"] = CanonicalOrderState(previous) if previous is not None else None
        values["state"] = CanonicalOrderState(str(values.get("state") or ""))
        return CanonicalOrderLifecycleEvent(**values)

    lifecycle = CanonicalOrderLifecycle.replay(
        request=request_from_dict(request_payload),
        attempts=[attempt_from_dict(raw) for raw in attempts_payload if isinstance(raw, Mapping)],
        events=[event_from_dict(raw) for raw in events_payload if isinstance(raw, Mapping)],
    )
    expected_hash = _optional_text(payload.get("replay_hash"))
    if expected_hash is not None and expected_hash != lifecycle.replay_hash:
        raise ValueError("order_lifecycle_replay_hash_mismatch")
    return lifecycle


def utc_now_text() -> str:
    """Return a canonical UTC timestamp for non-replay operational callers."""

    return str(serialize_datetime(datetime.now(timezone.utc)))


__all__ = [
    "ORDER_LIFECYCLE_REPLAY_SCHEMA_VERSION",
    "ORDER_LIFECYCLE_SCHEMA_VERSION",
    "CanonicalOrderAttempt",
    "CanonicalOrderAttemptSnapshot",
    "CanonicalOrderLifecycle",
    "CanonicalOrderLifecycleEvent",
    "CanonicalOrderRequest",
    "CanonicalOrderSnapshot",
    "CanonicalOrderState",
    "OrderLifecycleCommand",
    "OrderLifecycleRaceResolution",
    "TERMINAL_ORDER_STATES",
    "build_initial_order_attempt",
    "build_replacement_order_attempt",
    "execution_policy_hash",
    "lifecycle_from_dict",
    "stable_order_identity",
    "utc_now_text",
    "venue_lifecycle_event_name",
]
