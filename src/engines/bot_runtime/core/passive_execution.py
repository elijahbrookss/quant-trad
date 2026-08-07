"""Deterministic passive-order queue bounds and latency scenarios.

Aggregated L2 cannot reveal exact queue position.  This module therefore emits
named, versioned bounds from replay-certified book states and canonical trade
prints.  It never fits or samples a fill probability and contains no venue-name
branches.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping, Optional

from .book_execution import (
    BOOK_EXECUTION_EVIDENCE_SCHEMA_VERSION,
    BookExecutionModel,
    BookOrderExecutionBatch,
    ExecutionBookLevel,
    ExecutionBookSnapshot,
    ExecutionBookTape,
)
from .execution import FillRejection, FillResult
from .fees import FeeResolver


EXECUTION_LATENCY_SCENARIO_SCHEMA_VERSION = "execution_latency_scenario.v1"
PASSIVE_QUEUE_POLICY_SCHEMA_VERSION = "passive_queue_policy.v1"
PASSIVE_QUEUE_EVIDENCE_SCHEMA_VERSION = "passive_queue_evidence.v1"
PASSIVE_QUEUE_MODEL_VERSION = "passive_queue_bounds.v1"
DEFAULT_LATENCY_STRESS_GRID_MS = (10, 50, 150, 500)
_EPSILON = Decimal("1e-12")


def _stable_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _decimal(value: Any, *, field: str, positive: bool = True) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError(f"{field} must be positive and finite")
    return Decimal(0) if parsed == 0 else parsed


def _finite_non_negative(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be finite and non-negative")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be finite and non-negative") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{field} must be finite and non-negative")
    return parsed


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"{field} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _time_text(value: Any, *, field: str) -> str:
    return _utc(value, field=field).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _required_text(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


class PassiveQueueScenario(str, Enum):
    TAIL_NO_CANCEL_CREDIT = "TAIL_NO_CANCEL_CREDIT"
    TAIL_OBSERVED_TRADE_PROGRESS = "TAIL_OBSERVED_TRADE_PROGRESS"
    BOUNDED_CANCEL_CREDIT = "BOUNDED_CANCEL_CREDIT"


class PassiveFillSupport(str, Enum):
    DEFINITELY_SUPPORTED = "definitely_supported"
    POSSIBLY_SUPPORTED = "possibly_supported"
    NOT_SUPPORTED = "not_supported"
    ASSUMPTION_DEPENDENT = "assumption_dependent"


@dataclass(frozen=True)
class ExecutionLatencyScenario:
    """Pinned deterministic latency components; never an empirical distribution."""

    schema_version: str
    scenario_id: str
    decision_latency_ms: float
    network_latency_ms: float
    acknowledgement_latency_ms: float
    cancellation_latency_ms: float
    replacement_latency_ms: float
    source: str = "declared_stress_scenario"
    scenario_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != EXECUTION_LATENCY_SCENARIO_SCHEMA_VERSION:
            raise ValueError(f"unsupported latency scenario schema: {self.schema_version}")
        object.__setattr__(self, "scenario_id", _required_text(self.scenario_id, field="latency.scenario_id"))
        object.__setattr__(self, "source", _required_text(self.source, field="latency.source"))
        for name in (
            "decision_latency_ms",
            "network_latency_ms",
            "acknowledgement_latency_ms",
            "cancellation_latency_ms",
            "replacement_latency_ms",
        ):
            object.__setattr__(
                self,
                name,
                _finite_non_negative(getattr(self, name), field=f"latency.{name}"),
            )
        expected = _stable_hash(self._material())
        if self.scenario_hash and self.scenario_hash != expected:
            raise ValueError("execution_latency_scenario_hash_mismatch")
        object.__setattr__(self, "scenario_hash", expected)

    @property
    def arrival_latency_ms(self) -> float:
        return (
            self.decision_latency_ms
            + self.network_latency_ms
            + self.acknowledgement_latency_ms
        )

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "scenario_hash"}

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ExecutionLatencyScenario":
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            scenario_id=str(raw.get("scenario_id") or ""),
            decision_latency_ms=raw.get("decision_latency_ms", 0),
            network_latency_ms=raw.get("network_latency_ms", 0),
            acknowledgement_latency_ms=raw.get("acknowledgement_latency_ms", 0),
            cancellation_latency_ms=raw.get("cancellation_latency_ms", 0),
            replacement_latency_ms=raw.get("replacement_latency_ms", 0),
            source=str(raw.get("source") or "declared_stress_scenario"),
            scenario_hash=str(raw.get("scenario_hash") or ""),
        )

    @classmethod
    def total_arrival_stress(cls, latency_ms: int) -> "ExecutionLatencyScenario":
        value = int(latency_ms)
        if value not in DEFAULT_LATENCY_STRESS_GRID_MS:
            raise ValueError(
                "latency stress must be one of "
                + ",".join(str(item) for item in DEFAULT_LATENCY_STRESS_GRID_MS)
            )
        return cls(
            schema_version=EXECUTION_LATENCY_SCENARIO_SCHEMA_VERSION,
            scenario_id=f"arrival_{value}ms",
            decision_latency_ms=0,
            network_latency_ms=value,
            acknowledgement_latency_ms=0,
            cancellation_latency_ms=value,
            replacement_latency_ms=value,
        )

    def arrival_trace(self, metadata: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        decision_raw = next(
            (
                metadata[key]
                for key in ("decision_known_at", "known_at", "bar_time", "event_time")
                if metadata.get(key) not in (None, "")
            ),
            None,
        )
        if decision_raw is None:
            raise ValueError("latency execution requires decision_known_at or known_at")
        decision_at = _utc(decision_raw, field="latency.decision_known_at")
        arrival_at = decision_at + timedelta(milliseconds=self.arrival_latency_ms)
        supplied = metadata.get("arrival_at")
        if supplied not in (None, ""):
            supplied_at = _utc(supplied, field="latency.supplied_arrival_at")
            if supplied_at != arrival_at:
                raise ValueError(
                    "declared_order_arrival_conflicts_with_latency_scenario "
                    f"expected={_time_text(arrival_at, field='latency.arrival_at')} "
                    f"actual={_time_text(supplied_at, field='latency.supplied_arrival_at')}"
                )
        return (
            _time_text(arrival_at, field="latency.arrival_at"),
            {
                "latency_scenario_id": self.scenario_id,
                "latency_scenario_hash": self.scenario_hash,
                "latency_source": self.source,
                "decision_known_at": _time_text(decision_at, field="latency.decision_known_at"),
                "decision_latency_ms": self.decision_latency_ms,
                "network_latency_ms": self.network_latency_ms,
                "acknowledgement_latency_ms": self.acknowledgement_latency_ms,
                "arrival_latency_ms": self.arrival_latency_ms,
                "cancellation_latency_ms": self.cancellation_latency_ms,
                "replacement_latency_ms": self.replacement_latency_ms,
            },
        )


@dataclass(frozen=True)
class PassiveQueuePolicy:
    """One explicit deterministic interpretation of aggregated queue evidence."""

    schema_version: str
    policy_id: str
    scenario: PassiveQueueScenario | str
    latency: ExecutionLatencyScenario
    cancellation_credit_fraction: float = 0.0
    cancellation_credit_cap: float | None = None
    policy_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != PASSIVE_QUEUE_POLICY_SCHEMA_VERSION:
            raise ValueError(f"unsupported passive queue policy schema: {self.schema_version}")
        object.__setattr__(self, "policy_id", _required_text(self.policy_id, field="queue_policy.policy_id"))
        scenario = (
            self.scenario
            if isinstance(self.scenario, PassiveQueueScenario)
            else PassiveQueueScenario(str(self.scenario))
        )
        object.__setattr__(self, "scenario", scenario)
        fraction = _finite_non_negative(
            self.cancellation_credit_fraction,
            field="queue_policy.cancellation_credit_fraction",
        )
        if fraction > 1:
            raise ValueError("queue_policy.cancellation_credit_fraction must be <= 1")
        if scenario is not PassiveQueueScenario.BOUNDED_CANCEL_CREDIT and fraction != 0:
            raise ValueError("cancellation credit is admitted only for BOUNDED_CANCEL_CREDIT")
        object.__setattr__(self, "cancellation_credit_fraction", fraction)
        if self.cancellation_credit_cap is not None:
            object.__setattr__(
                self,
                "cancellation_credit_cap",
                _finite_non_negative(
                    self.cancellation_credit_cap,
                    field="queue_policy.cancellation_credit_cap",
                ),
            )
        expected = _stable_hash(self._material())
        if self.policy_hash and self.policy_hash != expected:
            raise ValueError("passive_queue_policy_hash_mismatch")
        object.__setattr__(self, "policy_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "scenario": self.scenario.value,
            "latency": self.latency.to_dict(),
            "cancellation_credit_fraction": self.cancellation_credit_fraction,
            "cancellation_credit_cap": self.cancellation_credit_cap,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "policy_hash": self.policy_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PassiveQueuePolicy":
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            policy_id=str(raw.get("policy_id") or ""),
            scenario=str(raw.get("scenario") or ""),
            latency=ExecutionLatencyScenario.from_dict(raw.get("latency") or {}),
            cancellation_credit_fraction=raw.get("cancellation_credit_fraction", 0),
            cancellation_credit_cap=raw.get("cancellation_credit_cap"),
            policy_hash=str(raw.get("policy_hash") or ""),
        )


@dataclass(frozen=True)
class _QueueProgress:
    initial_quantity_ahead: Decimal
    observed_trade_quantity: Decimal
    unexplained_displayed_decrease: Decimal
    allowed_cancellation_credit: Decimal
    definitely_supported_total: Decimal
    scenario_supported_total: Decimal
    trades: tuple[Any, ...]
    snapshots: tuple[ExecutionBookSnapshot, ...]


class PassiveBookExecutionModel(BookExecutionModel):
    """X5 deterministic passive bounds layered over causal L2 book replay."""

    def __init__(
        self,
        *,
        execution_context: Any,
        tape: ExecutionBookTape,
        queue_policy: PassiveQueuePolicy,
    ) -> None:
        super().__init__(execution_context=execution_context, tape=tape)
        self.queue_policy = queue_policy
        self._fees = FeeResolver(execution_context.fee_schedule)
        if execution_context.model.execution_quality_ceiling != "X5":
            raise ValueError("passive queue model requires an X5 execution artifact")
        if not execution_context.model.supports_resting_orders:
            raise ValueError("X5 execution artifact must support resting orders")
        if not execution_context.model.supports_latency:
            raise ValueError("X5 execution artifact must support latency")
        if not tape.replay_certified or tape.source_capability not in {"l2", "l3"}:
            raise ValueError("X5 passive execution requires replay-certified L2 or L3")
        if not tape.trades:
            raise ValueError("X5 passive execution requires causal trade prints")

    def _resolve_arrival(self, metadata: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        return self.queue_policy.latency.arrival_trace(metadata)

    @staticmethod
    def _level_quantity(
        snapshot: ExecutionBookSnapshot,
        *,
        side: str,
        price: Decimal,
    ) -> Decimal:
        levels: tuple[ExecutionBookLevel, ...] = snapshot.bids if side == "buy" else snapshot.asks
        return next((row.quantity for row in levels if row.price == price), Decimal(0))

    def _queue_progress(
        self,
        *,
        snapshot: ExecutionBookSnapshot,
        arrival_at: str,
        evaluation_at: str,
        side: str,
        price: Decimal,
        original_qty: Decimal,
    ) -> _QueueProgress:
        initial_ahead = self._level_quantity(snapshot, side=side, price=price)
        trades = self.tape.trades_between(
            after=arrival_at,
            through=evaluation_at,
            price=price,
            maker_side=side,
        )
        observed_trade_qty = sum((row.quantity for row in trades), Decimal(0))
        relevant_snapshots = tuple(
            row
            for row in self.tape.snapshots
            if _utc(arrival_at, field="queue.arrival_at")
            < row.known_at_time
            <= _utc(evaluation_at, field="queue.evaluation_at")
        )
        prior = initial_ahead
        gross_decrease = Decimal(0)
        for row in relevant_snapshots:
            current = self._level_quantity(row, side=side, price=price)
            if current < prior:
                gross_decrease += prior - current
            prior = current
        unexplained = max(gross_decrease - observed_trade_qty, Decimal(0))
        credit = Decimal(0)
        if self.queue_policy.scenario is PassiveQueueScenario.BOUNDED_CANCEL_CREDIT:
            credit = unexplained * Decimal(str(self.queue_policy.cancellation_credit_fraction))
            if self.queue_policy.cancellation_credit_cap is not None:
                credit = min(credit, Decimal(str(self.queue_policy.cancellation_credit_cap)))
            credit = min(credit, initial_ahead)
        definitely_supported = min(
            max(observed_trade_qty - initial_ahead, Decimal(0)),
            original_qty,
        )
        scenario_supported = min(
            max(observed_trade_qty + credit - initial_ahead, Decimal(0)),
            original_qty,
        )
        return _QueueProgress(
            initial_quantity_ahead=initial_ahead,
            observed_trade_quantity=observed_trade_qty,
            unexplained_displayed_decrease=unexplained,
            allowed_cancellation_credit=credit,
            definitely_supported_total=definitely_supported,
            scenario_supported_total=scenario_supported,
            trades=trades,
            snapshots=relevant_snapshots,
        )

    def _passive_evidence(
        self,
        *,
        snapshot: ExecutionBookSnapshot,
        arrival_at: str,
        evaluation_at: str,
        latency_trace: Mapping[str, Any],
        order: Any,
        original_qty: Decimal,
        cumulative_qty: Decimal,
        progress: _QueueProgress,
        support: PassiveFillSupport,
    ) -> dict[str, Any]:
        side = str(order.side or "").strip().lower()
        price = _decimal(order.price, field="passive_order.price")
        return {
            "schema_version": BOOK_EXECUTION_EVIDENCE_SCHEMA_VERSION,
            "passive_queue_schema_version": PASSIVE_QUEUE_EVIDENCE_SCHEMA_VERSION,
            "execution_model_version": self.execution_context.model.version,
            "execution_model_artifact_hash": self.execution_context.model.artifact_hash,
            "execution_quality_ceiling": "X5",
            "execution_book_tape_id": self.tape.tape_id,
            "execution_book_tape_hash": self.tape.tape_hash,
            "execution_book_replay_fingerprint": self.tape.replay_fingerprint,
            "execution_book_replay_certified": self.tape.replay_certified,
            "execution_book_source_capability": self.tape.source_capability,
            "execution_book_snapshot_hash": snapshot.snapshot_hash,
            "execution_book_state_hash": snapshot.reconstruction_state_hash,
            "execution_book_validity_interval_id": snapshot.validity_interval_id,
            "execution_book_source_reference": snapshot.source_reference.to_dict(),
            "execution_book_known_at": snapshot.known_at,
            "order_arrival_at": arrival_at,
            "queue_evaluation_at": evaluation_at,
            "best_bid": float(snapshot.best_bid.price),
            "best_ask": float(snapshot.best_ask.price),
            "spread": float(snapshot.best_ask.price - snapshot.best_bid.price),
            **dict(latency_trace),
            "queue_model_version": PASSIVE_QUEUE_MODEL_VERSION,
            "queue_policy_id": self.queue_policy.policy_id,
            "queue_policy_hash": self.queue_policy.policy_hash,
            "queue_scenario": self.queue_policy.scenario.value,
            "side": side,
            "resting_price": float(price),
            "requested_qty": float(original_qty),
            "cumulative_filled_qty_before": float(cumulative_qty),
            "initial_displayed_quantity_ahead": float(progress.initial_quantity_ahead),
            "observed_execution_quantity_at_price": float(progress.observed_trade_quantity),
            "unexplained_displayed_decrease": float(progress.unexplained_displayed_decrease),
            "allowed_cancellation_credit": float(progress.allowed_cancellation_credit),
            "quantity_ahead_conservative": float(
                max(
                    progress.initial_quantity_ahead - progress.observed_trade_quantity,
                    Decimal(0),
                )
            ),
            "quantity_ahead_with_credit": float(
                max(
                    progress.initial_quantity_ahead
                    - progress.observed_trade_quantity
                    - progress.allowed_cancellation_credit,
                    Decimal(0),
                )
            ),
            "definitely_supported_total_fill_qty": float(progress.definitely_supported_total),
            "scenario_supported_total_fill_qty": float(progress.scenario_supported_total),
            "passive_fill_support": support.value,
            "observed_trade_hashes": [row.trade_hash for row in progress.trades],
            "observed_trade_references": [row.source_reference.to_dict() for row in progress.trades],
            "queue_snapshot_hashes": [row.snapshot_hash for row in progress.snapshots],
            "limitations": sorted(
                set(self.tape.limitations)
                | {
                    "aggregated_depth_queue_bound",
                    "exact_queue_position_unavailable",
                    "hidden_liquidity_unavailable",
                    "declared_latency_not_calibrated",
                }
            ),
        }

    def execute_order_batch(self, order: Any) -> BookOrderExecutionBatch:
        order_type = str(order.order_type or "").strip().lower()
        metadata = dict(getattr(order, "metadata", None) or {})
        arrival_at, latency_trace = self._resolve_arrival(metadata)
        if order_type not in {"limit_maker", "limit_resting"}:
            delegated = replace(order, metadata={**metadata, "arrival_at": arrival_at})
            return super().execute_order_batch(delegated)

        time_in_force = str(getattr(order, "time_in_force", "gtc") or "gtc").strip().lower()
        if time_in_force != "gtc":
            rejection = FillRejection(
                "PASSIVE_QUEUE_REQUIRES_GTC",
                {**latency_trace, "order_arrival_at": arrival_at, "time_in_force": time_in_force},
            )
            return BookOrderExecutionBatch(
                None,
                rejection,
                (),
                "rejected",
                "rejected",
                float(order.requested_qty),
                rejection.metadata,
            )
        side = str(order.side or "").strip().lower()
        if side not in {"buy", "sell"}:
            raise ValueError("passive order side must be buy or sell")
        price = _decimal(order.price, field="passive_order.price")
        try:
            snapshot = self.tape.select_at(arrival_at)
        except LookupError as exc:
            rejection = FillRejection(
                "BOOK_SNAPSHOT_UNAVAILABLE_AT_ARRIVAL",
                {
                    **latency_trace,
                    "order_arrival_at": arrival_at,
                    "book_selection_failure": str(exc),
                },
            )
            return BookOrderExecutionBatch(
                None,
                rejection,
                (),
                "rejected",
                "rejected",
                float(order.requested_qty),
                rejection.metadata,
            )
        would_cross = price >= snapshot.best_ask.price if side == "buy" else price <= snapshot.best_bid.price
        if would_cross:
            rejection = FillRejection(
                "RESTING_ORDER_WOULD_CROSS",
                {
                    **latency_trace,
                    "order_arrival_at": arrival_at,
                    "execution_book_snapshot_hash": snapshot.snapshot_hash,
                    "resting_price": float(price),
                    "best_bid": float(snapshot.best_bid.price),
                    "best_ask": float(snapshot.best_ask.price),
                    "post_only_behavior": self.execution_context.venue.post_only_behavior,
                },
            )
            return BookOrderExecutionBatch(
                None,
                rejection,
                (),
                "rejected",
                "rejected",
                float(order.requested_qty),
                rejection.metadata,
            )

        evaluation_raw = metadata.get("evaluation_at") or arrival_at
        evaluation = max(
            _utc(evaluation_raw, field="passive_order.evaluation_at"),
            _utc(arrival_at, field="passive_order.arrival_at"),
        )
        terminal_status: str | None = None
        terminal_at: datetime | None = None
        terminal_candidates: list[tuple[datetime, int, str]] = []
        cancel_requested = metadata.get("cancel_requested_at")
        if cancel_requested not in (None, ""):
            cancel_at = _utc(cancel_requested, field="passive_order.cancel_requested_at") + timedelta(
                milliseconds=self.queue_policy.latency.cancellation_latency_ms
            )
            if cancel_at <= evaluation:
                terminal_candidates.append((cancel_at, 0, "canceled"))
        replacement_requested = metadata.get("replacement_requested_at")
        if replacement_requested not in (None, ""):
            replacement_at = _utc(
                replacement_requested,
                field="passive_order.replacement_requested_at",
            ) + timedelta(milliseconds=self.queue_policy.latency.replacement_latency_ms)
            if replacement_at <= evaluation:
                terminal_candidates.append((replacement_at, 1, "replaced"))
        expires = metadata.get("expiration_at")
        if expires not in (None, ""):
            expires_at = _utc(expires, field="passive_order.expiration_at")
            if expires_at <= evaluation:
                terminal_candidates.append((expires_at, 2, "expired"))
        if terminal_candidates:
            terminal_at, _priority, terminal_status = min(terminal_candidates)
        if terminal_at is not None:
            evaluation = terminal_at
        evaluation_at = _time_text(evaluation, field="passive_order.evaluation_at")

        cumulative_qty = _decimal(
            metadata.get("order_cumulative_filled_qty", 0),
            field="passive_order.cumulative_filled_qty",
            positive=False,
        )
        if cumulative_qty < 0:
            raise ValueError("passive order cumulative quantity must be non-negative")
        remaining_qty = _decimal(order.requested_qty, field="passive_order.remaining_qty")
        original_qty = _decimal(
            metadata.get("order_original_requested_qty", cumulative_qty + remaining_qty),
            field="passive_order.original_requested_qty",
        )
        if cumulative_qty > original_qty + _EPSILON:
            raise ValueError("passive order cumulative quantity exceeds original request")
        progress = self._queue_progress(
            snapshot=snapshot,
            arrival_at=arrival_at,
            evaluation_at=evaluation_at,
            side=side,
            price=price,
            original_qty=original_qty,
        )
        definitely_increment = max(
            progress.definitely_supported_total - cumulative_qty,
            Decimal(0),
        )
        scenario_increment = max(
            progress.scenario_supported_total - cumulative_qty,
            Decimal(0),
        )
        fill_qty = min(scenario_increment, remaining_qty)
        if fill_qty > 0:
            support = (
                PassiveFillSupport.DEFINITELY_SUPPORTED
                if fill_qty <= definitely_increment + _EPSILON
                else PassiveFillSupport.ASSUMPTION_DEPENDENT
            )
        elif progress.unexplained_displayed_decrease > 0:
            support = PassiveFillSupport.POSSIBLY_SUPPORTED
        else:
            support = PassiveFillSupport.NOT_SUPPORTED
        evidence = self._passive_evidence(
            snapshot=snapshot,
            arrival_at=arrival_at,
            evaluation_at=evaluation_at,
            latency_trace=latency_trace,
            order=order,
            original_qty=original_qty,
            cumulative_qty=cumulative_qty,
            progress=progress,
            support=support,
        )
        residual_after = max(remaining_qty - fill_qty, Decimal(0))
        if terminal_status is not None and residual_after > _EPSILON:
            residual_disposition = terminal_status
        else:
            residual_disposition = "none" if residual_after <= _EPSILON else "open"
        evidence.update(
            {
                "new_fill_qty": float(fill_qty),
                "remaining_qty": float(residual_after),
                "residual_disposition": residual_disposition,
                "replacement_ready": terminal_status == "replaced",
                "terminal_effective_at": (
                    _time_text(terminal_at, field="passive_order.terminal_effective_at")
                    if terminal_at is not None
                    else None
                ),
            }
        )
        if fill_qty <= _EPSILON:
            status = "open" if terminal_status == "replaced" else terminal_status or "open"
            return BookOrderExecutionBatch(
                None,
                None,
                (),
                status,
                residual_disposition,
                float(residual_after),
                evidence,
            )

        fee = self._fees.resolve(
            role="maker",
            price=float(price),
            quantity=float(fill_qty),
            contract_size=float(self.execution_context.instrument.contract_size),
        )
        fill_id = "passive_fill_" + _stable_hash(
            {
                "order_id": str(metadata.get("order_request_id") or metadata.get("order_id") or "unbound"),
                "queue_policy_hash": self.queue_policy.policy_hash,
                "arrival_at": arrival_at,
                "evaluation_at": evaluation_at,
                "scenario_supported_total": str(progress.scenario_supported_total),
                "cumulative_before": str(cumulative_qty),
                "quantity": str(fill_qty),
            }
        )
        fill_metadata = {
            **evidence,
            "fill_id": fill_id,
            "book_level_index": 1,
            "book_side": "bid" if side == "buy" else "ask",
            "passive_fill_price": float(price),
        }
        level_fill = FillResult(
            filled_qty=float(fill_qty),
            fill_price=float(price),
            notional=fee.notional,
            fee=fee.fee_paid,
            fee_rate=fee.fee_rate,
            side=side,
            metadata=fill_metadata,
            fee_role=fee.role,
            fee_source=fee.source,
            fee_version=fee.version,
        )
        aggregate = replace(
            level_fill,
            metadata={
                **fill_metadata,
                "price_level_fills": [
                    {
                        "filled_qty": level_fill.filled_qty,
                        "fill_price": level_fill.fill_price,
                        "notional": level_fill.notional,
                        "fee": level_fill.fee,
                        "fee_rate": level_fill.fee_rate,
                        "fee_role": level_fill.fee_role,
                        "fee_source": level_fill.fee_source,
                        "fee_version": level_fill.fee_version,
                        "metadata": fill_metadata,
                    }
                ],
            },
        )
        status = "filled" if residual_after <= _EPSILON else "partially_filled"
        return BookOrderExecutionBatch(
            aggregate,
            None,
            (level_fill,),
            status,
            residual_disposition,
            float(residual_after),
            evidence,
        )


def latency_stress_grid() -> tuple[ExecutionLatencyScenario, ...]:
    return tuple(
        ExecutionLatencyScenario.total_arrival_stress(value)
        for value in DEFAULT_LATENCY_STRESS_GRID_MS
    )


__all__ = [
    "DEFAULT_LATENCY_STRESS_GRID_MS",
    "EXECUTION_LATENCY_SCENARIO_SCHEMA_VERSION",
    "ExecutionLatencyScenario",
    "PASSIVE_QUEUE_EVIDENCE_SCHEMA_VERSION",
    "PASSIVE_QUEUE_MODEL_VERSION",
    "PASSIVE_QUEUE_POLICY_SCHEMA_VERSION",
    "PassiveBookExecutionModel",
    "PassiveFillSupport",
    "PassiveQueuePolicy",
    "PassiveQueueScenario",
    "latency_stress_grid",
]
