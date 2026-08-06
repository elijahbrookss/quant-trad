"""Canonical order execution primitives for bot runtime fills."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol, Tuple

from .execution import FillRejection, FillResult
from .execution_policy import fee_rate_for_role, normalize_liquidity_role
from .order_lifecycle import (
    CanonicalOrderLifecycle,
    CanonicalOrderRequest,
    CanonicalOrderState,
    build_initial_order_attempt,
    execution_policy_hash,
    stable_order_identity,
    venue_lifecycle_event_name,
)

if TYPE_CHECKING:
    from .execution_context import ResolvedExecutionContext


class OrderType(str, Enum):
    """Runtime order styles with explicit liquidity semantics."""

    MARKET = "market"
    LIMIT_AGGRESSIVE = "limit_aggressive"
    LIMIT_MAKER = "limit_maker"
    LIMIT_RESTING = "limit_resting"
    STOP_MARKET = "stop_market"


@dataclass(frozen=True)
class FillOrder:
    """Executable fill request after runtime has resolved order semantics."""

    side: str
    requested_qty: float
    price: float
    order_type: str
    liquidity_role: str
    price_source: str
    fee_rate: float
    fee_source: str = "instrument"
    fee_version: Optional[str] = None
    enforce_price_tick: bool = False
    time_in_force: str = "gtc"
    post_only: bool = False
    fee_currency: str = "quote"
    fee_calculation_basis: str = "quote_notional"
    fee_rounding_mode: str = "unrounded"
    fee_precision: Optional[int] = None
    fee_tier: str = "default"
    fee_schedule_hash: Optional[str] = None
    execution_context: Optional["ResolvedExecutionContext"] = None
    metadata: Dict[str, Any] | None = None


class OrderFillExecutor(Protocol):
    """Execution surface for deterministic/paper/live order fills."""

    def execute_order(
        self,
        order: FillOrder,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        ...


@dataclass(frozen=True)
class LifecycleFillExecution:
    """Compatibility fill result plus its authoritative order trace."""

    fill: Optional[FillResult]
    rejection: Optional[FillRejection]
    lifecycle: CanonicalOrderLifecycle
    emitted_event_ids: Tuple[str, ...]


def build_fill_order(
    *,
    side: str,
    requested_qty: float,
    price: float,
    order_type: str,
    liquidity_role: object,
    price_source: str,
    maker_fee_rate: float | None = None,
    taker_fee_rate: float | None = None,
    fee_source: str = "instrument",
    fee_version: Optional[str] = None,
    enforce_price_tick: bool = False,
    time_in_force: str = "gtc",
    post_only: bool | None = None,
    execution_context: Optional["ResolvedExecutionContext"] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> FillOrder:
    """Build a canonical order request with fee role resolved up front."""

    normalized_order_type = str(order_type).strip().lower()
    resolved_post_only = normalized_order_type == OrderType.LIMIT_MAKER.value if post_only is None else post_only
    if not isinstance(resolved_post_only, bool):
        raise ValueError("post_only must be a boolean")
    context_metadata: Dict[str, Any] = {}
    if execution_context is not None:
        expected_role = execution_context.venue.liquidity_role(normalized_order_type)
        supplied_role = normalize_liquidity_role(liquidity_role)
        if supplied_role != expected_role:
            raise ValueError(
                "LIQUIDITY_ROLE_MISMATCH "
                f"order_type={normalized_order_type} expected={expected_role} actual={supplied_role}"
            )
        role = expected_role
        schedule = execution_context.fee_schedule
        maker_fee_rate = schedule.maker_rate
        taker_fee_rate = schedule.taker_rate
        fee_source = schedule.source
        fee_version = schedule.version or schedule.schedule_hash
        context_metadata = execution_context.evidence_metadata()
    else:
        role = normalize_liquidity_role(liquidity_role)
    if maker_fee_rate is None or taker_fee_rate is None:
        raise ValueError("maker_fee_rate and taker_fee_rate must be explicitly resolved")
    schedule = execution_context.fee_schedule if execution_context is not None else None
    return FillOrder(
        side=str(side),
        requested_qty=float(requested_qty),
        price=float(price),
        order_type=normalized_order_type,
        liquidity_role=role,
        price_source=str(price_source),
        fee_rate=fee_rate_for_role(
            role=role,
            maker_fee_rate=float(maker_fee_rate),
            taker_fee_rate=float(taker_fee_rate),
        ),
        fee_source=str(fee_source or "unresolved"),
        fee_version=str(fee_version or "").strip() or None,
        enforce_price_tick=bool(enforce_price_tick),
        time_in_force=str(time_in_force or "gtc").strip().lower(),
        post_only=resolved_post_only,
        fee_currency=schedule.fee_currency if schedule is not None else "quote",
        fee_calculation_basis=schedule.calculation_basis if schedule is not None else "quote_notional",
        fee_rounding_mode=schedule.rounding_mode if schedule is not None else "unrounded",
        fee_precision=schedule.precision if schedule is not None else None,
        fee_tier=schedule.tier if schedule is not None else "default",
        fee_schedule_hash=schedule.schedule_hash if schedule is not None else None,
        execution_context=execution_context,
        metadata={**context_metadata, **dict(metadata or {})},
    )


def execute_fill_order(
    executor: OrderFillExecutor,
    order: FillOrder,
) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
    """Execute through the durable lifecycle while preserving the legacy tuple."""

    result = execute_fill_order_with_lifecycle(executor, order)
    return result.fill, result.rejection


def _compatibility_known_at(order: FillOrder) -> str:
    metadata = dict(order.metadata or {})
    for key in ("known_at", "bar_time", "event_time", "fill_time", "created_at"):
        value = str(metadata.get(key) or "").strip()
        if value:
            return value
    # Old direct callers did not carry a causal timestamp.  Keep their adapter
    # deterministic and explicitly disclose the compatibility sentinel.
    return "1970-01-01T00:00:00Z"


def canonical_order_request_for_fill_order(order: FillOrder) -> CanonicalOrderRequest:
    """Adapt an immediate FillOrder into the immutable Phase 2B request."""

    metadata = dict(order.metadata or {})
    context = order.execution_context
    context_hash = (
        str(context.context_hash)
        if context is not None
        else stable_order_identity(
            "legacy_execution_context",
            {
                "fee_source": order.fee_source,
                "fee_version": order.fee_version,
                "fee_schedule_hash": order.fee_schedule_hash,
            },
        )
    )
    instrument_id = str(metadata.get("instrument_id") or "").strip()
    symbol = str(metadata.get("symbol") or "").strip()
    if context is not None:
        instrument_id = instrument_id or str(context.instrument.instrument_id or "").strip()
        symbol = symbol or str(context.instrument.symbol or "").strip()
    instrument_id = instrument_id or "compatibility:unbound_instrument"
    symbol = symbol or "compatibility:unbound_symbol"
    policy_hash = execution_policy_hash(
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        post_only=order.post_only,
        liquidity_role=order.liquidity_role,
        price_source=order.price_source,
    )
    identity_material = {
        "run_id": metadata.get("run_id"),
        "bot_id": metadata.get("bot_id"),
        "strategy_id": metadata.get("strategy_id"),
        "instrument_id": instrument_id,
        "symbol": symbol,
        "trade_id": metadata.get("trade_id"),
        "leg_id": metadata.get("leg_id"),
        "side": order.side,
        "requested_qty": order.requested_qty,
        "requested_price": order.price,
        "execution_context_hash": context_hash,
        "execution_policy_hash": policy_hash,
        "known_at": _compatibility_known_at(order),
    }
    request_id = str(
        metadata.get("order_request_id")
        or metadata.get("order_id")
        or stable_order_identity("order_request", identity_material)
    )
    return CanonicalOrderRequest(
        request_id=request_id,
        run_id=str(metadata.get("run_id") or "compatibility:unbound_run"),
        bot_id=str(metadata.get("bot_id") or "compatibility:unbound_bot"),
        strategy_id=str(metadata.get("strategy_id") or "compatibility:unbound_strategy"),
        instrument_id=instrument_id,
        symbol=symbol,
        side=order.side,
        requested_qty=order.requested_qty,
        requested_price=order.price,
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        post_only=order.post_only,
        liquidity_role=order.liquidity_role,
        price_source=order.price_source,
        execution_context_hash=context_hash,
        execution_policy_hash=policy_hash,
        known_at=_compatibility_known_at(order),
        signal_id=metadata.get("signal_id"),
        decision_id=metadata.get("decision_id"),
        trade_id=metadata.get("trade_id"),
        metadata={
            **metadata,
            "binding_source": (
                "resolved_execution_context"
                if context is not None
                else "legacy_fill_order_compatibility"
            ),
            "known_at_source": (
                "fill_order_metadata"
                if any(metadata.get(key) for key in ("known_at", "bar_time", "event_time", "fill_time", "created_at"))
                else "compatibility_sentinel"
            ),
        },
    )


def _lifecycle_evidence(lifecycle: CanonicalOrderLifecycle, *, fill_id: Optional[str] = None) -> Dict[str, Any]:
    snapshot = lifecycle.snapshot()
    active_attempt_id = snapshot.active_attempt_id or lifecycle.attempts[-1].attempt_id
    attempt = next(item for item in lifecycle.attempts if item.attempt_id == active_attempt_id)
    fill_ids = [event.fill_id for event in lifecycle.events if event.fill_id is not None]
    return {
        "order_lifecycle_schema_version": lifecycle.request.schema_version,
        "order_request_id": lifecycle.request.request_id,
        "order_request_manifest_hash": lifecycle.request.manifest_hash,
        "order_attempt_id": attempt.attempt_id,
        "order_attempt_manifest_hash": attempt.manifest_hash,
        "order_lifecycle_state": snapshot.state.value,
        "order_cumulative_filled_qty": snapshot.cumulative_filled_qty,
        "order_remaining_qty": snapshot.remaining_qty,
        "order_fill_id": fill_id,
        "order_fill_ids": fill_ids,
        "order_lifecycle_replay_hash": snapshot.replay_hash,
        "order_lifecycle_event_ids": [event.event_id for event in lifecycle.events],
        "order_execution_context_hash": lifecycle.request.execution_context_hash,
        "order_execution_policy_hash": attempt.execution_policy_hash,
    }


def _validate_existing_lifecycle(order: FillOrder, lifecycle: CanonicalOrderLifecycle) -> None:
    context_hash = (
        str(order.execution_context.context_hash)
        if order.execution_context is not None
        else lifecycle.request.execution_context_hash
    )
    if context_hash != lifecycle.request.execution_context_hash:
        raise ValueError("fill_order_execution_context_changed_during_order_lifetime")
    snapshot = lifecycle.snapshot()
    active_attempt_id = snapshot.active_attempt_id
    if active_attempt_id is None:
        raise ValueError("fill_order_lifecycle_has_no_active_attempt")
    attempt = next(item for item in lifecycle.attempts if item.attempt_id == active_attempt_id)
    supplied_policy_hash = execution_policy_hash(
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        post_only=order.post_only,
        liquidity_role=order.liquidity_role,
        price_source=order.price_source,
    )
    if supplied_policy_hash != attempt.execution_policy_hash:
        raise ValueError("fill_order_execution_policy_changed_without_replacement")
    if str(order.side).strip().lower() != lifecycle.request.side:
        raise ValueError("fill_order_side_changed_during_order_lifetime")


def execute_fill_order_with_lifecycle(
    executor: OrderFillExecutor,
    order: FillOrder,
    *,
    lifecycle: Optional[CanonicalOrderLifecycle] = None,
) -> LifecycleFillExecution:
    """Execute one lifecycle step without taking ownership of accounting."""

    effective_order = order
    starting_seq = len(lifecycle.events) if lifecycle is not None else 0
    if lifecycle is not None:
        _validate_existing_lifecycle(order, lifecycle)
    if order.execution_context is not None:
        conformance = order.execution_context.validate_order(
            order_type=order.order_type,
            time_in_force=order.time_in_force,
            post_only=order.post_only,
            side=order.side,
            quantity=order.requested_qty,
            price=order.price,
            liquidity_role=order.liquidity_role,
        )
        if not conformance.accepted:
            if lifecycle is None:
                request = canonical_order_request_for_fill_order(order)
                attempt = build_initial_order_attempt(
                    request,
                    attempt_id=str(dict(order.metadata or {}).get("order_attempt_id") or "") or None,
                )
                lifecycle = CanonicalOrderLifecycle.create(
                    request,
                    attempt,
                    venue_event_name=venue_lifecycle_event_name(order.execution_context, CanonicalOrderState.REQUESTED),
                )
            active_attempt_id = lifecycle.snapshot().active_attempt_id
            if active_attempt_id is None:
                raise ValueError("rejected_fill_order_has_no_active_attempt")
            lifecycle.transition(
                attempt_id=active_attempt_id,
                state=CanonicalOrderState.REJECTED,
                known_at=_compatibility_known_at(order),
                reason=str(conformance.reason or "ORDER_CONFORMANCE_FAILED"),
                venue_event_name=venue_lifecycle_event_name(order.execution_context, CanonicalOrderState.REJECTED),
                metadata=dict(conformance.metadata),
            )
            rejection = FillRejection(
                reason=str(conformance.reason or "ORDER_CONFORMANCE_FAILED"),
                metadata={
                    **dict(conformance.metadata),
                    **_lifecycle_evidence(lifecycle),
                },
            )
            return LifecycleFillExecution(
                fill=None,
                rejection=rejection,
                lifecycle=lifecycle,
                emitted_event_ids=tuple(event.event_id for event in lifecycle.events_after(starting_seq)),
            )
        effective_order = replace(
            order,
            requested_qty=float(conformance.normalized_qty or order.requested_qty),
            price=float(conformance.normalized_price or order.price),
            metadata={**dict(order.metadata or {}), **dict(conformance.metadata)},
        )
    if lifecycle is None:
        request = canonical_order_request_for_fill_order(order)
        attempt = build_initial_order_attempt(
            request,
            attempt_id=str(dict(order.metadata or {}).get("order_attempt_id") or "") or None,
        )
        lifecycle = CanonicalOrderLifecycle.create(
            request,
            attempt,
            venue_event_name=venue_lifecycle_event_name(order.execution_context, CanonicalOrderState.REQUESTED),
        )
    snapshot = lifecycle.snapshot()
    active_attempt_id = snapshot.active_attempt_id
    if active_attempt_id is None:
        raise ValueError("fill_order_lifecycle_has_no_active_attempt")
    active_snapshot = lifecycle.attempt_snapshot(active_attempt_id)
    if active_snapshot is None:
        raise ValueError("fill_order_active_attempt_has_no_state")
    if active_snapshot.state == CanonicalOrderState.REQUESTED:
        lifecycle.transition(
            attempt_id=active_attempt_id,
            state=CanonicalOrderState.VALIDATED,
            known_at=_compatibility_known_at(order),
            venue_event_name=venue_lifecycle_event_name(order.execution_context, CanonicalOrderState.VALIDATED),
        )
        active_snapshot = lifecycle.attempt_snapshot(active_attempt_id)
    if active_snapshot is not None and active_snapshot.state == CanonicalOrderState.VALIDATED:
        lifecycle.transition(
            attempt_id=active_attempt_id,
            state=CanonicalOrderState.ACCEPTED,
            known_at=_compatibility_known_at(order),
            venue_event_name=venue_lifecycle_event_name(order.execution_context, CanonicalOrderState.ACCEPTED),
        )
        active_snapshot = lifecycle.attempt_snapshot(active_attempt_id)
    if active_snapshot is None or active_snapshot.state not in {
        CanonicalOrderState.ACCEPTED,
        CanonicalOrderState.OPEN,
        CanonicalOrderState.PARTIALLY_FILLED,
    }:
        raise ValueError(f"fill_order_attempt_not_executable state={active_snapshot.state.value if active_snapshot else None}")
    effective_order = replace(
        effective_order,
        requested_qty=float(active_snapshot.remaining_qty),
    )
    execute_order_batch = getattr(executor, "execute_order_batch", None)
    execute_order = getattr(executor, "execute_order", None)
    if not callable(execute_order_batch) and not callable(execute_order):
        raise RuntimeError(
            "execution adapter does not implement execute_order "
            f"order_type={order.order_type} liquidity_role={order.liquidity_role}"
        )
    batch = execute_order_batch(effective_order) if callable(execute_order_batch) else None
    if batch is not None:
        fill = getattr(batch, "fill", None)
        rejection = getattr(batch, "rejection", None)
        level_fills = tuple(getattr(batch, "level_fills", ()) or ())
        batch_status = str(getattr(batch, "status", "") or "").strip().lower()
        residual_disposition = str(
            getattr(batch, "residual_disposition", "") or ""
        ).strip().lower()
        batch_evidence = dict(getattr(batch, "evidence", {}) or {})
    else:
        fill, rejection = execute_order(effective_order)
        level_fills = ()
        batch_status = ""
        residual_disposition = ""
        batch_evidence = {}
    protected_fills = level_fills or ((fill,) if fill is not None else ())
    if protected_fills and effective_order.execution_context is not None:
        failed_protection = None
        for candidate_fill in protected_fills:
            protection = effective_order.execution_context.validate_fill_protections(
                order_type=effective_order.order_type,
                side=effective_order.side,
                requested_price=order.price,
                fill_price=candidate_fill.fill_price,
                filled_qty=candidate_fill.filled_qty,
            )
            if not protection.accepted:
                failed_protection = protection
                break
        if failed_protection is not None:
            lifecycle.transition(
                attempt_id=active_attempt_id,
                state=CanonicalOrderState.REJECTED,
                known_at=_compatibility_known_at(order),
                reason=str(failed_protection.reason or "MARKET_PROTECTION_FAILED"),
                venue_event_name=venue_lifecycle_event_name(order.execution_context, CanonicalOrderState.REJECTED),
                metadata=dict(failed_protection.metadata),
            )
            protected_rejection = FillRejection(
                reason=str(failed_protection.reason or "MARKET_PROTECTION_FAILED"),
                metadata={
                    **dict(failed_protection.metadata),
                    **_lifecycle_evidence(lifecycle),
                },
            )
            return LifecycleFillExecution(
                fill=None,
                rejection=protected_rejection,
                lifecycle=lifecycle,
                emitted_event_ids=tuple(event.event_id for event in lifecycle.events_after(starting_seq)),
            )
    if rejection is not None:
        lifecycle.transition(
            attempt_id=active_attempt_id,
            state=CanonicalOrderState.REJECTED,
            known_at=_compatibility_known_at(order),
            reason=rejection.reason,
            venue_event_name=venue_lifecycle_event_name(order.execution_context, CanonicalOrderState.REJECTED),
            metadata=dict(rejection.metadata or {}),
        )
        rejection = replace(
            rejection,
            metadata={
                **dict(rejection.metadata or {}),
                **_lifecycle_evidence(lifecycle),
            },
        )
    elif level_fills:
        fill_ids: list[str] = []
        for level_fill in level_fills:
            current_snapshot = lifecycle.attempt_snapshot(active_attempt_id)
            if current_snapshot is None:
                raise ValueError("fill_order_active_attempt_has_no_state")
            level_metadata = dict(level_fill.metadata or {})
            fill_material = {
                "request_id": lifecycle.request.request_id,
                "attempt_id": active_attempt_id,
                "next_event_seq": len(lifecycle.events) + 1,
                "known_at": _compatibility_known_at(order),
                "filled_qty": level_fill.filled_qty,
                "fill_price": level_fill.fill_price,
                "fee": level_fill.fee,
            }
            level_fill_id = str(
                level_metadata.get("fill_id")
                or stable_order_identity("order_fill", fill_material)
            )
            fill_event = lifecycle.record_fill(
                attempt_id=active_attempt_id,
                fill_id=level_fill_id,
                fill_qty=level_fill.filled_qty,
                fill_price=level_fill.fill_price,
                fill_fee=level_fill.fee,
                known_at=_compatibility_known_at(order),
                source_sequence=level_metadata.get("book_level_index"),
                venue_event_name=venue_lifecycle_event_name(
                    order.execution_context,
                    (
                        CanonicalOrderState.FILLED
                        if abs(float(level_fill.filled_qty) - float(current_snapshot.remaining_qty)) <= 1e-12
                        else CanonicalOrderState.PARTIALLY_FILLED
                    ),
                ),
                metadata=level_metadata,
            )
            fill_ids.append(str(fill_event.fill_id))
        if residual_disposition in {"canceled", "expired"}:
            terminal_state = (
                CanonicalOrderState.CANCELED
                if residual_disposition == "canceled"
                else CanonicalOrderState.EXPIRED
            )
            current_snapshot = lifecycle.attempt_snapshot(active_attempt_id)
            if current_snapshot is not None and current_snapshot.state not in {
                CanonicalOrderState.FILLED,
                terminal_state,
            }:
                lifecycle.transition(
                    attempt_id=active_attempt_id,
                    state=terminal_state,
                    known_at=_compatibility_known_at(order),
                    reason=f"residual_{residual_disposition}",
                    venue_event_name=venue_lifecycle_event_name(
                        order.execution_context,
                        terminal_state,
                    ),
                    metadata=batch_evidence,
                )
        fill = annotate_fill_order(fill, effective_order)
        if fill is not None:
            fill = replace(
                fill,
                metadata={
                    **dict(fill.metadata or {}),
                    **_lifecycle_evidence(
                        lifecycle,
                        fill_id=fill_ids[-1] if fill_ids else None,
                    ),
                    "order_fill_ids": fill_ids,
                    "residual_disposition": residual_disposition or None,
                },
            )
    elif fill is None:
        if batch_status in {"canceled", "expired"}:
            terminal_state = CanonicalOrderState(batch_status)
            lifecycle.transition(
                attempt_id=active_attempt_id,
                state=terminal_state,
                known_at=_compatibility_known_at(order),
                reason=str(batch_evidence.get("block_reason") or f"order_{batch_status}"),
                venue_event_name=venue_lifecycle_event_name(
                    order.execution_context,
                    terminal_state,
                ),
                metadata=batch_evidence,
            )
        elif active_snapshot.state == CanonicalOrderState.ACCEPTED:
            lifecycle.transition(
                attempt_id=active_attempt_id,
                state=CanonicalOrderState.OPEN,
                known_at=_compatibility_known_at(order),
                venue_event_name=venue_lifecycle_event_name(order.execution_context, CanonicalOrderState.OPEN),
                metadata=batch_evidence,
            )
    else:
        fill_material = {
            "request_id": lifecycle.request.request_id,
            "attempt_id": active_attempt_id,
            "next_event_seq": len(lifecycle.events) + 1,
            "known_at": _compatibility_known_at(order),
            "filled_qty": fill.filled_qty,
            "fill_price": fill.fill_price,
            "fee": fill.fee,
        }
        fill_id = str(
            dict(fill.metadata or {}).get("fill_id")
            or dict(order.metadata or {}).get("fill_id")
            or stable_order_identity("order_fill", fill_material)
        )
        fill_event = lifecycle.record_fill(
            attempt_id=active_attempt_id,
            fill_id=fill_id,
            fill_qty=fill.filled_qty,
            fill_price=fill.fill_price,
            fill_fee=fill.fee,
            known_at=_compatibility_known_at(order),
            venue_event_name=venue_lifecycle_event_name(
                order.execution_context,
                (
                    CanonicalOrderState.FILLED
                    if abs(float(fill.filled_qty) - float(active_snapshot.remaining_qty)) <= 1e-12
                    else CanonicalOrderState.PARTIALLY_FILLED
                ),
            ),
        )
        fill = annotate_fill_order(fill, effective_order)
        if fill is not None:
            fill = replace(
                fill,
                metadata={
                    **dict(fill.metadata or {}),
                    **_lifecycle_evidence(lifecycle, fill_id=fill_event.fill_id),
                },
            )
    return LifecycleFillExecution(
        fill=fill,
        rejection=rejection,
        lifecycle=lifecycle,
        emitted_event_ids=tuple(event.event_id for event in lifecycle.events_after(starting_seq)),
    )


def annotate_fill_order(fill: Optional[FillResult], order: FillOrder) -> Optional[FillResult]:
    """Attach canonical order/liquidity metadata to a fill result."""

    if fill is None:
        return None
    role = normalize_liquidity_role(order.liquidity_role)
    metadata = dict(fill.metadata or {})
    metadata.update(dict(order.metadata or {}))
    metadata["order_type"] = order.order_type
    metadata["liquidity_role"] = role
    metadata["price_source"] = order.price_source
    metadata["time_in_force"] = order.time_in_force
    metadata["post_only"] = order.post_only
    metadata["fee_currency"] = order.fee_currency
    metadata["fee_rounding_mode"] = order.fee_rounding_mode
    metadata["fee_precision"] = order.fee_precision
    metadata["fee_tier"] = order.fee_tier
    metadata["fee_schedule_hash"] = order.fee_schedule_hash
    return replace(
        fill,
        fee_role=role,
        fee_rate=float(order.fee_rate),
        fee_source=order.fee_source,
        fee_version=order.fee_version,
        metadata=metadata,
    )


__all__ = [
    "FillOrder",
    "LifecycleFillExecution",
    "OrderFillExecutor",
    "OrderType",
    "annotate_fill_order",
    "build_fill_order",
    "canonical_order_request_for_fill_order",
    "execute_fill_order",
    "execute_fill_order_with_lifecycle",
]
