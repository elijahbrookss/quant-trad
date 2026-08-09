"""Entry execution coordinator for bot runtime."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, replace
from typing import Optional, TYPE_CHECKING

from utils.log_context import build_log_context, with_log_context
from .execution_intent import ExecutionIntent, ExecutionOutcome
from .order_lifecycle import (
    CanonicalOrderLifecycle,
    CanonicalOrderRequest,
    CanonicalOrderState,
    build_initial_order_attempt,
    build_replacement_order_attempt,
    execution_policy_hash,
    stable_order_identity,
    venue_lifecycle_event_name,
)
if TYPE_CHECKING:
    from .domain import Candle, EntryFill, EntryFillResult, EntryRequest, LadderPosition, LadderRiskEngine

logger = logging.getLogger(__name__)

@dataclass
class PendingEntry:
    """Track pending entry intents awaiting fills."""

    request: "EntryRequest"
    intent: ExecutionIntent
    direction: str
    qty_raw: float
    requested_qty: float
    r_ticks: float
    r_value: Optional[float]
    atr_at_entry: Optional[float]
    r_multiple_at_entry: Optional[float]
    order_intent_id: str
    trade_id: str
    validity_remaining: int
    fallback: str
    filled_qty: float = 0.0
    filled_notional: float = 0.0
    fees_paid: float = 0.0
    remaining_qty: float = 0.0
    order_lifecycle: Optional[CanonicalOrderLifecycle] = None
    applied_fill_ids: tuple[str, ...] = ()


class EntryExecutionCoordinator:
    """Coordinate entry execution lifecycle for ladder risk engine."""

    def __init__(self, engine: "LadderRiskEngine") -> None:
        self._engine = engine
        self.pending_entry: Optional[PendingEntry] = None

    @property
    def has_pending(self) -> bool:
        return self.pending_entry is not None

    def _create_order_lifecycle(
        self,
        *,
        request: "EntryRequest",
        intent: ExecutionIntent,
        candle: "Candle",
    ) -> CanonicalOrderLifecycle:
        engine = self._engine
        context = engine.execution_context
        policy_hash = execution_policy_hash(
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            post_only=intent.post_only,
            liquidity_role=context.venue.liquidity_role(intent.order_type),
            price_source=("limit_price" if intent.limit_params is not None else "requested_price"),
        )
        requested_price = (
            float(intent.limit_params.limit_price)
            if intent.limit_params is not None and intent.limit_params.limit_price is not None
            else float(intent.requested_price)
        )
        order_request = CanonicalOrderRequest(
            request_id=str(intent.order_id),
            run_id=str(getattr(engine, "run_id", None) or "compatibility:unbound_run"),
            bot_id=str(getattr(engine, "bot_id", None) or "compatibility:unbound_bot"),
            strategy_id=str(getattr(engine, "strategy_id", None) or "compatibility:unbound_strategy"),
            instrument_id=str(engine.instrument.get("id") or "compatibility:unbound_instrument"),
            symbol=str(intent.symbol or engine.instrument.get("symbol") or "compatibility:unbound_symbol"),
            side=intent.side,
            requested_qty=float(intent.qty),
            requested_price=requested_price,
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            post_only=intent.post_only,
            liquidity_role=context.venue.liquidity_role(intent.order_type),
            price_source=("limit_price" if intent.limit_params is not None else "requested_price"),
            execution_context_hash=context.context_hash,
            execution_policy_hash=policy_hash,
            known_at=candle.time,
            signal_id=getattr(engine, "last_signal_id", None),
            decision_id=getattr(engine, "last_decision_id", None),
            trade_id=request.trade_id,
            metadata={
                **dict(intent.metadata),
                "entry_request_id": request.entry_request_id,
                "direction": request.direction,
            },
        )
        attempt = build_initial_order_attempt(
            order_request,
            attempt_id=stable_order_identity(
                "order_attempt",
                {"request_id": order_request.request_id, "attempt_number": 1},
            ),
        )
        return CanonicalOrderLifecycle.create(
            order_request,
            attempt,
            venue_event_name=venue_lifecycle_event_name(
                context,
                CanonicalOrderState.REQUESTED,
            ),
        )

    @staticmethod
    def _lifecycle_evidence(lifecycle: CanonicalOrderLifecycle) -> dict:
        snapshot = lifecycle.snapshot()
        attempt = lifecycle.attempts[-1]
        latest_fill = next(
            (event for event in reversed(lifecycle.events) if event.fill_id is not None),
            None,
        )
        return {
            "order_lifecycle_schema_version": lifecycle.request.schema_version,
            "order_request_id": lifecycle.request.request_id,
            "order_request_manifest_hash": lifecycle.request.manifest_hash,
            "order_attempt_id": attempt.attempt_id,
            "order_attempt_manifest_hash": attempt.manifest_hash,
            "order_lifecycle_state": snapshot.state.value,
            "order_cumulative_filled_qty": snapshot.cumulative_filled_qty,
            "order_remaining_qty": snapshot.remaining_qty,
            "order_fill_id": latest_fill.fill_id if latest_fill is not None else None,
            "order_fill_ids": [event.fill_id for event in lifecycle.events if event.fill_id is not None],
            "order_lifecycle_replay_hash": snapshot.replay_hash,
            "order_lifecycle_event_ids": [event.event_id for event in lifecycle.events],
            "order_execution_context_hash": lifecycle.request.execution_context_hash,
            "order_execution_policy_hash": attempt.execution_policy_hash,
        }

    def _record_lifecycle(self, lifecycle: CanonicalOrderLifecycle, *, after_seq: int) -> None:
        recorder = getattr(self._engine, "record_order_lifecycle", None)
        if not callable(recorder):
            raise RuntimeError("risk engine does not expose canonical order lifecycle recording")
        recorder(lifecycle, after_seq=after_seq)

    @staticmethod
    def _require_accounted_partial_disposition(
        pending: PendingEntry,
        *,
        disposition: str,
    ) -> None:
        """Permit residual termination only after every partial fill was settled."""

        if float(pending.filled_qty or 0.0) <= 1e-12:
            return
        if pending.applied_fill_ids:
            return
        raise RuntimeError(
            "partial entry disposition is not admitted because the partial fill "
            "was not atomically applied to canonical accounting "
            f"disposition={disposition!r} filled_qty={float(pending.filled_qty)!r} "
            f"remaining_qty={float(pending.remaining_qty)!r} "
            f"order_request_id={pending.order_intent_id!r}"
        )

    def _apply_evaluated_outcome(
        self,
        *,
        lifecycle: CanonicalOrderLifecycle,
        outcome: ExecutionOutcome,
        rejection: object,
        known_at: object,
    ) -> ExecutionOutcome:
        engine = self._engine
        context = engine.execution_context
        snapshot = lifecycle.snapshot()
        attempt_id = snapshot.active_attempt_id
        if attempt_id is None:
            raise RuntimeError("entry order lifecycle has no active attempt")
        attempt_snapshot = lifecycle.attempt_snapshot(attempt_id)
        if attempt_snapshot is None:
            raise RuntimeError("entry order lifecycle active attempt has no state")
        if rejection is not None:
            rejection_reason = str(getattr(rejection, "reason", None) or "ORDER_REJECTED")
            rejection_metadata = dict(getattr(rejection, "metadata", None) or {})
            target_state = (
                CanonicalOrderState.CANCELED
                if str(outcome.status).strip().lower() == "canceled"
                else CanonicalOrderState.REJECTED
            )
            if target_state == CanonicalOrderState.CANCELED and attempt_snapshot.state == CanonicalOrderState.VALIDATED:
                lifecycle.transition(
                    attempt_id=attempt_id,
                    state=CanonicalOrderState.ACCEPTED,
                    known_at=known_at,
                    venue_event_name=venue_lifecycle_event_name(context, CanonicalOrderState.ACCEPTED),
                )
            lifecycle.transition(
                attempt_id=attempt_id,
                state=target_state,
                known_at=known_at,
                reason=rejection_reason,
                venue_event_name=venue_lifecycle_event_name(context, target_state),
                metadata=rejection_metadata,
            )
            return replace(
                outcome,
                metadata={**dict(outcome.metadata), **self._lifecycle_evidence(lifecycle)},
            )
        if attempt_snapshot.state == CanonicalOrderState.VALIDATED:
            lifecycle.transition(
                attempt_id=attempt_id,
                state=CanonicalOrderState.ACCEPTED,
                known_at=known_at,
                venue_event_name=venue_lifecycle_event_name(context, CanonicalOrderState.ACCEPTED),
            )
        normalized_status = str(outcome.status or "").strip().lower()
        if normalized_status == "open":
            current = lifecycle.attempt_snapshot(attempt_id)
            if current is not None and current.state == CanonicalOrderState.ACCEPTED:
                lifecycle.transition(
                    attempt_id=attempt_id,
                    state=CanonicalOrderState.OPEN,
                    known_at=known_at,
                    venue_event_name=venue_lifecycle_event_name(context, CanonicalOrderState.OPEN),
                )
        elif normalized_status in {"filled", "partially_filled"}:
            level_rows = outcome.metadata.get("price_level_fills")
            if isinstance(level_rows, list) and level_rows:
                for row in level_rows:
                    row_payload = dict(row or {})
                    row_metadata = dict(row_payload.get("metadata") or {})
                    fill_id = str(
                        row_metadata.get("fill_id")
                        or stable_order_identity(
                            "order_fill",
                            {
                                "request_id": lifecycle.request.request_id,
                                "attempt_id": attempt_id,
                                "next_event_seq": len(lifecycle.events) + 1,
                                "known_at": str(known_at),
                                "filled_qty": row_payload.get("filled_qty"),
                                "fill_price": row_payload.get("fill_price"),
                                "fee_paid": row_payload.get("fee"),
                            },
                        )
                    )
                    lifecycle.record_fill(
                        attempt_id=attempt_id,
                        fill_id=fill_id,
                        fill_qty=float(row_payload.get("filled_qty") or 0.0),
                        fill_price=float(row_payload.get("fill_price") or 0.0),
                        fill_fee=float(row_payload.get("fee") or 0.0),
                        known_at=known_at,
                        source_sequence=row_metadata.get("book_level_index"),
                        venue_event_name=venue_lifecycle_event_name(
                            context,
                            CanonicalOrderState.PARTIALLY_FILLED,
                        ),
                        metadata=row_metadata,
                    )
            else:
                fill_id = stable_order_identity(
                    "order_fill",
                    {
                        "request_id": lifecycle.request.request_id,
                        "attempt_id": attempt_id,
                        "next_event_seq": len(lifecycle.events) + 1,
                        "known_at": str(known_at),
                        "filled_qty": outcome.filled_qty,
                        "fill_price": outcome.avg_fill_price,
                        "fee_paid": outcome.fee_paid,
                    },
                )
                lifecycle.record_fill(
                    attempt_id=attempt_id,
                    fill_id=fill_id,
                    fill_qty=float(outcome.filled_qty),
                    fill_price=float(outcome.avg_fill_price or 0.0),
                    fill_fee=float(outcome.fee_paid or 0.0),
                    known_at=known_at,
                    venue_event_name=venue_lifecycle_event_name(
                        context,
                        CanonicalOrderState.FILLED
                        if float(outcome.remaining_qty or 0.0) <= 1e-12
                        else CanonicalOrderState.PARTIALLY_FILLED,
                    ),
                )
            residual_disposition = str(
                outcome.metadata.get("residual_disposition") or ""
            ).strip().lower()
            if residual_disposition in {"canceled", "expired"}:
                terminal = (
                    CanonicalOrderState.CANCELED
                    if residual_disposition == "canceled"
                    else CanonicalOrderState.EXPIRED
                )
                current = lifecycle.attempt_snapshot(attempt_id)
                if current is not None and current.state is not CanonicalOrderState.FILLED:
                    lifecycle.transition(
                        attempt_id=attempt_id,
                        state=terminal,
                        known_at=known_at,
                        reason=f"residual_{residual_disposition}",
                        venue_event_name=venue_lifecycle_event_name(context, terminal),
                        metadata=dict(outcome.metadata),
                    )
        elif normalized_status in {"expired", "canceled", "rejected"}:
            target = CanonicalOrderState(normalized_status)
            lifecycle.transition(
                attempt_id=attempt_id,
                state=target,
                known_at=known_at,
                reason=str(outcome.fallback_reason or normalized_status),
                venue_event_name=venue_lifecycle_event_name(context, target),
            )
        return replace(
            outcome,
            metadata={**dict(outcome.metadata), **self._lifecycle_evidence(lifecycle)},
        )

    def submit_entry(self, candle: Candle, direction: str) -> Optional[LadderPosition]:
        engine = self._engine
        request = engine.build_entry_request(candle, direction)
        if not request.validation.ok:
            engine.last_rejection_reason = request.validation.rejection_reason
            engine.last_rejection_detail = self._finalize_rejection_detail(
                request,
                request.validation.rejection_detail,
                request.validation.rejection_reason,
            )
            return None

        intent = request.intent
        if intent is None:
            engine.last_rejection_reason = "ENTRY_REQUEST_INVALID"
            engine.last_rejection_detail = self._entry_rejection_detail(request, {"reason": "intent_missing"})
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason="ENTRY_REQUEST_INVALID",
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None
        execution_context = engine.execution_context
        intent_metadata = {
            **dict(intent.metadata),
            "known_at": candle.time,
        }
        if execution_context.model.supports_latency:
            intent_metadata.pop("arrival_at", None)
            intent_metadata["decision_known_at"] = candle.time
        else:
            intent_metadata["arrival_at"] = candle.time
        intent = replace(intent, metadata=intent_metadata)
        request = replace(request, intent=intent)

        lifecycle = self._create_order_lifecycle(
            request=request,
            intent=intent,
            candle=candle,
        )
        lifecycle_start_seq = 0
        conformance = execution_context.validate_order(
            order_type=intent.order_type,
            time_in_force=intent.time_in_force,
            post_only=intent.post_only,
            side=intent.side,
            quantity=intent.qty,
            price=(
                intent.limit_params.limit_price
                if intent.limit_params is not None and intent.limit_params.limit_price is not None
                else intent.requested_price
            ),
            liquidity_role=execution_context.venue.liquidity_role(intent.order_type),
        )
        if not conformance.accepted:
            lifecycle.transition(
                attempt_id=lifecycle.snapshot().active_attempt_id or lifecycle.attempts[-1].attempt_id,
                state=CanonicalOrderState.REJECTED,
                known_at=candle.time,
                reason=str(conformance.reason or "ORDER_CONFORMANCE_FAILED"),
                venue_event_name=venue_lifecycle_event_name(
                    execution_context,
                    CanonicalOrderState.REJECTED,
                ),
                metadata=dict(conformance.metadata),
            )
            self._record_lifecycle(lifecycle, after_seq=lifecycle_start_seq)
            engine.last_rejection_reason = str(conformance.reason or "ORDER_CONFORMANCE_FAILED")
            engine.last_rejection_detail = self._finalize_rejection_detail(
                request,
                {
                    **dict(conformance.metadata),
                    **self._lifecycle_evidence(lifecycle),
                },
                engine.last_rejection_reason,
            )
            return None
        lifecycle.transition(
            attempt_id=lifecycle.snapshot().active_attempt_id or lifecycle.attempts[-1].attempt_id,
            state=CanonicalOrderState.VALIDATED,
            known_at=candle.time,
            venue_event_name=venue_lifecycle_event_name(
                execution_context,
                CanonicalOrderState.VALIDATED,
            ),
            metadata=dict(conformance.metadata),
        )
        execution_model = engine._resolve_execution_model()
        if intent.order_type == "limit_maker":
            outcome, rejection = execution_model.evaluate(
                intent,
                candle_high=candle.close,
                candle_low=candle.close,
                candle_close=candle.close,
                candle_open=candle.close,
            )
        else:
            outcome, rejection = execution_model.evaluate(
                intent,
                candle_high=candle.high,
                candle_low=candle.low,
                candle_close=candle.close,
                candle_open=candle.open,
            )
        outcome = self._apply_evaluated_outcome(
            lifecycle=lifecycle,
            outcome=outcome,
            rejection=rejection,
            known_at=(
                outcome.updated_at
                if execution_context.model.supports_latency
                else candle.time
            ),
        )
        self._record_lifecycle(lifecycle, after_seq=lifecycle_start_seq)
        if rejection:
            engine.last_rejection_reason = rejection.reason
            engine.last_rejection_detail = self._finalize_rejection_detail(
                request,
                {
                    "requested_qty": request.requested_qty,
                    **(rejection.metadata or {}),
                    **self._lifecycle_evidence(lifecycle),
                },
                rejection.reason,
            )
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason=rejection.reason,
                requested_qty=request.requested_qty,
                order_type=request.order_type,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        def build_pending(validity_remaining: int) -> PendingEntry:
            remaining_qty = float(request.requested_qty)
            return PendingEntry(
                request=request,
                intent=intent,
                direction=request.direction,
                qty_raw=request.qty_raw,
                requested_qty=request.requested_qty,
                r_ticks=float(request.r_ticks),
                r_value=request.r_value,
                atr_at_entry=request.atr_at_entry,
                r_multiple_at_entry=request.r_multiple_at_entry,
                order_intent_id=str(request.order_intent_id),
                trade_id=str(request.trade_id),
                validity_remaining=validity_remaining,
                fallback=request.limit_params.fallback if request.limit_params else "cancel",
                remaining_qty=remaining_qty,
                order_lifecycle=lifecycle,
            )

        if outcome.status in {"open", "partially_filled"}:
            validity_remaining = request.limit_params.validity_window if request.limit_params else 1
            pending = build_pending(max(int(validity_remaining), 1))
            if outcome.status == "partially_filled":
                fill = self._build_entry_fill(
                    pending=pending,
                    outcome=outcome,
                    candle=candle,
                )
                result = self._apply_outcome_fills(
                    request=request,
                    pending=pending,
                    outcome=outcome,
                    candle=candle,
                )
                position = self._apply_fill_result(result)
                if position is not None:
                    return position
                pending = self.pending_entry or pending
            if pending.validity_remaining <= 0:
                return self._apply_entry_fallback(candle, pending, outcome)
            self.pending_entry = pending
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                order_id=request.order_intent_id,
                order_type=request.order_type,
                limit_price=outcome.limit_price,
                validity_remaining=pending.validity_remaining,
                fallback=pending.fallback,
            )
            logger.info(with_log_context("entry_order_pending", context))
            return None

        if outcome.status != "filled":
            engine.last_rejection_reason = "ENTRY_NOT_FILLED"
            engine.last_rejection_detail = self._entry_rejection_detail(request, {"status": outcome.status})
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                order_id=request.order_intent_id,
                status=outcome.status,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        pending = build_pending(0)
        fill = self._build_entry_fill(
            pending=pending,
            outcome=outcome,
            candle=candle,
        )
        result = self._apply_outcome_fills(
            request=request,
            pending=pending,
            outcome=outcome,
            candle=candle,
        )
        return self._apply_fill_result(result)

    def process_pending(self, candle: Candle) -> Optional[LadderPosition]:
        if not self.pending_entry:
            return None
        pending = self.pending_entry
        request = pending.request
        engine = self._engine
        lifecycle = pending.order_lifecycle
        if lifecycle is None:
            raise RuntimeError("pending entry is missing its canonical order lifecycle")
        lifecycle_start_seq = len(lifecycle.events)
        execution_model = engine._resolve_execution_model()
        pending_metadata = {
            **dict(pending.intent.metadata),
            "pending_evaluation": True,
        }
        if engine.execution_context.model.supports_latency:
            active_attempt_id = lifecycle.snapshot().active_attempt_id
            active_snapshot = (
                lifecycle.attempt_snapshot(active_attempt_id)
                if active_attempt_id is not None
                else None
            )
            if active_snapshot is None:
                raise RuntimeError("pending entry lifecycle has no active attempt state")
            pending_metadata.pop("arrival_at", None)
            pending_metadata.update(
                {
                    "evaluation_at": candle.time,
                    "order_request_id": lifecycle.request.request_id,
                    "order_original_requested_qty": float(lifecycle.request.requested_qty),
                    "order_cumulative_filled_qty": float(active_snapshot.cumulative_filled_qty),
                }
            )
        else:
            pending_metadata.update({"known_at": candle.time, "arrival_at": candle.time})
        pending_intent = replace(
            pending.intent,
            qty=float(pending.remaining_qty or pending.intent.qty),
            metadata=pending_metadata,
        )
        outcome, rejection = execution_model.evaluate(
            pending_intent,
            candle_high=candle.high,
            candle_low=candle.low,
            candle_close=candle.close,
            candle_open=candle.open,
        )
        normalized_status = str(outcome.status or "").strip().lower()
        if rejection is not None or normalized_status in {"expired", "canceled", "rejected"}:
            self._require_accounted_partial_disposition(
                pending,
                disposition=(
                    str(getattr(rejection, "reason", None) or "rejected")
                    if rejection is not None
                    else normalized_status
                ),
            )
        outcome = self._apply_evaluated_outcome(
            lifecycle=lifecycle,
            outcome=outcome,
            rejection=rejection,
            known_at=(
                outcome.updated_at
                if engine.execution_context.model.supports_latency
                else candle.time
            ),
        )
        self._record_lifecycle(lifecycle, after_seq=lifecycle_start_seq)
        if rejection:
            engine.last_rejection_reason = rejection.reason
            engine.last_rejection_detail = self._finalize_rejection_detail(
                request,
                {**dict(rejection.metadata or {}), **self._lifecycle_evidence(lifecycle)},
                rejection.reason,
            )
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                reason=rejection.reason,
                order_id=pending.order_intent_id,
            )
            logger.warning(with_log_context("entry_rejected", context))
            self.pending_entry = None
            return None
        if outcome.status in {"filled", "partially_filled"}:
            result = self._apply_outcome_fills(
                request=request,
                pending=pending,
                outcome=outcome,
                candle=candle,
            )
            position = self._apply_fill_result(result)
            residual_disposition = str(
                outcome.metadata.get("residual_disposition") or ""
            ).strip().lower()
            if residual_disposition in {"canceled", "expired"}:
                self.pending_entry = None
                return position
            if position is not None:
                return position
            if result.status == "pending" and result.pending is not None:
                result.pending.validity_remaining -= 1
                self.pending_entry = result.pending
                if result.pending.validity_remaining <= 0:
                    self.pending_entry = None
                    return self._apply_entry_fallback(candle, result.pending, outcome)
            return None
        if outcome.status == "open":
            pending.validity_remaining -= 1
            if pending.validity_remaining <= 0:
                self.pending_entry = None
                return self._apply_entry_fallback(candle, pending, outcome)
            self.pending_entry = pending
            context = build_log_context(
                symbol=engine.instrument.get("symbol"),
                order_id=pending.order_intent_id,
                remaining=pending.validity_remaining,
                status=outcome.status,
            )
            logger.info(with_log_context("entry_order_pending", context))
            return None
        self.pending_entry = None
        engine.last_rejection_reason = "ENTRY_NOT_FILLED"
        engine.last_rejection_detail = self._entry_rejection_detail(request, {"status": outcome.status})
        context = build_log_context(
            symbol=engine.instrument.get("symbol"),
            order_id=pending.order_intent_id,
            status=outcome.status,
        )
        logger.warning(with_log_context("entry_rejected", context))
        return None

    def _build_entry_fill(
        self,
        *,
        pending: PendingEntry,
        outcome: ExecutionOutcome,
        candle: Candle,
    ) -> "EntryFill":
        engine = self._engine
        return engine.build_entry_fill(
            pending=pending,
            outcome=outcome,
            candle=candle,
        )

    def _apply_outcome_fills(
        self,
        *,
        request: "EntryRequest",
        pending: PendingEntry,
        outcome: ExecutionOutcome,
        candle: "Candle",
    ) -> "EntryFillResult":
        """Apply exact book levels individually; preserve one-fill bar behavior."""

        engine = self._engine
        level_rows = outcome.metadata.get("price_level_fills")
        if not isinstance(level_rows, list) or not level_rows:
            fill = self._build_entry_fill(pending=pending, outcome=outcome, candle=candle)
            return engine.apply_entry_fill(request=request, pending=pending, fill=fill)
        result = None
        for index, raw in enumerate(level_rows):
            row = dict(raw or {})
            row_metadata = dict(row.get("metadata") or {})
            level_outcome = ExecutionOutcome(
                order_id=outcome.order_id,
                status=("filled" if index == len(level_rows) - 1 and float(outcome.remaining_qty or 0.0) <= 1e-12 else "partially_filled"),
                filled_qty=float(row.get("filled_qty") or 0.0),
                avg_fill_price=float(row.get("fill_price") or 0.0),
                fee_paid=float(row.get("fee") or 0.0),
                fee_role=str(row.get("fee_role") or outcome.fee_role),
                fee_rate=float(row.get("fee_rate") or 0.0),
                fee_source=str(row.get("fee_source") or outcome.fee_source),
                fee_version=row.get("fee_version") or outcome.fee_version,
                created_at=outcome.created_at,
                updated_at=outcome.updated_at,
                filled_at=outcome.filled_at,
                remaining_qty=max(float(pending.remaining_qty or pending.requested_qty) - float(row.get("filled_qty") or 0.0), 0.0),
                fallback_applied=outcome.fallback_applied,
                fallback_reason=outcome.fallback_reason,
                limit_price=outcome.limit_price,
                validity_window=outcome.validity_window,
                metadata={
                    **row_metadata,
                    "schema_version": outcome.metadata.get("schema_version"),
                    "residual_disposition": outcome.metadata.get("residual_disposition"),
                    **(
                        self._lifecycle_evidence(pending.order_lifecycle)
                        if pending.order_lifecycle is not None
                        else {}
                    ),
                },
            )
            fill = self._build_entry_fill(
                pending=pending,
                outcome=level_outcome,
                candle=candle,
            )
            result = engine.apply_entry_fill(request=request, pending=pending, fill=fill)
            if result.rejection_reason:
                return result
        if result is None:
            raise RuntimeError("book execution outcome declared no applicable level fills")
        return result

    def _apply_fill_result(self, result: "EntryFillResult") -> Optional[LadderPosition]:
        engine = self._engine
        if result.rejection_reason:
            engine.last_rejection_reason = result.rejection_reason
            engine.last_rejection_detail = result.rejection_detail
        if result.status in {"pending", "opened_partial", "augmented_partial"}:
            self.pending_entry = result.pending
            return result.position
        if result.status in {"opened", "augmented"}:
            self.pending_entry = None
            return result.position
        if result.status == "duplicate":
            self.pending_entry = result.pending
            return None
        self.pending_entry = None
        return None

    @staticmethod
    def _entry_rejection_detail(request: "EntryRequest", metadata: Optional[dict]) -> dict:
        detail = dict(metadata or {})
        detail.setdefault("entry_request_id", request.entry_request_id)
        detail.setdefault("attempt_id", request.entry_request_id)
        if request.order_intent_id:
            detail.setdefault("order_request_id", str(request.order_intent_id))
        return detail

    def _finalize_rejection_detail(
        self,
        request: "EntryRequest",
        metadata: Optional[dict],
        reason: Optional[str],
    ) -> dict:
        detail = self._entry_rejection_detail(request, metadata)
        finalizer = getattr(self._engine, "finalize_entry_rejection_detail", None)
        if callable(finalizer):
            return finalizer(request=request, detail=detail, reason=reason)
        return detail

    def _apply_entry_fallback(
        self,
        candle: Candle,
        pending: PendingEntry,
        outcome: ExecutionOutcome,
    ) -> Optional[LadderPosition]:
        engine = self._engine
        fallback = pending.fallback
        lifecycle = pending.order_lifecycle
        if lifecycle is None:
            raise RuntimeError("pending entry fallback is missing its canonical order lifecycle")
        lifecycle_start_seq = len(lifecycle.events)
        if fallback == "convert_to_market":
            predecessor_attempt_id = lifecycle.snapshot().active_attempt_id
            if predecessor_attempt_id is None:
                raise RuntimeError("pending entry fallback has no active order attempt")
            residual_qty = lifecycle.snapshot().remaining_qty
            replacement_attempt = build_replacement_order_attempt(
                lifecycle,
                predecessor_attempt_id=predecessor_attempt_id,
                requested_price=float(candle.close),
                known_at=candle.time,
                order_type="market",
                time_in_force="gtc",
                post_only=False,
                liquidity_role=engine.execution_context.venue.liquidity_role("market"),
                reason="convert_to_market",
            )
            market_intent = ExecutionIntent(
                order_id=replacement_attempt.attempt_id,
                side=pending.intent.side,
                qty=residual_qty,
                symbol=pending.intent.symbol,
                order_type="market",
                requested_price=float(candle.close),
                contract_size=float(getattr(pending.intent, "contract_size", 1.0) or 1.0),
                time_in_force="gtc",
                post_only=False,
                limit_params=None,
                metadata={
                    **dict(pending.intent.metadata),
                    "known_at": candle.time,
                    "arrival_at": candle.time,
                },
            )
            lifecycle.replace(
                attempt_id=predecessor_attempt_id,
                replacement_attempt=replacement_attempt,
                known_at=candle.time,
                reason="convert_to_market",
                venue_event_name=venue_lifecycle_event_name(
                    engine.execution_context,
                    CanonicalOrderState.REPLACED,
                ),
            )
            lifecycle.transition(
                attempt_id=replacement_attempt.attempt_id,
                state=CanonicalOrderState.VALIDATED,
                known_at=candle.time,
                venue_event_name=venue_lifecycle_event_name(
                    engine.execution_context,
                    CanonicalOrderState.VALIDATED,
                ),
            )
            execution_model = engine._resolve_execution_model()
            market_outcome, rejection = execution_model.evaluate(
                market_intent,
                candle_high=candle.high,
                candle_low=candle.low,
                candle_close=candle.close,
                candle_open=candle.open,
            )
            normalized_status = str(market_outcome.status or "").strip().lower()
            if rejection is not None or normalized_status not in {"filled", "partially_filled"}:
                self._require_accounted_partial_disposition(
                    pending,
                    disposition=str(
                        getattr(rejection, "reason", None)
                        or normalized_status
                        or "unfilled"
                    ),
                )
            market_outcome = self._apply_evaluated_outcome(
                lifecycle=lifecycle,
                outcome=market_outcome,
                rejection=rejection,
                known_at=candle.time,
            )
            self._record_lifecycle(lifecycle, after_seq=lifecycle_start_seq)
            if rejection:
                engine.last_rejection_reason = rejection.reason
                engine.last_rejection_detail = self._finalize_rejection_detail(
                    pending.request,
                    {**dict(rejection.metadata or {}), **self._lifecycle_evidence(lifecycle)},
                    rejection.reason,
                )
                context = build_log_context(
                    symbol=engine.instrument.get("symbol"),
                    reason=rejection.reason,
                    order_id=market_intent.order_id,
                    fallback="convert_to_market",
                )
                logger.warning(with_log_context("entry_fallback_rejected", context))
                return None
            market_outcome = ExecutionOutcome(
                **{
                    **asdict(market_outcome),
                    "fallback_applied": True,
                    "fallback_reason": "convert_to_market",
                }
            )
            result = self._apply_outcome_fills(
                request=pending.request,
                pending=pending,
                outcome=market_outcome,
                candle=candle,
            )
            return self._apply_fill_result(result)

        self._require_accounted_partial_disposition(
            pending,
            disposition=fallback or "expired",
        )
        outcome_payload = ExecutionOutcome(
            **{
                **asdict(outcome),
                "status": "expired",
                "fallback_applied": True,
                "fallback_reason": fallback,
            }
        )
        attempt_id = lifecycle.snapshot().active_attempt_id
        if attempt_id is None:
            raise RuntimeError("pending entry expiry has no active order attempt")
        lifecycle.transition(
            attempt_id=attempt_id,
            state=CanonicalOrderState.EXPIRED,
            known_at=candle.time,
            reason=fallback,
            venue_event_name=venue_lifecycle_event_name(
                engine.execution_context,
                CanonicalOrderState.EXPIRED,
            ),
        )
        self._record_lifecycle(lifecycle, after_seq=lifecycle_start_seq)
        outcome_payload = replace(
            outcome_payload,
            metadata={
                **dict(outcome_payload.metadata),
                **self._lifecycle_evidence(lifecycle),
            },
        )
        engine.last_rejection_reason = "ENTRY_UNFILLED"
        engine.last_rejection_detail = self._entry_rejection_detail(pending.request, asdict(outcome_payload))
        context = build_log_context(
            symbol=engine.instrument.get("symbol"),
            order_id=pending.order_intent_id,
            status="expired",
            fallback=fallback,
        )
        logger.warning(with_log_context("entry_order_expired", context))
        return None


__all__ = ["EntryExecutionCoordinator", "PendingEntry"]
