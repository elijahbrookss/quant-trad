from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from engines.bot_runtime.core.book_execution import (
    EXECUTION_BOOK_SNAPSHOT_SCHEMA_VERSION,
    EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION,
    EXECUTION_BOOK_TAPE_SCHEMA_VERSION,
    BookExecutionModel,
    ExecutionBookLevel,
    ExecutionBookSnapshot,
    ExecutionBookSourceReference,
    ExecutionBookTape,
    ExecutionBookTapeBundle,
    ExecutionBookValidityClosure,
)
from engines.bot_runtime.core.execution_assumptions import (
    CONSERVATIVE_BAR_MODEL_VERSION,
    resolve_execution_assumptions,
)
from engines.bot_runtime.core.execution_context import (
    execution_model_artifact_from_book_tape,
    resolve_execution_context,
)
from engines.bot_runtime.core.execution_order import FillOrder, execute_fill_order_with_lifecycle
from engines.bot_runtime.core.order_lifecycle import CanonicalOrderState
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile


BASE = datetime(2026, 8, 5, 14, 0, tzinfo=UTC)


def _assumptions():
    return resolve_execution_assumptions(
        "economic",
        {
            "model_version": CONSERVATIVE_BAR_MODEL_VERSION,
            "market_slippage_bps": 5.0,
            "stop_slippage_bps": 10.0,
            "passive_fill_policy": "strict_penetration",
            "fee_policy": "instrument_resolved",
            "full_fill_assumption": True,
            "cost_stress_scenarios": [
                {"id": "stress", "additional_slippage_bps": 5.0, "fee_multiplier": 1.25}
            ],
        },
    )


def _instrument(*, capability: str = "l2") -> dict:
    order_types = ["market", "limit_aggressive", "limit_maker", "limit_resting", "stop_market"]
    return {
        "id": "btc-usd",
        "symbol": "BTC-USD",
        "instrument_type": "spot",
        "datasource": "fixture",
        "exchange": "synthetic-venue",
        "tick_size": 0.01,
        "contract_size": 1.0,
        "tick_value": 0.01,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "min_order_size": 0.001,
        "qty_step": 0.001,
        "max_qty": 100.0,
        "min_notional": 1.0,
        "maker_fee_rate": 0.001,
        "taker_fee_rate": 0.002,
        "fee_source": "fixture",
        "fee_schedule_version": "fees.v1",
        "venue_execution_profile": {
            "profile_id": "synthetic-l2",
            "version": "synthetic-l2.v1",
            "venue_id": "synthetic-venue",
            "supported_order_types": order_types,
            "supported_time_in_force": ["gtc", "ioc", "fok"],
            "post_only_supported": True,
            "post_only_behavior": "reject_would_cross",
            "liquidity_role_by_order_type": {
                "market": "taker",
                "limit_aggressive": "taker",
                "limit_maker": "maker",
                "limit_resting": "maker",
                "stop_market": "taker",
            },
            "price_increment_policy": "reject",
            "quantity_increment_policy": "reject",
            "max_market_order_notional": None,
            "market_price_collar_bps": None,
            "book_data_capability": capability,
            "lifecycle_event_mapping": {
                "requested": "submitted",
                "validated": "validated",
                "accepted": "accepted",
                "open": "open",
                "partially_filled": "partially_filled",
                "filled": "filled",
                "canceled": "canceled",
                "rejected": "rejected",
                "expired": "expired",
                "replaced": "replaced",
            },
            "external_order_submission_enabled": False,
            "source": "fixture",
        },
        "fee_schedule": {
            "schedule_id": "synthetic-l2:retail",
            "venue_profile_id": "synthetic-l2",
            "version": "fees.v1",
            "maker_rate": 0.001,
            "taker_rate": 0.002,
            "source": "fixture",
            "fee_currency": "USD",
            "calculation_basis": "quote_notional",
            "rounding_mode": "unrounded",
            "precision": None,
            "tier": "retail",
            "configured": True,
            "verified_zero": False,
        },
    }


def _snapshot(
    ordinal: int,
    *,
    known_offset: int,
    bids: tuple[tuple[str, str], ...] = (("99", "3"), ("100", "2")),
    asks: tuple[tuple[str, str], ...] = (("101", "1"), ("102", "2"), ("103", "4")),
) -> ExecutionBookSnapshot:
    return ExecutionBookSnapshot(
        schema_version=EXECUTION_BOOK_SNAPSHOT_SCHEMA_VERSION,
        instrument_id="btc-usd",
        series_id=7,
        validity_interval_id="validity-1",
        source_reference=ExecutionBookSourceReference(
            definition_id="book-definition",
            session_id="session-1",
            connection_epoch=0,
            source_product_id="source-product",
            source_sequence=ordinal,
            receive_ordinal=ordinal,
            event_ordinal=0,
        ),
        product_definition_version_id="product.v1",
        quantity_unit="base",
        effective_at=BASE + timedelta(seconds=known_offset - 1),
        known_at=BASE + timedelta(seconds=known_offset),
        reconstruction_state_hash=f"state-{ordinal}",
        bids=tuple(ExecutionBookLevel(price, qty) for price, qty in bids),
        asks=tuple(ExecutionBookLevel(price, qty) for price, qty in asks),
    )


def _tape(*, capability: str = "l2") -> ExecutionBookTape:
    return ExecutionBookTape(
        schema_version=EXECUTION_BOOK_TAPE_SCHEMA_VERSION,
        tape_id="",
        instrument_id="btc-usd",
        source_capability=capability,
        reconstruction_version="fixture-reconstruction.v1",
        replay_fingerprint="fixture-replay-hash",
        replay_certified=True,
        snapshots=(_snapshot(1, known_offset=1), _snapshot(2, known_offset=3, asks=(("100.5", "5"), ("101", "2")))),
        limitations=("aggregated_depth_only",),
    )


def _context(*, capability: str = "l2"):
    assumptions = _assumptions()
    instrument = _instrument(capability=capability)
    profile = compile_series_execution_profile(instrument)
    return resolve_execution_context(
        profile,
        assumptions,
        instrument_payload=instrument,
        execution_model_artifact=execution_model_artifact_from_book_tape(
            assumptions,
            source_capability=capability,
        ),
        source="phase3a-test",
    )


def _order(
    *,
    side: str = "buy",
    qty: float = 2.5,
    price: float = 105.0,
    order_type: str = "market",
    tif: str = "gtc",
    arrival_offset: int = 2,
) -> FillOrder:
    context = _context()
    role = context.venue.liquidity_role(order_type)
    return FillOrder(
        side=side,
        requested_qty=qty,
        price=price,
        order_type=order_type,
        liquidity_role=role,
        price_source="requested_price",
        fee_rate=context.fee_schedule.taker_rate if role == "taker" else context.fee_schedule.maker_rate,
        fee_source=context.fee_schedule.source,
        fee_version=context.fee_schedule.version,
        time_in_force=tif,
        post_only=order_type == "limit_maker",
        execution_context=context,
        metadata={
            "order_id": "order-1",
            "order_request_id": "order-1",
            "arrival_at": BASE + timedelta(seconds=arrival_offset),
        },
    )


def test_snapshot_and_tape_hashes_detect_tampering_and_bundle_round_trips() -> None:
    tape = _tape()
    restored = ExecutionBookTape.from_dict(tape.to_dict())
    assert restored == tape
    bundle = ExecutionBookTapeBundle(
        schema_version=EXECUTION_BOOK_TAPE_BUNDLE_SCHEMA_VERSION,
        tapes=(tape,),
    )
    assert ExecutionBookTapeBundle.from_dict(bundle.to_dict()) == bundle

    tampered = deepcopy(tape.to_dict())
    tampered["snapshots"][0]["asks"][0]["quantity"] = "999"
    with pytest.raises(ValueError, match="execution_book_snapshot_hash_mismatch"):
        ExecutionBookTape.from_dict(tampered)


def test_arrival_selection_is_causal_and_prefix_invariant() -> None:
    tape = _tape()
    first = tape.select_at(BASE + timedelta(seconds=2))
    assert first.source_reference.receive_ordinal == 1
    assert first.known_at <= (BASE + timedelta(seconds=2)).isoformat()

    prefix = ExecutionBookTape(
        schema_version=EXECUTION_BOOK_TAPE_SCHEMA_VERSION,
        tape_id="",
        instrument_id=tape.instrument_id,
        source_capability=tape.source_capability,
        reconstruction_version=tape.reconstruction_version,
        replay_fingerprint="prefix-replay",
        replay_certified=True,
        snapshots=(tape.snapshots[0],),
    )
    assert prefix.select_at(BASE + timedelta(seconds=2)).snapshot_hash == first.snapshot_hash
    with pytest.raises(LookupError, match="unavailable"):
        tape.select_at(BASE)


def test_buy_walk_consumes_exact_asks_with_price_improvement_and_per_level_fees() -> None:
    order = _order(qty=2.5, price=105.0, order_type="limit_aggressive", tif="gtc")
    model = BookExecutionModel(execution_context=order.execution_context, tape=_tape())
    batch = model.execute_order_batch(order)

    assert batch.status == "filled"
    assert batch.remaining_qty == 0.0
    assert [row.fill_price for row in batch.level_fills] == [101.0, 102.0]
    assert [row.filled_qty for row in batch.level_fills] == [1.0, 1.5]
    assert batch.fill is not None
    assert batch.fill.fill_price == pytest.approx((101.0 + 102.0 * 1.5) / 2.5)
    assert batch.fill.metadata["consumed_qty"] == 2.5
    assert batch.fill.metadata["eligible_visible_depth"] == 7.0
    assert all(row.metadata["price_improvement"] > 0 for row in batch.level_fills)
    assert sum(row.filled_qty for row in batch.level_fills) <= batch.fill.metadata["eligible_visible_depth"]


def test_sell_walk_consumes_best_bids_in_descending_order() -> None:
    order = _order(side="sell", qty=4.0, price=95.0, order_type="limit_aggressive", tif="gtc")
    batch = BookExecutionModel(execution_context=order.execution_context, tape=_tape()).execute_order_batch(order)

    assert [row.fill_price for row in batch.level_fills] == [100.0, 99.0]
    assert [row.filled_qty for row in batch.level_fills] == [2.0, 2.0]
    assert batch.status == "filled"


def test_marketable_limit_never_walks_beyond_its_price() -> None:
    order = _order(qty=5.0, price=101.0, order_type="limit_aggressive", tif="gtc")
    batch = BookExecutionModel(execution_context=order.execution_context, tape=_tape()).execute_order_batch(order)

    assert [row.fill_price for row in batch.level_fills] == [101.0]
    assert batch.remaining_qty == 4.0
    assert batch.residual_disposition == "open"
    assert batch.status == "partially_filled"


def test_resting_limit_never_crosses_or_receives_maker_fees_in_phase_3a() -> None:
    order = _order(qty=1.0, price=105.0, order_type="limit_resting", tif="gtc")
    model = BookExecutionModel(execution_context=order.execution_context, tape=_tape())

    batch = model.execute_order_batch(order)

    assert batch.fill is None
    assert batch.level_fills == ()
    assert batch.status == "open"
    assert batch.residual_disposition == "open"
    assert "resting_order_execution_not_admitted" in batch.evidence["limitations"]

    lifecycle_result = execute_fill_order_with_lifecycle(model, order)
    assert lifecycle_result.lifecycle.snapshot().state is CanonicalOrderState.OPEN
    assert lifecycle_result.lifecycle.events[-1].metadata[
        "execution_book_snapshot_hash"
    ] == batch.evidence["execution_book_snapshot_hash"]


def test_x3_spread_model_never_consumes_depth_beyond_top_of_book() -> None:
    base_order = _order(qty=2.5, order_type="market", tif="ioc")
    context = _context(capability="l1")
    order = replace(base_order, execution_context=context)
    model = BookExecutionModel(
        execution_context=context,
        tape=_tape(capability="l1"),
    )

    batch = model.execute_order_batch(order)

    assert context.model.execution_quality_ceiling == "X3"
    assert [fill.fill_price for fill in batch.level_fills] == [101.0]
    assert batch.fill is not None and batch.fill.filled_qty == 1.0
    assert batch.remaining_qty == 1.5
    assert batch.residual_disposition == "canceled"


def test_fok_ioc_and_gtc_residual_dispositions_are_explicit() -> None:
    tape = _tape()

    fok_order = _order(qty=8.0, tif="fok")
    fok = BookExecutionModel(execution_context=fok_order.execution_context, tape=tape).execute_order_batch(fok_order)
    assert fok.status == "canceled"
    assert fok.level_fills == ()
    assert fok.evidence["block_reason"] == "FOK_VISIBLE_DEPTH_INSUFFICIENT"

    ioc_order = _order(qty=8.0, tif="ioc")
    ioc = BookExecutionModel(execution_context=ioc_order.execution_context, tape=tape).execute_order_batch(ioc_order)
    assert ioc.status == "partially_filled"
    assert ioc.residual_disposition == "canceled"
    assert ioc.remaining_qty == 1.0

    gtc_order = _order(qty=8.0, order_type="limit_aggressive", price=105.0, tif="gtc")
    gtc = BookExecutionModel(execution_context=gtc_order.execution_context, tape=tape).execute_order_batch(gtc_order)
    assert gtc.status == "partially_filled"
    assert gtc.residual_disposition == "open"
    assert gtc.remaining_qty == 1.0


def test_gtc_residual_can_fill_later_against_a_new_causal_snapshot() -> None:
    tape = _tape()
    first_order = _order(
        qty=8.0,
        price=105.0,
        order_type="limit_aggressive",
        tif="gtc",
        arrival_offset=2,
    )
    model = BookExecutionModel(
        execution_context=first_order.execution_context,
        tape=tape,
    )

    first = execute_fill_order_with_lifecycle(model, first_order)
    assert first.lifecycle.snapshot().state is CanonicalOrderState.PARTIALLY_FILLED
    assert first.lifecycle.snapshot().remaining_qty == 1.0
    assert first.fill is not None and first.fill.filled_qty == 7.0

    later_order = _order(
        qty=8.0,
        price=105.0,
        order_type="limit_aggressive",
        tif="gtc",
        arrival_offset=4,
    )
    completed = execute_fill_order_with_lifecycle(
        model,
        later_order,
        lifecycle=first.lifecycle,
    )

    assert completed.lifecycle.snapshot().state is CanonicalOrderState.FILLED
    assert completed.lifecycle.snapshot().remaining_qty == 0.0
    assert completed.fill is not None and completed.fill.filled_qty == 1.0
    assert completed.fill.fill_price == 100.5
    replayed = type(completed.lifecycle).replay(
        request=completed.lifecycle.request,
        attempts=completed.lifecycle.attempts,
        events=completed.lifecycle.events,
    )
    assert replayed.snapshot().to_dict() == completed.lifecycle.snapshot().to_dict()


def test_future_book_state_cannot_affect_earlier_arrival_fill() -> None:
    order = _order(qty=1.0, arrival_offset=2)
    full = BookExecutionModel(execution_context=order.execution_context, tape=_tape()).execute_order_batch(order)
    prefix_tape = ExecutionBookTape(
        schema_version=EXECUTION_BOOK_TAPE_SCHEMA_VERSION,
        tape_id="",
        instrument_id="btc-usd",
        source_capability="l2",
        reconstruction_version="fixture-reconstruction.v1",
        replay_fingerprint="prefix",
        replay_certified=True,
        snapshots=(_tape().snapshots[0],),
    )
    prefix = BookExecutionModel(execution_context=order.execution_context, tape=prefix_tape).execute_order_batch(order)

    assert full.fill is not None and prefix.fill is not None
    assert full.fill.fill_price == prefix.fill.fill_price == 101.0
    assert full.level_fills[0].metadata["execution_book_snapshot_hash"] == prefix.level_fills[0].metadata["execution_book_snapshot_hash"]


def test_l2_levels_are_individual_lifecycle_fills_and_ioc_residual_cancels() -> None:
    order = _order(qty=8.0, tif="ioc")
    model = BookExecutionModel(execution_context=order.execution_context, tape=_tape())
    result = execute_fill_order_with_lifecycle(model, order)

    snapshot = result.lifecycle.snapshot()
    fill_events = [event for event in result.lifecycle.events if event.fill_id]
    assert [event.fill_qty for event in fill_events] == [1.0, 2.0, 4.0]
    assert [event.fill_price for event in fill_events] == [101.0, 102.0, 103.0]
    assert snapshot.cumulative_filled_qty == 7.0
    assert snapshot.remaining_qty == 1.0
    assert snapshot.state is CanonicalOrderState.CANCELED
    assert result.fill is not None
    assert result.fill.metadata["order_fill_ids"] == [event.fill_id for event in fill_events]
    assert all(event.metadata["execution_book_snapshot_hash"] for event in fill_events)


def test_fok_insufficient_depth_cancels_without_any_lifecycle_fill() -> None:
    order = _order(qty=8.0, tif="fok")
    model = BookExecutionModel(execution_context=order.execution_context, tape=_tape())
    result = execute_fill_order_with_lifecycle(model, order)

    assert result.fill is None
    assert result.rejection is None
    assert result.lifecycle.snapshot().state is CanonicalOrderState.CANCELED
    assert result.lifecycle.snapshot().fill_count == 0
    assert result.lifecycle.events[-1].reason == "FOK_VISIBLE_DEPTH_INSUFFICIENT"


def test_arrival_during_closed_validity_interval_fails_closed() -> None:
    base_tape = _tape()
    first = base_tape.snapshots[0]
    closure = ExecutionBookValidityClosure(
        validity_interval_id=first.validity_interval_id,
        status="closed_invalidated",
        known_at=BASE + timedelta(seconds=2),
        reason="sequence_gap",
        source_reference=first.source_reference,
        evidence_hash="gap-evidence-hash",
    )
    tape = ExecutionBookTape(
        schema_version=EXECUTION_BOOK_TAPE_SCHEMA_VERSION,
        tape_id="",
        instrument_id="btc-usd",
        source_capability="l2",
        reconstruction_version="fixture-reconstruction.v1",
        replay_fingerprint="gap-replay",
        replay_certified=True,
        snapshots=base_tape.snapshots,
        validity_closures=(closure,),
    )
    with pytest.raises(LookupError, match="sequence_gap"):
        tape.select_at(BASE + timedelta(seconds=2, milliseconds=1))

    order = _order(qty=1.0, arrival_offset=2)
    order = FillOrder(
        **{
            **order.__dict__,
            "metadata": {
                **dict(order.metadata or {}),
                "arrival_at": BASE + timedelta(seconds=2, milliseconds=1),
            },
        }
    )
    result = BookExecutionModel(
        execution_context=order.execution_context,
        tape=tape,
    ).execute_order_batch(order)
    assert result.rejection is not None
    assert result.rejection.reason == "BOOK_STATE_INVALID_AT_ARRIVAL"
