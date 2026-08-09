from __future__ import annotations

from dataclasses import replace

import pytest

from engines.bot_runtime.core.order_lifecycle import (
    CanonicalOrderLifecycle,
    CanonicalOrderRequest,
    CanonicalOrderState,
    OrderLifecycleCommand,
    build_initial_order_attempt,
    build_replacement_order_attempt,
    execution_policy_hash,
    lifecycle_from_dict,
)


KNOWN_AT = "2026-08-05T12:00:00Z"


def _request(*, quantity: float = 10.0) -> CanonicalOrderRequest:
    policy_hash = execution_policy_hash(
        order_type="limit_resting",
        time_in_force="gtc",
        post_only=True,
        liquidity_role="maker",
        price_source="limit_price",
    )
    return CanonicalOrderRequest(
        request_id="order-request-1",
        run_id="run-1",
        bot_id="bot-1",
        strategy_id="strategy-1",
        instrument_id="instrument-1",
        symbol="BTC-USD",
        side="buy",
        requested_qty=quantity,
        requested_price=100.0,
        order_type="limit_resting",
        time_in_force="gtc",
        post_only=True,
        liquidity_role="maker",
        price_source="limit_price",
        execution_context_hash="context-hash-1",
        execution_policy_hash=policy_hash,
        known_at=KNOWN_AT,
        signal_id="signal-1",
        decision_id="decision-1",
        trade_id="trade-1",
    )


def _open_lifecycle(*, quantity: float = 10.0) -> CanonicalOrderLifecycle:
    request = _request(quantity=quantity)
    attempt = build_initial_order_attempt(request, attempt_id="attempt-1")
    lifecycle = CanonicalOrderLifecycle.create(request, attempt)
    lifecycle.transition(
        attempt_id=attempt.attempt_id,
        state=CanonicalOrderState.VALIDATED,
        known_at=KNOWN_AT,
    )
    lifecycle.transition(
        attempt_id=attempt.attempt_id,
        state=CanonicalOrderState.ACCEPTED,
        known_at=KNOWN_AT,
    )
    lifecycle.transition(
        attempt_id=attempt.attempt_id,
        state=CanonicalOrderState.OPEN,
        known_at=KNOWN_AT,
    )
    return lifecycle


def test_lifecycle_replay_is_equal_and_hash_stable() -> None:
    lifecycle = _open_lifecycle()
    lifecycle.record_fill(
        attempt_id="attempt-1",
        fill_id="fill-1",
        fill_qty=10.0,
        fill_price=99.5,
        fill_fee=0.25,
        known_at="2026-08-05T12:00:01Z",
    )

    replayed = lifecycle_from_dict(lifecycle.to_dict())

    assert replayed.snapshot() == lifecycle.snapshot()
    assert replayed.events == lifecycle.events
    assert replayed.replay_hash == lifecycle.replay_hash
    assert replayed.snapshot().state == CanonicalOrderState.FILLED
    assert replayed.snapshot().remaining_qty == 0.0


def test_illegal_transition_fails_closed() -> None:
    request = _request()
    attempt = build_initial_order_attempt(request, attempt_id="attempt-1")
    lifecycle = CanonicalOrderLifecycle.create(request, attempt)

    with pytest.raises(ValueError, match="illegal_order_transition"):
        lifecycle.transition(
            attempt_id="attempt-1",
            state=CanonicalOrderState.FILLED,
            known_at=KNOWN_AT,
            fill_id="fill-1",
            fill_qty=10.0,
            fill_price=100.0,
            fill_fee=0.1,
        )


def test_duplicate_event_and_fill_are_idempotent_but_divergence_fails() -> None:
    lifecycle = _open_lifecycle()
    fill = lifecycle.record_fill(
        attempt_id="attempt-1",
        fill_id="fill-1",
        fill_qty=3.0,
        fill_price=100.0,
        fill_fee=0.03,
        known_at="2026-08-05T12:00:01Z",
    )
    event_count = len(lifecycle.events)

    assert lifecycle.append(fill) is False
    duplicate_fill = replace(
        fill,
        event_id="another-event-id",
        order_event_seq=event_count + 1,
    )
    assert lifecycle.append(duplicate_fill) is False
    assert len(lifecycle.events) == event_count

    with pytest.raises(ValueError, match="order_fill_identity_reused_with_different_material"):
        lifecycle.append(replace(duplicate_fill, fill_qty=2.0))


def test_partial_fill_replacement_preserves_exact_residual_and_lineage() -> None:
    lifecycle = _open_lifecycle()
    lifecycle.record_fill(
        attempt_id="attempt-1",
        fill_id="fill-1",
        fill_qty=4.0,
        fill_price=100.0,
        fill_fee=0.04,
        known_at="2026-08-05T12:00:01Z",
    )
    replacement = build_replacement_order_attempt(
        lifecycle,
        predecessor_attempt_id="attempt-1",
        requested_price=101.0,
        known_at="2026-08-05T12:00:02Z",
        order_type="market",
        post_only=False,
        liquidity_role="taker",
        reason="bounded_chase",
        attempt_id="attempt-2",
    )
    lifecycle.replace(
        attempt_id="attempt-1",
        replacement_attempt=replacement,
        known_at="2026-08-05T12:00:02Z",
        reason="bounded_chase",
    )
    lifecycle.transition(
        attempt_id="attempt-2",
        state=CanonicalOrderState.VALIDATED,
        known_at="2026-08-05T12:00:02Z",
    )
    lifecycle.transition(
        attempt_id="attempt-2",
        state=CanonicalOrderState.ACCEPTED,
        known_at="2026-08-05T12:00:02Z",
    )
    lifecycle.record_fill(
        attempt_id="attempt-2",
        fill_id="fill-2",
        fill_qty=6.0,
        fill_price=101.0,
        fill_fee=0.06,
        known_at="2026-08-05T12:00:03Z",
    )

    snapshot = lifecycle.snapshot()
    assert replacement.replaces_attempt_id == "attempt-1"
    assert replacement.requested_qty == 6.0
    assert [item.state for item in snapshot.attempts] == [
        CanonicalOrderState.REPLACED,
        CanonicalOrderState.FILLED,
    ]
    assert snapshot.cumulative_filled_qty == 10.0
    assert snapshot.remaining_qty == 0.0


def test_replacement_with_non_residual_quantity_fails_closed() -> None:
    lifecycle = _open_lifecycle()
    lifecycle.record_fill(
        attempt_id="attempt-1",
        fill_id="fill-1",
        fill_qty=4.0,
        fill_price=100.0,
        fill_fee=0.04,
        known_at="2026-08-05T12:00:01Z",
    )
    replacement = build_replacement_order_attempt(
        lifecycle,
        predecessor_attempt_id="attempt-1",
        requested_price=101.0,
        known_at="2026-08-05T12:00:02Z",
        reason="replace",
        attempt_id="attempt-2",
    )
    replacement = replace(replacement, requested_qty=5.0)

    with pytest.raises(ValueError, match="replacement_attempt_quantity_must_equal_predecessor_residual"):
        lifecycle.replace(
            attempt_id="attempt-1",
            replacement_attempt=replacement,
            known_at="2026-08-05T12:00:02Z",
            reason="replace",
        )
    assert [attempt.attempt_id for attempt in lifecycle.attempts] == ["attempt-1"]
    assert lifecycle.snapshot().state == CanonicalOrderState.PARTIALLY_FILLED


def test_fill_wins_full_fill_cancel_race_independent_of_input_order() -> None:
    fill_command = OrderLifecycleCommand(
        state=CanonicalOrderState.FILLED,
        known_at="2026-08-05T12:00:01Z",
        source_sequence=7,
        fill_id="fill-1",
        fill_qty=10.0,
        fill_price=100.0,
        fill_fee=0.1,
    )
    cancel_command = OrderLifecycleCommand(
        state=CanonicalOrderState.CANCELED,
        known_at="2026-08-05T12:00:01Z",
        source_sequence=7,
        reason="cancel_ack",
    )

    first = _open_lifecycle()
    first_resolution = first.apply_competing(
        attempt_id="attempt-1",
        commands=[cancel_command, fill_command],
    )
    second = _open_lifecycle()
    second_resolution = second.apply_competing(
        attempt_id="attempt-1",
        commands=[fill_command, cancel_command],
    )

    assert first.snapshot().state == CanonicalOrderState.FILLED
    assert second.snapshot().state == CanonicalOrderState.FILLED
    assert first.replay_hash == second.replay_hash
    assert first_resolution.suppressed == second_resolution.suppressed
    assert first_resolution.suppressed[0]["state"] == "canceled"


def test_partial_fill_precedes_replace_and_replacement_uses_post_fill_residual() -> None:
    lifecycle = _open_lifecycle()
    replacement = build_replacement_order_attempt(
        lifecycle,
        predecessor_attempt_id="attempt-1",
        requested_price=101.0,
        known_at="2026-08-05T12:00:01Z",
        reason="replace_ack",
        attempt_id="attempt-2",
    )
    # A replacement manifest created before the competing fill has stale size.
    # The authority must reject it after applying the conservative fill-first rule.
    with pytest.raises(ValueError, match="replacement_attempt_quantity_must_equal_predecessor_residual"):
        lifecycle.apply_competing(
            attempt_id="attempt-1",
            commands=[
                OrderLifecycleCommand(
                    state=CanonicalOrderState.REPLACED,
                    known_at="2026-08-05T12:00:01Z",
                    source_sequence=9,
                    reason="replace_ack",
                    replacement_attempt=replacement,
                ),
                OrderLifecycleCommand(
                    state=CanonicalOrderState.PARTIALLY_FILLED,
                    known_at="2026-08-05T12:00:01Z",
                    source_sequence=9,
                    fill_id="fill-1",
                    fill_qty=3.0,
                    fill_price=100.0,
                    fill_fee=0.03,
                ),
            ],
        )
    assert lifecycle.snapshot().cumulative_filled_qty == 3.0
    assert lifecycle.attempt_snapshot("attempt-1").state == CanonicalOrderState.PARTIALLY_FILLED


def test_context_binding_cannot_change_during_replay() -> None:
    lifecycle = _open_lifecycle()
    event = lifecycle.record_fill(
        attempt_id="attempt-1",
        fill_id="fill-1",
        fill_qty=10.0,
        fill_price=100.0,
        fill_fee=0.1,
        known_at="2026-08-05T12:00:01Z",
    )
    request = lifecycle.request
    attempt = lifecycle.attempts[0]
    tampered = replace(event, execution_context_hash="other-context")

    with pytest.raises(ValueError, match="order_event_execution_context_hash_mismatch"):
        CanonicalOrderLifecycle.replay(
            request=request,
            attempts=[attempt],
            events=[*lifecycle.events[:-1], tampered],
        )
