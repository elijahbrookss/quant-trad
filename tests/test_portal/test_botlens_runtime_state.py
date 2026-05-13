from __future__ import annotations

import pytest

from portal.backend.service.bots.botlens_runtime_state import (
    BotLensRuntimeState,
    InvalidRuntimeStateTransition,
    guard_runtime_state_transition,
    is_startup_bootstrap_state,
    normalize_runtime_state,
    startup_bootstrap_admission,
)
from portal.backend.service.bots.botlens_state import (
    ProjectionBatch,
    _build_run_health_state,
    apply_overlay_delta,
    apply_run_batch,
    apply_symbol_batch,
    empty_run_projection_snapshot,
    empty_run_health_state,
    empty_symbol_projection_snapshot,
    is_open_trade,
    read_run_projection_snapshot,
    read_symbol_projection_snapshot,
    serialize_symbol_projection_snapshot,
)
from portal.backend.service.bots.botlens_domain_events import (
    build_botlens_domain_events_from_fact_batch,
    deserialize_botlens_domain_event,
)
from portal.backend.service.bots.botlens_transport import selected_symbol_snapshot_contract


def test_runtime_state_machine_blocks_live_back_to_awaiting_first_snapshot() -> None:
    with pytest.raises(InvalidRuntimeStateTransition, match="live -> awaiting_first_snapshot"):
        guard_runtime_state_transition(
            current_state=BotLensRuntimeState.LIVE,
            next_state=BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT,
            transition_reason="illegal_regression",
            source_component="test",
        )


def test_runtime_state_machine_allows_degraded_recovery_back_to_live() -> None:
    transition = guard_runtime_state_transition(
        current_state=BotLensRuntimeState.DEGRADED,
        next_state=BotLensRuntimeState.LIVE,
        transition_reason="continuity_recovered",
        source_component="test",
    )

    assert transition.from_state == BotLensRuntimeState.DEGRADED.value
    assert transition.to_state == BotLensRuntimeState.LIVE.value


def test_startup_bootstrap_is_allowed_only_in_startup_states() -> None:
    assert is_startup_bootstrap_state(BotLensRuntimeState.INITIALIZING) is True
    assert is_startup_bootstrap_state(BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT) is True
    assert is_startup_bootstrap_state(BotLensRuntimeState.LIVE) is False
    assert is_startup_bootstrap_state(BotLensRuntimeState.DEGRADED) is False


def test_startup_bootstrap_admission_infers_startup_from_lifecycle_phase_when_health_not_projected() -> None:
    admission = startup_bootstrap_admission(runtime_state=None, lifecycle_phase="waiting_for_series_bootstrap", projection_seq=4)

    assert admission.allowed is True
    assert admission.runtime_state == BotLensRuntimeState.INITIALIZING.value


def test_startup_bootstrap_admission_rejects_live_phase_without_runtime_state() -> None:
    admission = startup_bootstrap_admission(runtime_state=None, lifecycle_phase="live", projection_seq=12)

    assert admission.allowed is False
    assert admission.runtime_state == BotLensRuntimeState.LIVE.value


def test_stopping_is_not_part_of_public_runtime_state_contract() -> None:
    with pytest.raises(ValueError, match="unsupported runtime state"):
        normalize_runtime_state("stopping")


def test_read_run_projection_snapshot_treats_null_recent_transitions_as_empty_history() -> None:
    snapshot = read_run_projection_snapshot(
        {
            "projection": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "seq": 3,
                "concerns": {
                    "health": {
                        "status": "running",
                        "runtime_state": "live",
                        "recent_transitions": None,
                    }
                },
            }
        },
        bot_id="bot-1",
        run_id="run-1",
    )

    assert snapshot.health.status == "running"
    assert snapshot.health.runtime_state == "live"
    assert snapshot.health.recent_transitions == ()


def test_read_run_projection_snapshot_keeps_only_canonical_active_warning_conditions() -> None:
    snapshot = read_run_projection_snapshot(
        {
            "projection": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "seq": 3,
                "concerns": {
                    "health": {
                        "status": "running",
                        "warning_count": 2,
                        "last_event_at": "2026-01-01T00:00:05Z",
                        "warnings": [
                            {
                                "warning_id": "indicator_budget::typed_regime::instrument-btc|1m",
                                "warning_type": "indicator_budget",
                                "indicator_id": "typed_regime",
                                "message": "Execution budget exceeded.",
                                "count": 4,
                                "first_seen_at": "2026-01-01T00:00:00Z",
                                "last_seen_at": "2026-01-01T00:00:04Z",
                            },
                            {
                                "id": "legacy-warning-id",
                                "warning_type": "indicator_budget",
                                "message": "legacy warning shape",
                            },
                        ],
                    }
                },
            }
        },
        bot_id="bot-1",
        run_id="run-1",
    )

    assert snapshot.health.warning_count == 1
    assert len(snapshot.health.warnings) == 1
    assert snapshot.health.warnings[0]["warning_id"] == "indicator_budget::typed_regime::instrument-btc|1m"
    assert snapshot.health.warnings[0]["count"] == 4


def test_build_run_health_state_coalesces_repeated_warning_conditions() -> None:
    base = empty_run_health_state()

    first = _build_run_health_state(
        base,
        status="running",
        warning_count=1,
        warnings=[
            {
                "warning_id": "indicator_budget::typed_regime::instrument-btc|1m",
                "warning_type": "indicator_budget",
                "indicator_id": "typed_regime",
                "message": "Execution budget exceeded.",
            }
        ],
        last_event_at="2026-01-01T00:00:00Z",
    )
    second = _build_run_health_state(
        first,
        status="running",
        warning_count=1,
        warnings=[
            {
                "warning_id": "indicator_budget::typed_regime::instrument-btc|1m",
                "warning_type": "indicator_budget",
                "indicator_id": "typed_regime",
                "message": "Execution budget exceeded.",
            }
        ],
        last_event_at="2026-01-01T00:00:05Z",
    )

    assert second.warning_count == 1
    assert len(second.warnings) == 1
    assert second.warnings[0]["count"] == 2
    assert second.warnings[0]["first_seen_at"] == "2026-01-01T00:00:00Z"
    assert second.warnings[0]["last_seen_at"] == "2026-01-01T00:00:05Z"
    assert second.warning_types == ("indicator_budget",)
    assert second.highest_warning_severity == "warning"


def test_read_run_projection_snapshot_preserves_compact_health_warning_summary_fields() -> None:
    snapshot = read_run_projection_snapshot(
        {
            "projection": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "seq": 3,
                "concerns": {
                    "health": {
                        "status": "degraded",
                        "warning_count": 3,
                        "warning_types": ["indicator_overlay_payload_exceeded", "indicator_time_budget_exceeded"],
                        "highest_warning_severity": "error",
                        "warnings": [],
                    }
                },
            }
        },
        bot_id="bot-1",
        run_id="run-1",
    )

    assert snapshot.health.warning_count == 3
    assert snapshot.health.warning_types == (
        "indicator_overlay_payload_exceeded",
        "indicator_time_budget_exceeded",
    )
    assert snapshot.health.highest_warning_severity == "error"


def test_read_run_projection_snapshot_rejects_lifecycle_status_phase_mismatch() -> None:
    with pytest.raises(RuntimeError, match="lifecycle status does not match phase"):
        read_run_projection_snapshot(
            {
                "projection": {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "seq": 3,
                    "concerns": {
                        "lifecycle": {
                            "run_id": "run-1",
                            "phase": "completed",
                            "status": "running",
                        }
                    },
                }
            },
            bot_id="bot-1",
            run_id="run-1",
        )


def test_read_run_projection_snapshot_rejects_completed_run_with_open_trades() -> None:
    with pytest.raises(RuntimeError, match="completed run retains open trades"):
        read_run_projection_snapshot(
            {
                "projection": {
                    "run_id": "run-1",
                    "bot_id": "bot-1",
                    "seq": 3,
                    "concerns": {
                        "lifecycle": {
                            "run_id": "run-1",
                            "phase": "completed",
                            "status": "completed",
                        },
                        "open_trades": {
                            "entries": {
                                "trade-1": {
                                    "trade_id": "trade-1",
                                    "trade_state": "open",
                                }
                            }
                        },
                    },
                }
            },
            bot_id="bot-1",
            run_id="run-1",
        )


def test_is_open_trade_rejects_closed_status_without_closing_fields() -> None:
    with pytest.raises(RuntimeError, match="closed trade missing closing event fields"):
        is_open_trade(
            {
                "trade_id": "trade-1",
                "status": "closed",
            }
        )


def test_run_open_trades_close_dominates_same_seq_open_reorder() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_closed",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "series_key": "instr-1|1m",
                        "status": "closed",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "closed_at": "2026-02-01T00:00:00Z",
                        "exit_time": "2026-02-01T00:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "exit_price": 97.5,
                        "stop_price": 97.5,
                        "reason_code": "STOP",
                        "position_commit_seq": 2,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "series_key": "instr-1|1m",
                        "status": "open",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "stop_price": 97.5,
                        "position_commit_seq": 1,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
            ],
        },
    )
    snapshot, deltas = apply_run_batch(
        empty_run_projection_snapshot(bot_id="bot-1", run_id="run-1"),
        batch=ProjectionBatch(
            batch_kind="ledger_rebuild",
            run_id="run-1",
            bot_id="bot-1",
            seq=10,
            event_time="2026-02-01T00:00:00Z",
            known_at="2026-02-01T00:00:00Z",
            symbol_key=None,
            bridge_session_id=None,
            events=tuple(events),
        ),
    )

    assert snapshot.open_trades.entries == {}
    assert not [delta for delta in deltas if getattr(delta, "upserts", ())]


def test_run_open_trades_position_clock_blocks_late_open_after_close_batch() -> None:
    close_events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_closed",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "series_key": "instr-1|1m",
                        "status": "closed",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "closed_at": "2026-02-01T00:00:00Z",
                        "exit_time": "2026-02-01T00:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "exit_price": 97.5,
                        "stop_price": 97.5,
                        "reason_code": "STOP",
                        "position_commit_seq": 2,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
            ],
        },
    )
    late_open_events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:01Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "series_key": "instr-1|1m",
                        "status": "open",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "stop_price": 97.5,
                        "position_commit_seq": 1,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
            ],
        },
    )

    snapshot, _ = apply_run_batch(
        empty_run_projection_snapshot(bot_id="bot-1", run_id="run-1"),
        batch=ProjectionBatch(
            batch_kind="ledger_rebuild",
            run_id="run-1",
            bot_id="bot-1",
            seq=2,
            event_time="2026-02-01T00:00:00Z",
            known_at="2026-02-01T00:00:00Z",
            symbol_key=None,
            bridge_session_id=None,
            events=tuple(close_events),
        ),
    )
    snapshot, deltas = apply_run_batch(
        snapshot,
        batch=ProjectionBatch(
            batch_kind="ledger_rebuild",
            run_id="run-1",
            bot_id="bot-1",
            seq=3,
            event_time="2026-02-01T00:00:01Z",
            known_at="2026-02-01T00:00:01Z",
            symbol_key=None,
            bridge_session_id=None,
            events=tuple(late_open_events),
        ),
    )

    assert snapshot.open_trades.entries == {}
    assert snapshot.open_trades.closed_trades["trade-1"]["position_commit_seq"] == 2
    assert not [delta for delta in deltas if getattr(delta, "upserts", ())]


def test_run_open_trades_close_removes_existing_even_when_same_seq_open_arrives_late() -> None:
    opened_events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "open",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "stop_price": 97.5,
                        "position_commit_seq": 1,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
            ],
        },
    )
    close_then_open_events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T00:01:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_closed",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "closed",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "closed_at": "2026-02-01T00:00:00Z",
                        "exit_time": "2026-02-01T00:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "exit_price": 97.5,
                        "stop_price": 97.5,
                        "reason_code": "STOP",
                        "position_commit_seq": 2,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "open",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "stop_price": 97.5,
                        "position_commit_seq": 1,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
            ],
        },
    )

    snapshot, _ = apply_run_batch(
        empty_run_projection_snapshot(bot_id="bot-1", run_id="run-1"),
        batch=ProjectionBatch(
            batch_kind="ledger_rebuild",
            run_id="run-1",
            bot_id="bot-1",
            seq=9,
            event_time="2026-02-01T00:00:00Z",
            known_at="2026-02-01T00:00:00Z",
            symbol_key=None,
            bridge_session_id=None,
            events=tuple(opened_events),
        ),
    )
    snapshot, deltas = apply_run_batch(
        snapshot,
        batch=ProjectionBatch(
            batch_kind="ledger_rebuild",
            run_id="run-1",
            bot_id="bot-1",
            seq=10,
            event_time="2026-02-01T00:01:00Z",
            known_at="2026-02-01T00:01:00Z",
            symbol_key=None,
            bridge_session_id=None,
            events=tuple(close_then_open_events),
        ),
    )

    open_trade_deltas = [delta for delta in deltas if hasattr(delta, "removals")]
    assert snapshot.open_trades.entries == {}
    assert open_trade_deltas[-1].removals == ("trade-1",)


def test_apply_symbol_batch_accepts_durable_overlay_summary_without_geometry() -> None:
    event = deserialize_botlens_domain_event(
        {
            "schema_version": 1,
            "event_id": "evt-overlay",
            "event_ts": "2026-02-01T00:00:00Z",
            "event_name": "OVERLAY_STATE_CHANGED",
            "root_id": "evt-overlay",
            "parent_id": None,
            "correlation_id": "corr-overlay",
            "context": {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "series_key": "instr-1|1m",
                "overlay_delta": {
                    "overlay_commit_seq": 9,
                    "base_overlay_commit_seq": 8,
                    "overlay_commit_seq_status": "overlay_scoped",
                    "op_counts": {"upsert": 1},
                    "point_count": 12,
                    "ops": [
                        {
                            "op": "upsert",
                            "key": "overlay-1",
                            "overlay": {
                                "overlay_id": "overlay-1",
                                "type": "regime_overlay",
                                "pane_key": "volatility",
                                "pane_views": ["polyline"],
                                "detail_level": "summary",
                                "payload_summary": {
                                    "geometry_keys": ["polylines"],
                                    "payload_counts": {"polylines": 1},
                                    "point_count": 12,
                                },
                            },
                        }
                    ],
                },
            },
        }
    )
    snapshot, deltas = apply_symbol_batch(
        empty_symbol_projection_snapshot("instr-1|1m"),
        batch=ProjectionBatch(
            batch_kind="ledger_rebuild",
            run_id="run-1",
            bot_id="bot-1",
            seq=9,
            event_time="2026-02-01T00:00:00Z",
            known_at="2026-02-01T00:00:00Z",
            symbol_key="instr-1|1m",
            bridge_session_id=None,
            events=(event,),
        ),
    )

    assert snapshot.overlays.overlays[0]["detail_level"] == "summary"
    assert snapshot.overlays.overlays[0]["payload_summary"]["point_count"] == 12
    assert snapshot.overlays.overlay_commit_seq == 9
    assert snapshot.overlays.overlay_commit_seq_status == "overlay_scoped"
    assert "payload" not in snapshot.overlays.overlays[0]
    assert deltas[0].overlay_ops["ops"][0]["overlay"]["detail_level"] == "summary"
    assert deltas[0].overlay_ops["overlay_commit_seq"] == 9
    assert deltas[0].overlay_ops["base_overlay_commit_seq"] == 8
    roundtripped = read_symbol_projection_snapshot(
        serialize_symbol_projection_snapshot(snapshot),
        symbol_key="instr-1|1m",
    )
    assert roundtripped.overlays.overlay_commit_seq == 9
    assert roundtripped.overlays.overlay_commit_seq_status == "overlay_scoped"


def test_apply_overlay_delta_rejects_missing_overlay_clock() -> None:
    with pytest.raises(ValueError, match="overlay_commit_seq is required"):
        apply_overlay_delta(
            (),
            {
                "ops": [
                    {
                        "op": "upsert",
                        "key": "overlay-1",
                        "overlay": {"overlay_id": "overlay-1", "type": "regime_overlay"},
                    }
                ]
            },
        )


def test_symbol_trade_delta_retains_closed_trade_visual_payload() -> None:
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T02:00:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_closed",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "closed",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "closed_at": "2026-02-01T02:00:00Z",
                        "exit_time": "2026-02-01T02:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "exit_price": 105.0,
                        "stop_price": 97.5,
                        "gross_pnl": 5.0,
                        "fees_paid": 0.5,
                        "net_pnl": 4.5,
                        "reason_code": "BACKTEST_END",
                        "position_commit_seq": 2,
                        "position_commit_seq_status": "position_scoped",
                        "legs": [
                            {
                                "id": "leg-1",
                                "target_price": 106.0,
                                "exit_time": "2026-02-01T02:00:00Z",
                                "exit_price": 105.0,
                                "status": "backtest_end",
                                "contracts": 1,
                                "pnl": 5.0,
                            }
                        ],
                    },
                },
            ],
        },
    )
    snapshot, deltas = apply_symbol_batch(
        empty_symbol_projection_snapshot("instr-1|1m"),
        batch=ProjectionBatch(
            batch_kind="ledger_rebuild",
            run_id="run-1",
            bot_id="bot-1",
            seq=10,
            event_time="2026-02-01T02:00:00Z",
            known_at="2026-02-01T02:00:00Z",
            symbol_key="instr-1|1m",
            bridge_session_id=None,
            events=tuple(events),
        ),
    )

    trade_delta = next(delta for delta in deltas if hasattr(delta, "trade_upserts"))
    projected = snapshot.trades.trades[-1]

    assert projected["status"] == "closed"
    assert projected["entry_time"] == "2026-02-01T00:00:00Z"
    assert projected["exit_time"] == "2026-02-01T02:00:00Z"
    assert projected["stop_price"] == 97.5
    assert projected["fees_paid"] == 0.5
    assert projected["net_pnl"] == 4.5
    assert projected["close_reason"] == "BACKTEST_END"
    assert projected["legs"][0]["target_price"] == 106.0
    assert trade_delta.trade_upserts[0]["trade_id"] == "trade-1"
    assert trade_delta.trade_upserts[0]["status"] == "closed"
    assert trade_delta.trade_removals == ("trade-1",)


def _closed_trade_projection_with_late_update(*, reason_code: str, exit_price: float = 105.0):
    events = build_botlens_domain_events_from_fact_batch(
        bot_id="bot-1",
        run_id="run-1",
        payload={
            "known_at": "2026-02-01T02:05:00Z",
            "facts": [
                {
                    "fact_type": "series_state_observed",
                    "series_key": "instr-1|1m",
                    "instrument_id": "instr-1",
                    "symbol": "BTC",
                    "timeframe": "1m",
                },
                {
                    "fact_type": "trade_opened",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "open",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "stop_price": 97.5,
                        "position_commit_seq": 1,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
                {
                    "fact_type": "trade_closed",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "closed",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "closed_at": "2026-02-01T02:00:00Z",
                        "exit_time": "2026-02-01T02:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "exit_price": exit_price,
                        "stop_price": 97.5,
                        "gross_pnl": 5.0,
                        "fees_paid": 0.5,
                        "net_pnl": 4.5,
                        "reason_code": reason_code,
                        "position_commit_seq": 2,
                        "position_commit_seq_status": "position_scoped",
                        "legs": [
                            {
                                "id": "leg-1",
                                "exit_time": "2026-02-01T02:00:00Z",
                                "exit_price": exit_price,
                                "status": reason_code.lower(),
                                "contracts": 1,
                                "pnl": 5.0,
                            }
                        ],
                        "metrics": {"bars_held": 5},
                    },
                },
                {
                    "fact_type": "trade_updated",
                    "series_key": "instr-1|1m",
                    "trade": {
                        "trade_id": "trade-1",
                        "status": "closed",
                        "entry_time": "2026-02-01T00:00:00Z",
                        "closed_at": "2026-02-01T02:00:00Z",
                        "direction": "long",
                        "quantity": 1,
                        "entry_price": 100.0,
                        "stop_price": 97.5,
                        "position_commit_seq": 3,
                        "position_commit_seq_status": "position_scoped",
                    },
                },
            ],
        },
    )
    snapshot, deltas = apply_symbol_batch(
        empty_symbol_projection_snapshot("instr-1|1m"),
        batch=ProjectionBatch(
            batch_kind="ledger_rebuild",
            run_id="run-1",
            bot_id="bot-1",
            seq=11,
            event_time="2026-02-01T02:05:00Z",
            known_at="2026-02-01T02:05:00Z",
            symbol_key="instr-1|1m",
            bridge_session_id=None,
            events=tuple(events),
        ),
    )
    trade_deltas = [delta for delta in deltas if hasattr(delta, "trade_upserts")]
    return snapshot, trade_deltas


def test_projection_preserves_closed_trade_fields_after_late_trade_update() -> None:
    snapshot, trade_deltas = _closed_trade_projection_with_late_update(reason_code="TARGET")
    projected = snapshot.trades.trades[-1]
    final_delta = trade_deltas[-1].trade_upserts[0]

    assert projected["status"] == "closed"
    assert projected["exit_time"] == "2026-02-01T02:00:00Z"
    assert projected["closed_at"] == "2026-02-01T02:00:00Z"
    assert projected["exit_price"] == 105.0
    assert projected["close_reason"] == "TARGET"
    assert projected["reason_code"] == "TARGET"
    assert projected["gross_pnl"] == 5.0
    assert projected["fees_paid"] == 0.5
    assert projected["net_pnl"] == 4.5
    assert projected["legs"][0]["exit_price"] == 105.0
    assert final_delta["exit_price"] == 105.0
    assert final_delta["close_reason"] == "TARGET"
    assert final_delta["reason_code"] == "TARGET"


def test_mixed_close_reason_survives_late_trade_update() -> None:
    snapshot, _trade_deltas = _closed_trade_projection_with_late_update(reason_code="MIXED")
    projected = snapshot.trades.trades[-1]

    assert projected["close_reason"] == "MIXED"
    assert projected["reason_code"] == "MIXED"


def test_backtest_end_close_reason_survives_late_trade_update() -> None:
    snapshot, _trade_deltas = _closed_trade_projection_with_late_update(reason_code="BACKTEST_END")
    projected = snapshot.trades.trades[-1]

    assert projected["close_reason"] == "BACKTEST_END"
    assert projected["reason_code"] == "BACKTEST_END"
    assert projected["legs"][0]["status"] == "backtest_end"


def test_selected_symbol_recent_trades_keep_closed_trade_visual_contract_after_late_update() -> None:
    snapshot, _trade_deltas = _closed_trade_projection_with_late_update(reason_code="STOP", exit_price=96.5)
    payload = selected_symbol_snapshot_contract(
        bot_id="bot-1",
        run_id="run-1",
        symbol_key="instr-1|1m",
        symbol_state=snapshot,
        symbol_catalog_entry=None,
        run_health={"status": "completed", "warning_count": 0, "warnings": []},
        run_bootstrap_seq=11,
        base_seq=11,
        stream_session_id="stream-1",
        run_live=True,
        transport_eligible=False,
        message="BotLens selected-symbol snapshot ready.",
    )

    trade = payload["selected_symbol"]["current"]["recent_trades"][0]

    assert trade["trade_id"] == "trade-1"
    assert trade["symbol"] == "BTC"
    assert trade["timeframe"] == "1m"
    assert trade["status"] == "closed"
    assert trade["side"] is None
    assert trade["direction"] == "long"
    assert trade["entry_time"] == "2026-02-01T00:00:00Z"
    assert trade["entry_price"] == 100.0
    assert trade["exit_time"] == "2026-02-01T02:00:00Z"
    assert trade["closed_at"] == "2026-02-01T02:00:00Z"
    assert trade["exit_price"] == 96.5
    assert trade["stop_price"] == 97.5
    assert trade["quantity"] == 1.0
    assert trade["legs"][0]["exit_price"] == 96.5
    assert trade["gross_pnl"] == 5.0
    assert trade["fees_paid"] == 0.5
    assert trade["net_pnl"] == 4.5
    assert trade["close_reason"] == "STOP"
    assert trade["reason_code"] == "STOP"
    assert trade["metrics"]["bars_held"] == 5
