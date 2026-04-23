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
    apply_symbol_batch,
    empty_run_health_state,
    empty_symbol_projection_snapshot,
    is_open_trade,
    read_run_projection_snapshot,
)
from portal.backend.service.bots.botlens_domain_events import deserialize_botlens_domain_event


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
                    "seq": 9,
                    "base_seq": 8,
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
    assert "payload" not in snapshot.overlays.overlays[0]
    assert deltas[0].overlay_ops["ops"][0]["overlay"]["detail_level"] == "summary"
