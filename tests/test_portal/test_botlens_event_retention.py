from __future__ import annotations

from portal.backend.service.bots.botlens_event_retention import (
    RuntimeEventRetentionAction,
    RuntimeEventRetentionTier,
    retention_policy_for_event_name,
    tier_map,
)


def test_retention_tier_map_keeps_material_truth_permanent() -> None:
    for event_name in (
        "RUN_STARTED",
        "RUN_PHASE_REPORTED",
        "RUN_LIFECYCLE_CHANGED",
        "RUN_COMPLETED",
        "SIGNAL_EMITTED",
        "DECISION_EMITTED",
        "DECISION_ACCEPTED",
        "DECISION_REJECTED",
        "ENTRY_FILLED",
        "EXIT_FILLED",
        "TRADE_OPENED",
        "TRADE_CLOSED",
        "MARGIN_RESERVED",
        "MARGIN_REJECTED",
        "FEE_APPLIED",
        "EQUITY_UPDATED",
        "RUNTIME_ERROR",
        "EXECUTION_INTRABAR_FALLBACK_PESSIMISTIC",
    ):
        policy = retention_policy_for_event_name(event_name)
        assert policy.tier == RuntimeEventRetentionTier.CANONICAL_RUN_TRUTH
        assert policy.action == RuntimeEventRetentionAction.PERSIST
        assert policy.persist_raw is True


def test_retention_tier_map_removes_raw_high_frequency_transport_from_permanent_ledger() -> None:
    expected = {
        "CANDLE_OBSERVED": RuntimeEventRetentionAction.SUMMARIZE,
        "SERIES_STATS_REPORTED": RuntimeEventRetentionAction.SUMMARIZE,
        "HEALTH_STATUS_REPORTED": RuntimeEventRetentionAction.AGGREGATE,
        "OVERLAY_STATE_CHANGED": RuntimeEventRetentionAction.TRANSPORT_ONLY,
    }
    for event_name, action in expected.items():
        policy = retention_policy_for_event_name(event_name)
        assert policy.action == action
        assert policy.persist_raw is False


def test_retention_keeps_only_material_diagnostics() -> None:
    nonmaterial = retention_policy_for_event_name(
        "DIAGNOSTIC_RECORDED",
        context={"level": "INFO", "diagnostic_code": "overlay_summary"},
    )
    material = retention_policy_for_event_name(
        "DIAGNOSTIC_RECORDED",
        context={"level": "WARN", "diagnostic_code": "run_notification_queue_overflow"},
    )

    assert nonmaterial.action == RuntimeEventRetentionAction.AGGREGATE
    assert nonmaterial.persist_raw is False
    assert material.action == RuntimeEventRetentionAction.PERSIST
    assert material.persist_raw is True


def test_tier_map_exposes_standard_event_budget_contract() -> None:
    rows = {row["event_name"]: row for row in tier_map()}

    assert rows["CANDLE_OBSERVED"]["action"] == "summarize"
    assert rows["HEALTH_STATUS_REPORTED"]["tier"] == "tier_3_observability_metric"
    assert rows["OVERLAY_STATE_CHANGED"]["tier"] == "tier_4_live_transport"
    assert rows["DECISION_EMITTED"]["action"] == "persist"
