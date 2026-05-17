from __future__ import annotations

import pytest

from portal.backend.service.reports import comparison
from portal.backend.service.reports.schemas import FirstDivergenceDTO, GoldenEvidenceDTO


def _ready_status(run_id: str, status: str = "ready", *, can_view: bool = True) -> dict:
    return {
        "contract_version": "run_report_v2",
        "schema_version": "run_report_materialization_status.v1",
        "run_id": run_id,
        "report_status": {
            "status": status,
            "contract_version": "run_report_v2",
            "artifact_id": f"{run_id}:run_report_v2" if status == "ready" else None,
            "built_at": "2026-05-16T01:00:00Z" if status == "ready" else None,
            "started_at": "2026-05-16T00:59:45Z" if status == "ready" else None,
            "duration_ms": 15_000 if status == "ready" else None,
            "error": "boom" if status == "failed" else None,
            "can_view": can_view,
            "can_build": status in {"not_started", "failed", "stale"},
            "can_retry": status == "failed",
        },
    }


def _metric(value: float | int | None, *, valid: bool = True, reason: str | None = None, unit: str = "number") -> dict:
    return {
        "value": value,
        "valid": valid,
        "unit": unit,
        "method": "fixture",
        "source": "test",
        "invalid_reason": reason,
    }


def _report(run_id: str, *, semantic: str = "semantic-a", operational: str = "operational-a", sharpe: dict | None = None) -> dict:
    return {
        "contract_version": "run_report_v2",
        "schema_version": "run_report.v2",
        "run_id": run_id,
        "identity": {"run_id": run_id, "symbols": ["BTC"], "timeframe": "1h"},
        "trust": {
            "lifecycle_status": "completed",
            "readiness_status": "ready",
            "golden_status": "certified",
            "semantic_fingerprint": semantic,
            "operational_fingerprint": operational,
            "data_snapshot_hash": "snapshot-a",
            "config_hash": f"run-config-{run_id}",
            "material_config_hash": "material-config-a",
            "strategy_hash": "strategy-a",
            "runtime_ordering_status": "gapless",
            "wallet_trace_complete": True,
            "candle_continuity_status": "source_sparse",
            "observer_invariance_status": "safe",
        },
        "performance": {
            "net_pnl": _metric(100.0, unit="currency"),
            "total_return_pct": _metric(0.10, unit="ratio"),
            "max_drawdown_pct": _metric(0.02, unit="ratio"),
            "sharpe": sharpe or _metric(1.5, unit="ratio"),
            "sortino": _metric(2.0, unit="ratio"),
            "calmar": _metric(1.0, unit="ratio"),
            "profit_factor": _metric(1.8, unit="ratio"),
            "expectancy": _metric(4.2, unit="currency"),
            "win_rate": _metric(0.6, unit="ratio"),
            "trade_count": _metric(10, unit="count"),
            "fees": _metric(5.0, unit="currency"),
        },
        "behavior": {
            "total_decisions": 103,
            "accepted_decisions": 10,
            "rejected_decisions": 93,
            "rejection_reasons": {"below_threshold": 90},
            "action_distribution": {"enter": 5, "exit": 5},
            "entry_count": 5,
            "exit_count": 5,
        },
        "wallet": {
            "wallet_trace_complete": True,
            "missing_wallet_trace_count": 0,
            "wallet_projection_status": "passed",
            "final_wallet_value": _metric(1100.0, unit="currency"),
            "margin_warnings": [],
            "reservation_leaks": {},
        },
        "symbol_breakdown": [
            {
                "symbol": "BTC",
                "trade_count": 10,
                "decision_count": 103,
                "accepted_decisions": 10,
                "rejected_decisions": 93,
                "rejection_reasons": {"below_threshold": 90},
                "net_pnl": _metric(100.0, unit="currency"),
                "fees": _metric(5.0, unit="currency"),
                "win_rate": _metric(0.6, unit="ratio"),
                "contribution_pct": _metric(1.0, unit="ratio"),
            }
        ],
        "coordinator_waits": {"status": "ready", "total_wait_ms": 10.0, "wait_count": 1, "max_wait_ms": 10.0, "top_waits": []},
        "operational_diagnostics": {
            "operational_fingerprint": operational,
            "operational_drift_status": "not_computed",
            "db_slow_write_warning_count": 0,
            "step_trace_warnings": [],
            "telemetry_warnings": [],
            "diagnostics_degraded_status": "clean",
        },
        "sections": {},
        "raw_refs": {},
    }


def test_comparison_uses_ready_materialized_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = {"left": _report("left"), "right": _report("right")}
    monkeypatch.setattr(comparison, "report_materialization_status", lambda run_id, **_kwargs: _ready_status(run_id))
    monkeypatch.setattr(comparison, "materialized_run_report", lambda run_id: reports[run_id])

    result = comparison.compare_materialized_run_reports("left", "right")

    assert result.can_compare is True
    assert result.comparison_status == "ready"
    assert result.comparison_verdict == "semantic_match"
    assert result.trust_comparison.semantic_fingerprint_match is True
    assert result.trust_comparison.config_hash_match is True
    assert result.performance_delta.net_pnl.valid is True
    assert result.behavior_delta.decision_count_delta == 0
    assert result.raw_refs["cold_build_triggered"] is False


def test_report_not_ready_blocks_without_materialized_artifact_read(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"materialized": 0}

    def status(run_id: str, **_kwargs: object) -> dict:
        return _ready_status(run_id, "building", can_view=False) if run_id == "right" else _ready_status(run_id)

    def artifact(_run_id: str) -> dict:
        called["materialized"] += 1
        raise AssertionError("comparison should not read artifacts when status blocks")

    monkeypatch.setattr(comparison, "report_materialization_status", status)
    monkeypatch.setattr(comparison, "materialized_run_report", artifact)

    result = comparison.compare_materialized_run_reports("left", "right")

    assert result.can_compare is False
    assert result.blocked_reason == "right_report_building"
    assert called["materialized"] == 0


def test_metric_validity_is_preserved_in_deltas(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = {
        "left": _report("left", sharpe=_metric(None, valid=False, reason="zero_return_stddev", unit="ratio")),
        "right": _report("right", sharpe=_metric(1.5, unit="ratio")),
    }
    monkeypatch.setattr(comparison, "report_materialization_status", lambda run_id, **_kwargs: _ready_status(run_id))
    monkeypatch.setattr(comparison, "materialized_run_report", lambda run_id: reports[run_id])

    result = comparison.compare_materialized_run_reports("left", "right")

    assert result.performance_delta.sharpe.valid is False
    assert result.performance_delta.sharpe.delta is None
    assert "zero_return_stddev" in (result.performance_delta.sharpe.invalid_reason or "")


def test_operational_drift_is_separated_from_semantic_match(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = {
        "left": _report("left", semantic="semantic-a", operational="operational-a"),
        "right": _report("right", semantic="semantic-a", operational="operational-b"),
    }
    monkeypatch.setattr(comparison, "report_materialization_status", lambda run_id, **_kwargs: _ready_status(run_id))
    monkeypatch.setattr(comparison, "materialized_run_report", lambda run_id: reports[run_id])

    result = comparison.compare_materialized_run_reports("left", "right")

    assert result.trust_comparison.semantic_fingerprint_match is True
    assert result.trust_comparison.operational_fingerprint_match is False
    assert result.comparison_verdict == "semantic_match_operational_drift"
    assert result.operational_drift.operational_drift_summary == "operational_drift_only"
    assert result.first_divergence.present is False


def test_existing_golden_artifact_populates_comparison_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = {"left": _report("left"), "right": _report("right")}
    monkeypatch.setattr(comparison, "report_materialization_status", lambda run_id, **_kwargs: _ready_status(run_id))
    monkeypatch.setattr(comparison, "materialized_run_report", lambda run_id: reports[run_id])
    monkeypatch.setattr(
        comparison,
        "read_golden_comparison_evidence",
        lambda left_run_id, right_run_id: GoldenEvidenceDTO(
            available=True,
            status="available",
            artifact_path="logs/reports/golden-repeatability/pair/comparison_summary.json",
            verdict="PASS",
            semantic_fingerprint_match=True,
            operational_fingerprint_match=True,
            data_snapshot_hash_match=True,
            material_config_hash_match=True,
            strategy_hash_match=True,
            decision_count_left=103,
            decision_count_right=103,
            missing_decision_count=0,
            extra_decision_count=0,
            verdict_change_count=0,
            trade_lifecycle_equal=True,
            trade_count_left=91,
            trade_count_right=91,
            wallet_trace_missing_left=0,
            wallet_trace_missing_right=0,
            wallet_market_time_overtake_left=0,
            wallet_market_time_overtake_right=0,
            runtime_ordering_left={"status": "ready", "gap_count": 0, "duplicate_values": []},
            runtime_ordering_right={"status": "ready", "gap_count": 0, "duplicate_values": []},
            first_divergence=FirstDivergenceDTO(
                present=False,
                divergence_type="none",
                explanation="No semantic divergence detected by golden evidence.",
                source="golden",
            ),
        ),
    )

    result = comparison.compare_materialized_run_reports("left", "right")

    assert result.golden_evidence.available is True
    assert result.behavior_delta.trade_lifecycle_equal is True
    assert result.behavior_delta.trade_lifecycle_source == "golden"
    assert result.behavior_delta.verdict_changes == 0
    assert result.wallet_comparison.missing_wallet_trace_count_left == 0
    assert result.golden_evidence.wallet_market_time_overtake_left == 0
    assert result.golden_evidence.runtime_ordering_left["gap_count"] == 0
    assert result.first_divergence.source == "golden"
    assert result.first_divergence.present is False


def test_missing_golden_artifact_does_not_break_report_level_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = {"left": _report("left"), "right": _report("right")}
    monkeypatch.setattr(comparison, "report_materialization_status", lambda run_id, **_kwargs: _ready_status(run_id))
    monkeypatch.setattr(comparison, "materialized_run_report", lambda run_id: reports[run_id])
    monkeypatch.setattr(comparison, "read_golden_comparison_evidence", lambda _left, _right: GoldenEvidenceDTO())

    result = comparison.compare_materialized_run_reports("left", "right")

    assert result.can_compare is True
    assert result.comparison_status == "ready"
    assert result.golden_evidence.available is False
    assert result.golden_evidence.status == "not_available"
    assert result.raw_refs["cold_build_triggered"] is False


def test_require_golden_blocks_when_artifact_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = {"left": _report("left"), "right": _report("right")}
    monkeypatch.setattr(comparison, "report_materialization_status", lambda run_id, **_kwargs: _ready_status(run_id))
    monkeypatch.setattr(comparison, "materialized_run_report", lambda run_id: reports[run_id])
    monkeypatch.setattr(comparison, "read_golden_comparison_evidence", lambda _left, _right: GoldenEvidenceDTO())

    result = comparison.compare_materialized_run_reports("left", "right", require_golden=True)

    assert result.can_compare is False
    assert result.blocked_reason == "golden_evidence_not_available"


def test_golden_evidence_first_divergence_populates_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = {
        "left": _report("left", semantic="semantic-a"),
        "right": _report("right", semantic="semantic-b"),
    }
    monkeypatch.setattr(comparison, "report_materialization_status", lambda run_id, **_kwargs: _ready_status(run_id))
    monkeypatch.setattr(comparison, "materialized_run_report", lambda run_id: reports[run_id])
    monkeypatch.setattr(
        comparison,
        "read_golden_comparison_evidence",
        lambda _left, _right: GoldenEvidenceDTO(
            available=True,
            status="available",
            verdict="FAIL",
            fail_reasons=["semantic_fingerprint_mismatch"],
            semantic_fingerprint_match=False,
            operational_fingerprint_match=True,
            first_divergence=FirstDivergenceDTO(
                present=True,
                divergence_type="decision_divergence",
                symbol="BTC",
                timeframe="1h",
                bar_time="2026-01-01T00:00:00Z",
                decision_id="decision-1",
                field_path="decisions[0].status",
                left_value="accepted",
                right_value="rejected",
                explanation="Golden comparison first divergence in decisions[0].status.",
                source="golden",
            ),
        ),
    )

    result = comparison.compare_materialized_run_reports("left", "right")

    assert result.comparison_verdict == "semantic_drift"
    assert result.first_divergence.present is True
    assert result.first_divergence.source == "golden"
    assert result.first_divergence.decision_id == "decision-1"
