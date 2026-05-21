from __future__ import annotations

import json

from scripts.reporting import golden_repeatability as golden


def _decision(
    decision_id: str,
    *,
    status: str = "accepted",
    accepted: bool = True,
    reason_code: str | None = None,
    action: str = "enter_long",
    symbol: str = "BTC",
    bar_time: str = "2026-01-01T00:00:00Z",
) -> dict:
    return {
        "decision_id": decision_id,
        "symbol": symbol,
        "bar_time": bar_time,
        "action": action,
        "status": status,
        "accepted": accepted,
        "reason_code": reason_code,
        "decision_context": {"wallet_snapshot": {"cash": 1000}},
    }


def _dataset(run_id: str, decisions: list[dict]) -> dict:
    return {
        "schema_version": "run_research_dataset.v1",
        "metadata": {
            "run_id": run_id,
            "status": "completed",
            "run_type": "backtest",
            "symbols": ["BTC"],
            "strategy_hash": "strategy-a",
            "material_config_hash": "config-a",
            "data_snapshot_hash": "data-a",
            "report_semantic_fingerprint": "semantic-a",
            "report_operational_fingerprint": "operational-a",
        },
        "readiness": {
            "golden_candidate_status": "certified",
            "golden_blocking_reasons": [],
            "repeatability_status": "ready",
            "comparison_status": "ready",
        },
        "summary": {"total_decisions": len(decisions)},
        "diagnostics": {
            "summary": {
                "blocking_codes": [],
                "degraded_codes": [],
                "by_code": {},
                "readiness_impact": {},
            }
        },
        "decisions": decisions,
        "trades": [],
    }


def test_golden_artifact_persists_full_decision_differences(monkeypatch, tmp_path) -> None:
    left = _dataset(
        "left",
        [
            _decision("same"),
            _decision("missing-1"),
            _decision("missing-2"),
            _decision("changed-1", status="accepted", accepted=True, reason_code=None, action="enter_long"),
            _decision("changed-2", status="rejected", accepted=False, reason_code="WALLET_INSUFFICIENT_MARGIN", action="enter_short"),
        ],
    )
    right = _dataset(
        "right",
        [
            _decision("same"),
            _decision("extra-1"),
            _decision("extra-2"),
            _decision("changed-1", status="rejected", accepted=False, reason_code="RULE_BLOCKED", action="enter_long"),
            _decision("changed-2", status="accepted", accepted=True, reason_code=None, action="enter_short"),
        ],
    )
    monkeypatch.setattr(golden, "get_run_research_dataset", lambda run_id: {"left": left, "right": right}[run_id])
    monkeypatch.setattr(golden, "_runtime_ordering_summary", lambda run_id: {"status": "ready", "gap_count": 0, "duplicate_values": []})

    result = golden.compare_runs("left", "right", out_dir=tmp_path, check_prior=False)
    artifact = json.loads((tmp_path / "comparison_summary.json").read_text())
    decision_compare = artifact["decision_compare"]

    assert result["verdict"] == "FAIL"
    assert "decision_verdict_or_id_mismatch" in result["fail_reasons"]
    assert decision_compare["missing_ids_count"] == 2
    assert decision_compare["extra_ids_count"] == 2
    assert decision_compare["missing_decision_count"] == 2
    assert decision_compare["extra_decision_count"] == 2
    assert decision_compare["missing_decision_ids"] == ["missing-1", "missing-2"]
    assert decision_compare["extra_decision_ids"] == ["extra-1", "extra-2"]
    assert decision_compare["verdict_change_count"] == 2
    assert [row["decision_id"] for row in decision_compare["verdict_changes"]] == ["changed-1", "changed-2"]
    assert decision_compare["verdict_changes"][0]["left_verdict"] == "accepted"
    assert decision_compare["verdict_changes"][0]["right_verdict"] == "rejected"
    assert decision_compare["first_missing_id"] == "missing-1"
    assert decision_compare["first_extra_id"] == "extra-1"
    assert decision_compare["first_verdict_change"]["decision_id"] == "changed-1"
    assert "left" in decision_compare["first_verdict_change"]
    assert "right" in decision_compare["first_verdict_change"]


def test_golden_verdict_logic_unchanged_for_matching_pair(monkeypatch, tmp_path) -> None:
    left = _dataset("left", [_decision("same-1"), _decision("same-2", status="rejected", accepted=False, reason_code="RULE_BLOCKED")])
    right = _dataset("right", [_decision("same-1"), _decision("same-2", status="rejected", accepted=False, reason_code="RULE_BLOCKED")])
    monkeypatch.setattr(golden, "get_run_research_dataset", lambda run_id: {"left": left, "right": right}[run_id])
    monkeypatch.setattr(golden, "_runtime_ordering_summary", lambda run_id: {"status": "ready", "gap_count": 0, "duplicate_values": []})

    result = golden.compare_runs("left", "right", out_dir=tmp_path, check_prior=False)

    assert result["verdict"] == "PASS"
    assert result["fail_reasons"] == []
    assert result["material_diff"] == {}
    assert result["decision_compare"]["missing_decision_ids"] == []
    assert result["decision_compare"]["extra_decision_ids"] == []
    assert result["decision_compare"]["verdict_changes"] == []
