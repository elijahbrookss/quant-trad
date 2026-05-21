from __future__ import annotations

import json

from portal.backend.service.reports.golden_evidence import read_golden_comparison_evidence


def test_existing_golden_artifact_is_located_and_normalized(tmp_path) -> None:
    artifact_dir = tmp_path / "golden-repeatability" / "pair"
    artifact_dir.mkdir(parents=True)
    artifact = artifact_dir / "comparison_summary.json"
    artifact.write_text(
        json.dumps(
            {
                "run_ids": ["left", "right"],
                "verdict": "PASS",
                "fail_reasons": [],
                "material": {
                    "left": {
                        "report_semantic_fingerprint": "semantic",
                        "report_operational_fingerprint": "operational-a",
                        "data_snapshot_hash": "data",
                        "material_config_hash": "config",
                        "strategy_hash": "strategy",
                    },
                    "right": {
                        "report_semantic_fingerprint": "semantic",
                        "report_operational_fingerprint": "operational-b",
                        "data_snapshot_hash": "data",
                        "material_config_hash": "config",
                        "strategy_hash": "strategy",
                    },
                },
                "material_diff": {
                    "report_operational_fingerprint": {"left": "operational-a", "right": "operational-b"}
                },
                "decision_compare": {
                    "left_count": 103,
                    "right_count": 103,
                    "missing_decision_count": 0,
                    "extra_decision_count": 0,
                    "missing_ids_count": 0,
                    "extra_ids_count": 0,
                    "verdict_change_count": 0,
                    "missing_decision_ids": [],
                    "extra_decision_ids": [],
                    "verdict_changes": [],
                },
                "trade_lifecycle_compare": {"left_count": 91, "right_count": 91, "equal": True, "first_diff": None},
                "wallet_trace": {
                    "left": {"missing_wallet_trace_count": 0},
                    "right": {"missing_wallet_trace_count": 0},
                },
                "wallet_market_time_ordering": {
                    "left": {"checked": True, "first_overtake": None},
                    "right": {"checked": True, "first_overtake": None},
                },
                "runtime_ordering": {
                    "left": {"status": "ready", "gap_count": 0},
                    "right": {"status": "ready", "gap_count": 0},
                },
                "first_divergence": {
                    "section": "diagnostics",
                    "field": "by_code",
                    "left": {"db_write_slow": 1},
                    "right": {"db_write_slow": 2},
                },
            }
        )
    )

    evidence = read_golden_comparison_evidence("left", "right", search_roots=[tmp_path])

    assert evidence.available is True
    assert evidence.verdict == "PASS"
    assert evidence.semantic_fingerprint_match is True
    assert evidence.operational_fingerprint_match is False
    assert evidence.decision_count_left == 103
    assert evidence.decision_diff_full_lists_available is True
    assert evidence.verdict_changes_full_available is True
    assert evidence.trade_lifecycle_equal is True
    assert evidence.wallet_market_time_overtake_left == 0
    assert evidence.first_divergence.present is False
    assert evidence.first_divergence.explanation == "No semantic divergence detected by golden evidence."


def test_golden_artifact_first_divergence_is_normalized(tmp_path) -> None:
    artifact = tmp_path / "comparison_summary.json"
    artifact.write_text(
        json.dumps(
            {
                "run_ids": ["left", "right"],
                "verdict": "FAIL",
                "fail_reasons": ["decision_verdict_or_id_mismatch"],
                "material": {
                    "left": {"report_semantic_fingerprint": "semantic-a"},
                    "right": {"report_semantic_fingerprint": "semantic-b"},
                },
                "material_diff": {"report_semantic_fingerprint": {"left": "semantic-a", "right": "semantic-b"}},
                "decision_compare": {
                    "left_count": 1,
                    "right_count": 1,
                    "missing_decision_count": 0,
                    "extra_decision_count": 0,
                    "missing_ids_count": 0,
                    "extra_ids_count": 0,
                    "verdict_change_count": 1,
                    "missing_decision_ids": [],
                    "extra_decision_ids": [],
                    "verdict_changes": [
                        {
                            "decision_id": "decision-1",
                            "symbol": "BTC",
                            "bar_time": "2026-01-01T00:00:00Z",
                            "left_verdict": "accepted",
                            "right_verdict": "rejected",
                            "left_reason": None,
                            "right_reason": "RULE_BLOCKED",
                            "left_action": "enter_long",
                            "right_action": "enter_long",
                        }
                    ],
                    "first_verdict_change": {
                        "decision_id": "decision-1",
                        "left": {"decision_id": "decision-1", "symbol": "BTC", "bar_time": "2026-01-01T00:00:00Z", "status": "accepted"},
                        "right": {"decision_id": "decision-1", "symbol": "BTC", "bar_time": "2026-01-01T00:00:00Z", "status": "rejected"},
                    },
                },
                "trade_lifecycle_compare": {"equal": True},
                "first_divergence": {
                    "section": "decisions",
                    "index": 0,
                    "left": {"decision_id": "decision-1", "symbol": "BTC", "bar_time": "2026-01-01T00:00:00Z", "status": "accepted"},
                    "right": {"decision_id": "decision-1", "symbol": "BTC", "bar_time": "2026-01-01T00:00:00Z", "status": "rejected"},
                },
            }
        )
    )

    evidence = read_golden_comparison_evidence("left", "right", search_roots=[tmp_path])

    assert evidence.available is True
    assert evidence.first_divergence.present is True
    assert evidence.first_divergence.source == "golden"
    assert evidence.first_divergence.divergence_type == "decision_divergence"
    assert evidence.first_divergence.decision_id == "decision-1"
    assert evidence.verdict_change_count == 1
    assert evidence.verdict_changes[0]["decision_id"] == "decision-1"
    assert evidence.verdict_changes[0]["left_verdict"] == "accepted"
    assert evidence.verdict_changes_full_available is True


def test_golden_evidence_reader_uses_full_decision_arrays(tmp_path) -> None:
    artifact = tmp_path / "comparison_summary.json"
    artifact.write_text(
        json.dumps(
            {
                "run_ids": ["left", "right"],
                "verdict": "FAIL",
                "fail_reasons": ["decision_verdict_or_id_mismatch"],
                "material": {"left": {}, "right": {}},
                "material_diff": {},
                "decision_compare": {
                    "left_count": 5,
                    "right_count": 5,
                    "missing_decision_count": 2,
                    "extra_decision_count": 2,
                    "missing_ids_count": 2,
                    "extra_ids_count": 2,
                    "verdict_change_count": 2,
                    "missing_decision_ids": ["missing-1", "missing-2"],
                    "extra_decision_ids": ["extra-1", "extra-2"],
                    "verdict_changes": [
                        {"decision_id": "changed-1", "left_verdict": "accepted", "right_verdict": "rejected"},
                        {"decision_id": "changed-2", "left_verdict": "rejected", "right_verdict": "accepted"},
                    ],
                    "first_missing_id": "missing-1",
                    "first_extra_id": "extra-1",
                },
                "trade_lifecycle_compare": {"equal": True},
            }
        )
    )

    evidence = read_golden_comparison_evidence("left", "right", search_roots=[tmp_path])

    assert evidence.available is True
    assert evidence.missing_decision_count == 2
    assert evidence.extra_decision_count == 2
    assert evidence.missing_decision_ids == ["missing-1", "missing-2"]
    assert evidence.extra_decision_ids == ["extra-1", "extra-2"]
    assert evidence.decision_diff_full_lists_available is True
    assert evidence.verdict_change_count == 2
    assert [row["decision_id"] for row in evidence.verdict_changes] == ["changed-1", "changed-2"]
    assert evidence.verdict_changes_full_available is True


def test_legacy_golden_artifact_partial_decision_fields_remain_supported(tmp_path) -> None:
    artifact = tmp_path / "comparison_summary.json"
    artifact.write_text(
        json.dumps(
            {
                "run_ids": ["left", "right"],
                "verdict": "FAIL",
                "fail_reasons": ["decision_verdict_or_id_mismatch"],
                "material": {"left": {}, "right": {}},
                "material_diff": {},
                "decision_compare": {
                    "left_count": 3,
                    "right_count": 3,
                    "missing_ids_count": 2,
                    "extra_ids_count": 1,
                    "verdict_change_count": 1,
                    "first_missing_id": "missing-1",
                    "first_extra_id": "extra-1",
                    "first_verdict_change": {
                        "decision_id": "decision-1",
                        "left": {"decision_id": "decision-1", "status": "accepted"},
                        "right": {"decision_id": "decision-1", "status": "rejected"},
                    },
                },
                "trade_lifecycle_compare": {"equal": True},
            }
        )
    )

    evidence = read_golden_comparison_evidence("left", "right", search_roots=[tmp_path])

    assert evidence.available is True
    assert evidence.missing_decision_count == 2
    assert evidence.extra_decision_count == 1
    assert evidence.missing_decision_ids == ["missing-1"]
    assert evidence.extra_decision_ids == ["extra-1"]
    assert evidence.decision_diff_full_lists_available is False
    assert evidence.verdict_change_count == 1
    assert evidence.verdict_changes[0]["decision_id"] == "decision-1"
    assert evidence.verdict_changes_full_available is False


def test_missing_golden_artifact_returns_not_available(tmp_path) -> None:
    evidence = read_golden_comparison_evidence("left", "right", search_roots=[tmp_path])

    assert evidence.available is False
    assert evidence.status == "not_available"
    assert evidence.first_divergence.divergence_type == "not_available"
