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
    assert decision_compare["missing_decision_count"] == 2
    assert decision_compare["extra_decision_count"] == 2
    assert decision_compare["missing_decision_ids"] == ["missing-1", "missing-2"]
    assert decision_compare["extra_decision_ids"] == ["extra-1", "extra-2"]
    assert decision_compare["verdict_change_count"] == 2
    assert [row["decision_id"] for row in decision_compare["verdict_changes"]] == ["changed-1", "changed-2"]
    assert decision_compare["verdict_changes"][0]["left_verdict"] == "accepted"
    assert decision_compare["verdict_changes"][0]["right_verdict"] == "rejected"
    assert "missing_ids_count" not in decision_compare
    assert "extra_ids_count" not in decision_compare
    assert "first_missing_id" not in decision_compare
    assert "first_extra_id" not in decision_compare
    assert "first_verdict_change" not in decision_compare
    trace = artifact["disagreement_trace"]
    assert trace["status"] == "available"
    assert trace["first_divergence"]["section"] == "decisions"
    assert set(trace["runs"]) == {"left", "right"}
    assert (
        trace["runs"]["left"]["strategy_decision"]["value"]["decision_id"]
        == "changed-1"
    )
    assert trace["runs"]["left"]["input_dataset"]["fingerprints"] == {
        "data_snapshot_hash": "data-a",
        "material_config_hash": "config-a",
        "strategy_hash": "strategy-a",
        "semantic_fingerprint": "semantic-a",
        "operational_fingerprint": "operational-a",
    }
    assert (
        trace["runs"]["left"]["generated_order"]["availability"]
        == "unavailable"
    )
    assert (
        trace["runs"]["left"]["available_candles"]["availability"]
        == "catalog_only"
    )


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
    assert result["disagreement_trace"]["status"] == "not_required"


def test_golden_disagreement_trace_links_persisted_execution_evidence(
    monkeypatch,
    tmp_path,
) -> None:
    left_decision = _decision("decision-1")
    left_decision["trade_id"] = "trade-1"
    left_decision["known_at"] = "2026-01-01T00:00:00Z"
    left_decision["decision_context"].update(
        {
            "normalized_execution_plan": {
                "entry_order_type": "market",
                "stop_price": 96.0,
                "target_prices": [110.0],
            },
            "generated_order": {
                "side": "buy",
                "qty": 1.0,
                "order_type": "market",
            },
            "fill_decision": {
                "status": "filled",
                "price": 100.0,
                "fee": 0.2,
            },
        }
    )
    right_decision = _decision(
        "decision-1",
        status="rejected",
        accepted=False,
        reason_code="RULE_BLOCKED",
    )
    left = _dataset("left", [left_decision])
    right = _dataset("right", [right_decision])

    left["metadata"].update(
        {
            "simulated_window": {
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T01:00:00Z",
            },
            "configuration": {
                "source": "portal_bot_runs.provenance_columns",
                "data": {
                    "date_range": {
                        "start": "2026-01-01T00:00:00Z",
                        "end": "2026-01-01T01:00:00Z",
                    }
                },
                "execution": {"execution_mode": "fast"},
                "atm": {"template_id": "atm-1"},
                "risk": {"base_risk_per_trade": 4.0},
                "indicators": [{"id": "profile-1", "type": "market_profile"}],
            },
        }
    )
    left["trades"] = [
        {
            "id": "trade-1",
            "decision_id": "decision-1",
            "symbol": "BTC",
            "direction": "long",
            "status": "closed",
            "entry_time": "2026-01-01T00:00:00Z",
            "entry_price": 100.0,
            "exit_time": "2026-01-01T01:00:00Z",
            "exit_price": 110.0,
            "gross_pnl": 10.0,
            "fees": 0.31,
            "net_pnl": 9.69,
            "close_reason": "EXEC_EXIT_TARGET",
        }
    ]
    left["candidate_lifecycle"] = {
        "items": [
            {
                "decision_id": "decision-1",
                "trade_id": "trade-1",
                "symbol": "BTC",
                "stage": "confirmed",
                "known_at": "2026-01-01T00:00:00Z",
            }
        ]
    }
    left["candle_catalog"] = {
        "items": [
            {
                "symbol": "BTC",
                "timeframe": "1h",
                "first_candle_at": "2026-01-01T00:00:00Z",
                "last_candle_at": "2026-01-01T01:00:00Z",
                "candle_count": 2,
                "fingerprint": "candles-a",
                "continuity_status": "source_sparse",
            }
        ]
    }
    left["candle_gaps"] = {
        "canonical_evidence_status": "available",
        "provider_gap_count": 1,
        "blocking_gap_count": 0,
        "caveats": ["candle_continuity_provider_sparse"],
        "facts": [
            {
                "symbol": "BTC",
                "timeframe": "1h",
                "classification": "provider_missing_data",
                "reason_code": "provider_response_empty",
            }
        ],
    }
    left["fee_accounting"] = {
        "total_fees": 0.31,
        "fee_sanity_checks": {"trade_fees_match_summary": True},
    }
    left["wallet_accounting"] = {
        "wallet_replay_status": "ready",
        "ending_equity": 1009.69,
        "caveats": [],
    }
    left["summary"].update(
        {
            "gross_pnl": 10.0,
            "fees": 0.31,
            "net_pnl": 9.69,
            "equity_end": 1009.69,
        }
    )
    left["readiness"].update(
        {
            "data_quality_status": "degraded",
            "execution_quality_status": "ready",
            "caveats": ["candle_continuity_provider_sparse"],
        }
    )

    monkeypatch.setattr(
        golden,
        "get_run_research_dataset",
        lambda run_id: {"left": left, "right": right}[run_id],
    )
    monkeypatch.setattr(
        golden,
        "_runtime_ordering_summary",
        lambda run_id: {
            "status": "ready",
            "gap_count": 0,
            "duplicate_values": [],
        },
    )

    result = golden.compare_runs(
        "left",
        "right",
        out_dir=tmp_path,
        check_prior=False,
    )
    trace = result["disagreement_trace"]["runs"]["left"]

    assert trace["known_at_state"]["value"]["boundary"] == "2026-01-01T00:00:00Z"
    assert trace["input_dataset"]["requested_range"]["end"] == "2026-01-01T01:00:00Z"
    assert trace["input_dataset"]["loaded_ranges"] == [
        {
            "symbol": "BTC",
            "instrument_id": None,
            "timeframe": "1h",
            "first_candle_at": "2026-01-01T00:00:00Z",
            "last_candle_at": "2026-01-01T01:00:00Z",
            "candle_count": 2,
            "fingerprint": "candles-a",
        }
    ]
    assert trace["normalized_execution_plan"]["value"]["stop_price"] == 96.0
    assert trace["generated_order"]["value"]["qty"] == 1.0
    assert trace["fill_or_rejection_decision"]["value"]["status"] == "filled"
    assert trace["lifecycle_transitions"]["candidate_events"][0]["stage"] == "confirmed"
    assert trace["position_changes"]["value"]["close_reason"] == "EXEC_EXIT_TARGET"
    assert trace["accounting_effects"]["wallet"]["ending_equity"] == 1009.69
    assert trace["accounting_effects"]["fees"]["total_fees"] == 0.31
    assert trace["report_output"]["summary"]["net_pnl"] == 9.69
    assert trace["gap_and_continuity"]["facts"][0]["reason_code"] == "provider_response_empty"
    assert trace["provenance_caveats_and_quality"]["quality_status"] == {
        "data": "degraded",
        "execution": "ready",
    }


def test_golden_disagreement_trace_does_not_attach_unrelated_rows(
    monkeypatch,
    tmp_path,
) -> None:
    left = _dataset("left", [_decision("decision-1")])
    right = _dataset(
        "right",
        [
            _decision(
                "decision-1",
                status="rejected",
                accepted=False,
                reason_code="RULE_BLOCKED",
            )
        ],
    )
    left["candidate_lifecycle"] = {
        "items": [
            {
                "decision_id": "different-decision",
                "symbol": "ETH",
                "stage": "confirmed",
            }
        ]
    }
    left["candle_catalog"] = {
        "items": [
            {
                "symbol": "ETH",
                "timeframe": "1h",
                "fingerprint": "unrelated-candles",
            }
        ]
    }
    monkeypatch.setattr(
        golden,
        "get_run_research_dataset",
        lambda run_id: {"left": left, "right": right}[run_id],
    )
    monkeypatch.setattr(
        golden,
        "_runtime_ordering_summary",
        lambda run_id: {
            "status": "ready",
            "gap_count": 0,
            "duplicate_values": [],
        },
    )

    result = golden.compare_runs(
        "left",
        "right",
        out_dir=tmp_path,
        check_prior=False,
    )
    trace = result["disagreement_trace"]["runs"]["left"]

    assert trace["lifecycle_transitions"]["candidate_events"] == []
    assert trace["input_dataset"]["loaded_ranges"] == []
    assert trace["available_candles"]["catalog"] == []
