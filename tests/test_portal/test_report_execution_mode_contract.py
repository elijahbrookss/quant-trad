from __future__ import annotations

from portal.backend.service.reports import contract


def _dataset() -> dict:
    warning = {
        "warning_type": "execution_intrabar_fallback_pessimistic",
        "symbol": "BTCUSDT",
        "timeframe": "5m",
        "message": "FULL execution fell back to pessimistic same-bar policy.",
    }
    diagnostics = {
        "schema_version": "report_diagnostics.v1",
        "run_id": "run-1",
        "items": [
            {
                "severity": "warning",
                "source": "execution",
                "code": "intrabar_fallback_pessimistic",
                "message": warning["message"],
                "affected_identity": {"symbol": "BTCUSDT", "timeframe": "5m"},
                "readiness_impact": "degrades_metrics",
            }
        ],
        "summary": {"total": 1, "by_severity": {"warning": 1}},
    }
    return {
        "schema_version": "run_research_dataset.v1",
        "metadata": {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "strategy_id": "strategy-1",
            "execution_mode": "full",
            "symbols": ["BTCUSDT"],
            "timeframe": "5m",
        },
        "readiness": {
            "dataset_ready": True,
            "results_ready": True,
            "safe_to_compare": True,
            "reason": "ready",
            "conditions": {"export_ready": True},
            "export_status": "available",
            "dataset_status": "ready",
            "caveats": [],
        },
        "summary": {"net_pnl": 0.0, "closed_trades": 0},
        "sections": {"schema_version": "report_sections.v1", "items": []},
        "diagnostics": diagnostics,
        "execution": {
            "execution_mode": "full",
            "warnings": [warning],
        },
        "decisions": [],
        "signals": [],
        "trades": [],
        "fee_accounting": {},
        "wallet_accounting": {},
        "candle_gaps": {},
        "portfolio_metrics": {"schema_version": "portfolio_metrics.v1"},
        "performance": {},
        "strategy_insights": {},
        "narrative_summary": "",
    }


def _run_report_dataset() -> dict:
    dataset = _dataset()
    dataset["metadata"] = {
        **dataset["metadata"],
        "status": "completed",
        "run_type": "backtest",
        "strategy_hash": "strategy-hash",
        "config_hash": "config-hash",
        "material_config_hash": "material-config-hash",
        "data_snapshot_hash": "canonical-data-hash",
        "report_semantic_fingerprint": "semantic-fingerprint",
        "report_operational_fingerprint": "operational-fingerprint",
        "simulated_window": {"start": "2026-01-01T00:00:00Z", "end": "2026-05-01T00:00:00Z"},
        "wall_clock_window": {"start": "2026-05-01T00:00:00Z", "end": "2026-05-01T00:10:00Z"},
        "starting_capital": 10000.0,
    }
    dataset["readiness"] = {
        **dataset["readiness"],
        "results_status": "ready",
        "comparison_status": "ready",
        "golden_candidate_status": "certified",
        "repeatability_status": "fingerprinted",
        "semantic_fingerprint": "semantic-fingerprint",
        "operational_fingerprint": "operational-fingerprint",
        "blocking_reasons": [],
        "golden_blocking_reasons": [],
        "degraded_sections": [],
        "unavailable_sections": [],
    }
    dataset["summary"] = {
        "total_decisions": 2,
        "accepted_decisions": 1,
        "rejected_decisions": 1,
        "trades": 1,
        "closed_trades": 1,
        "open_trades": 0,
        "wins": 1,
        "losses": 0,
        "win_rate": 1.0,
        "loss_rate": 0.0,
        "gross_pnl": 15.0,
        "fees": 1.0,
        "net_pnl": 14.0,
        "equity_start": 10000.0,
        "equity_end": 10014.0,
        "return_pct": 0.0014,
        "max_drawdown": 0.0,
        "max_drawdown_pct": 0.0,
        "profit_factor": None,
        "expectancy": 14.0,
        "avg_win": 14.0,
        "avg_loss": None,
        "largest_win": 14.0,
        "largest_loss": None,
        "average_holding_seconds": 3600.0,
        "drawdown_duration_seconds": 0.0,
        "cagr": 0.0042,
        "sharpe": 1.25,
        "sortino": 1.5,
        "calmar": None,
        "exposure_pct": 0.01,
        "unavailable_metrics": {"calmar": "requires_nonzero_drawdown"},
    }
    dataset["timeseries"] = {
        "schema_version": "report_timeseries.v1",
        "items": {
            "returns_series": {"row_count": 3, "items": [{"return": 0.01}, {"return": -0.01}, {"return": 0.02}]},
            "equity_curve": {"row_count": 2, "items": [{"equity": 10000.0}, {"equity": 10014.0}]},
        },
    }
    dataset["signals"] = [{"signal_id": "sig-1"}]
    dataset["decisions"] = [
        {
            "decision_id": "decision-1",
            "symbol": "BTCUSDT",
            "bar_time": "2026-02-01T00:00:00Z",
            "run_seq": 1,
            "accepted": True,
            "rejected": False,
            "action": "enter_long",
            "trade_id": "trade-1",
        },
        {
            "decision_id": "decision-2",
            "symbol": "BTCUSDT",
            "bar_time": "2026-02-02T00:00:00Z",
            "run_seq": 2,
            "accepted": False,
            "rejected": True,
            "action": "enter_long",
            "reason_code": "MARGIN_REJECTED",
        },
    ]
    dataset["trades"] = [
        {
            "trade_id": "trade-1",
            "symbol": "BTCUSDT",
            "entry_time": "2026-02-01T00:00:00Z",
            "exit_time": "2026-02-01T01:00:00Z",
            "gross_pnl": 15.0,
            "fees_paid": 1.0,
            "net_pnl": 14.0,
        }
    ]
    dataset["wallet_accounting"] = {
        "wallet_replay_status": "passed",
        "missing_wallet_trace_count": 0,
        "margin_warnings": [],
        "reservation_leaks": {},
        "wallet_diagnostics": {
            "replay_projection": {
                "balances": {"USD": 10014.0},
                "free_collateral": {"USD": 10014.0},
            }
        },
        "caveats": [],
    }
    dataset["candle_gaps"] = {
        "canonical_evidence_status": "present",
        "blocking_gap_count": 0,
        "provider_gap_count": 0,
        "noncanonical_fact_count": 2,
        "caveats": [],
        "diagnostic_facts": [{"boundary_name": "selected_symbol_snapshot"}],
    }
    dataset["portfolio_metrics"] = {
        "schema_version": "portfolio_metrics.v1",
        "basis": {"risk_free_rate": 0.0},
        "annualization_periods": 365,
        "cagr": 0.0042,
        "sharpe": 1.25,
        "sortino": 1.5,
        "calmar": None,
        "exposure_pct": 0.01,
        "caveats": ["calmar_unavailable"],
    }
    dataset["strategy_insights"] = {
        "per_symbol_performance": [
            {
                "symbol": "BTCUSDT",
                "trades": 1,
                "gross_pnl": 15.0,
                "fees": 1.0,
                "net_pnl": 14.0,
                "win_rate": 1.0,
            }
        ]
    }
    return dataset


def _install_run_report_dataset(monkeypatch, dataset: dict) -> None:
    contract.clear_report_dataset_cache()
    monkeypatch.setattr(contract, "build_run_research_dataset", lambda _run_id: dataset)
    monkeypatch.setattr(
        contract.report_data,
        "list_run_events",
        lambda _run_id: [
            {"run_seq": 1, "payload": {"context": {"run_seq": 1}}},
            {"run_seq": 2, "payload": {"context": {"run_seq": 2}}},
        ],
    )
    monkeypatch.setattr(
        contract.report_data,
        "list_observability_events",
        lambda _run_id, **_kwargs: [
            {
                "event_name": "decision_order_top_waits_merged",
                "observed_at": "2026-05-01T00:10:00Z",
                "details": {
                    "total_wait_ms": 100.0,
                    "wait_count": 1,
                    "max_wait_ms": 100.0,
                    "release_count": 1,
                    "fail_count": 0,
                    "top_waits": [
                        {
                            "candidate_id": "candidate-1",
                            "decision_id": "decision-1",
                            "candidate_symbol": "BTCUSDT",
                            "candidate_timeframe": "5m",
                            "candidate_bar_time": "2026-02-01T00:00:00Z",
                            "elapsed_wait_ms": 100.0,
                            "poll_count": 4,
                            "final_action": "released",
                            "release_reason": "blocker_advanced",
                            "blocking_participants": [
                                {"participant_key": "ETHUSDT|5m", "symbol": "ETHUSDT", "timeframe": "5m", "next_bar_time": "2026-02-01T00:00:00Z"}
                            ],
                            "release_participants": [
                                {"participant_key": "ETHUSDT|5m", "symbol": "ETHUSDT", "timeframe": "5m", "next_bar_time": "2026-02-01T00:05:00Z"}
                            ],
                            "worker_id": "worker-1",
                        }
                    ],
                },
            }
        ],
    )


def test_run_research_dataset_contract_exposes_execution_mode_and_diagnostics(monkeypatch) -> None:
    monkeypatch.setattr(contract, "build_run_research_dataset", lambda _run_id: _dataset())

    payload = contract.get_run_research_dataset("run-1")

    assert payload["metadata"]["execution_mode"] == "full"
    assert payload["readiness"]["export_status"] == "available"
    assert payload["diagnostics"]["items"][0]["code"] == "intrabar_fallback_pessimistic"


def test_metric_explanation_reads_portfolio_metrics(monkeypatch) -> None:
    dataset = _dataset()
    dataset["portfolio_metrics"] = {"schema_version": "portfolio_metrics.v1", "sharpe": 1.25}
    dataset["trades"] = [{"trade_id": "trade-1", "net_pnl": 10.0}]
    monkeypatch.setattr(contract, "build_run_research_dataset", lambda _run_id: dataset)

    explanation = contract.get_metric_explanation("run-1", "sharpe")

    assert explanation["availability"] == "available"
    assert explanation["value"] == 1.25
    assert "portfolio_metrics" in explanation["source_sections"]


def test_list_report_summaries_exposes_execution_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        contract.report_data,
        "list_runs",
        lambda **_kwargs: [
            {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "bot_name": "Bot",
                "strategy_name": "Strategy",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "status": "completed",
                "ended_at": "2026-01-02T00:00:00Z",
                "summary": {"net_pnl": 1.0, "total_trades": 1},
                "config_snapshot": {"execution_mode": "full"},
            }
        ],
    )
    monkeypatch.setattr(
        contract.report_data,
        "get_result_readiness",
        lambda *_args, **_kwargs: {
            "dataset_ready": True,
            "results_ready": True,
            "safe_to_compare": True,
            "reason": "ready",
            "dataset_status": "ready",
            "export_status": "available",
        },
    )

    payload = contract.list_report_summaries()

    assert payload["items"][0]["execution_mode"] == "full"
    assert payload["items"][0]["readiness"]["results_ready"] is True
    assert payload["items"][0]["readiness"]["safe_to_compare"] is True


def test_report_contract_reuses_dataset_build_for_burst(monkeypatch) -> None:
    calls = {"count": 0}

    def build(run_id: str) -> dict:
        calls["count"] += 1
        payload = _dataset()
        payload["metadata"] = {**payload["metadata"], "run_id": run_id}
        payload["diagnostics"] = {**payload["diagnostics"], "run_id": run_id}
        return payload

    monkeypatch.setattr(contract, "build_run_research_dataset", build)
    contract.clear_report_dataset_cache()

    try:
        readiness = contract.get_report_readiness("run-cache")
        summary = contract.get_run_report_summary("run-cache")
        sections = contract.get_report_sections("run-cache")
    finally:
        contract.clear_report_dataset_cache()

    assert readiness["schema_version"] == "report_readiness.v1"
    assert summary["schema_version"] == "run_report_summary.v1"
    assert sections["schema_version"] == "report_sections.v1"
    assert calls["count"] == 1


def test_run_report_v2_builds_from_existing_dataset(monkeypatch) -> None:
    dataset = _run_report_dataset()
    _install_run_report_dataset(monkeypatch, dataset)

    payload = contract.get_run_report("run-1")

    assert payload["contract_version"] == "run_report_v2"
    assert payload["identity"]["run_id"] == "run-1"
    assert payload["trust"]["research_status"] == "research_valid"
    assert payload["performance"]["net_pnl"]["value"] == 14.0
    assert payload["behavior"]["total_decisions"] == 2
    assert payload["wallet"]["wallet_trace_complete"] is True
    assert payload["symbol_breakdown"][0]["symbol"] == "BTCUSDT"
    assert payload["coordinator_waits"]["status"] == "available"
    assert payload["operational_diagnostics"]["operational_fingerprint"] == "operational-fingerprint"


def test_run_report_v2_metric_values_include_validity_metadata(monkeypatch) -> None:
    dataset = _run_report_dataset()
    _install_run_report_dataset(monkeypatch, dataset)

    performance = contract.get_run_report("run-1")["performance"]

    assert performance["sharpe"]["valid"] is True
    assert performance["sharpe"]["method"]
    assert performance["sharpe"]["metadata"]["frequency"] == "daily"
    assert performance["sharpe"]["metadata"]["annualization_factor"] == 365
    assert performance["sharpe"]["minimum_sample_count"] == 2
    assert performance["sortino"]["valid"] is True
    assert performance["calmar"]["valid"] is False
    assert performance["calmar"]["invalid_reason"] == "requires_cagr_and_nonzero_drawdown"
    assert performance["unrealized_pnl"]["valid"] is False
    assert performance["unrealized_pnl"]["invalid_reason"] == "not_modeled"
    assert performance["slippage"]["invalid_reason"] == "not_modeled"


def test_run_report_v2_trust_fields_are_backend_computed(monkeypatch) -> None:
    dataset = _run_report_dataset()
    _install_run_report_dataset(monkeypatch, dataset)

    trust = contract.get_run_report("run-1")["trust"]

    assert trust["readiness_status"] == "ready"
    assert trust["semantic_fingerprint"] == "semantic-fingerprint"
    assert trust["operational_fingerprint"] == "operational-fingerprint"
    assert trust["data_snapshot_hash"] == "canonical-data-hash"
    assert trust["runtime_ordering_status"] == "gapless"
    assert trust["wallet_market_time_overtake_count"] == 0
    assert trust["entry_decision_order_timeout_count"] == 0


def test_run_report_v2_observer_diagnostics_stay_non_material(monkeypatch) -> None:
    dataset = _run_report_dataset()
    _install_run_report_dataset(monkeypatch, dataset)

    payload = contract.get_run_report("run-1")

    assert payload["trust"]["data_snapshot_hash"] == "canonical-data-hash"
    assert payload["trust"]["semantic_fingerprint"] == "semantic-fingerprint"
    assert payload["trust"]["observer_invariance_status"] == "observer_diagnostics_ignored"
    assert payload["coordinator_waits"]["top_waits"][0]["blocker_symbols"] == ["ETHUSDT"]
