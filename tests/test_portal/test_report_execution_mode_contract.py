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
