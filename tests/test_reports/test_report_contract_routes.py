from __future__ import annotations

pytestmark = []

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from portal.backend.controller import reports as reports_controller
from portal.backend.main import app


def _dataset() -> dict:
    diagnostics = {
        "schema_version": "report_diagnostics.v1",
        "run_id": "run-1",
        "items": [],
        "summary": {"total": 0},
    }
    return {
        "schema_version": "run_research_dataset.v1",
        "metadata": {"run_id": "run-1", "symbols": ["BTC"], "timeframe": "1h"},
        "readiness": {
            "dataset_ready": True,
            "results_ready": True,
            "safe_to_compare": False,
            "reason": "comparison_blocked",
            "conditions": {},
            "export_status": "available",
            "dataset_status": "ready",
            "caveats": [],
        },
        "summary": {"net_pnl": 10.0, "closed_trades": 1},
        "sections": {"schema_version": "report_sections.v1", "items": []},
        "timeseries": {"schema_version": "report_timeseries.v1", "items": {}},
        "diagnostics": diagnostics,
        "decisions": [],
        "signals": [],
        "trades": [{"trade_id": "trade-1", "symbol": "BTC", "net_pnl": 10.0}],
        "context": {"schema_version": "report_context.v1"},
        "candle_catalog": {"schema_version": "candle_catalog.v1", "run_id": "run-1", "items": []},
        "fee_accounting": {},
        "wallet_accounting": {},
        "execution": {},
        "candle_gaps": {},
        "portfolio_metrics": {"schema_version": "portfolio_metrics.v1", "sharpe": 1.25},
        "performance": {},
        "operational_health": {"schema_version": "operational_health.v1", "run_id": "run-1"},
        "strategy_insights": {},
        "narrative_summary": "Run summary.",
    }


def test_report_contract_routes_expose_canonical_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reports_controller, "_get_run_research_dataset", lambda _run_id: _dataset())
    monkeypatch.setattr(
        reports_controller,
        "_get_report_readiness",
        lambda run_id: {"schema_version": "report_readiness.v1", "run_id": run_id, **_dataset()["readiness"], "diagnostics": _dataset()["diagnostics"]},
    )
    monkeypatch.setattr(
        reports_controller,
        "_get_run_report_summary",
        lambda run_id: {
            "schema_version": "run_report_summary.v1",
            "run_id": run_id,
            "metadata": _dataset()["metadata"],
            "readiness": _dataset()["readiness"],
            "summary": _dataset()["summary"],
            "portfolio_metrics": _dataset()["portfolio_metrics"],
            "sections": _dataset()["sections"],
        },
    )
    monkeypatch.setattr(reports_controller, "_get_report_sections", lambda _run_id: _dataset()["sections"])
    monkeypatch.setattr(reports_controller, "_get_report_diagnostics", lambda _run_id: _dataset()["diagnostics"])
    monkeypatch.setattr(reports_controller, "_get_report_metrics", lambda run_id: {"schema_version": "report_metrics.v1", "run_id": run_id})
    monkeypatch.setattr(reports_controller, "_get_operational_health", lambda run_id: {"schema_version": "operational_health.v1", "run_id": run_id})
    monkeypatch.setattr(
        reports_controller,
        "_get_timeseries_dataset",
        lambda run_id, section, **_kwargs: {
            "schema_version": "timeseries.equity_curve_dataset.v1",
            "run_id": run_id,
            "section": f"timeseries.{section}",
            "limit": 100,
            "offset": 0,
            "total": 1,
            "items": [{"timestamp": "2026-01-01T00:00:00Z", "value": 100.0}],
        },
    )
    monkeypatch.setattr(
        reports_controller,
        "_get_context_dataset",
        lambda run_id, **_kwargs: {
            "schema_version": "context.decision_context_dataset.v1",
            "run_id": run_id,
            "section": "context.decision_context",
            "limit": 100,
            "offset": 0,
            "total": 1,
            "items": [{"decision_id": "decision-1"}],
        },
    )
    monkeypatch.setattr(reports_controller, "_get_candle_catalog", lambda run_id: {"schema_version": "candle_catalog.v1", "run_id": run_id, "items": [], "caveats": []})
    monkeypatch.setattr(
        reports_controller,
        "_get_metric_explanation",
        lambda run_id, metric_name: {
            "schema_version": "metric_explanation.v1",
            "run_id": run_id,
            "metric_name": metric_name,
            "value": 1.25,
            "unit": "ratio",
            "formula": {"version": "v1", "description": "test"},
            "source_sections": ["portfolio_metrics"],
            "source_refs": [],
            "availability": "available",
            "caveats": [],
        },
    )
    monkeypatch.setattr(
        reports_controller,
        "_get_trade_dataset",
        lambda run_id, **_kwargs: {
            "schema_version": "trades_dataset.v1",
            "run_id": run_id,
            "section": "trades",
            "limit": 100,
            "offset": 0,
            "total": 1,
            "items": _dataset()["trades"],
        },
    )

    client = TestClient(app)
    response = client.get("/api/reports/run-1")
    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "run_research_dataset.v1"
    assert "charts" not in payload
    assert "tables" not in payload
    assert "decision_ledger" not in payload

    assert client.get("/api/reports/run-1/readiness").json()["schema_version"] == "report_readiness.v1"
    assert client.get("/api/reports/run-1/summary").json()["schema_version"] == "run_report_summary.v1"
    assert client.get("/api/reports/run-1/summary").json()["portfolio_metrics"]["sharpe"] == 1.25
    assert client.get("/api/reports/run-1/sections").json()["schema_version"] == "report_sections.v1"
    assert client.get("/api/reports/run-1/diagnostics").json()["schema_version"] == "report_diagnostics.v1"
    assert client.get("/api/reports/run-1/metrics").json()["schema_version"] == "report_metrics.v1"
    assert client.get("/api/reports/run-1/operational-health").json()["schema_version"] == "operational_health.v1"
    assert client.get("/api/reports/run-1/trades").json()["schema_version"] == "trades_dataset.v1"
    assert client.get("/api/reports/run-1/timeseries/equity_curve").json()["section"] == "timeseries.equity_curve"
    assert client.get("/api/reports/run-1/context").json()["section"] == "context.decision_context"
    assert client.get("/api/reports/run-1/candles/catalog").json()["schema_version"] == "candle_catalog.v1"
    assert client.get("/api/reports/run-1/metrics/sharpe/explanation").json()["value"] == 1.25


def test_report_compare_contract_blocks_without_deltas(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        reports_controller,
        "_compare_run_datasets",
        lambda _run_ids: {
            "schema_version": "run_comparison_result.v1",
            "status": "blocked",
            "run_ids": ["run-1", "run-2"],
            "baseline_run_id": "run-1",
            "dataset_schema_version": "run_research_dataset.v1",
            "readiness": {},
            "compatibility": {"timeframe_match": False},
            "blocked_reasons": [{"code": "timeframe_match", "message": "Compatibility check failed: timeframe_match."}],
            "reports": [],
            "comparisons": [],
        },
    )

    client = TestClient(app)
    response = client.post("/api/reports/compare", json={"run_ids": ["run-1", "run-2"]})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "blocked"
    assert payload["comparisons"] == []
    assert payload["blocked_reasons"]


def test_report_export_contract_uses_manifest_and_zip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        reports_controller,
        "build_export_manifest",
        lambda _run_id, **_kwargs: {
            "schema_version": "export_manifest.v1",
            "run_id": "run-1",
            "dataset_schema_version": "run_research_dataset.v1",
            "status": "ready",
            "filename": "run_run-1_report_export.zip",
            "files": [{"path": "trades.csv"}],
            "unavailable_sections": [],
            "diagnostics": {},
        },
    )
    monkeypatch.setattr(
        reports_controller,
        "build_export_archive",
        lambda _run_id, **_kwargs: (b"zip-bytes", "run_run-1_report_export.zip"),
    )

    client = TestClient(app)
    manifest = client.get("/api/reports/run-1/export/manifest")
    assert manifest.status_code == 200
    assert manifest.json()["schema_version"] == "export_manifest.v1"

    bundle = client.post("/api/reports/run-1/export", json={})
    assert bundle.status_code == 200
    assert bundle.content == b"zip-bytes"
