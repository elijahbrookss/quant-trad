from __future__ import annotations

import io
import json
import zipfile

from portal.backend.service.reports import export_bundle


def _dataset() -> dict:
    diagnostics = {
        "schema_version": "report_diagnostics.v1",
        "run_id": "run-1",
        "items": [{"severity": "warning", "source": "data_quality", "code": "gap", "message": "Gap.", "readiness_impact": "degrades_metrics"}],
        "summary": {"total": 1, "blocking_codes": [], "degraded_codes": ["gap"]},
    }
    return {
        "schema_version": "run_research_dataset.v1",
        "metadata": {"run_id": "run-1"},
        "readiness": {"dataset_ready": True},
        "summary": {"trades": 1},
        "sections": {"items": [{"name": "trades", "available": True, "row_count": 1}]},
        "timeseries": {"items": {"equity_curve": {"items": [{"timestamp": "2026-01-01T00:00:00Z", "value": 100.0}]}}},
        "diagnostics": diagnostics,
        "trades": [{"trade_id": "trade-1", "net_pnl": 1.0}],
        "decisions": [{"decision_id": "decision-1"}],
        "signals": [{"signal_id": "signal-1"}],
        "context": {
            "decision_context": {"items": [{"decision_id": "decision-1"}]},
            "indicator_snapshots": {"items": []},
            "trade_context": {"items": []},
            "market_state": {"items": []},
        },
        "candle_catalog": {
            "items": [
                {
                    "instrument_id": "instrument-btc",
                    "symbol": "BTC",
                    "timeframe": "1h",
                    "start_time": "2026-01-01T00:00:00Z",
                    "end_time": "2026-01-01T01:00:00Z",
                }
            ]
        },
        "fee_accounting": {},
        "wallet_accounting": {},
        "execution": {},
        "candle_gaps": {},
        "portfolio_metrics": {"schema_version": "portfolio_metrics.v1", "sharpe": 1.25},
        "performance": {},
        "operational_health": {"schema_version": "operational_health.v1"},
        "strategy_insights": {},
    }


def test_export_manifest_includes_file_metadata_without_full_dataset(monkeypatch) -> None:
    monkeypatch.setattr(export_bundle, "get_run_research_dataset", lambda _run_id: _dataset())

    manifest = export_bundle.build_export_manifest("run-1")

    files = {entry["path"]: entry for entry in manifest["files"]}
    assert manifest["schema_version"] == "export_manifest.v1"
    assert manifest["dataset_schema_version"] == "run_research_dataset.v1"
    assert "generated_at" in manifest
    assert "run_research_dataset.json" not in files
    assert files["trades.json"]["row_count"] == 1
    assert files["timeseries/equity_curve.json"]["row_count"] == 1
    assert files["candle_catalog.json"]["row_count"] == 1
    assert files["trades.json"]["size_bytes"] > 0
    assert len(files["trades.json"]["sha256"]) == 64
    assert files["trades.csv"]["row_count"] == 1
    assert files["manifest.json"]["format"] == "json"
    metrics_entry = files["metrics.json"]
    assert metrics_entry["section"] == "metrics"
    assert manifest["diagnostics"] == {
        "schema_version": "report_diagnostics.v1",
        "run_id": "run-1",
        "summary": {"total": 1, "blocking_codes": [], "degraded_codes": ["gap"]},
    }


def test_export_archive_manifest_matches_section_files(monkeypatch) -> None:
    monkeypatch.setattr(export_bundle, "get_run_research_dataset", lambda _run_id: _dataset())

    archive_bytes, filename = export_bundle.build_export_archive("run-1")

    assert filename == "run_run-1_report_export.zip"
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        names = set(archive.namelist())
        manifest = json.loads(archive.read("manifest.json"))
        metrics = json.loads(archive.read("metrics.json"))
    manifest_paths = {entry["path"] for entry in manifest["files"]}
    assert "run_research_dataset.json" not in names
    assert manifest_paths == names
    assert metrics["portfolio_metrics"]["sharpe"] == 1.25


def test_research_export_can_include_optional_candle_files(monkeypatch) -> None:
    monkeypatch.setattr(export_bundle, "get_run_research_dataset", lambda _run_id: _dataset())
    monkeypatch.setattr(
        export_bundle,
        "get_candle_dataset",
        lambda *_args, **_kwargs: {
            "items": [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                }
            ]
        },
    )

    archive_bytes, _filename = export_bundle.build_export_archive("run-1", include_candles=True)

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        names = set(archive.namelist())
    assert "candles/instrument-btc_1h.json" in names
    assert "candles/instrument-btc_1h.csv" in names
