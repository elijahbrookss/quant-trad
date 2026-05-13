import uuid

import pytest

pytest.importorskip("fastapi")
pytestmark = pytest.mark.db
from fastapi.testclient import TestClient

from portal.backend.main import app
from portal.backend.service.storage import storage
from tests.helpers.builders.report_storage_builder import (
    build_run_payload,
    build_trade_payload,
    ensure_report_bot,
)


def _iso(ts: str) -> str:
    return ts


def test_reports_list_and_fetch():
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    bot_id = f"bot-{uuid.uuid4().hex[:6]}"
    summary = {
        "net_pnl": 25.0,
        "total_return": 0.025,
        "max_drawdown_pct": 0.05,
        "sharpe": 1.1,
        "total_trades": 2,
    }
    ensure_report_bot(bot_id, name="Test Bot", strategy_id="strategy-1")
    storage.upsert_bot_run(
        build_run_payload(
            run_id=run_id,
            bot_id=bot_id,
            bot_name="Test Bot",
            strategy_id="strategy-1",
            strategy_name="Momentum",
            symbol="BTCUSD",
            timeframe="1h",
            backtest_start=_iso("2024-01-01T00:00:00Z"),
            backtest_end=_iso("2024-01-31T00:00:00Z"),
            summary=summary,
        )
    )
    storage.record_bot_trade(
        build_trade_payload(
            trade_id=f"trade-{uuid.uuid4().hex[:8]}",
            run_id=run_id,
            bot_id=bot_id,
            symbol="BTCUSD",
            direction="long",
            entry_time="2024-01-05T00:00:00Z",
            exit_time="2024-01-06T00:00:00Z",
            gross_pnl=15.0,
            fees_paid=1.0,
            net_pnl=14.0,
        )
    )
    storage.record_bot_trade(
        build_trade_payload(
            trade_id=f"trade-{uuid.uuid4().hex[:8]}",
            run_id=run_id,
            bot_id=bot_id,
            symbol="BTCUSD",
            direction="short",
            entry_time="2024-01-10T00:00:00Z",
            exit_time="2024-01-11T00:00:00Z",
            gross_pnl=12.0,
            fees_paid=1.0,
            net_pnl=11.0,
        )
    )

    client = TestClient(app)
    response = client.get("/api/reports?type=backtest&status=completed")
    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == "report_list.v1"
    run_ids = [item["run_id"] for item in body["items"]]
    assert run_id in run_ids
    item = next(item for item in body["items"] if item["run_id"] == run_id)
    assert item["summary"]["net_pnl"] == pytest.approx(25.0)

    response = client.get(f"/api/reports/{run_id}")
    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "run_research_dataset.v1"
    assert payload["metadata"]["run_id"] == run_id
    assert payload["summary"]["net_pnl"] == pytest.approx(25.0)
    assert "charts" not in payload
    assert payload["sections"]["schema_version"] == "report_sections.v1"
    assert payload["diagnostics"]["schema_version"] == "report_diagnostics.v1"

    response = client.get(f"/api/reports/{run_id}/summary")
    assert response.status_code == 200
    assert response.json()["schema_version"] == "run_report_summary.v1"

    response = client.get(f"/api/reports/{run_id}/readiness")
    assert response.status_code == 200
    readiness = response.json()
    assert readiness["schema_version"] == "report_readiness.v1"
    assert readiness["run_id"] == run_id

    response = client.get(f"/api/reports/{run_id}/trades?limit=1")
    assert response.status_code == 200
    trades_page = response.json()
    assert trades_page["schema_version"] == "trades_dataset.v1"
    assert trades_page["total"] == 2
    assert len(trades_page["items"]) == 1

    response = client.get(f"/api/reports/{run_id}/diagnostics")
    assert response.status_code == 200
    assert response.json()["schema_version"] == "report_diagnostics.v1"

    response = client.get(f"/api/reports/{run_id}/export/manifest")
    assert response.status_code == 200
    manifest = response.json()
    assert manifest["schema_version"] == "export_manifest.v1"
    assert any(entry["path"] == "trades.csv" for entry in manifest["files"])

    response = client.post(f"/api/reports/{run_id}/export", json={})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"

    response = client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert f"/api/reports/{'{'}run_id{'}'}" in paths
    assert f"/api/reports/{'{'}run_id{'}'}/trades" in paths


def test_compare_returns_blocked_result_when_runs_are_not_ready():
    run_a = f"run-{uuid.uuid4().hex[:8]}"
    run_b = f"run-{uuid.uuid4().hex[:8]}"
    bot_id = f"bot-{uuid.uuid4().hex[:6]}"
    ensure_report_bot(bot_id, name="Test Bot", strategy_id="strategy-1")
    for run_id, pnl in ((run_a, 10.0), (run_b, 15.0)):
        storage.upsert_bot_run(
            build_run_payload(
                run_id=run_id,
                bot_id=bot_id,
                bot_name="Test Bot",
                strategy_id="strategy-1",
                strategy_name="Momentum",
                symbol="BTCUSD",
                timeframe="1h",
                summary={"net_pnl": pnl, "total_trades": 0},
            )
        )

    client = TestClient(app)
    response = client.post("/api/reports/compare", json={"run_ids": [run_a, run_b]})

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "run_comparison_result.v1"
    assert payload["status"] == "blocked"
    assert payload["comparisons"] == []
    assert payload["blocked_reasons"]
