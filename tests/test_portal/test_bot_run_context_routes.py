from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from portal.backend.controller import bots as bots_controller
from portal.backend.main import app


def test_bot_run_context_routes_are_compact_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    context = {
        "schema_version": "bot_run_context.v1",
        "bot_id": "bot-1",
        "name": "Research Bot",
        "status": "completed",
        "phase": "completed",
        "can_start": True,
        "can_stop": False,
        "strategy": {
            "strategy_id": "strategy-1",
            "strategy_variant_id": "variant-1",
            "strategy_variant_name": "expanding-only",
            "effective_params": {},
        },
        "execution": {"run_type": "backtest", "symbols": ["BTC"], "timeframe": "1h"},
        "active_run": {"run_id": None},
        "latest_run": {"run_id": "run-1", "summary": {"net_pnl": -1.0}, "report_status": "ready"},
    }
    monkeypatch.setattr(
        bots_controller.bot_service,
        "list_bot_run_contexts",
        lambda: {"schema_version": "bot_run_context_list.v1", "items": [context], "total": 1},
    )
    monkeypatch.setattr(bots_controller.bot_service, "get_bot_run_context", lambda bot_id: {**context, "bot_id": bot_id})
    monkeypatch.setattr(
        bots_controller.bot_service,
        "start_bot_run_context",
        lambda bot_id, request_id=None: {
            "schema_version": "bot_run_start.v1",
            "bot_id": bot_id,
            "run_id": "run-2",
            "request_id": request_id,
            "context": {**context, "bot_id": bot_id},
        },
    )
    monkeypatch.setattr(
        bots_controller.bot_service,
        "get_bot_run_status",
        lambda bot_id, run_id: {
            "schema_version": "bot_run_status.v1",
            "bot_id": bot_id,
            "run_id": run_id,
            "status": "completed",
            "terminal": True,
            "summary": {"net_pnl": -1.0},
            "report": {"status": "ready"},
        },
    )

    client = TestClient(app)

    assert client.get("/api/bots/run-contexts").json()["schema_version"] == "bot_run_context_list.v1"
    assert client.get("/api/bots/bot-1/run-context").json()["schema_version"] == "bot_run_context.v1"
    start = client.post("/api/bots/bot-1/runs/start", json={"request_id": "req-1"}).json()
    assert start["schema_version"] == "bot_run_start.v1"
    assert start["request_id"] == "req-1"
    status = client.get("/api/bots/bot-1/runs/run-2/status").json()
    assert status["schema_version"] == "bot_run_status.v1"
    assert status["terminal"] is True


def test_bot_update_route_exposes_backtest_window_for_experiment_plans(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_update_bot(bot_id: str, **payload):
        observed["bot_id"] = bot_id
        observed["payload"] = payload
        return {
            "id": bot_id,
            "name": "Research Bot",
            "strategy_id": "strategy-1",
            "strategy_variant_id": None,
            "strategy_variant_name": None,
            "atm_template_id": None,
            "risk_config": {},
            "datasource": None,
            "exchange": None,
            "mode": "walk-forward",
            "execution_mode": "fast",
            "run_type": "backtest",
            "playback_speed": 0,
            "backtest_start": payload.get("backtest_start"),
            "backtest_end": payload.get("backtest_end"),
            "wallet_config": {},
            "snapshot_interval_ms": 1000,
            "bot_env": {},
            "instrument_type": None,
            "status": "idle",
        }

    monkeypatch.setattr(bots_controller.bot_service, "update_bot", fake_update_bot)

    client = TestClient(app)
    response = client.put(
        "/api/bots/bot-1",
        json={"backtest_start": "2026-01-01T00:00:00Z", "backtest_end": "2026-01-31T23:59:59Z"},
    )

    assert response.status_code == 200
    assert observed == {
        "bot_id": "bot-1",
        "payload": {"backtest_start": "2026-01-01T00:00:00Z", "backtest_end": "2026-01-31T23:59:59Z"},
    }


def test_bot_data_preflight_route_returns_compact_candle_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_preflight_bot_data(bot_id: str, *, start: str, end: str):
        observed["bot_id"] = bot_id
        observed["start"] = start
        observed["end"] = end
        return {
            "schema_version": "bot_data_preflight.v1",
            "bot_id": bot_id,
            "status": "warning",
            "checks": [
                {
                    "schema_version": "candle_coverage_preflight.v1",
                    "instrument_id": "inst-1",
                    "symbol": "BTC/USD",
                    "provider": "kraken",
                    "exchange": "spot",
                    "timeframe": "1h",
                    "status": "warning",
                    "severity": "warning",
                    "missing_ranges": [{"start": start, "end": end}],
                }
            ],
        }

    monkeypatch.setattr(bots_controller.bot_service, "preflight_bot_data", fake_preflight_bot_data)

    client = TestClient(app)
    response = client.post(
        "/api/bots/bot-1/data-preflight",
        json={"start": "2026-01-01T00:00:00Z", "end": "2026-01-31T23:59:59Z"},
    )

    assert response.status_code == 200
    assert response.json()["schema_version"] == "bot_data_preflight.v1"
    assert response.json()["checks"][0]["missing_ranges"]
    assert observed == {
        "bot_id": "bot-1",
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-31T23:59:59Z",
    }
