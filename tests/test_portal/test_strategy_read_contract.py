from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import strategies as controller


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(controller.router, prefix="/api/strategies")
    return TestClient(app)


def test_strategy_read_routes_return_split_contracts(monkeypatch) -> None:
    client = _client()

    monkeypatch.setattr(
        controller.strategy_service,
        "get_strategy",
        lambda strategy_id: {
            "id": strategy_id,
            "name": "Breakout",
            "description": "Strategy description",
            "symbols": ["ES"],
            "instrument_slots": [{"symbol": "ES"}],
            "timeframe": "5m",
            "datasource": "ALPACA",
            "exchange": "cme",
            "indicator_ids": ["indicator-1"],
            "indicators": [
                {
                    "id": "indicator-1",
                    "status": "active",
                    "meta": {
                        "id": "indicator-1",
                        "type": "market_profile",
                        "name": "Profile",
                        "typed_outputs": [
                            {"name": "confirmed_balance_breakout", "type": "signal"},
                            {"name": "value_location", "type": "context"},
                            {"name": "value_area_metrics", "type": "metric"},
                        ],
                        "runtime_supported": True,
                        "compute_supported": False,
                    },
                }
            ],
            "missing_indicators": [],
            "instruments": [{"symbol": "ES", "id": "instrument-1"}],
            "instrument_messages": [],
            "rules": [
                {
                    "id": "rule-1",
                    "name": "Breakout Long",
                    "intent": "enter_long",
                    "priority": 1,
                    "trigger": {"type": "signal_match"},
                    "guards": [],
                    "description": None,
                    "enabled": True,
                    "created_at": "2026-04-05T00:00:00Z",
                    "updated_at": "2026-04-05T00:00:00Z",
                }
            ],
            "atm_template": {"name": "ATM"},
            "atm_template_id": "atm-1",
            "risk_config": {"base_risk_per_trade": 100.0},
            "created_at": "2026-04-05T00:00:00Z",
            "updated_at": "2026-04-05T00:00:00Z",
        },
    )
    definition_response = client.get("/api/strategies/strategy-1")
    bindings_response = client.get("/api/strategies/strategy-1/bindings")
    rules_response = client.get("/api/strategies/strategy-1/rules")

    assert definition_response.status_code == 200
    assert bindings_response.status_code == 200
    assert rules_response.status_code == 200
    assert definition_response.json() == {
        "schema_version": "strategy_definition.v1",
        "strategy": {
            "id": "strategy-1",
            "name": "Breakout",
            "description": "Strategy description",
            "timeframe": "5m",
            "datasource": "ALPACA",
            "exchange": "cme",
            "atm_template_id": "atm-1",
            "atm_template": {"name": "ATM"},
            "risk_config": {"base_risk_per_trade": 100.0},
            "created_at": "2026-04-05T00:00:00Z",
            "updated_at": "2026-04-05T00:00:00Z",
        },
        "read_context": {
            "missing_indicators": [],
            "instrument_messages": [],
        },
        "counts": {"instrument_count": 1, "indicator_count": 1, "rule_count": 1},
    }
    assert bindings_response.json()["schema_version"] == "strategy_bindings.v1"
    assert bindings_response.json()["bindings"]["indicators"] == [
        {
            "id": "indicator-1",
            "status": "active",
            "type": "market_profile",
            "name": "Profile",
            "runtime_supported": True,
            "compute_supported": False,
            "output_counts": {"signal": 1, "context": 1, "metric": 1, "other": 0},
        }
    ]
    assert rules_response.json()["schema_version"] == "strategy_rules.v1"
    assert rules_response.json()["total"] == 1
