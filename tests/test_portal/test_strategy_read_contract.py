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


def test_get_strategy_returns_nested_detail_contract(monkeypatch) -> None:
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
            "indicators": [{"id": "indicator-1", "status": "active", "meta": {"id": "indicator-1"}}],
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
    monkeypatch.setattr(
        controller.strategy_service,
        "list_strategy_variants",
        lambda strategy_id: [
            {
                "id": "variant-1",
                "strategy_id": strategy_id,
                "name": "default",
                "description": None,
                "param_overrides": {},
                "atm_template_id": None,
                "is_default": True,
                "created_at": "2026-04-05T00:00:00Z",
                "updated_at": "2026-04-05T00:00:00Z",
            }
        ],
    )

    response = client.get("/api/strategies/strategy-1")

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "strategy": {
            "id": "strategy-1",
            "name": "Breakout",
            "description": "Strategy description",
            "timeframe": "5m",
            "datasource": "ALPACA",
            "exchange": "cme",
            "provider_id": "ALPACA",
            "venue_id": "cme",
            "atm_template_id": "atm-1",
            "atm_template": {"name": "ATM"},
            "risk_config": {"base_risk_per_trade": 100.0},
            "created_at": "2026-04-05T00:00:00Z",
            "updated_at": "2026-04-05T00:00:00Z",
        },
        "bindings": {
            "symbols": ["ES"],
            "instrument_slots": [{"symbol": "ES"}],
            "instruments": [{"symbol": "ES", "id": "instrument-1"}],
            "indicator_ids": ["indicator-1"],
            "indicators": [{"id": "indicator-1", "status": "active", "meta": {"id": "indicator-1"}}],
        },
        "decision": {
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
            ]
        },
        "read_context": {
            "missing_indicators": [],
            "instrument_messages": [],
        },
        "variants": [
            {
                "id": "variant-1",
                "strategy_id": "strategy-1",
                "name": "default",
                "description": None,
                "param_overrides": {},
                "atm_template_id": None,
                "is_default": True,
                "created_at": "2026-04-05T00:00:00Z",
                "updated_at": "2026-04-05T00:00:00Z",
            }
        ],
    }
