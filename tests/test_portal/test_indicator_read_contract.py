from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import indicators as controller


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(controller.router, prefix="/api/indicators")
    return TestClient(app)


def test_get_indicator_returns_nested_whole_indicator_contract(monkeypatch) -> None:
    client = _client()

    monkeypatch.setattr(
        controller,
        "get_instance_meta",
        lambda inst_id: {
            "id": inst_id,
            "type": "market_profile",
            "name": "Profile",
            "params": {"lookback": 5},
            "dependencies": [{"kind": "instrument", "required": True}],
            "enabled": True,
            "color": "#fff",
            "color_palette": "warm",
            "datasource": "ALPACA",
            "exchange": "cme",
            "output_prefs": {"balance_breakout": {"enabled": True}},
            "manifest": {
                "type": "market_profile",
                "label": "Market Profile",
                "outputs": [{"name": "balance_breakout", "type": "signal"}],
            },
            "typed_outputs": [{"name": "balance_breakout", "type": "signal", "enabled": True}],
            "overlay_outputs": [{"name": "value_area", "kind": "band"}],
            "runtime_supported": True,
            "compute_supported": True,
        },
    )

    response = client.get("/api/indicators/indicator-1")

    assert response.status_code == 200
    assert response.json() == {
        "instance": {
            "id": "indicator-1",
            "type": "market_profile",
            "name": "Profile",
            "params": {"lookback": 5},
            "dependencies": [{"kind": "instrument", "required": True}],
            "enabled": True,
            "color": "#fff",
            "color_palette": "warm",
            "datasource": "ALPACA",
            "exchange": "cme",
            "output_prefs": {"balance_breakout": {"enabled": True}},
        },
        "manifest": {
            "type": "market_profile",
            "label": "Market Profile",
            "outputs": [{"name": "balance_breakout", "type": "signal"}],
        },
        "outputs": {
            "typed": [{"name": "balance_breakout", "type": "signal", "enabled": True}],
            "overlays": [{"name": "value_area", "kind": "band"}],
        },
        "capabilities": {
            "runtime_supported": True,
            "compute_supported": True,
        },
    }
