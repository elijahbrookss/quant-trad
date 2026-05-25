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


def test_validate_config_returns_nested_whole_indicator_contract(monkeypatch) -> None:
    client = _client()
    calls = []

    def _validate_instance_config(
        type_str,
        name,
        params,
        *,
        dependencies=None,
        color=None,
        color_palette=None,
        output_prefs=None,
    ):
        calls.append(
            {
                "type": type_str,
                "name": name,
                "params": params,
                "dependencies": dependencies,
                "color": color,
                "color_palette": color_palette,
                "output_prefs": output_prefs,
            }
        )
        return {
            "id": "",
            "type": type_str,
            "name": name or "Candle Stats",
            "params": {"warmup_bars": 5},
            "dependencies": dependencies or [],
            "enabled": True,
            "color": color,
            "color_palette": color_palette,
            "datasource": "ALPACA",
            "exchange": None,
            "output_prefs": output_prefs or {},
            "manifest": {"type": type_str, "label": "Candle Stats"},
            "typed_outputs": [{"name": "candle_stats", "type": "metric"}],
            "overlay_outputs": [],
            "runtime_supported": True,
            "compute_supported": False,
        }

    monkeypatch.setattr(controller, "validate_instance_config", _validate_instance_config)

    response = client.post(
        "/api/indicators/validate-config",
        json={
            "type": "candle_stats",
            "name": "ATR Check",
            "params": {"warmup_bars": 5},
            "dependencies": [],
            "output_prefs": {"candle_stats": {"enabled": True}},
            "color": "#00ffaa",
            "color_palette": "cool",
        },
    )

    assert response.status_code == 200
    assert calls == [
        {
            "type": "candle_stats",
            "name": "ATR Check",
            "params": {"warmup_bars": 5},
            "dependencies": [],
            "color": "#00ffaa",
            "color_palette": "cool",
            "output_prefs": {"candle_stats": {"enabled": True}},
        }
    ]
    assert response.json() == {
        "instance": {
            "id": "",
            "type": "candle_stats",
            "name": "ATR Check",
            "params": {"warmup_bars": 5},
            "dependencies": [],
            "enabled": True,
            "color": "#00ffaa",
            "color_palette": "cool",
            "datasource": "ALPACA",
            "exchange": None,
            "output_prefs": {"candle_stats": {"enabled": True}},
        },
        "manifest": {"type": "candle_stats", "label": "Candle Stats"},
        "outputs": {
            "typed": [{"name": "candle_stats", "type": "metric"}],
            "overlays": [],
        },
        "capabilities": {
            "runtime_supported": True,
            "compute_supported": False,
        },
    }
