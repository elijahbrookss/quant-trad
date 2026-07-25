from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from portal.backend.controller import candles as candles_controller
from portal.backend.controller import instruments as instruments_controller
from portal.backend.main import app


def test_candle_coverage_route_resolves_symbol_to_instrument(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_require_instrument_id(datasource: str | None, exchange: str | None, symbol: str | None) -> str:
        observed["resolve"] = {"datasource": datasource, "exchange": exchange, "symbol": symbol}
        return "inst-btc"

    def fake_preflight(instrument_id: str, start: str, end: str, interval: str):
        observed["preflight"] = {
            "instrument_id": instrument_id,
            "start": start,
            "end": end,
            "interval": interval,
        }
        return {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": instrument_id,
            "status": "ok",
        }

    monkeypatch.setattr(candles_controller.instrument_service, "require_instrument_id", fake_require_instrument_id)
    monkeypatch.setattr(candles_controller, "preflight_candle_coverage_by_instrument", fake_preflight)

    response = TestClient(app).post(
        "/api/candles/coverage",
        json={
            "symbol": "BTC/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            "timeframe": "1h",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert observed == {
        "resolve": {"datasource": "CCXT", "exchange": "coinbase", "symbol": "BTC/USD"},
        "preflight": {
            "instrument_id": "inst-btc",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            "interval": "1h",
        },
    }


def test_instrument_coverage_matrix_filters_and_summarizes(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        {
            "id": "btc-spot",
            "symbol": "BTC/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
            "instrument_type": "spot",
            "research_ready": True,
            "runtime_ready": True,
            "runtime_policy": "proxy_derivative",
            "execution_semantics": "proxy_derivative",
        },
        {
            "id": "eth-spot",
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
            "instrument_type": "spot",
            "research_ready": True,
            "runtime_ready": False,
            "runtime_policy": "spot",
            "execution_semantics": "spot",
        },
    ]
    observed = []

    def fake_preflight(instrument_id: str, start: str, end: str, interval: str):
        observed.append(
            {
                "instrument_id": instrument_id,
                "start": start,
                "end": end,
                "interval": interval,
            }
        )
        return {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": instrument_id,
            "severity": "ok",
        }

    monkeypatch.setattr(instruments_controller.instrument_service, "list_instruments", lambda: records)
    monkeypatch.setattr(instruments_controller.instrument_service, "instrument_api_payload", lambda record: record)
    monkeypatch.setattr(instruments_controller, "preflight_candle_coverage_by_instrument", fake_preflight)

    response = TestClient(app).post(
        "/api/instruments/coverage-matrix",
        json={
            "start": "1767225600000",
            "end": "1767312000000",
            "timeframe": "1h",
            "symbol": "btc/usd",
            "datasource": "ccxt",
            "exchange": "coinbase",
            "instrument_type": "SPOT",
            "runtime_ready": True,
            "research_ready": True,
            "execution_semantics": "proxy_derivative",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "instrument_coverage_matrix.v1"
    assert payload["requested_window"] == {
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-01-02T00:00:00Z",
        "timeframe": "1h",
    }
    assert payload["summary"] == {"instrument_count": 1, "severity_counts": {"ok": 1}}
    assert payload["items"][0]["instrument"]["id"] == "btc-spot"
    assert payload["items"][0]["coverage"]["instrument_id"] == "btc-spot"
    assert observed == [
        {
            "instrument_id": "btc-spot",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            "interval": "1h",
        }
    ]
