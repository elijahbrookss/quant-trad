from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from portal.backend.controller import candles as candles_controller
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
