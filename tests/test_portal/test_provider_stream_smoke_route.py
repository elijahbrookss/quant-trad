from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import providers as controller


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(controller.router, prefix="/api/providers")
    return TestClient(app)


def test_provider_stream_smoke_route_returns_backend_summary(monkeypatch) -> None:
    async def fake_smoke(**kwargs):
        assert kwargs["provider_id"] == "COINBASE"
        assert kwargs["venue_id"] == "COINBASE_DIRECT"
        assert kwargs["symbol"] == "BIP-20DEC30-CDE"
        return {
            "schema_version": "provider_stream_smoke.v1",
            "status": "completed",
            "counts": {"market_ticker": 1},
        }

    monkeypatch.setattr(controller, "run_provider_stream_smoke", fake_smoke)

    response = _client().post(
        "/api/providers/stream-smoke",
        json={"provider_id": "COINBASE", "venue_id": "COINBASE_DIRECT", "symbol": "BIP-20DEC30-CDE"},
    )

    assert response.status_code == 200
    assert response.json()["counts"] == {"market_ticker": 1}
