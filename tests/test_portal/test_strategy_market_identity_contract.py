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


@pytest.mark.parametrize(
    "path,payload",
    [
        (
            "/api/strategies/",
            {
                "name": "Canonical strategy",
                "timeframe": "1h",
                "datasource": "CCXT",
                "exchange": "coinbase",
                "provider_id": "COINBASE",
            },
        ),
        (
            "/api/strategies/strategy-1",
            {"venue_id": "COINBASE_DIRECT"},
        ),
        (
            "/api/strategies/presets/symbols",
            {
                "label": "BTC",
                "timeframe": "1h",
                "symbol": "BTC/USD",
                "provider_id": "COINBASE",
            },
        ),
    ],
)
def test_strategy_writes_reject_provider_venue_aliases(
    path: str,
    payload: dict[str, object],
) -> None:
    client = _client()
    method = client.put if path.endswith("strategy-1") else client.post

    response = method(path, json=payload)

    assert response.status_code == 422
    assert any(
        error["type"] == "extra_forbidden"
        for error in response.json()["detail"]
    )


def test_strategy_read_contract_emits_only_canonical_market_identity() -> None:
    payload = controller._build_strategy_definition(
        {
            "id": "strategy-1",
            "name": "Canonical strategy",
            "timeframe": "1h",
            "datasource": "CCXT",
            "exchange": "coinbase",
            "provider_id": "COINBASE",
            "venue_id": "COINBASE_DIRECT",
        }
    )

    assert payload["strategy"]["datasource"] == "CCXT"
    assert payload["strategy"]["exchange"] == "coinbase"
    assert "provider_id" not in payload["strategy"]
    assert "venue_id" not in payload["strategy"]


def test_strategy_create_passes_canonical_market_identity_to_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def create_strategy(name: str, **kwargs: object) -> dict[str, object]:
        observed.update({"name": name, **kwargs})
        return {
            "id": "strategy-1",
            "name": name,
            "timeframe": kwargs["timeframe"],
            "datasource": kwargs["datasource"],
            "exchange": kwargs["exchange"],
        }

    monkeypatch.setattr(
        controller.strategy_service,
        "create_strategy",
        create_strategy,
    )

    response = _client().post(
        "/api/strategies/",
        json={
            "name": "Canonical strategy",
            "timeframe": "1h",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )

    assert response.status_code == 201
    assert observed["datasource"] == "CCXT"
    assert observed["exchange"] == "coinbase"
    assert response.json()["strategy"]["datasource"] == "CCXT"
    assert response.json()["strategy"]["exchange"] == "coinbase"
    assert "provider_id" not in response.json()["strategy"]
    assert "venue_id" not in response.json()["strategy"]
