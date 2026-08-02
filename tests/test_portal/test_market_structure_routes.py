from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import market_data as controller


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(controller.router, prefix="/api/market-data")
    return TestClient(app)


def test_market_structure_pair_route_never_implies_production(monkeypatch) -> None:
    observed = {}

    def fake_configure(**kwargs):
        observed.update(kwargs)
        return {"pair_id": kwargs["pair_id"], "production_admitted": False}

    monkeypatch.setattr(
        controller.market_structure_service, "configure_pair", fake_configure
    )
    response = _client().post(
        "/api/market-data/market-structure/pairs",
        json={"pair_id": "bip_btc", "auth_mode": "authenticated"},
    )
    assert response.status_code == 200
    assert response.json()["production_admitted"] is False
    assert observed["enable_production"] is False


def test_market_structure_operator_routes_preserve_typed_boundaries(
    monkeypatch,
) -> None:
    async def fake_capture(**kwargs):
        assert kwargs["definition_id"] == "definition-a"
        assert kwargs["duration_seconds"] == 12.0
        return {"schema_version": "market_structure_bounded_capture.v1"}

    monkeypatch.setattr(
        controller.market_structure_service, "capture_bounded", fake_capture
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "replay_manifest",
        lambda **kwargs: {
            "schema_version": "market_structure_manifest_replay.v1",
            "manifest_id": kwargs["manifest_id"],
        },
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "reconcile_recent_trades",
        lambda **kwargs: {
            "schema_version": "market.recent_trade_reconciliation.v1",
            "rest_limit": kwargs["limit"],
            "historical_completeness_claim": "none",
        },
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "archive_status",
        lambda **_kwargs: {
            "schema_version": "market.stream_archive_status.v1",
            "archive_mapping_lag_records": 0,
            "production_admitted": False,
        },
    )
    client = _client()
    capture = client.post(
        "/api/market-data/market-structure/definitions/definition-a/capture",
        json={"duration_seconds": 12},
    )
    status = client.get(
        "/api/market-data/market-structure/definitions/definition-a/status"
    )
    replay = client.post(
        "/api/market-data/market-structure/manifests/manifest-a/replay",
        json={},
    )
    recent = client.post(
        "/api/market-data/market-structure/definitions/definition-a/reconcile-recent",
        params={"limit": 25},
    )
    assert capture.status_code == status.status_code == 200
    assert replay.status_code == recent.status_code == 200
    assert status.json()["archive_mapping_lag_records"] == 0
    assert replay.json()["manifest_id"] == "manifest-a"
    assert recent.json()["historical_completeness_claim"] == "none"
    assert recent.json()["rest_limit"] == 25
