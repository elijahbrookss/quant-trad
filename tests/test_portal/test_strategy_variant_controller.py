from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import strategies as controller


def _variant_payload(
    *,
    variant_id: str = "variant-1",
    strategy_id: str = "strategy-1",
    name: str = "aggressive",
    description: str | None = None,
    param_overrides: dict[str, Any] | None = None,
    atm_template_id: str | None = None,
    is_default: bool = False,
) -> dict[str, Any]:
    return {
        "id": variant_id,
        "strategy_id": strategy_id,
        "name": name,
        "description": description,
        "param_overrides": dict(param_overrides or {"conviction_min": 0.5}),
        "atm_template_id": atm_template_id,
        "is_default": is_default,
        "created_at": "2026-04-04T00:00:00Z",
        "updated_at": "2026-04-04T00:00:00Z",
    }


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(controller.router, prefix="/api/strategies")
    return TestClient(app)


def test_strategy_variant_crud_routes_are_thin_service_wrappers(monkeypatch) -> None:
    client = _client()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        controller.strategy_service,
        "list_strategy_variants",
        lambda strategy_id: [_variant_payload(strategy_id=strategy_id)],
    )

    def _create(strategy_id: str, **payload: Any) -> dict[str, Any]:
        captured["create"] = {"strategy_id": strategy_id, **payload}
        return _variant_payload(strategy_id=strategy_id, **payload)

    def _update(strategy_id: str, variant_id: str, **payload: Any) -> dict[str, Any]:
        captured["update"] = {
            "strategy_id": strategy_id,
            "variant_id": variant_id,
            **payload,
        }
        return _variant_payload(
            variant_id=variant_id,
            strategy_id=strategy_id,
            name=payload.get("name", "aggressive"),
            description=payload.get("description"),
            param_overrides=payload.get("param_overrides"),
            atm_template_id=payload.get("atm_template_id"),
            is_default=payload.get("is_default", False),
        )

    def _delete(strategy_id: str, variant_id: str) -> None:
        captured["delete"] = {"strategy_id": strategy_id, "variant_id": variant_id}

    monkeypatch.setattr(controller.strategy_service, "create_strategy_variant", _create)
    monkeypatch.setattr(controller.strategy_service, "update_strategy_variant", _update)
    monkeypatch.setattr(controller.strategy_service, "delete_strategy_variant", _delete)

    response = client.get("/api/strategies/strategy-1/variants")
    assert response.status_code == 200
    assert response.json()[0]["name"] == "aggressive"

    response = client.post(
        "/api/strategies/strategy-1/variants",
        json={
            "name": "aggressive",
            "description": "Looser threshold",
            "param_overrides": {"conviction_min": 0.5},
            "atm_template_id": "atm-fast",
            "is_default": False,
        },
    )
    assert response.status_code == 201
    assert captured["create"] == {
        "strategy_id": "strategy-1",
        "name": "aggressive",
        "description": "Looser threshold",
        "param_overrides": {"conviction_min": 0.5},
        "atm_template_id": "atm-fast",
        "is_default": False,
    }

    response = client.put(
        "/api/strategies/strategy-1/variants/variant-1",
        json={
            "description": "Updated",
            "param_overrides": {"conviction_min": 0.55},
            "atm_template_id": "atm-slower",
        },
    )
    assert response.status_code == 200
    assert captured["update"] == {
        "strategy_id": "strategy-1",
        "variant_id": "variant-1",
        "description": "Updated",
        "param_overrides": {"conviction_min": 0.55},
        "atm_template_id": "atm-slower",
    }

    response = client.delete("/api/strategies/strategy-1/variants/variant-1")
    assert response.status_code == 204
    assert captured["delete"] == {
        "strategy_id": "strategy-1",
        "variant_id": "variant-1",
    }


def test_strategy_variant_delete_returns_400_for_default_variant_guard(monkeypatch) -> None:
    client = _client()

    def _delete(_strategy_id: str, _variant_id: str) -> None:
        raise ValueError("Default strategy variant cannot be deleted")

    monkeypatch.setattr(controller.strategy_service, "delete_strategy_variant", _delete)

    response = client.delete("/api/strategies/strategy-1/variants/default-id")

    assert response.status_code == 400
    assert "Default strategy variant cannot be deleted" in str(response.json()["detail"])


def test_strategy_preview_and_compile_routes_forward_variant_id(monkeypatch) -> None:
    client = _client()
    captured: dict[str, Any] = {}

    def _preview(strategy_id: str, **payload: Any) -> dict[str, Any]:
        captured["preview"] = {"strategy_id": strategy_id, **payload}
        return {"preview_id": "preview-1"}

    def _compile(strategy_id: str, **payload: Any) -> dict[str, Any]:
        captured["compile"] = {"strategy_id": strategy_id, **payload}
        return {"strategy_id": strategy_id, "variant": {"id": payload.get("variant_id")}}

    monkeypatch.setattr(controller.strategy_service, "run_strategy_preview", _preview)
    monkeypatch.setattr(controller.strategy_service, "compile_strategy_contract", _compile)

    response = client.post(
        "/api/strategies/strategy-1/preview",
        json={
            "start": "2026-02-01T00:00:00Z",
            "end": "2026-02-01T01:00:00Z",
            "interval": "1h",
            "instrument_ids": ["instrument-1"],
            "variant_id": "variant-1",
        },
    )
    assert response.status_code == 200
    assert captured["preview"] == {
        "strategy_id": "strategy-1",
        "start": "2026-02-01T00:00:00Z",
        "end": "2026-02-01T01:00:00Z",
        "interval": "1h",
        "instrument_ids": ["instrument-1"],
        "variant_id": "variant-1",
    }

    response = client.post(
        "/api/strategies/strategy-1/compile",
        json={"variant_id": "variant-1"},
    )
    assert response.status_code == 200
    assert captured["compile"] == {
        "strategy_id": "strategy-1",
        "variant_id": "variant-1",
    }
