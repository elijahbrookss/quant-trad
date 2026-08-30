from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import strategies as controller
from portal.backend.service.strategies.strategy_service import facade


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
        (
            "/api/strategies/",
            {
                "name": "Canonical strategy",
                "timeframe": "1h",
                "instrument_slots": [
                    {
                        "symbol": "BTC/USD",
                        "provider_id": "COINBASE",
                    }
                ],
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


@pytest.mark.parametrize(
    "method,path,service_name,payload,forbidden_key",
    [
        (
            "post",
            "/api/strategies/",
            "create_strategy",
            {
                "name": "Rejected create",
                "timeframe": "1h",
                "instrument_slots": [
                    {
                        "symbol": "BTC/USD",
                        "metadata": {"routing": {"provider_id": "COINBASE"}},
                    }
                ],
            },
            "provider_id",
        ),
        (
            "put",
            "/api/strategies/strategy-1",
            "update_strategy",
            {
                "instrument_slots": [
                    {
                        "symbol": "BTC/USD",
                        "metadata": {"routing": [{"venue_id": "DIRECT"}]},
                    }
                ],
            },
            "venue_id",
        ),
        (
            "post",
            "/api/strategies/strategy-1/clone",
            "clone_strategy",
            {
                "name": "Rejected clone",
                "instrument_slots": [
                    {
                        "symbol": "BTC/USD",
                        "metadata": {"routing": {"provider_id": "COINBASE"}},
                    }
                ],
            },
            "provider_id",
        ),
    ],
)
def test_controller_rejects_recursive_aliases_before_resolution_or_service_mutation(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    service_name: str,
    payload: dict[str, object],
    forbidden_key: str,
) -> None:
    calls = {"resolver": 0, "service": 0}

    def fail_resolver(*args: object, **kwargs: object) -> None:
        calls["resolver"] += 1
        raise AssertionError("instrument resolution must not run")

    def fail_service(*args: object, **kwargs: object) -> None:
        calls["service"] += 1
        raise AssertionError("strategy mutation must not run")

    monkeypatch.setattr(controller, "_resolve_slot_instrument", fail_resolver)
    monkeypatch.setattr(controller.strategy_service, service_name, fail_service)

    response = getattr(_client(), method)(path, json=payload)

    assert response.status_code == 400
    assert forbidden_key in response.json()["detail"]
    assert calls == {"resolver": 0, "service": 0}


def test_direct_service_writes_reject_recursive_aliases_before_registry_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"create": 0, "update": 0}

    def fail_create(*args: object, **kwargs: object) -> None:
        calls["create"] += 1
        raise AssertionError("registry create must not run")

    def fail_update(*args: object, **kwargs: object) -> None:
        calls["update"] += 1
        raise AssertionError("registry update must not run")

    monkeypatch.setattr(facade._REGISTRY, "create", fail_create)
    monkeypatch.setattr(facade._REGISTRY, "update", fail_update)

    with pytest.raises(ValueError, match="provider_id"):
        facade.create_strategy(
            "Rejected direct create",
            symbols=[
                {
                    "symbol": "BTC/USD",
                    "metadata": {"nested": {"provider_id": "COINBASE"}},
                }
            ],
            timeframe="1h",
        )
    with pytest.raises(ValueError, match="venue_id"):
        facade.update_strategy(
            "strategy-1",
            instrument_slots=[
                {
                    "symbol": "BTC/USD",
                    "metadata": {"nested": [{"venue_id": "DIRECT"}]},
                }
            ],
        )

    assert calls == {"create": 0, "update": 0}


def test_direct_clone_rejects_recursive_aliases_before_source_or_variant_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"source": 0, "variants": 0}

    def fail_source(*args: object, **kwargs: object) -> None:
        calls["source"] += 1
        raise AssertionError("source lookup must not run")

    def fail_variants(*args: object, **kwargs: object) -> None:
        calls["variants"] += 1
        raise AssertionError("variant storage must not run")

    monkeypatch.setattr(facade._REGISTRY, "get", fail_source)
    monkeypatch.setattr(facade, "list_strategy_variants", fail_variants)

    with pytest.raises(ValueError, match="provider_id"):
        facade.clone_strategy(
            "strategy-1",
            name="Rejected clone",
            symbols=[
                {
                    "symbol": "BTC/USD",
                    "metadata": {"provider_id": "COINBASE"},
                }
            ],
        )

    assert calls == {"source": 0, "variants": 0}


def test_explicit_instrument_id_precedes_all_lookup_hints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical = {
        "id": "instrument-1",
        "symbol": "BTC/USD",
        "datasource": "CANONICAL",
        "exchange": "canonical-exchange",
    }
    calls: list[str] = []

    def get_record(instrument_id: str) -> dict[str, object]:
        calls.append(instrument_id)
        return canonical

    monkeypatch.setattr(controller.instrument_service, "get_instrument_record", get_record)
    monkeypatch.setattr(
        controller.instrument_service,
        "resolve_instrument",
        lambda *args, **kwargs: pytest.fail("hint lookup must not run"),
    )
    monkeypatch.setattr(
        controller.instrument_service,
        "validate_instrument",
        lambda *args, **kwargs: pytest.fail("instrument admission must not run"),
    )

    resolved = controller._resolve_slot_instrument(
        {"datasource": "strategy-default", "exchange": "strategy-default"},
        {
            "symbol": "BTC/USD",
            "instrument_id": "instrument-1",
            "datasource": "slot-hint",
            "exchange": "slot-hint",
        },
    )

    assert resolved == canonical
    assert calls == ["instrument-1"]


def test_datasource_exchange_and_symbol_are_the_only_fallback_lookup_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def resolve(*args: object, **kwargs: object) -> None:
        calls.append(("resolve", args, kwargs))
        return None

    def validate(*args: object, **kwargs: object) -> tuple[dict[str, object], None]:
        calls.append(("validate", args, kwargs))
        return ({"id": "instrument-1"}, None)

    monkeypatch.setattr(controller.instrument_service, "resolve_instrument", resolve)
    monkeypatch.setattr(controller.instrument_service, "validate_instrument", validate)

    resolved = controller._resolve_slot_instrument(
        {"datasource": "strategy-default", "exchange": "strategy-default"},
        {
            "symbol": "ETH/USD",
            "metadata": {
                "datasource": "slot-source-hint",
                "exchange": "slot-exchange-hint",
            },
        },
    )

    expected = ("slot-source-hint", "slot-exchange-hint", "ETH/USD")
    assert resolved == {"id": "instrument-1"}
    assert calls == [
        ("resolve", expected, {}),
        ("validate", expected, {}),
    ]


def test_canonical_instrument_overwrites_slot_lookup_hints_before_service_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    canonical = {
        "id": "instrument-1",
        "symbol": "BTC/USD",
        "datasource": "CANONICAL",
        "exchange": "canonical-exchange",
    }

    monkeypatch.setattr(
        controller,
        "_resolve_slot_instrument",
        lambda *args, **kwargs: dict(canonical),
    )
    monkeypatch.setattr(
        controller.instrument_service,
        "validate_instrument",
        lambda *args, **kwargs: (dict(canonical), None),
    )

    def create_strategy(name: str, **kwargs: object) -> dict[str, object]:
        observed.update({"name": name, **kwargs})
        return {
            "id": "strategy-1",
            "name": name,
            "timeframe": kwargs["timeframe"],
            "datasource": kwargs["datasource"],
            "exchange": kwargs["exchange"],
        }

    monkeypatch.setattr(controller.strategy_service, "create_strategy", create_strategy)

    response = _client().post(
        "/api/strategies/",
        json={
            "name": "Canonical strategy",
            "timeframe": "1h",
            "datasource": "strategy-hint",
            "exchange": "strategy-hint",
            "instrument_slots": [
                {
                    "symbol": "BTC/USD",
                    "instrument_id": "instrument-1",
                    "datasource": "slot-hint",
                    "exchange": "slot-hint",
                }
            ],
        },
    )

    assert response.status_code == 201
    slots = observed["symbols"]
    assert isinstance(slots, list)
    metadata = slots[0]["metadata"]
    assert metadata == {
        "instrument_id": "instrument-1",
        "datasource": "CANONICAL",
        "exchange": "canonical-exchange",
    }


def test_direct_service_accepts_canonical_identity_and_lookup_hints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def create(name: str, **kwargs: object) -> dict[str, object]:
        observed.update({"name": name, **kwargs})
        return {"id": "strategy-1"}

    monkeypatch.setattr(facade._REGISTRY, "create", create)

    created = facade.create_strategy(
        "Canonical direct create",
        symbols=[
            {
                "symbol": "BTC/USD",
                "instrument_id": "instrument-1",
                "metadata": {
                    "datasource": "CCXT",
                    "exchange": "coinbase",
                },
            }
        ],
        timeframe="1h",
        datasource="CCXT",
        exchange="coinbase",
    )

    assert created == {"id": "strategy-1"}
    assert observed["symbols"][0]["instrument_id"] == "instrument-1"


def test_legacy_bootstrap_preserves_provider_and_venue_aliases_for_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        facade,
        "storage_load_strategies",
        lambda: [
            {
                "id": "legacy-strategy",
                "name": "Legacy strategy",
                "timeframe": "1h",
                "symbols": [
                    {
                        "symbol": "BTC/USD",
                        "provider_id": "COINBASE",
                        "metadata": {"venue_id": "COINBASE_DIRECT"},
                    }
                ],
                "indicator_links": [],
                "rules_raw": [],
            }
        ],
    )

    registry = facade.StrategyRegistry()
    slot = registry.get("legacy-strategy").instruments[0]

    assert slot.metadata["provider_id"] == "COINBASE"
    assert slot.metadata["venue_id"] == "COINBASE_DIRECT"
