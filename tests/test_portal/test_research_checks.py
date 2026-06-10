from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("pandas")

import pandas as pd
from fastapi.testclient import TestClient

from portal.backend.controller import research as research_controller
from portal.backend.main import app
from portal.backend.service.research import checks, service


def _candles() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 102.5, 104.0, 105.0],
            "high": [101.0, 101.4, 103.0, 103.0, 106.0, 107.0],
            "low": [99.0, 100.8, 101.8, 102.1, 103.5, 104.5],
            "close": [101.0, 101.2, 102.6, 102.8, 105.5, 106.0],
            "volume": [10.0, 9.0, 20.0, 19.0, 30.0, 32.0],
        },
        index=pd.to_datetime(
            [
                "2026-01-01T00:00:00Z",
                "2026-01-01T01:00:00Z",
                "2026-01-01T02:00:00Z",
                "2026-01-01T03:00:00Z",
                "2026-01-01T04:00:00Z",
                "2026-01-01T05:00:00Z",
            ],
            utc=True,
        ),
    )


def test_candle_event_check_summarizes_forward_outcomes() -> None:
    payload = checks.evaluate_candle_event_check(
        _candles(),
        detector={"field": "range_pct", "operator": "lt", "value": 0.01},
        outcomes={"forward_bars": [1, 2], "min_sample_count": 1},
        data_quality={"status": "clean"},
    )

    assert payload["schema_version"] == "research_check_result.v1"
    assert payload["status"] == "completed"
    assert payload["sample_count"] == 2
    assert payload["outcomes"]["summary"]["1"]["sample_count"] == 2
    assert payload["outcomes"]["summary"]["2"]["sample_count"] == 2
    assert payload["recommendation"] == "promote_to_hypothesis"
    assert payload["events"][0]["event_time"] == "2026-01-01T01:00:00Z"


def test_research_check_service_creates_observation_check_and_link(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []

    def fake_create_item(**kwargs):
        item_id = kwargs.get("item_id") or f"item-{len(created) + 1}"
        item = {
            "id": item_id,
            "kind": kwargs["kind"],
            "status": kwargs["status"],
            "title": kwargs["title"],
            "payload": dict(kwargs.get("payload") or {}),
        }
        item.update({key: kwargs.get(key) for key in ("symbol", "timeframe", "instrument_id")})
        created.append(item)
        return item

    def fake_create_link(**kwargs):
        link = {"id": "link-1", **kwargs}
        links.append(link)
        return link

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", fake_create_item)
    monkeypatch.setattr(service.repository, "create_link", fake_create_link)
    monkeypatch.setattr(
        service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )
    monkeypatch.setattr(
        service.candle_service,
        "preflight_candle_coverage_by_instrument",
        lambda *args: {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": "inst-eth",
            "symbol": "ETH/USD",
            "provider": "CCXT",
            "exchange": "coinbase",
            "timeframe": "1h",
            "status": "ok",
            "continuity": {"final_status": "clean"},
            "row_count": 6,
            "missing_ranges": [],
        },
    )
    monkeypatch.setattr(service.candle_service, "fetch_ohlcv_by_instrument", lambda *args: _candles())

    payload = service.run_research_check(
        {
            "title": "ETH contraction follow-through",
            "scope": {
                "instrument_id": "inst-eth",
                "timeframe": "1h",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T06:00:00Z",
            },
            "detector": {"field": "range_pct", "operator": "lt", "value": 0.01},
            "outcomes": {"forward_bars": [1], "min_sample_count": 1},
        }
    )

    assert payload["schema_version"] == "research_check_run.v1"
    assert payload["status"] == "completed"
    assert created[0]["kind"] == "observation"
    assert created[1]["kind"] == "research_check"
    assert created[1]["payload"]["result"]["status"] == "completed"
    assert links == [
        {
            "id": "link-1",
            "source_item_id": created[1]["id"],
            "target_type": "research_item",
            "target_id": created[0]["id"],
            "relation": "tests",
            "metadata": {"target_kind": "observation"},
        }
    ]


def test_research_check_route_delegates_to_service(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_run_research_check(payload):
        observed["payload"] = payload
        return {"schema_version": "research_check_run.v1", "status": "completed"}

    monkeypatch.setattr(research_controller.research_service, "run_research_check", fake_run_research_check)

    response = TestClient(app).post(
        "/api/research/checks/run",
        json={
            "title": "Quick check",
            "scope": {"instrument_id": "inst-1", "timeframe": "1h", "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            "detector": {"field": "range_pct", "operator": "lt", "value": 0.01},
        },
    )

    assert response.status_code == 201
    assert response.json()["status"] == "completed"
    assert observed["payload"]["title"] == "Quick check"
