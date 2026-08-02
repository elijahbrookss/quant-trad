from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from portal.backend.controller import candles as candles_controller
from portal.backend.controller import instruments as instruments_controller
from portal.backend.main import app
from market_data.store import IngestionOutcome


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


def test_candle_ingestion_route_is_explicit_and_returns_auditable_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {}
    monkeypatch.setattr(
        candles_controller.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "datasource": "CCXT",
            "exchange": "coinbase",
            "symbol": "BTC/USD",
        },
    )

    def fake_ingest(instrument, **kwargs):
        observed.update(instrument=instrument, kwargs=kwargs)
        return SimpleNamespace(
            source_id=3,
            series_id=7,
            gap_evidence_count=1,
            outcome=IngestionOutcome(
                ingestion_run_id="ingest-1",
                requested_count=24,
                inserted_count=23,
                corrected_count=0,
                noop_count=0,
                max_commit_seq=42,
            ),
        )

    monkeypatch.setattr(
        candles_controller.historical_candle_ingestor,
        "ingest_by_instrument",
        fake_ingest,
    )
    response = TestClient(app).post(
        "/api/candles/ingest",
        json={
            "instrument_id": "inst-btc",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            "timeframe": "1h",
        },
    )

    assert response.status_code == 200
    assert response.json()["outcome"]["ingestion_run_id"] == "ingest-1"
    assert observed["kwargs"]["interval"] == "1h"


def test_dataset_freeze_route_resolves_canonical_series_and_returns_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {}
    def fake_resolve_series_id(**kwargs):
        observed["identity"] = kwargs
        return 7

    monkeypatch.setattr(
        candles_controller.market_data_repo,
        "resolve_series_id",
        fake_resolve_series_id,
    )

    def fake_freeze(requests, **kwargs):
        observed["requests"] = requests
        observed["freeze"] = kwargs
        return SimpleNamespace(
            dataset_id="mds_abc",
            contract_version="market_dataset.v1",
            name="reference",
            purpose="research",
            metadata={"schema_version": "market_dataset_request.v1"},
            dataset_hash="abc",
            max_commit_seq=42,
            series=({"series_id": 7, "material_hash": "material"},),
        )

    monkeypatch.setattr(candles_controller.market_data_repo, "freeze_dataset", fake_freeze)
    response = TestClient(app).post(
        "/api/candles/datasets/freeze",
        json={
            "series": [
                {
                    "instrument_id": "inst-btc",
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-02T00:00:00Z",
                    "timeframe": "1h",
                }
            ],
            "name": "reference",
            "created_by": "operator",
        },
    )

    assert response.status_code == 200
    assert response.json()["dataset_id"] == "mds_abc"
    assert observed["identity"]["contract_version"] == "candle.ohlcv.v1"
    assert observed["requests"][0].series_id == 7


def test_dataset_freeze_route_accepts_exact_typed_series_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {}

    def fail_candle_resolution(**_kwargs):
        raise AssertionError("typed series freeze must not resolve a candle")

    def fake_freeze(requests, **kwargs):
        observed["requests"] = requests
        observed["freeze"] = kwargs
        return SimpleNamespace(
            dataset_id="mds_typed",
            contract_version="market_dataset.v1",
            name="phase4-proof",
            purpose="validation",
            metadata=kwargs["metadata"],
            dataset_hash="typed",
            max_commit_seq=99,
            series=({"series_id": 94, "material_hash": "material"},),
        )

    monkeypatch.setattr(
        candles_controller.market_data_repo,
        "resolve_series_id",
        fail_candle_resolution,
    )
    monkeypatch.setattr(
        candles_controller.market_data_repo,
        "freeze_dataset",
        fake_freeze,
    )
    response = TestClient(app).post(
        "/api/candles/datasets/freeze",
        json={
            "series": [
                {
                    "series_id": 94,
                    "start": "2026-08-02T18:13:56Z",
                    "end": "2026-08-02T18:14:25Z",
                }
            ],
            "name": "phase4-proof",
            "purpose": "validation",
            "created_by": "operator",
        },
    )

    assert response.status_code == 200
    assert response.json()["dataset_id"] == "mds_typed"
    assert observed["requests"][0].series_id == 94
    assert observed["freeze"]["metadata"]["resolved_requests"] == [
        {
            "series_id": 94,
            "instrument_id": None,
            "timeframe": None,
            "start": "2026-08-02T18:13:56Z",
            "end": "2026-08-02T18:14:25Z",
        }
    ]
