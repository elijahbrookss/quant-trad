from __future__ import annotations
import pytest
pytest.importorskip("pandas")

from fastapi.testclient import TestClient

from portal.backend.main import app
from portal.backend.service.indicators.indicator_service.runtime_contract import (
    SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
)


def test_signals_endpoint_rejects_non_engine_runtime_path(monkeypatch) -> None:
    from portal.backend.controller import indicators as controller

    monkeypatch.setattr(controller, "enqueue_signal_job", lambda **kwargs: "job-1")

    async def _fake_wait_for_job(job_id: str):
        return {"signals": [], "runtime_path": "legacy"}

    monkeypatch.setattr(controller, "wait_for_job", _fake_wait_for_job)

    client = TestClient(app)
    response = client.post(
        "/api/indicators/ind-1/signals",
        json={
            "start": "2026-02-01T00:00:00Z",
            "end": "2026-02-01T01:00:00Z",
            "interval": "1h",
            "symbol": "ES",
            "datasource": "ALPACA",
        },
    )
    assert response.status_code == 500
    assert "runtime_path_mismatch" in str(response.json().get("detail") or "")


def test_signals_endpoint_accepts_engine_runtime_path(monkeypatch) -> None:
    from portal.backend.controller import indicators as controller

    monkeypatch.setattr(controller, "enqueue_signal_job", lambda **kwargs: "job-1")

    async def _fake_wait_for_job(job_id: str):
        return {"signals": [], "runtime_path": SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT}

    monkeypatch.setattr(controller, "wait_for_job", _fake_wait_for_job)

    client = TestClient(app)
    response = client.post(
        "/api/indicators/ind-1/signals",
        json={
            "start": "2026-02-01T00:00:00Z",
            "end": "2026-02-01T01:00:00Z",
            "interval": "1h",
            "symbol": "ES",
            "datasource": "ALPACA",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body.get("runtime_path") == SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
