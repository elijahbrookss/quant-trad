from __future__ import annotations

import logging

import pytest
fastapi = pytest.importorskip("fastapi")
from fastapi import HTTPException

from portal.backend.controller import indicators


@pytest.mark.asyncio
async def test_overlay_endpoint_logs_handled_runtime_errors(caplog, monkeypatch):
    def _raise_runtime_error(**kwargs):
        raise RuntimeError("market_profile_profile_missing_known_at: every profile must include known_at")

    monkeypatch.setattr(indicators, "overlays_for_instance", _raise_runtime_error)
    request = indicators.OverlayRequest(
        start="2025-12-19T23:00:00Z",
        end="2026-03-19T22:00:00Z",
        interval="1h",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
    )

    with caplog.at_level(logging.ERROR, logger="portal.backend.controller.indicators"):
        with pytest.raises(HTTPException) as exc_info:
            await indicators.overlays("indicator-1", request)

    assert exc_info.value.status_code == 500
    assert "market_profile_profile_missing_known_at" in exc_info.value.detail
    assert "event=indicator_overlay_request_failed" in caplog.text
    assert "indicator_id=indicator-1" in caplog.text
    assert "symbol=BIP-20DEC30-CDE" in caplog.text
