from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from fastapi import FastAPI

from portal.backend.controller import research as research_controller


@pytest.mark.asyncio
async def test_slow_sync_route_does_not_block_unrelated_async_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def slow_create(_payload):
        time.sleep(0.2)
        return {"id": "item-1", "kind": "observation", "title": "Slow item"}

    monkeypatch.setattr(
        research_controller.research_service,
        "create_research_item",
        slow_create,
    )

    app = FastAPI()
    app.include_router(research_controller.router, prefix="/api/research")

    @app.get("/async-probe")
    async def async_probe() -> dict[str, bool]:
        await asyncio.sleep(0)
        return {"ready": True}

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        started = time.perf_counter()
        slow_request = asyncio.create_task(
            client.post(
                "/api/research/items",
                json={"kind": "observation", "title": "Slow item"},
            )
        )
        await asyncio.sleep(0.02)
        scheduling_delay = time.perf_counter() - started
        probe_started = time.perf_counter()
        probe_response = await client.get("/async-probe")
        probe_delay = time.perf_counter() - probe_started
        slow_response = await slow_request

    assert scheduling_delay < 0.1
    assert probe_delay < 0.1
    assert probe_response.status_code == 200
    assert slow_response.status_code == 201
