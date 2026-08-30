from __future__ import annotations

import asyncio
import threading

import httpx
import pytest
from fastapi import FastAPI

from portal.backend.controller import research as research_controller


@pytest.mark.asyncio
async def test_slow_sync_route_does_not_block_unrelated_async_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    def slow_create(_payload):
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("test did not release the held synchronous handler")
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
        slow_request = asyncio.create_task(
            client.post(
                "/api/research/items",
                json={"kind": "observation", "title": "Slow item"},
            )
        )
        try:
            handler_entered = await asyncio.wait_for(
                asyncio.to_thread(entered.wait, 5),
                timeout=6,
            )
            assert handler_entered is True
            assert slow_request.done() is False

            probe_response = await asyncio.wait_for(client.get("/async-probe"), timeout=5)
            assert slow_request.done() is False
        finally:
            release.set()

        slow_response = await asyncio.wait_for(slow_request, timeout=5)

    assert probe_response.status_code == 200
    assert probe_response.json() == {"ready": True}
    assert slow_response.status_code == 201
