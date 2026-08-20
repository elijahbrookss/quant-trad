from __future__ import annotations

import asyncio

from portal.backend.service.bots import botlens_lifecycle_bridge as bridge
from portal.backend.service.bots.telemetry_stream import BotTelemetryHub


def test_sync_lifecycle_bridge_submits_to_bound_serving_loop(monkeypatch) -> None:
    submitted: list[dict] = []

    class _Hub:
        def submit_ingest_threadsafe(self, payload) -> bool:
            submitted.append(dict(payload))
            return True

    monkeypatch.setattr(bridge, "_telemetry_hub", lambda: _Hub())

    bridge.emit_lifecycle_event(
        {
            "bot_id": "bot-1",
            "run_id": "run-1",
            "seq": 3,
            "phase": "starting_container",
            "status": "starting",
        }
    )

    assert len(submitted) == 1
    assert submitted[0]["run_id"] == "run-1"
    assert submitted[0]["kind"] == "botlens_lifecycle_event"


def test_threadsafe_ingest_executes_on_bound_serving_loop() -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        owner_loop = asyncio.get_running_loop()
        completed = asyncio.Event()
        observed: list[asyncio.AbstractEventLoop] = []

        async def _ingest(_payload) -> None:
            observed.append(asyncio.get_running_loop())
            completed.set()

        hub.ingest = _ingest  # type: ignore[method-assign]
        hub.bind_serving_loop()
        try:
            submitted = await asyncio.to_thread(
                hub.submit_ingest_threadsafe,
                {"bot_id": "bot-1", "run_id": "run-1"},
            )
            assert submitted is True
            await asyncio.wait_for(completed.wait(), timeout=1.0)
            assert observed == [owner_loop]
        finally:
            hub.unbind_serving_loop()

    asyncio.run(scenario())
