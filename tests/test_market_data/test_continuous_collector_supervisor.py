from __future__ import annotations

import asyncio
import time

from portal.backend.service.market.collector_supervisor import (
    CollectorAdapterRegistry,
    ContinuousCollectorSupervisor,
)


class _Repository:
    def list_stream_definitions(self):
        runtime = {
            "collector_runtime": {
                "mode": "production",
                "stop_at": None,
                "policy": {},
            }
        }
        return [
            {
                "id": "supported",
                "enabled": True,
                "provider": "TEST",
                "channels": ("trades",),
                "config": runtime,
            },
            {
                "id": "unsupported",
                "enabled": True,
                "provider": "UNKNOWN",
                "channels": ("future",),
                "config": runtime,
            },
        ]


class _Adapter:
    adapter_id = "test.trades.v1"

    def supports(self, definition):
        return definition["provider"] == "TEST"

    async def run(
        self,
        *,
        definition_id,
        owner_id,
        stop_requested,
        bounded_validation,
    ):
        while not stop_requested():
            await asyncio.sleep(0.01)
        return {"definition_id": definition_id, "owner_id": owner_id}


def test_supervisor_quarantines_unsupported_definition_without_stopping_others() -> None:
    supervisor = ContinuousCollectorSupervisor(
        owner_id="test-worker",
        repository=_Repository(),
        registry=CollectorAdapterRegistry((_Adapter(),)),
        poll_seconds=0.02,
    )
    supervisor.start()
    deadline = time.monotonic() + 2.0
    snapshot = supervisor.snapshot()
    while time.monotonic() < deadline:
        snapshot = supervisor.snapshot()
        if (
            "supported" in snapshot["tasks"]
            and "unsupported" in snapshot["errors"]
        ):
            break
        time.sleep(0.02)
    try:
        assert snapshot["state"] == "running"
        assert snapshot["tasks"]["supported"]["adapter_id"] == "test.trades.v1"
        assert "adapter_resolution_failed" in snapshot["errors"]["unsupported"]
    finally:
        supervisor.stop(timeout_seconds=2.0)
    assert supervisor.snapshot()["state"] == "stopped"


def test_registry_rejects_ambiguous_adapter_matches() -> None:
    registry = CollectorAdapterRegistry((_Adapter(),))

    class _SecondAdapter(_Adapter):
        adapter_id = "test.trades.v2"

    registry.register(_SecondAdapter())
    try:
        registry.resolve({"id": "ambiguous", "provider": "TEST"})
    except ValueError as exc:
        assert "matches=2" in str(exc)
    else:
        raise AssertionError("ambiguous collector adapter resolution was accepted")
