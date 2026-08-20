from __future__ import annotations

import asyncio
import time

from market_data.structure import ProductContract

from portal.backend.service.market.collector_supervisor import (
    CollectorAdapterRegistry,
    ContinuousCollectorSupervisor,
)


class _Repository:
    def __init__(self):
        self.safety_events = []

    def list_stream_definitions(self):
        runtime = {
            "runtime_policy": {},
            "fleet_id": "test-fleet",
            "product_definition_version_id": "test.product.v1",
            "safety_policy": {
                "schema_version": "market.collector_safety_policy.v1",
                "policy_id": "test.v1",
                "warning_free_bytes": 2,
                "critical_free_bytes": 1,
                "warning_spool_ratio": 0.7,
                "critical_spool_ratio": 0.9,
                "warning_projected_exhaustion_hours": 2,
                "critical_projected_exhaustion_hours": 1,
                "evaluation_interval_seconds": 5,
            },
        }
        return [
            {
                "id": "supported",
                "enabled": True,
                "desired_state": "running",
                "control_generation": 1,
                "provider": "TEST",
                "channels": ("trades",),
                "max_spool_bytes": 1024,
                "config": runtime,
            },
            {
                "id": "unsupported",
                "enabled": True,
                "desired_state": "running",
                "control_generation": 1,
                "provider": "UNKNOWN",
                "channels": ("future",),
                "max_spool_bytes": 1024,
                "config": runtime,
            },
        ]

    def active_safety_halts(self, **_kwargs):
        return []

    def get_product_contract(self, _definition_version_id):
        return ProductContract(
            provider_product_id="TEST-USD",
            provider_size_unit="base",
            base_currency="TEST",
            quote_currency="USD",
            product_definition_version_id="test.product.v1",
        )

    def stream_storage_growth(self, **_kwargs):
        return {"bytes_per_hour": 0.0, "window_seconds": 0.0}

    def record_safety_event(self, **kwargs):
        self.safety_events.append(kwargs)
        return kwargs



class _OperationsRepository:
    def __init__(self):
        self.actions = []

    def apply_lifecycle_action(self, **kwargs):
        self.actions.append(kwargs)
        return kwargs


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
    operations = _OperationsRepository()
    supervisor = ContinuousCollectorSupervisor(
        owner_id="test-worker",
        repository=_Repository(),
        operations_repository=operations,
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
        assert snapshot["tasks"]["supported"]["control_generation"] == 1
        assert "collector_safety_not_qualified" in snapshot["errors"]["unsupported"]
    finally:
        supervisor.stop(timeout_seconds=2.0)
    assert supervisor.snapshot()["state"] == "stopped"
    assert operations.actions[0]["collector_id"] == "unsupported"
    assert operations.actions[0]["action"].value == "pause"


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
