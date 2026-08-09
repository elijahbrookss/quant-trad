from __future__ import annotations

import shutil
from pathlib import Path

from market_data.structure import ProductContract
from portal.backend.service.market.collector_safety import evaluate_collector_safety


class _Repository:
    def __init__(self, *, bytes_per_hour: float = 0.0, halts=()) -> None:
        self.bytes_per_hour = bytes_per_hour
        self.halts = list(halts)

    def active_safety_halts(self, **_kwargs):
        return list(self.halts)

    def get_product_contract(self, _definition_version_id):
        return ProductContract(
            provider_product_id="TEST-USD",
            provider_size_unit="base",
            base_currency="TEST",
            quote_currency="USD",
            product_definition_version_id="test.product.v1",
        )

    def stream_storage_growth(self, **_kwargs):
        return {
            "bytes_per_hour": self.bytes_per_hour,
            "window_seconds": 3600.0,
        }


def _definition(*, warning_free: int, critical_free: int) -> dict:
    return {
        "id": "definition-a",
        "max_spool_bytes": 1024,
        "config": {
            "fleet_id": "fleet-a",
            "product_definition_version_id": "test.product.v1",
            "safety_policy": {
                "schema_version": "market.collector_safety_policy.v1",
                "policy_id": "test.v1",
                "warning_free_bytes": warning_free,
                "critical_free_bytes": critical_free,
                "warning_spool_ratio": 0.7,
                "critical_spool_ratio": 0.9,
                "warning_projected_exhaustion_hours": 168,
                "critical_projected_exhaustion_hours": 24,
                "evaluation_interval_seconds": 5,
            },
        },
    }


def test_warning_is_visible_but_does_not_disqualify(tmp_path: Path) -> None:
    free = shutil.disk_usage(tmp_path).free
    qualification, evaluation = evaluate_collector_safety(
        definition=_definition(warning_free=free + 1, critical_free=1),
        repository=_Repository(),
        adapter_supported=True,
        storage_root=tmp_path,
    )

    assert qualification.qualified is True
    assert evaluation.severity == "warning"
    assert evaluation.reasons == ("filesystem_free_bytes_warning",)


def test_critical_storage_condition_fails_closed(tmp_path: Path) -> None:
    free = shutil.disk_usage(tmp_path).free
    qualification, evaluation = evaluate_collector_safety(
        definition=_definition(
            warning_free=free + 2,
            critical_free=free + 1,
        ),
        repository=_Repository(),
        adapter_supported=True,
        storage_root=tmp_path,
    )

    assert qualification.qualified is False
    assert evaluation.severity == "critical"
    assert "filesystem_free_bytes_critical" in qualification.reasons


def test_projected_exhaustion_and_persistent_halt_are_qualification_inputs(
    tmp_path: Path,
) -> None:
    free = shutil.disk_usage(tmp_path).free
    qualification, _ = evaluate_collector_safety(
        definition=_definition(warning_free=2, critical_free=1),
        repository=_Repository(
            bytes_per_hour=float(free),
            halts=({"scope_type": "fleet", "scope_id": "fleet-a"},),
        ),
        adapter_supported=True,
        storage_root=tmp_path,
    )

    assert "projected_storage_exhaustion_critical" in qualification.reasons
    assert "safety_halt_active" in qualification.reasons
