from __future__ import annotations

from datetime import UTC, datetime
from threading import Event
import json
import logging
from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.capacity_observability import (
    DatabaseCapacitySampler,
    capacity_sample_bucket,
)
from portal.backend.service import capacity_observability as module


def test_capacity_sample_bucket_aligns_to_interval_in_utc() -> None:
    assert capacity_sample_bucket(
        datetime(2026, 8, 4, 12, 7, 43, tzinfo=UTC),
        interval_seconds=300,
    ) == datetime(2026, 8, 4, 12, 5)

    assert capacity_sample_bucket(
        datetime(2026, 8, 4, 12, 7, 43),
        interval_seconds=300,
    ) == datetime(2026, 8, 4, 12, 5)


def test_capacity_sampler_passes_aligned_identity_and_retention() -> None:
    captured: list[dict] = []

    def sample_fn(**kwargs):
        captured.append(dict(kwargs))
        return {
            "sampled": True,
            "database_size_bytes": 1024,
            "relation_count": 3,
            "relation_rows_inserted": 3,
            "retention_rows_deleted": 0,
            "sample_query_ms": 1.5,
        }

    sampler = DatabaseCapacitySampler(
        sample_fn=sample_fn,
        enabled=True,
        interval_seconds=300,
        retention_days=30,
    )

    result = sampler.sample_once(now=datetime(2026, 8, 4, 12, 7, 43, tzinfo=UTC))

    assert result["sampled"] is True
    assert captured == [
        {
            "sampled_at": datetime(2026, 8, 4, 12, 5),
            "retention_days": 30,
        }
    ]


def test_capacity_sampler_starts_immediately_and_stops_cleanly() -> None:
    sampled = Event()

    def sample_fn(**_kwargs):
        sampled.set()
        return {"sampled": False, "reason": "test"}

    sampler = DatabaseCapacitySampler(
        sample_fn=sample_fn,
        enabled=True,
        interval_seconds=60,
        retention_days=1,
    )

    sampler.start()
    assert sampled.wait(timeout=2.0)
    sampler.stop(timeout_s=2.0)


def test_storage_metrics_reuse_existing_capacity_cadence_and_structured_logs(caplog):
    data = {"operating": {"schema_version": "qt.storage_operating_snapshot.v1",
        "state": "partial", "backlog_rows": None, "published_rows_window": 10},
        "allocation": [{"family": "raw_custody_lookup", "tablespace": "pg_default", "allocated_bytes": 100}]}
    sampler = DatabaseCapacitySampler(sample_fn=lambda **kwargs: {"sampled": True, "storage_metrics": data}, enabled=True)
    with caplog.at_level(logging.INFO):
        sampler.sample_once(now=datetime(2026, 8, 4, 12, 7, 43, tzinfo=UTC))
    snapshots = [json.loads(row.message.split(" | ", 1)[1]) for row in caplog.records
                 if row.message.startswith("storage_operating_snapshot | ")]
    assert snapshots == [data["operating"]]
    assert snapshots[0]["backlog_rows"] is None
    assert sum(row.message.startswith("storage_relation_allocation_snapshot | ") for row in caplog.records) == 1


class _Observer:
    def __init__(self): self.rows = []
    def gauge(self, name, value, **labels): self.rows.append((name, value, labels))


def test_fixed_operating_gauges_persist_unknown_health_without_inventing_zero(monkeypatch):
    observer = _Observer()
    monkeypatch.setattr(module, "_STORAGE_OBSERVER", observer)
    module._observe_storage_snapshot({"state": "partial", "partial": True,
        "backlog_rows": None, "known_backlog_rows": 12, "eligible_cohorts": 2})
    values = {name: value for name, value, _ in observer.rows}
    assert "storage_backlog_rows" not in values
    assert values["storage_known_backlog_rows"] == 12
    assert values["storage_backlog_rows_available"] == 0
    assert values["storage_snapshot_complete"] == 0 and values["storage_snapshot_available"] == 1
    assert all(labels == {"storage_target": "canonical", "pipeline_stage": "lifecycle"}
               for _, _, labels in observer.rows)


def test_mount_gauges_require_pinned_native_identity_and_use_existing_df_basis(monkeypatch):
    observer = _Observer()
    node_samples = []
    monkeypatch.setattr(module, "_MOUNT_OBSERVER", observer)
    monkeypatch.setattr(module, "_observe_node_pressure", lambda: node_samples.append(True))
    for name, value in (("QT_MARKET_DATA_WORKING_EXPECTED_UUID", "uuid-hot"),
                        ("MARKET_STRUCTURE_WORKING_ROOT", "/working"),
                        ("QT_MARKET_DATA_EXPECTED_UUID", "uuid-cold"),
                        ("MARKET_STRUCTURE_STORAGE_ROOT", "/archive")):
        monkeypatch.setenv(name, value)
    def evidence(uuid):
        return SimpleNamespace(filesystem_uuid=uuid, total_bytes=120, used_bytes=30, available_bytes=70)
    monkeypatch.setattr(module, "require_configured_working_mount", lambda **kwargs: evidence("uuid-hot"))
    monkeypatch.setattr(module, "require_configured_archive_mount", lambda **kwargs: evidence("uuid-cold"))
    module._observe_mount_capacity()
    data = [(name, value, labels) for name, value, labels in observer.rows
            if name != "storage_mount_sample_available"]
    assert len(data) == 8
    assert {value for name, value, _ in data if name == "storage_mount_used_percent"} == {30.0}
    assert {labels["storage_target"] for _, _, labels in data} == {"hot", "cold"}
    assert node_samples == [True]


def test_unconfigured_or_mismatched_mount_is_unavailable_not_a_zero_capacity(monkeypatch):
    observer = _Observer()
    monkeypatch.setattr(module, "_MOUNT_OBSERVER", observer)
    for name in ("QT_MARKET_DATA_WORKING_EXPECTED_UUID", "MARKET_STRUCTURE_WORKING_ROOT",
                 "QT_MARKET_DATA_EXPECTED_UUID", "MARKET_STRUCTURE_STORAGE_ROOT"):
        monkeypatch.delenv(name, raising=False)
    def forbidden(**kwargs): raise AssertionError("directory mode is not native physical authority")
    monkeypatch.setattr(module, "require_configured_working_mount", forbidden)
    monkeypatch.setattr(module, "require_configured_archive_mount", forbidden)
    module._observe_mount_capacity()
    assert len(observer.rows) == 2 and all(name == "storage_mount_sample_available" and value == 0
                                        for name, value, _ in observer.rows)


def test_node_pressure_uses_fixed_avg300_host_gauges_and_invalid_data_is_unavailable(monkeypatch):
    observer = _Observer()
    monkeypatch.setattr(module, "_NODE_OBSERVER", observer)
    raw = "some avg10=1 avg60=2 avg300=3.5 total=100\nfull avg10=0 avg60=1 avg300=2.5 total=50\n"
    monkeypatch.setattr(module, "Path", lambda *parts: SimpleNamespace(read_text=lambda: raw))
    module._observe_node_pressure()
    values = {name: value for name, value, _ in observer.rows}
    assert values["storage_node_io_stall_percent"] == 3.5
    assert values["storage_node_memory_full_stall_percent"] == 2.5
    assert values["storage_node_pressure_available"] == 1
    assert all(labels == {"storage_target": "node", "pipeline_stage": "pressure"}
               for _, _, labels in observer.rows)
    observer.rows.clear()
    raw = "some avg300=nan\nfull avg300=0\n"
    module._observe_node_pressure()
    assert observer.rows == [("storage_node_pressure_available", 0,
                             {"storage_target": "node", "pipeline_stage": "pressure"})]
