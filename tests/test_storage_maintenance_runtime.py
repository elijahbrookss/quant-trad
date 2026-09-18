"""Explicit operating budgets connect the existing worker, without activation."""
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest

from portal.backend.service.storage import maintenance_runtime as runtime


def configuration():
    return {"schema_version": "qt.storage_maintenance_limits.v1",
        "history": {"wal_bytes": 64*1024**2,
            "temporary_bytes": {"ssd": 16*1024**2, "hdd": 16*1024**2},
            "growth_bytes_per_second": {"ssd": 1024**2, "hdd": 1024**2},
            "maintenance_bytes": {"ssd": 0, "hdd": 0},
            "movement_timeout_seconds": 30, "cancellation_grace_seconds": 2},
        "recovery": {"max_bytes": 16*1024**2, "timeout_seconds": 120,
                     "headroom_bytes": {"ssd": 1024**2, "hdd": 1024**2},
                     "max_objects": 1000}}


def test_absent_configuration_does_not_read_files_or_access_database(monkeypatch):
    monkeypatch.setattr(runtime.Path, "open", lambda *a, **k: pytest.fail("unexpected file access"))
    assert runtime.storage_maintenance_runners(object(), storage_root="/not-accessed") == {}


def test_configured_runners_share_database_limits_and_cancellation(tmp_path, monkeypatch):
    config = configuration()
    path = tmp_path/"limits.json"
    path.write_text(json.dumps(config))
    seen = []
    def capture(database, **kwargs):
        seen.append((database, kwargs))
        return {"state": "disabled"}
    monkeypatch.setattr(runtime, "run_history_maintenance", capture)
    monkeypatch.setattr(runtime, "run_due_local_recovery", capture)
    database = object()
    root = tmp_path/"archive"
    runners = runtime.storage_maintenance_runners(database, storage_root=root, limits_path=path)
    assert not seen
    cancelled = lambda: True
    for runner in runners.values():
        assert runner(cancelled=cancelled) == {"state": "disabled"}
    assert all(item[0] is database and item[1]["cancelled"] is cancelled for item in seen)
    assert seen[0][1]["resource_limits"] == config["history"]
    assert seen[1][1]["storage_root"] == root
    assert seen[1][1]["max_bytes"] == config["recovery"]["max_bytes"]
    assert str(seen[1][1]["pg_dump"]) == "/usr/lib/postgresql/15/bin/pg_dump"


@pytest.mark.parametrize("change", [
    lambda value: value.update(schema_version="unknown"),
    lambda value: value.update(policy={}),
    lambda value: value.update(PG_DSN="not-an-accepted-setting"),
    lambda value: value["history"].update(movement_timeout_seconds=True),
    lambda value: value["recovery"].update(max_bytes=0),
    lambda value: value["recovery"].update(max_objects=1000001),
    lambda value: value["recovery"].update(headroom_bytes={}),
    lambda value: value["recovery"].pop("timeout_seconds"),
])
def test_invalid_limits_are_rejected_before_any_runner_exists(tmp_path, change):
    value = configuration()
    change(value)
    path = tmp_path/"limits.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        runtime.storage_maintenance_runners(object(), storage_root=tmp_path, limits_path=path)


def test_missing_relative_oversized_and_ambiguous_files_fail_loudly(tmp_path):
    with pytest.raises(ValueError, match="absolute"):
        runtime.storage_maintenance_runners(object(), storage_root=tmp_path, limits_path="limits.json")
    path = tmp_path/"limits.json"
    with pytest.raises(FileNotFoundError):
        runtime.storage_maintenance_runners(object(), storage_root=tmp_path, limits_path=path)
    path.write_text(" "*(128*1024+1))
    with pytest.raises(ValueError, match="too_large"):
        runtime.storage_maintenance_runners(object(), storage_root=tmp_path, limits_path=path)
    path.write_text('{"history": {}, "history": {}}')
    with pytest.raises(ValueError, match="duplicate"):
        runtime.storage_maintenance_runners(object(), storage_root=tmp_path, limits_path=path)


def test_collector_entrypoint_supplies_runners_to_existing_supervisor(tmp_path, monkeypatch):
    from portal.backend.workers import market_data_collector as entry
    path = tmp_path/"limits.json"
    calls = []
    runners = {"history_runner": lambda **kw: {"state": "disabled"},
               "recovery_runner": lambda **kw: {"state": "disabled"}}
    def compose(database, **kwargs):
        assert database is entry.db
        assert kwargs == {"storage_root": entry.DEFAULT_STORAGE_ROOT, "limits_path": str(path)}
        calls.append("configured")
        return runners
    class Supervisor:
        def __init__(self, **kwargs):
            assert kwargs["history_runner"] is runners["history_runner"]
            assert kwargs["recovery_runner"] is runners["recovery_runner"]
            assert kwargs["storage_root"] == entry.DEFAULT_STORAGE_ROOT
        def start(self): calls.append("started")
        def stop(self): calls.append("stopped")
        def snapshot(self): return {"state": "running"}
    passive = SimpleNamespace(start=lambda: None, stop=lambda **kw: None, snapshot=lambda: {})
    monkeypatch.setattr(entry, "_SETTINGS", replace(entry._SETTINGS,
        storage=replace(entry._SETTINGS.storage, maintenance_limits_path=str(path))))
    monkeypatch.setattr(entry, "_STOP", True)
    monkeypatch.setattr(entry, "require_configured_archive_mount", lambda: None)
    monkeypatch.setattr(entry.signal, "signal", lambda *a: None)
    monkeypatch.setattr(entry, "wait_for_database_ready", lambda **kw: True)
    monkeypatch.setattr(entry, "storage_maintenance_runners", compose)
    monkeypatch.setattr(entry, "MarketStorageLifecycleSupervisor", Supervisor)
    monkeypatch.setattr(entry, "ContinuousCollectorSupervisor", lambda **kw: passive)
    monkeypatch.setattr(entry, "_WorkerHeartbeat", lambda *a, **kw: passive)
    assert entry.main() == 0
    assert calls == ["configured", "started", "stopped"]
