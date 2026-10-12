"""One existing lifecycle supervisor in the database-owned worker."""
from dataclasses import replace
import threading
from types import SimpleNamespace

import pytest

from core.settings import StorageSettings, get_settings
from portal.backend.workers import storage_maintenance as worker
from portal.backend.service.storage.maintenance_runtime import storage_maintenance_runners


@pytest.fixture
def runtime(monkeypatch):
    events = []
    stop = threading.Event()
    settings = replace(get_settings(), storage=StorageSettings(
        maintenance_owner="dedicated", archive_shared_group_id=70,
        maintenance_limits_path="/run/owned-limits.json"))
    class Supervisor:
        def __init__(self, **kwargs): events.append(("construct", kwargs))
        def start(self): events.append(("start",))
        def snapshot(self): return {"state": "running", "maintenance": {"local_recovery": {"configured": True}}}
        def stop(self): events.append(("stop",))
    class Registry:
        def register_worker(self, **kwargs): events.append(("register", kwargs))
        def heartbeat_worker(self, **kwargs): events.append(("heartbeat", kwargs)); stop.set()
        def stop_worker(self, **kwargs): events.append(("retire", kwargs))
    def runners(*args, **kwargs):
        assert kwargs["require_incremental"] is True
        events.append(("runners",))
        return {}
    monkeypatch.setattr(worker.os, "geteuid", lambda: 70)
    monkeypatch.setattr(worker, "require_configured_archive_mount", lambda: None)
    monkeypatch.setattr(worker, "require_configured_working_mount", lambda: None)
    monkeypatch.setattr(worker, "FilesystemRawArchiveObjectStore", lambda *a, **k: None)
    monkeypatch.setattr(worker, "wait_for_database_ready", lambda **kwargs: True)
    monkeypatch.setattr(worker, "storage_maintenance_runners", runners)
    monkeypatch.setattr(worker, "MarketStorageLifecycleSupervisor", Supervisor)
    monkeypatch.setattr(worker, "market_collection_repo", Registry())
    return settings, stop, events


def test_worker_uses_one_existing_supervisor_and_reports_its_state(runtime):
    settings, stop, events = runtime
    assert worker.run(settings=settings, stop=stop) == 0
    assert [e[0] for e in events] == ["runners", "construct", "register", "start", "heartbeat", "stop", "retire"]
    registration = events[2][1]
    assert registration["worker_role"] == "market_storage_maintenance"
    assert registration["capabilities"]["collector_modes"] == []
    assert events[4][1]["context"]["storage_lifecycle"]["maintenance"]["local_recovery"]["configured"]
    assert events[-1][1]["state"] == "stopped"


@pytest.mark.parametrize("failure", ["start", "heartbeat", "stop", "retire"])
def test_worker_failure_never_reports_clean_exit(runtime, monkeypatch, failure):
    settings, stop, events = runtime
    def fail(*args, **kwargs):
        if failure == "start": events.append(("start_attempt",))
        raise RuntimeError("owned fixture failure")
    if failure in {"start", "stop"}:
        monkeypatch.setattr(worker.MarketStorageLifecycleSupervisor, failure, fail)
    else:
        monkeypatch.setattr(worker.market_collection_repo,
            "heartbeat_worker" if failure == "heartbeat" else "stop_worker", fail)
    assert worker.run(settings=settings, stop=stop) == 5
    if failure != "stop": assert ("stop",) in events
    if failure != "retire": assert events[-1][1]["state"] == "degraded"


def test_separated_worker_requires_explicit_configuration(runtime, monkeypatch):
    settings, stop, events = runtime
    with pytest.raises(ValueError, match="dedicated_owner_required"):
        worker.run(settings=replace(settings, storage=StorageSettings()), stop=stop)
    monkeypatch.setattr(worker.os, "geteuid", lambda: 1000)
    with pytest.raises(PermissionError, match="database_user_required"):
        worker.run(settings=settings, stop=stop)
    assert not events


def test_dedicated_runners_never_silently_disable_recovery():
    with pytest.raises(ValueError, match="incremental_configuration_required"):
        storage_maintenance_runners(None, storage_root="/unused", require_incremental=True)


@pytest.mark.parametrize("owner", [None, "", "external", True])
def test_invalid_owner_refuses(owner):
    with pytest.raises(ValueError, match="maintenance_owner_invalid"):
        StorageSettings(maintenance_owner=owner)


def test_missing_working_mount_refuses_before_registry_or_lifecycle(runtime, monkeypatch):
    from core.storage_mounts import StorageMountError
    settings, stop, events = runtime
    def unavailable():
        raise StorageMountError("storage_mount_unavailable: owned fixture")
    monkeypatch.setattr(worker, "require_configured_working_mount", unavailable)
    with pytest.raises(StorageMountError, match="storage_mount_unavailable"):
        worker.run(settings=settings, stop=stop)
    assert not events
