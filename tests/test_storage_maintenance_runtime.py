"""Explicit operating budgets connect the existing worker, without activation."""
from dataclasses import replace
import json
from types import SimpleNamespace
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime

import pytest

from portal.backend.service.storage import maintenance_runtime as runtime


@pytest.fixture
def cutover_maintenance(tmp_path, monkeypatch):
    from core.storage_targets import StoragePolicy
    from portal.backend.service.storage import incremental_recovery, recovery_maintenance as maintenance
    now = datetime.now(UTC)
    policy = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",), backups=("hdd",),
                           backup_enabled=True)
    source, history = tmp_path/"source", tmp_path/"history"
    source.mkdir()
    (history/"objects").mkdir(parents=True)
    stats = {"ssd": SimpleNamespace(total_bytes=1000, available_bytes=200, device_id="ssd", path=str(source)),
             "hdd": SimpleNamespace(total_bytes=10000, available_bytes=9000, device_id="hdd", path=str(history))}
    targets = {key: SimpleNamespace(target_id=key, medium=key, root=str(path), roles=("recent","history","archives","backups"),
                                   state="active", filesystem_uuid="uuid-"+key,
                                   inspect=lambda require_writable=True, key=key: stats[key])
               for key, path in (("ssd", source), ("hdd", history))}
    records = [SimpleNamespace(id=key, reserved_bytes=reserved, auxiliary_reserved_bytes=aux)
               for key, reserved, aux in (("ssd", 11, 13), ("hdd", 19, 23))]
    layout = {"layout_version": "market.fact_storage_tiers.v2", "certificate_sha256": "a"*64}
    config = SimpleNamespace(revision=7, policy=policy.to_dict())
    class Owner:
        bind = SimpleNamespace(engine=SimpleNamespace(url=SimpleNamespace()))
        def connection(self):
            return SimpleNamespace(get_isolation_level=lambda: "READ COMMITTED")
        def get(self, *args):
            return config
        def scalar(self, statement):
            query = str(statement)
            if "SHOW statement_timeout" in query:
                return "0"
            if "setting::bigint" in query:
                return 0
            if "clock_timestamp" in query:
                return now
            return True
        def execute(self, *args):
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: {
                "mode": "on", "command": "trusted-archive-push", "last_failed_time": None,
                "last_archived_time": None}))
    @contextmanager
    def session(**kwargs):
        yield Owner()
    database = SimpleNamespace(session=session, locked_snapshot_session=session)
    monkeypatch.setattr(maintenance, "registered_header_targets", lambda owner: records)
    monkeypatch.setattr(maintenance, "_target", lambda record: targets[record.id])
    monkeypatch.setattr(maintenance, "_identity", lambda owner: ("123/456", "fixture"))
    monkeypatch.setattr(maintenance, "_snapshot_layout", lambda owner: layout)
    monkeypatch.setattr(maintenance, "observe_header_resources", lambda *a, **kw: SimpleNamespace(
        database_identity="123/456", bindings=[{"role": "database", "target_id": "ssd", "directory": str(source)}]))
    previous = {"name": "copy_"+"1"*32, "completed_at": now.isoformat(), "storage_layout": layout}
    state = SimpleNamespace(calls=[], prune=[], completed=[(now, None, previous)], during=None)
    class Copies:
        def __init__(self, **kwargs):
            state.options = kwargs
            self.resources = kwargs["check_resources"]
        def lock(self):
            return nullcontext()
        def _prune(self, keep):
            state.prune.append(keep)
            self.resources()
        def completed(self):
            return state.completed
        def create(self, snapshot, **kwargs):
            state.calls.append(kwargs)
            if state.during:
                state.during()
            self.resources()
            return previous | {"name": kwargs["cutover_capture"].generation}
    monkeypatch.setattr(incremental_recovery, "EncryptedRecoveryCopies", Copies)
    request = incremental_recovery.CutoverCapture("copy_"+"2"*32, previous["name"], 100)
    arguments = dict(storage_root=history, pg_dump=tmp_path/"pg_dump", pg_controldata=tmp_path/"pg_controldata",
                     max_bytes=500, timeout_seconds=30, headroom_bytes={"ssd":17,"hdd":29},
                     incremental=SimpleNamespace(pg_path=source), max_objects=10)
    return SimpleNamespace(run=lambda **kw: maintenance.run_due_local_recovery(database, **(arguments|kw)),
                           state=state, stats=stats, request=request, config=config)


def test_named_capture_replaces_only_source_percentage_and_skips_due_pruning(cutover_maintenance):
    f = cutover_maintenance
    assert f.run(cutover_capture=f.request)["state"] == "completed"
    assert len(f.state.calls) == 1 and not f.state.prune
    assert f.state.options["reserve_bytes"] == 2071
    assert f.config.policy.get("cutover_capture") is None
    f.stats["ssd"].available_bytes = 140  # 100 + 11 reserved + 13 auxiliary + 17 headroom = 141.
    with pytest.raises(RuntimeError, match="capacity_or_filesystem_changed"):
        f.run(cutover_capture=f.request)


def test_named_capture_retains_runtime_abort_and_full_hdd_admission(cutover_maintenance):
    f = cutover_maintenance
    assert f.run(cutover_capture=f.request, max_bytes=8000) == {
        "state": "blocked", "reason": "recovery_creation_capacity"}
    assert not f.state.calls
    f.state.during = lambda: setattr(f.stats["ssd"], "available_bytes", 140)
    with pytest.raises(RuntimeError, match="capacity_or_filesystem_changed"):
        f.run(cutover_capture=f.request)


def test_completed_named_request_does_not_reserve_another_full_allocation(cutover_maintenance):
    f = cutover_maintenance
    f.state.completed.append((f.state.completed[0][0], None,
                              f.state.completed[0][2] | {"name": f.request.generation}))
    assert f.run(cutover_capture=f.request, max_bytes=8000)["state"] == "completed"
    assert not f.state.prune


def test_standard_maintenance_keeps_percentage_floor_and_not_due(cutover_maintenance):
    f = cutover_maintenance
    with pytest.raises(RuntimeError, match="capacity_or_filesystem_changed"):
        f.run()
    f.stats["ssd"].available_bytes = 241
    assert f.run()["state"] == "not_due"
    assert not f.state.calls and len(f.state.prune) == 2


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
    config["recovery"]["max_objects"] = 10_000_000
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
    assert runners["service"].use_saved_history_policy
    assert runners["service"].canonical_repository.database is database
    for key in ("history_runner", "recovery_runner"):
        assert runners[key](cancelled=cancelled) == {"state": "disabled"}
    assert all(item[0] is database and item[1]["cancelled"] is cancelled for item in seen)
    assert seen[0][1]["resource_limits"] == config["history"]
    assert seen[1][1]["storage_root"] == root
    assert seen[1][1]["max_bytes"] == config["recovery"]["max_bytes"]
    assert seen[1][1]["max_objects"] == 10_000_000
    assert str(seen[1][1]["pg_dump"]) == "/usr/lib/postgresql/15/bin/pg_dump"


@pytest.mark.parametrize("change", [
    lambda value: value.update(schema_version="unknown"),
    lambda value: value.update(policy={}),
    lambda value: value.update(PG_DSN="not-an-accepted-setting"),
    lambda value: value["history"].update(movement_timeout_seconds=True),
    lambda value: value["recovery"].update(max_bytes=0),
    lambda value: value["recovery"].update(max_objects=10_000_001),
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


def test_saved_policy_pause_and_absence_do_not_inspect_filesystems(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
    from core.storage_targets import StoragePolicy
    from portal.backend.service.storage import history_policy
    @contextmanager
    def session():
        yield object()
    database = SimpleNamespace(session=session)
    base = CanonicalFactRetentionPolicy(execution_enabled=True, hot_days=90,
        hot_days_by_fact_type={"derivatives.funding_rate": 120})
    monkeypatch.setattr(history_policy, "_archive_policy",
        lambda *a: pytest.fail("paused or unconfigured policy must not inspect storage"))
    monkeypatch.setattr(history_policy, "_read", lambda _: (None, None))
    effective, witness = history_policy.saved_canonical_policy(database,
        policy=base, storage_root=tmp_path/"missing")
    assert not effective.execution_enabled and witness is None
    paused = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",),
                           backups=("hdd",), recent_days=7, movement_enabled=False)
    monkeypatch.setattr(history_policy, "_read", lambda _: (paused, {"policy_revision": 3}))
    effective, _ = history_policy.saved_canonical_policy(database,
        policy=base, storage_root=tmp_path/"missing")
    assert not effective.execution_enabled and effective.hot_days == 7
    assert effective.hot_days_by_fact_type == {} and not (tmp_path/"missing").exists()


def test_saved_policy_cannot_enable_disabled_archival_deployment_gate(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
    from core.storage_targets import StoragePolicy
    from portal.backend.service.storage import history_policy
    @contextmanager
    def session():
        yield object()
    saved = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",),
                          backups=("hdd",), recent_days=7, movement_enabled=True)
    monkeypatch.setattr(history_policy, "_read", lambda _: (saved, {"policy_revision": 1}))
    monkeypatch.setattr(history_policy, "_archive_policy", lambda session, saved, base, root: base)
    effective, _ = history_policy.saved_canonical_policy(SimpleNamespace(session=session),
        policy=CanonicalFactRetentionPolicy(execution_enabled=False), storage_root=tmp_path)
    assert effective.hot_days == 7 and not effective.execution_enabled


def test_adding_a_history_target_does_not_require_relocating_existing_archive_root(tmp_path, monkeypatch):
    from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
    from core.storage_targets import StoragePolicy, StorageTarget
    from portal.backend.service.storage import history_policy
    archive = tmp_path/"archive"
    archive.mkdir()
    records = [SimpleNamespace(id=name, label=name, filesystem_uuid="uuid-"+name,
        root=str(archive if name == "hdd" else tmp_path/name), medium="ssd" if name == "ssd" else "hdd",
        roles=["recent", "history", "archives", "backups"], state="active",
        reserved_bytes=100, auxiliary_reserved_bytes=200) for name in ("ssd", "hdd", "hdd2")]
    monkeypatch.setattr(history_policy, "registered_header_targets", lambda _: records)
    inspected = []
    def inspect(target, **kwargs):
        inspected.append(target.target_id)
        return SimpleNamespace(path=target.root, total_bytes=10000)
    monkeypatch.setattr(StorageTarget, "inspect", inspect)
    saved = StoragePolicy(recent=("ssd",), history=("hdd", "hdd2"),
        archives=("hdd",), backups=("hdd",), recent_days=7, movement_enabled=True)
    effective = history_policy._archive_policy(object(), saved,
        CanonicalFactRetentionPolicy(archive_min_free_bytes=0), archive)
    assert effective.hot_days == 7 and effective.archive_min_free_bytes == 2300
    assert inspected == ["hdd"]


def test_encrypted_config_is_explicit_and_uses_existing_runner_without_io(tmp_path, monkeypatch):
    config = configuration()
    config["schema_version"] = "qt.storage_maintenance_limits.v2"
    config["recovery"]["incremental"] = {
        "pgbackrest": "/usr/local/bin/pgbackrest", "restic": "/usr/local/bin/restic",
        "pg_path": "/var/lib/postgresql/data", "pg_socket_path": "/run/postgresql",
        "database_key_path": "/run/secrets/database-key",
        "archive_key_path": "/run/secrets/archive-key", "max_chain_backups": 30,
    }
    path = tmp_path/"limits.json"
    path.write_text(json.dumps(config))
    calls = []
    monkeypatch.setattr(runtime, "run_due_local_recovery",
                        lambda *args, **kwargs: calls.append(kwargs))
    runners = runtime.storage_maintenance_runners(object(), storage_root=tmp_path, limits_path=path)
    assert not calls
    runners["recovery_runner"]()
    assert calls[0]["incremental"].max_chain_backups == 30
    assert str(calls[0]["incremental"].database_key_path) == "/run/secrets/database-key"
    for mutate in (
        lambda c: c["recovery"].pop("incremental"),
        lambda c: c["recovery"]["incremental"].update(PG_DSN="not another authority"),
        lambda c: c["recovery"]["incremental"].update(max_chain_backups=True),
        lambda c: c["recovery"]["incremental"].update(database_key_path="relative"),
    ):
        value = json.loads(json.dumps(config))
        mutate(value)
        path.write_text(json.dumps(value))
        with pytest.raises(ValueError):
            runtime.storage_maintenance_runners(object(), storage_root=tmp_path, limits_path=path)
