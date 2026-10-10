"""Initial source resumption, fixed clocks and drift refusal."""
from scripts.automation import storage_host_boundary as host_boundary
from copy import deepcopy
import json
from pathlib import Path

import pytest

from scripts.automation import storage_online_prepare as online
from tests.test_storage_handoff_pause import database_setup, PROJECT, REVISION


@pytest.fixture
def source_setup(database_setup, monkeypatch):
    state, docker = database_setup
    working = state.parent/"working"; working.mkdir(); (working/"objects").mkdir()
    for row in docker.rows.values():
        row.update(running=True, pid=100, status="running", exit_code=0)
    docker.rows["initialize"].update(running=False, pid=0, status="exited", exit_code=0)
    base = docker.details[docker.original["id"]]
    for name, row in docker.rows.items():
        if name == "tsdb":
            continue
        details = deepcopy(base)
        details.update(id=row["id"], image=row["image"], mounts=[])
        details["config"].update(Hostname=name, Healthcheck=None)
        if name == "market-data-collector":
            details["mounts"] = [{"Type":"bind", "Source":str(working),
                "Destination":"/app/logs/market-structure", "RW":True}]
        docker.details[row["id"]] = details
    query = host_boundary.database_query
    monkeypatch.setattr(host_boundary, "database_query", lambda container, sql:
        json.dumps({"archive_mode":"off","archive_command_disabled":True,"archive_configuration":0,"captures":0})
        if "'archive_mode'" in sql else query(container, sql))
    actual = docker.__call__
    starts = []
    interrupt = {"after_start": False}
    def call(action, *args, **kwargs):
        if action == "stop":
            assert online._load(state)["phase"] == "preparing"
        if action == "inspect" and args[1] == "{{json .State}}":
            row = next(row for row in docker.rows.values() if row["id"] == args[-1])
            return json.dumps({"Running": row["running"]})
        if action == "start" and args[0] != docker.rows["tsdb"]["id"]:
            saved = online._load(state)
            assert saved["phase"] == "resuming"
            row = next(row for row in docker.rows.values() if row["id"] == args[0])
            row.update(running=True, pid=100, status="running")
            starts.append(args[0])
            if interrupt["after_start"]:
                interrupt["after_start"] = False
                raise TimeoutError("lost source start reply")
            return args[0]
        return actual(action, *args, **kwargs)
    monkeypatch.setattr(host_boundary, "docker", call)
    return state, docker, working, starts, interrupt


def prepare(state):
    return online.prepare_online_source(state, project=PROJECT,
        source_revision=REVISION, history_uuid="fixture-hdd")


def test_success_and_read_only_reentry_keep_hold_and_original_deadlines(source_setup, monkeypatch):
    state, docker, working, starts, _ = source_setup
    result = prepare(state)
    assert result["phase"] == "serving"
    assert len(starts) == (len(host_boundary.STOP)-1)
    hold = host_boundary.load_receipt(state/host_boundary.HOLD)
    assert hold["database_preparation"]["deadline"] == result["deadline"]
    assert state.joinpath(host_boundary.HOLD).exists()
    before = (state/online.STATE).read_bytes()
    monkeypatch.setattr(online.time, "time", lambda: result["deadline"]+1000)
    assert prepare(state) == result
    assert (state/online.STATE).read_bytes() == before
    assert len(starts) == (len(host_boundary.STOP)-1)
    # Completed preparation is read-only admission, never permission to restart.
    docker.rows["backend"].update(running=False, status="exited", pid=0)
    with pytest.raises(RuntimeError, match="source_not_running"):
        prepare(state)


@pytest.mark.parametrize("point", ["created", "source_started"])
def test_interrupted_preparation_and_partial_resumption_retain_bindings(source_setup, point):
    state, docker, working, starts, interrupt = source_setup
    if point == "created":
        docker.interrupt = point
    else:
        interrupt["after_start"] = True
    with pytest.raises(TimeoutError):
        prepare(state)
    first = online._load(state)
    result = prepare(state)
    assert result["deadline"] == first["deadline"]
    assert result["started_at"] == first["started_at"]
    assert len(starts) == len(set(starts)) == (len(host_boundary.STOP)-1)
    assert docker.operations.count("create") == 1


@pytest.mark.parametrize("drift", ["deadline", "client", "source", "recipe", "hold", "capture"])
def test_partial_resumption_refuses_drift_without_starting_another_client(source_setup, monkeypatch, drift):
    state, docker, working, starts, interrupt = source_setup
    interrupt["after_start"] = True
    with pytest.raises(TimeoutError):
        prepare(state)
    saved = online._load(state)
    if drift == "deadline":
        monkeypatch.setattr(online.time, "time", lambda: saved["deadline"]+1)
    elif drift == "client":
        docker.details[docker.rows["frontend"]["id"]]["host"]["Privileged"] = True
    elif drift == "source":
        working.chmod(0o700)
    elif drift == "recipe":
        path = state/online.held.DATABASE_RECIPE
        model = json.loads(path.read_text())
        model["services"]["tsdb"]["environment"]["CHANGED"] = "true"
        path.write_text(json.dumps(model))
    elif drift == "hold":
        path = state/host_boundary.HOLD
        model = json.loads(path.read_text()); model["phase"] = "paused"
        path.write_text(json.dumps(model))
    else:
        monkeypatch.setattr(online, "_before_capture",
            lambda _: (_ for _ in ()).throw(RuntimeError("capture unexpectedly exists")))
    with pytest.raises(RuntimeError):
        prepare(state)
    assert len(starts) == 1
    assert online._load(state)["deadline"] == saved["deadline"]
    assert (state/host_boundary.HOLD).exists()


def test_existing_recovery_or_capture_is_refused_before_any_stop(source_setup, monkeypatch):
    state, docker, working, starts, _ = source_setup
    monkeypatch.setattr(online, "_before_capture",
        lambda _: (_ for _ in ()).throw(RuntimeError("existing recovery guarantee")))
    with pytest.raises(RuntimeError, match="existing recovery"):
        prepare(state)
    assert not docker.stops and not starts
    assert not (state/online.STATE).exists()

@pytest.mark.parametrize("change", [{"archive_mode":"on"}, {"archive_configuration":1}, {"captures":1}])
def test_real_initial_admission_refuses_existing_archive_or_capture(source_setup, monkeypatch, change):
    state, docker, _, starts, _ = source_setup
    original = host_boundary.database_query
    value = dict(archive_mode="off", archive_command_disabled=True, archive_configuration=0, captures=0)
    value.update(change)
    monkeypatch.setattr(host_boundary, "database_query", lambda container, sql:
        json.dumps(value) if "'archive_mode'" in sql else original(container,sql))
    with pytest.raises(RuntimeError, match="uncaptured_key_free_source"):
        prepare(state)
    assert not docker.stops and not starts


def test_health_wait_cannot_extend_preparation_or_mark_it_completed(source_setup, monkeypatch):
    state, docker, _, starts, _ = source_setup
    def unhealthy(rows):
        deadline = online._load(state)["deadline"]
        monkeypatch.setattr(online.time, "time", lambda: deadline+1)
        return False
    monkeypatch.setattr(online, "_source_healthy", unhealthy)
    with pytest.raises(RuntimeError, match="deadline_expired"):
        prepare(state)
    saved = online._load(state)
    assert saved["phase"] == "resuming" and saved["completed_at"] is None
    with pytest.raises(RuntimeError, match="deadline_expired"):
        prepare(state)
    assert len(starts) == (len(host_boundary.STOP)-1)


def test_mount_order_is_not_identity_but_duplicate_or_changed_mount_is():
    a = {"Destination":"/pg","Source":"/retained","RW":True}
    b = {"Destination":"/history","Source":"/hdd","RW":True}
    assert online._mount_digest([a,b]) == online._mount_digest([b,a])
    assert online._mount_digest([a,b]) != online._mount_digest([a,{**b,"RW":False}])
    with pytest.raises(RuntimeError, match="duplicate_mount"):
        online._mount_digest([a,a])


def test_completed_initializer_drift_refuses_before_any_more_starts(source_setup):
    state, docker, _, starts, interrupt = source_setup
    interrupt["after_start"] = True
    with pytest.raises(TimeoutError):
        prepare(state)
    docker.rows["initialize"].update(running=True, pid=100, status="running")
    with pytest.raises(RuntimeError, match="completed_initializer_changed"):
        prepare(state)
    assert len(starts) == 1
