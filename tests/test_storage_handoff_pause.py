from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys

import pytest

from scripts.automation import storage_handoff_pause as pause

REVISION = "a" * 40
PROJECT = "qt-pause-fixture"


class Docker:
    def __init__(self, state):
        self.state = state
        self.stops = []
        self.fail_after_stop = None
        self.forced = None
        self.rows = {}
        for i, service in enumerate(pause.STOP + pause.PASSIVE, 1):
            self.rows[service] = dict(id=f"{i:064x}", image="sha256:" + f"{i:064x}",
                project=PROJECT, service=service, oneoff="False", restart="unless-stopped",
                running=True, restarting=False, paused=False, pid=i, status="running", exit_code=0, oom=False)
        self.rows["initialize"].update(running=False, pid=0, status="exited", restart="no")

    def __call__(self, action, *args, **kwargs):
        if action == "ps":
            return "\n".join(x["id"] for x in self.rows.values())
        if action == "inspect":
            return "\n".join(json.dumps(x) for x in self.rows.values())
        assert action == "stop"
        assert (self.state / pause.HOLD).exists(), "hold must precede the first stop"
        assert kwargs["timeout"] <= 330
        row = next(x for x in self.rows.values() if x["id"] == args[-1])
        self.stops.append(row["service"])
        row.update(running=False, pid=0, status="exited", exit_code=137 if row["service"] == self.forced else 0)
        if row["service"] == self.fail_after_stop:
            self.fail_after_stop = None
            raise TimeoutError("reply lost after stopping service")
        return row["id"]


@pytest.fixture
def setup(tmp_path, monkeypatch):
    state = tmp_path / "state"
    state.mkdir()
    (state / "release.env").write_text(f"current_revision={REVISION}\n")
    docker = Docker(state)
    monkeypatch.setattr(pause, "_docker", docker)
    return state, docker


def enter(state):
    return pause.paused_storage_clients(state, project=PROJECT, source_revision=REVISION)


def test_pause_holds_existing_lock_and_keeps_database_and_telemetry_running(setup):
    state, docker = setup
    with enter(state) as receipt:
        assert receipt["phase"] == "paused"
        assert not any(docker.rows[x]["running"] for x in pause.STOP)
        assert all(docker.rows[x]["running"] for x in pause.PASSIVE)
        with (state / "deployment.lock").open("w") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert (state / pause.HOLD).stat().st_mode & 0o777 == 0o600
    prior = docker.stops[:]
    with enter(state):
        pass
    assert docker.stops == prior
    assert (state / pause.HOLD).exists()  # success is not permission to restart


def test_lost_stop_reply_can_resume_without_restarting_already_stopped_services(setup):
    state, docker = setup
    docker.fail_after_stop = "market-data-collector"
    with pytest.raises(TimeoutError):
        with enter(state):
            pytest.fail("pause must not authorize a cutover")
    assert json.loads((state / pause.HOLD).read_text())["phase"] == "pausing"
    with enter(state):
        pass
    assert docker.stops.count("backend") == docker.stops.count("market-data-collector") == 1


def test_caller_failure_keeps_clients_stopped_and_hold_present(setup):
    state, docker = setup
    with pytest.raises(RuntimeError, match="cutover interrupted"):
        with enter(state):
            raise RuntimeError("cutover interrupted")
    assert (state / pause.HOLD).exists()
    assert not any(docker.rows[x]["running"] for x in pause.STOP)


@pytest.mark.parametrize("change", ["unexpected", "restarting", "always", "database_down", "oneoff", "missing", "oom"])
def test_unsafe_initial_state_never_stops_anything(setup, change):
    state, docker = setup
    if change == "unexpected":
        docker.rows["runtime"] = {**docker.rows["backend"], "service": "runtime", "id": "f" * 64}
    elif change == "restarting":
        docker.rows["backend"]["restarting"] = True
    elif change == "always":
        docker.rows["backend"]["restart"] = "always"
    elif change == "database_down":
        docker.rows["tsdb"].update(running=False, pid=0, status="exited")
    elif change == "oneoff":
        docker.rows["backend"]["oneoff"] = "True"
    elif change == "missing":
        del docker.rows["market-data-collector"]
    else:
        docker.rows["backend"]["oom"] = True
    with pytest.raises(RuntimeError):
        with enter(state):
            pytest.fail("unsafe source accepted")
    assert not docker.stops
    assert not (state / pause.HOLD).exists()


def test_forced_kill_is_not_accepted_as_clean_pause(setup):
    state, docker = setup
    docker.forced = "market-data-collector"
    with pytest.raises(RuntimeError, match="unclean_stop"):
        with enter(state):
            pytest.fail("SIGKILL incorrectly qualified")
    assert (state / pause.HOLD).exists()


@pytest.mark.parametrize("change", ["id", "image", "project", "source_revision", "corrupt", "duplicate", "symlink"])
def test_changed_or_corrupt_hold_cannot_resume(setup, change):
    state, docker = setup
    with enter(state):
        pass
    path = state / pause.HOLD
    receipt = json.loads(path.read_text())
    if change in ("id", "image"):
        docker.rows["backend"][change] = "e" * 64
    elif change in ("project", "source_revision"):
        receipt[change] = "other"
        path.write_text(json.dumps(receipt))
    elif change == "corrupt":
        path.write_text("{")
    elif change == "duplicate":
        path.write_text('{"phase":"paused",' + json.dumps(receipt)[1:])
    else:
        path.unlink()
        path.symlink_to(state / "absent")
    prior = docker.stops[:]
    with pytest.raises((RuntimeError, ValueError, OSError)):
        with enter(state):
            pytest.fail("changed hold accepted")
    assert docker.stops == prior
    assert os.path.lexists(path)


def test_container_created_after_backend_stops_blocks_cutover(setup, monkeypatch):
    state, docker = setup
    def boundary(action, *args, **kwargs):
        result = docker(action, *args, **kwargs)
        if action == "stop":
            docker.rows["runtime"] = {**docker.rows["backend"], "service": "runtime", "id": "f" * 64}
        return result
    monkeypatch.setattr(pause, "_docker", boundary)
    with pytest.raises(RuntimeError, match="unexpected_client"):
        with enter(state):
            pytest.fail("late runtime accepted")
    assert docker.stops == ["backend"]
    assert (state / pause.HOLD).exists()


def test_failure_to_persist_hold_never_stops_anything(setup, monkeypatch):
    state, docker = setup
    def fail(*args, **kwargs):
        raise OSError("disk full")
    monkeypatch.setattr(pause.os, "fsync", fail)
    with pytest.raises(OSError, match="disk full"):
        with enter(state):
            pytest.fail("unpersisted hold accepted")
    assert not docker.stops
    assert (state / pause.HOLD).exists()


def test_process_death_keeps_hold_and_releases_only_transient_lock(setup):
    state, docker = setup
    # Fresh interpreter avoids forking the test suite's existing worker threads.
    code = "\n".join([
        "import importlib.util, signal, sys",
        "from pathlib import Path",
        "from scripts.automation import storage_handoff_pause as pause",
        "spec = importlib.util.spec_from_file_location('pause_fixture', sys.argv[1])",
        "fixture = importlib.util.module_from_spec(spec)",
        "spec.loader.exec_module(fixture)",
        "state = Path(sys.argv[2])",
        "pause._docker = fixture.Docker(state)",
        "with fixture.enter(state):",
        "    print('READY', flush=True)",
        "    signal.pause()",
    ])
    child = subprocess.Popen([sys.executable, "-c", code, __file__, str(state)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        import select
        assert select.select([child.stdout], [], [], 10)[0]
        assert child.stdout.readline() == b"READY\n"
    finally:
        child.kill()
        child.communicate(timeout=10)
    assert json.loads((state / pause.HOLD).read_text())["phase"] == "paused"
    with (state / "deployment.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_existing_deployment_lock_prevents_pause(setup):
    state, docker = setup
    with (state / "deployment.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="lock_busy"):
            with enter(state):
                pytest.fail("overlap accepted")
    assert not docker.stops


def test_recorded_release_and_unfinished_promotions_are_checked_before_docker(setup):
    state, docker = setup
    (state / "release.env").write_text("current_revision=wrong\n")
    with pytest.raises(RuntimeError, match="release_mismatch"):
        with enter(state):
            pass
    (state / "promotion.env").write_text("unfinished")
    with pytest.raises(RuntimeError, match="unfinished_server_operation"):
        with enter(state):
            pass
    assert not docker.stops
