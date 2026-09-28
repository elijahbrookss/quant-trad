"""Durable final stop clocks, interruption and independent deployment exclusion."""
from scripts.automation import storage_host_boundary as host_boundary
from types import SimpleNamespace

import pytest

from scripts.automation import storage_online_final as final

PROJECT, REVISION, WORKER, CONTROLLER = "qt-owned", "a"*40, "b"*64, "c"*32


@pytest.fixture
def pause_setup(tmp_path, monkeypatch):
    rows = {name: dict(id=str(i+1)*64, running=name != "initialize",
                      status="exited" if name == "initialize" else "running", exit_code=0)
            for i, name in enumerate(host_boundary.STOP)}
    preparation = {"clients": {k: {"was_running": v["running"]} for k, v in rows.items()}}
    clock = {"wall": 1000., "boot": 100., "boot_id": "a"*36}
    state = {"binding": {"runtime": 1}, "limit": 90, "capture_deadline": 1200.,
             "interrupt": False, "stops": []}
    monkeypatch.setattr(final.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(final, "_boot_seconds", lambda: clock["boot"])
    monkeypatch.setattr(final, "_boot_id", lambda: clock["boot_id"])
    def observe(root, **args):
        return (dict(state["binding"], **args), preparation, rows,
                {"seconds": state["limit"], "capture_deadline": state["capture_deadline"]})
    monkeypatch.setattr(final, "_observe", observe)
    monkeypatch.setattr(final.initial, "_source_healthy", lambda _: True)
    def docker(action, *args, **kwargs):
        assert action == "stop" and args[:4] == ("--signal", "SIGTERM", "--timeout", "-1")
        saved = final._load(tmp_path/final.STATE)
        assert saved["phase"] == "stopping" and saved["started_at"] == 1000
        row = next(r for r in rows.values() if r["id"] == args[-1])
        state["stops"].append(args[-1])
        row.update(running=False, status="exited")
        if state["interrupt"]:
            state["interrupt"] = False
            raise TimeoutError("lost graceful stop reply")
        return ""
    monkeypatch.setattr(host_boundary, "docker", docker)
    def stop(**changed):
        return final.stop_online_source_locked(tmp_path, **{
            "project": PROJECT, "source_revision": REVISION, "worker_id": WORKER,
            "controller_id": CONTROLLER, "max_duration_seconds": 60, **changed})
    return tmp_path, rows, clock, state, stop


def test_final_stop_persists_intent_and_never_restarts_initializer(pause_setup):
    path, rows, clock, state, stop = pause_setup
    receipt = stop()
    assert receipt["phase"] == "paused"
    assert len(state["stops"]) == len(host_boundary.STOP)-1
    assert receipt["deadline"] == 1060 and receipt["deadline_boot"] == 160
    before = (path/final.STATE).read_bytes()
    assert stop() == receipt and (path/final.STATE).read_bytes() == before
    rows["backend"]["running"] = True
    with pytest.raises(RuntimeError, match="stopped_client_restarted"):
        stop()


def test_lost_stop_reply_reentry_retains_original_window_and_skips_stopped(pause_setup):
    path, rows, clock, state, stop = pause_setup
    state["interrupt"] = True
    with pytest.raises(TimeoutError):
        stop()
    original = final._load(path/final.STATE)
    clock.update(wall=1005, boot=105)
    result = stop()
    assert result["deadline"] == original["deadline"]
    assert result["deadline_boot"] == original["deadline_boot"]
    assert len(state["stops"]) == len(set(state["stops"])) == len(host_boundary.STOP)-1


@pytest.mark.parametrize("drift", ["boot", "wall", "expiry", "runtime", "controller", "duration", "resource", "capture"])
def test_interrupted_final_refuses_drift_without_more_stops(pause_setup, drift):
    path, rows, clock, state, stop = pause_setup
    state["interrupt"] = True
    with pytest.raises(TimeoutError): stop()
    before = (path/final.STATE).read_bytes()
    changed = {}
    if drift == "boot": clock["boot_id"] = "b"*36
    elif drift == "wall": clock["wall"] = 999
    elif drift == "expiry": clock["boot"] = 161
    elif drift == "runtime": state["binding"]["runtime"] = 2
    elif drift == "controller": changed["controller_id"] = "d"*32
    elif drift == "duration": changed["max_duration_seconds"] = 61
    elif drift == "resource": state["limit"] = 30
    else: state["capture_deadline"] = 1030
    with pytest.raises(RuntimeError): stop(**changed)
    assert len(state["stops"]) == 1 and (path/final.STATE).read_bytes() == before


def test_final_resource_refusal_precedes_intent_and_stop(pause_setup):
    path, rows, clock, state, stop = pause_setup
    state["limit"] = 30
    with pytest.raises(RuntimeError, match="window_not_admitted"): stop()
    assert not state["stops"] and not (path/final.STATE).exists()


def test_nested_docker_calls_share_one_absolute_budget(monkeypatch):
    clock = [10.]
    timeouts = []
    monkeypatch.setattr(host_boundary.time, "monotonic", lambda: clock[0])
    def run(args, **kwargs):
        timeouts.append(kwargs["timeout"])
        clock[0] += 2
        return SimpleNamespace(returncode=0, stdout="ok")
    monkeypatch.setattr(host_boundary.subprocess, "run", run)
    with host_boundary.docker_deadline(15):
        assert host_boundary.docker("inspect", "owned", timeout=30) == "ok"
        with host_boundary.docker_deadline(99):
            assert host_boundary.docker("inspect", "owned", timeout=30) == "ok"
        with pytest.raises(RuntimeError, match="host_deadline_expired"):
            host_boundary.docker("stop", "--timeout", "-1", "owned", timeout=30)
    assert timeouts == [5,3,1]
    assert host_boundary.current_docker_deadline() is None


def test_source_drain_rechecks_held_source_and_original_deadline(pause_setup):
    path, rows, clock, state, stop = pause_setup
    saved = stop()
    host_boundary.save_receipt(path/"storage-online-request.json", {"command_seconds": 30}, initial=True)
    before = (path/final.STATE).read_bytes()
    calls = []
    def exchange(operation, **kwargs):
        calls.append(kwargs)
        return dict(controller_id=CONTROLLER, operation=operation, state="background",
                    final_switch_authorized=False, collection_resume_authorized=False,
                    result=dict(spool_empty_at_observation=False,publisher_drain_authorized=False,
                                final_switch_authorized=False,collection_resume_authorized=False))
    assert not final.observe_source_drain_locked(path, exchange=exchange,max_entries=10)["spool_empty_at_observation"]
    assert calls and (path/final.STATE).read_bytes() == before
    def restarted(*args, **kwargs):
        reply = exchange(*args, **kwargs)
        rows["backend"]["running"] = True
        return reply
    with pytest.raises(RuntimeError, match="spool_source_changed"):
        final.observe_source_drain_locked(path, exchange=restarted,max_entries=10)
    rows["backend"]["running"] = False
    clock.update(wall=1061,boot=161)
    with pytest.raises(RuntimeError, match="deadline_expired"):
        final.observe_source_drain_locked(path, exchange=exchange,max_entries=10)
    assert (path/final.STATE).read_bytes() == before


@pytest.mark.parametrize("drift", [None, "source", "worker", "expiry", "reply", "families"])
def test_final_delta_host_rechecks_original_window_and_exact_worker(pause_setup, drift):
    path, rows, clock, state, stop = pause_setup
    stop()
    before = (path/final.STATE).read_bytes()
    deadline = final.time.monotonic()+20
    calls = []
    def exchange(operation, **kwargs):
        assert operation == "final_delta" and kwargs == {"deadline": deadline}
        calls.append(True)
        if drift == "source": rows["backend"]["running"] = True
        if drift == "worker": state["binding"]["runtime"] = 2
        if drift == "expiry": clock.update(wall=1061, boot=161)
        result = dict(migration_ready=False,final_switch_authorized=False,collection_resume_authorized=False,
            sql={"outcome":"both_tails_observed_empty"},archives=[
                {"family":f,"captured_tail_empty_at_observation":True} for f in
                ("fact_archive_manifests","raw_archive_manifests","book_checkpoint_manifests")])
        if drift == "families": result["archives"] *= 2
        return dict(controller_id="f"*32 if drift=="reply" else CONTROLLER,
            operation=operation,state="background",final_switch_authorized=False,
            collection_resume_authorized=False,result=result)
    if drift:
        with pytest.raises(RuntimeError):
            final.copy_final_delta_locked(path,exchange=exchange,deadline=deadline,max_rounds=2)
    else:
        result=final.copy_final_delta_locked(path,exchange=exchange,deadline=deadline,max_rounds=2)
        assert result["rounds"]==1 and not result["final_switch_authorized"]
        with pytest.raises(RuntimeError,match="deadline_invalid"):
            final.copy_final_delta_locked(path,exchange=exchange,
                deadline=final.time.monotonic()+80,max_rounds=1)
    assert len(calls)==1 and (path/final.STATE).read_bytes()==before


@pytest.mark.parametrize("drift", [None, "deadline", "controller", "source", "sequence", "unbound"])
def test_switch_entry_requires_exact_live_final_window(pause_setup, drift):
    import time
    path, rows, clock, state, stop = pause_setup
    paused = stop();before = (path/final.STATE).read_bytes()
    deadline = time.monotonic()+20
    def observe_worker(**args):
        assert args == {"deadline": deadline}
        reply = dict(controller_id=CONTROLLER, operation="status", state="background",
            bound_final_deadline=deadline, last_sequence=5, migration_ready=False,
            final_switch_authorized=False, collection_resume_authorized=False)
        if drift == "deadline":clock.update(wall=1061, boot=161)
        elif drift == "controller":reply["controller_id"] = "d"*32
        elif drift == "source":rows["backend"]["running"] = True
        elif drift == "sequence":reply["last_sequence"] = True
        elif drift == "unbound":reply["bound_final_deadline"] = None
        return reply
    if drift:
        with pytest.raises(RuntimeError):
            final.record_switch_entry_locked(path, observe_worker=observe_worker, deadline=deadline)
        assert (path/final.STATE).read_bytes() == before
    else:
        result = final.record_switch_entry_locked(path, observe_worker=observe_worker, deadline=deadline)
        assert not result["database_switch_authorized"] and not result["collection_resume_authorized"]
        entered = final._load(path/final.STATE)
        assert entered["phase"] == "switch_entered"
        assert entered["deadline"] == paused["deadline"] and entered["deadline_boot"] == paused["deadline_boot"]
        assert entered["binding"] == paused["binding"]
        with pytest.raises(RuntimeError, match="switch_reconciliation_required"):stop()


def test_switch_intent_survives_lost_save_reply_without_reentry(pause_setup, monkeypatch):
    import time
    path, rows, clock, state, stop = pause_setup
    paused = stop();deadline = time.monotonic()+20
    original = host_boundary.save_receipt
    def save_then_lose(*args, **kwargs):
        original(*args, **kwargs)
        raise TimeoutError("lost durable write response")
    monkeypatch.setattr(host_boundary, "save_receipt", save_then_lose)
    def live(**args):
        return dict(controller_id=CONTROLLER, operation="status", state="background",
            bound_final_deadline=deadline, last_sequence=3, migration_ready=False,
            final_switch_authorized=False, collection_resume_authorized=False)
    with pytest.raises(TimeoutError):
        final.record_switch_entry_locked(path, observe_worker=live, deadline=deadline)
    entered = final._load(path/final.STATE);checkpoint = (path/final.STATE).read_bytes()
    assert entered["phase"] == "switch_entered" and entered["deadline"] == paused["deadline"]
    def forbidden(**args):raise AssertionError("reentered worker")
    with pytest.raises(RuntimeError, match="switch_reconciliation_required"):
        final.record_switch_entry_locked(path, observe_worker=forbidden, deadline=deadline)
    assert (path/final.STATE).read_bytes() == checkpoint
    with pytest.raises(RuntimeError, match="paused_source_required"):
        final.copy_final_delta_locked(path, exchange=forbidden, deadline=deadline, max_rounds=1)


@pytest.mark.parametrize("outcome", ["pending", "uncommitted", "committed", "invalid", "source_drift", "expired"])
def test_outcome_observation_never_releases_host_intent(pause_setup, outcome):
    import time
    path, rows, clock, state, stop = pause_setup
    stop();deadline = time.monotonic()+20
    status = dict(controller_id=CONTROLLER, operation="status", state="background",
        bound_final_deadline=deadline, last_sequence=3, migration_ready=False,
        final_switch_authorized=False, collection_resume_authorized=False)
    final.record_switch_entry_locked(path, observe_worker=lambda **_:status, deadline=deadline)
    before = (path/final.STATE).read_bytes()
    host_boundary.save_receipt(path/"storage-online-request.json", {"command_seconds": 1}, initial=True)
    def exchange(operation, **args):
        assert operation == "inspect_outcome" and 0 < args["deadline"]-time.monotonic() <= 1
        if outcome == "source_drift":rows["backend"]["running"] = True
        elif outcome == "expired":clock.update(wall=1061, boot=161)
        value = {"pending": None, "uncommitted": False, "committed": True}.get(outcome)
        return status | dict(operation=operation, result=dict(outcome=outcome,
            database_handoff_committed=value, collection_resume_authorized=False,
            runtime_activation_authorized=False))
    if outcome in {"invalid", "source_drift", "expired"}:
        with pytest.raises(RuntimeError):final.inspect_switch_outcome_locked(path, exchange=exchange)
    else:
        result = final.inspect_switch_outcome_locked(path, exchange=exchange)
        assert result["outcome"] == outcome and not result["collection_resume_authorized"]
        assert not result["runtime_activation_authorized"]
    assert (path/final.STATE).read_bytes() == before
    with pytest.raises(RuntimeError, match="switch_reconciliation_required"):stop()


@pytest.fixture
def resume_setup(pause_setup, monkeypatch):
    path, rows, clock, state, stop = pause_setup
    saved = stop()
    deadline = final.time.monotonic()+30
    saved.update(phase="switch_entered", switch={"entered_at": 1000.,
        "deadline_monotonic": deadline, "worker_sequence": 5})
    host_boundary.save_receipt(path/final.STATE, saved, initial=False)
    calls = []
    def exchange(operation, **kwargs):
        assert kwargs == {"deadline": deadline}
        calls.append(operation)
        ending = operation == "rollback_fence_end"
        return dict(controller_id=CONTROLLER, operation=operation,
            state="aborted" if ending else "resume_fenced", bound_final_deadline=deadline,
            last_sequence=5+len(calls), final_switch_authorized=False,
            collection_resume_authorized=False, result=dict(database_handoff_committed=False,
                database_resume_fence_held=not ending, collection_resume_authorized=False,
                runtime_activation_authorized=False))
    starts = []
    def start(container_id, *, deadline, check):
        check()
        receipt = final._load(path/final.STATE)
        assert receipt["phase"] == "source_resuming"
        assert receipt["resume"]["inflight"]["container_id"] == container_id
        starts.append(container_id)
        row = next(r for r in rows.values() if r["id"] == container_id)
        row.update(running=True, status="running")
        check()
    monkeypatch.setattr(final, "_supervised_source_start", start)
    return path, rows, clock, state, stop, exchange, calls, starts, deadline


def test_resume_retains_live_fence_original_clocks_and_initializer(resume_setup):
    path, rows, clock, state, stop, exchange, calls, starts, deadline = resume_setup
    original = final._load(path/final.STATE)
    result = final.resume_online_source_locked(path, exchange=exchange)
    saved = final._load(path/final.STATE)
    assert result == dict(original_source_resumed=True, final_marker_retained=True,
                         database_switch_authorized=False, runtime_activation_authorized=False)
    assert saved["phase"] == "source_resumed"
    assert all(saved[k] == v for k, v in original.items() if k != "phase")
    assert calls[0] == "rollback_fence_begin" and calls[-1] == "rollback_fence_end"
    assert len(starts) == len(host_boundary.STOP)-1 and not rows["initialize"]["running"]
    assert saved["resume"]["completed"] == [n for n in host_boundary.STOP if n != "initialize"]
    before = (path/final.STATE).read_bytes()
    with pytest.raises(RuntimeError, match="switch_intent_required"):
        final.resume_online_source_locked(path, exchange=exchange)
    with pytest.raises(RuntimeError, match="reconciliation_required"): stop()
    assert (path/final.STATE).read_bytes() == before


@pytest.mark.parametrize("failure", ["ownership", "expiry", "source", "lost_end"])
def test_resume_failure_never_replays_or_claims_all_clients_stopped(resume_setup, failure):
    path, rows, clock, state, stop, exchange, calls, starts, deadline = resume_setup
    def fail(operation, **kwargs):
        reply = exchange(operation, **kwargs)
        if starts:
            if failure == "ownership": raise RuntimeError("owned SQL fence lost")
            if failure == "expiry": clock["boot"] = 161
            if failure == "source": state["binding"]["runtime"] = 2
            if failure == "lost_end" and operation == "rollback_fence_end":
                raise TimeoutError("lost terminal fence reply")
        return reply
    with pytest.raises((RuntimeError, TimeoutError)):
        final.resume_online_source_locked(path, exchange=fail)
    saved = final._load(path/final.STATE)
    assert saved["phase"] == "source_resuming" and saved["resume"]["finished_at"] is None
    assert any(row["running"] for row in rows.values())
    if failure != "lost_end":
        assert len(starts) == 1 and saved["resume"]["inflight"]["container_id"] == starts[0]
    before, count = (path/final.STATE).read_bytes(), len(starts)
    with pytest.raises(RuntimeError): final.resume_online_source_locked(path, exchange=exchange)
    with pytest.raises(RuntimeError): stop()
    assert len(starts) == count and (path/final.STATE).read_bytes() == before


@pytest.mark.parametrize("bad", ["committed", "cached", "controller", "deadline", "authority"])
def test_resume_invalid_live_admission_never_starts(resume_setup, bad):
    path, rows, clock, state, stop, exchange, calls, starts, deadline = resume_setup
    before = (path/final.STATE).read_bytes()
    def invalid(operation, **kwargs):
        reply = exchange(operation, **kwargs)
        if bad == "committed": reply["result"]["database_handoff_committed"] = True
        elif bad == "cached": reply["last_sequence"] = 5
        elif bad == "controller": reply["controller_id"] = "d"*32
        elif bad == "deadline": reply["bound_final_deadline"] += 1
        else: reply["result"]["collection_resume_authorized"] = True
        return reply
    with pytest.raises(RuntimeError, match="fence_reply_invalid"):
        final.resume_online_source_locked(path, exchange=invalid)
    assert not starts and (path/final.STATE).read_bytes() == before


@pytest.mark.parametrize("loss", ["fence", "deadline", "cancel", "exit"])
def test_supervised_start_checks_during_inflight_action_and_reaps(monkeypatch, loss):
    clock, checks = [10.], []
    process = SimpleNamespace(returncode=None, killed=False, reaped=False)
    process.poll = lambda: process.returncode
    def kill():
        process.killed = True
        process.returncode = -9
    def wait(**kwargs):
        assert kwargs == {"timeout": 1}
        process.reaped = True
    process.kill, process.wait = kill, wait
    def popen(args, **kwargs):
        assert args == ["docker", "start", "b"*64]
        assert checks == [1]
        return process
    monkeypatch.setattr(final.subprocess, "Popen", popen)
    monkeypatch.setattr(final.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(final.time, "sleep", lambda _: None)
    def check():
        checks.append(len(checks)+1)
        if len(checks) == 3:
            if loss == "fence": raise RuntimeError("live ownership lost")
            if loss == "deadline": clock[0] = 20
            if loss == "cancel": raise KeyboardInterrupt()
            if loss == "exit": process.returncode = 1
    with pytest.raises((RuntimeError, KeyboardInterrupt)):
        final._supervised_source_start("b"*64, deadline=15, check=check)
    assert len(checks) == 3 and process.reaped
    assert process.killed is (loss != "exit")



def test_supervised_start_reaps_real_local_child_when_live_check_fails(tmp_path, monkeypatch):
    import subprocess
    import sys
    import time
    original_popen = subprocess.Popen
    child, checks = [], []
    ready = tmp_path/"ready"
    def popen(args, **kwargs):
        assert args == ["docker", "start", "b"*64]
        process = original_popen([sys.executable, "-c",
            "from pathlib import Path; import sys,time; Path(sys.argv[1]).write_text('ready'); time.sleep(30)",
            str(ready)], **kwargs)
        child.append(process)
        return process
    monkeypatch.setattr(final.subprocess, "Popen", popen)
    def check():
        checks.append(True)
        if ready.exists():
            assert child[0].poll() is None
            raise RuntimeError("live fence lost while local action is in flight")
    with pytest.raises(RuntimeError, match="live fence lost"):
        final._supervised_source_start("b"*64, deadline=time.monotonic()+5, check=check)
    assert len(checks) > 1 and child[0].returncode is not None



def test_resume_terminal_reply_does_not_require_worker_to_stay_alive(resume_setup):
    path, rows, clock, state, stop, exchange, calls, starts, deadline = resume_setup
    def end_and_exit(operation, **kwargs):
        reply = exchange(operation, **kwargs)
        if operation == "rollback_fence_end":
            state["binding"]["runtime"] = 2  # Normal terminal process exit.
        return reply
    assert final.resume_online_source_locked(path, exchange=end_and_exit)["original_source_resumed"]
    assert final._load(path/final.STATE)["phase"] == "source_resumed"


@pytest.mark.parametrize("drift", [None, "inflight", "incomplete", "stopped", "initializer",
    "unhealthy", "worker", "expiry", "reboot", "pending", "committed", "active", "controller", "sequence"])
def test_terminal_resume_reconciliation_never_replays_starts(resume_setup, monkeypatch, drift):
    path, rows, clock, state, stop, exchange, calls, starts, deadline = resume_setup
    host_boundary.save_receipt(path/"storage-online-request.json", {"command_seconds": 5}, initial=True)
    def lost(operation, **kwargs):
        reply = exchange(operation, **kwargs)
        if operation == "rollback_fence_end":raise TimeoutError("received end reply discarded")
        return reply
    with pytest.raises(TimeoutError):final.resume_online_source_locked(path, exchange=lost)
    saved = final._load(path/final.STATE)
    count = len(starts)
    if drift == "inflight":
        saved["resume"]["completed"].remove("backend")
        saved["resume"]["inflight"] = dict(service="backend",container_id=rows["backend"]["id"],requested_at=1000.)
    if drift == "incomplete":saved["resume"]["completed"].pop()
    host_boundary.save_receipt(path/final.STATE, saved, initial=False)
    before = (path/final.STATE).read_bytes()
    if drift == "stopped":rows["backend"]["running"] = False
    if drift == "initializer":rows["initialize"]["running"] = True
    if drift == "unhealthy":monkeypatch.setattr(final.initial,"_source_healthy",lambda _:False)
    if drift == "worker":state["binding"]["runtime"] = 2
    if drift == "expiry":clock["boot"] = 161
    if drift == "reboot":clock["boot_id"] = "b"*36
    def inspect(operation, **kwargs):
        assert operation == "inspect_outcome" and kwargs["deadline"] <= deadline
        return dict(controller_id="d"*32 if drift=="controller" else CONTROLLER,
            operation=operation,state="background" if drift=="active" else "aborted",
            last_sequence=5 if drift=="sequence" else 1000,bound_final_deadline=deadline,
            final_switch_authorized=False,collection_resume_authorized=False,
            result=dict(outcome=drift if drift in {"pending","committed"} else "uncommitted",
                database_handoff_committed=None if drift=="pending" else drift=="committed",
                collection_resume_authorized=False,runtime_activation_authorized=False))
    if drift:
        with pytest.raises(RuntimeError):final.reconcile_source_resumed_locked(path,exchange=inspect)
        assert (path/final.STATE).read_bytes()==before
    else:
        result=final.reconcile_source_resumed_locked(path,exchange=inspect)
        assert result["original_source_resumed"] and result["final_marker_retained"]
        after=final._load(path/final.STATE)
        assert after["phase"]=="source_resumed"
        assert all(after[k]==v for k,v in saved.items() if k not in {"phase","resume"})
        with pytest.raises(RuntimeError):final.reconcile_source_resumed_locked(path,exchange=inspect)
    assert len(starts)==count


@pytest.fixture
def mount_writer_setup(monkeypatch):
    import copy
    import json
    identities = [str(i)*64 for i in range(1, 5)]
    rows = {"tsdb": {"id": identities[0]}, "market-data-collector": {"id": identities[1]}}
    def mount(source, rw=True):
        return {"Type": "bind", "Source": source, "Destination": "/data", "RW": rw}
    def peer(identity, mounts):
        return dict(id=identity, running=True, paused=False, restarting=False,
                    pid=100, started="original-start", mounts=mounts,
                    name="/peer-"+identity[:12], network_mode="none", pid_mode="")
    peers = {identities[0]: peer(identities[0], [mount("/ssd/database")]),
             identities[1]: peer(identities[1], [mount("/ssd/archives")]),
             identities[2]: peer(identities[2], [mount("/ssd/archives", False)]),
             identities[3]: peer(identities[3], [mount("/unrelated")])}
    controls = {"inspections": 0, "drift": None, "calls": []}
    def docker(action, *args, **kwargs):
        assert action in {"ps", "inspect"}  # Observation must never stop peers.
        controls["calls"].append(action)
        if action == "ps":
            return "\n".join(peers)
        controls["inspections"] += 1
        data = copy.deepcopy(peers)
        if controls["inspections"] == 2 and controls["drift"]:
            controls["drift"](data)
        return "\n".join(json.dumps(data[i]) for i in sorted(data))
    monkeypatch.setattr(host_boundary, "docker", docker)
    def admit():
        with host_boundary.docker_deadline(final.time.monotonic()+5):
            return final._admit_mount_writers(rows, operator_id=identities[2])
    return peers[identities[3]], controls, admit


@pytest.mark.parametrize("path", ["/ssd/archives", "/ssd/archives/spool", "/ssd", "/", "/ssd/database"])
def test_outside_inventory_writable_storage_peer_refuses(mount_writer_setup, path):
    peer, controls, admit = mount_writer_setup
    peer["mounts"][0]["Source"] = path
    with pytest.raises(RuntimeError, match="unadmitted_mount_writer"):
        admit()
    assert controls["calls"] == ["ps", "inspect"]


@pytest.mark.parametrize("disposition", ["unrelated", "readonly", "stopped", "prefix-sibling"])
def test_mount_writer_observation_preserves_nonwriters(mount_writer_setup, disposition):
    peer, controls, admit = mount_writer_setup
    if disposition == "readonly":
        peer["mounts"][0].update(Source="/ssd/archives", RW=False)
    elif disposition == "stopped":
        peer["mounts"][0]["Source"] = "/ssd/archives"
        peer.update(running=False, pid=0)
    elif disposition == "prefix-sibling":
        peer["mounts"][0]["Source"] = "/ssd/archives-other"
    assert admit() is None  # No receipt or switch authority is created.
    assert controls["calls"] == ["ps", "inspect", "ps", "inspect"]


@pytest.mark.parametrize("field,value", [("paused", True), ("restarting", True), ("pid", 41)])
def test_transitional_peer_with_source_write_mount_refuses(mount_writer_setup, field, value):
    peer, _, admit = mount_writer_setup
    peer.update(running=False, pid=0)
    peer[field] = value
    peer["mounts"][0]["Source"] = "/ssd/archives"
    with pytest.raises(RuntimeError, match="unadmitted_mount_writer"): admit()


def test_mount_writer_inventory_change_between_observations_refuses(mount_writer_setup):
    _, controls, admit = mount_writer_setup
    controls["drift"] = lambda rows: rows["4"*64].update(started="replacement-start")
    with pytest.raises(RuntimeError, match="writer_inventory_changed"): admit()


@pytest.mark.parametrize("source", ["relative", "/ssd/archives/../archives", "/ssd//archives", "/ssd/archives/"])
def test_noncanonical_mount_source_refuses(mount_writer_setup, source):
    peer, _, admit = mount_writer_setup
    peer["mounts"][0]["Source"] = source
    with pytest.raises(RuntimeError, match="writer_mount_invalid"): admit()


def test_mount_writer_observation_requires_original_caller_deadline():
    with pytest.raises(RuntimeError, match="writer_deadline_required"):
        final._admit_mount_writers({}, operator_id=WORKER)


def test_docker_mount_array_order_is_not_source_drift(mount_writer_setup):
    peer, controls, admit = mount_writer_setup
    peer["mounts"].append({"Type": "bind", "Source": "/another-unrelated",
                           "Destination": "/other", "RW": True})
    controls["drift"] = lambda rows: rows["4"*64]["mounts"].reverse()
    assert admit() is None


def test_admitted_source_lifecycle_remains_owned_by_supervised_transition(mount_writer_setup):
    _, controls, admit = mount_writer_setup
    controls["drift"] = lambda rows: rows["2"*64].update(running=False, pid=0)
    assert admit() is None


def test_admitted_source_mount_change_still_refuses(mount_writer_setup):
    _, controls, admit = mount_writer_setup
    controls["drift"] = lambda rows: rows["2"*64]["mounts"][0].update(Source="/different")
    with pytest.raises(RuntimeError, match="writer_inventory_changed"): admit()


def test_worker_runtime_change_still_refuses(mount_writer_setup):
    _, controls, admit = mount_writer_setup
    controls["drift"] = lambda rows: rows["3"*64].update(started="changed")
    with pytest.raises(RuntimeError, match="writer_inventory_changed"): admit()


@pytest.mark.parametrize("field", ["network_mode", "pid_mode"])
@pytest.mark.parametrize("alias", ["1"*64, "1"*12, "peer-"+"1"*12, "/peer-"+"1"*12])
def test_mountless_namespace_alias_peer_refuses(mount_writer_setup, field, alias):
    peer, controls, admit = mount_writer_setup
    peer["mounts"] = []
    peer[field] = "container:"+alias
    with pytest.raises(RuntimeError, match="unadmitted_namespace_peer"): admit()
    assert controls["calls"] == ["ps", "inspect"]


@pytest.mark.parametrize("alias", ["", "missing", "4"*64])
def test_live_missing_or_cyclic_namespace_alias_refuses(mount_writer_setup, alias):
    peer, _, admit = mount_writer_setup
    peer["network_mode"] = "container:"+alias
    with pytest.raises(RuntimeError, match="namespace_alias_invalid"): admit()


def test_stopped_namespace_peer_is_preserved(mount_writer_setup):
    peer, _, admit = mount_writer_setup
    peer.update(running=False, pid=0, network_mode="container:"+"1"*64)
    assert admit() is None


@pytest.mark.parametrize("field", ["network_mode", "pid_mode", "name"])
def test_source_namespace_binding_drift_refuses(mount_writer_setup, field):
    _, controls, admit = mount_writer_setup
    controls["drift"] = lambda rows: rows["2"*64].update({field: "changed"})
    with pytest.raises(RuntimeError, match="writer_inventory_changed|writer_inventory_invalid"): admit()


def test_transitive_namespace_alias_through_worker_refuses(mount_writer_setup):
    peer, controls, admit = mount_writer_setup
    # Populate the real first snapshot rather than injecting second-read drift.
    original = host_boundary.docker
    def docker(action, *args, **kwargs):
        import json
        value = original(action, *args, **kwargs)
        if action == "inspect":
            rows = [json.loads(line) for line in value.splitlines()]
            for row in rows:
                if row["id"] == "3"*64: row["network_mode"] = "container:"+"1"*64
            return "\n".join(json.dumps(row) for row in rows)
        return value
    from unittest.mock import patch
    peer["network_mode"] = "container:"+"3"*64
    with patch.object(host_boundary, "docker", docker):
        with pytest.raises(RuntimeError, match="unadmitted_namespace_peer"): admit()


def test_ambiguous_namespace_name_and_id_prefix_refuses(mount_writer_setup):
    peer, _, admit = mount_writer_setup
    # A container name can resemble another container's hexadecimal ID prefix.
    peer.update(name="/"+"1"*12, network_mode="container:"+"1"*12)
    with pytest.raises(RuntimeError, match="namespace_alias_invalid"): admit()


@pytest.mark.parametrize("fault", [None, "save_reply", "sql_reply", "worker_reply", "jobs_reply", "jobs_unconfirmed", "identity", "builtins", "expiry"])
def test_host_login_gate_journals_before_mutation_and_never_reopens(pause_setup, monkeypatch, fault):
    path, rows, clock, state, stop = pause_setup
    state["binding"]["capture"] = {"original": "capture"}
    saved = stop()
    deadline = final.time.monotonic()+30
    original_observe = final._observe
    def observe(root, **args):
        args.pop("session", None)
        binding, prep, observed, limits = original_observe(root, **args)
        return binding, {**prep, "cluster": "1234"}, {**observed, "tsdb": {"id": "d"*64}}, limits
    monkeypatch.setattr(final, "_observe", observe)
    final.record_switch_entry_locked(path, deadline=deadline, observe_worker=lambda **kw: dict(
        controller_id=CONTROLLER, operation="status", state="background", bound_final_deadline=deadline,
        last_sequence=1, migration_ready=False, final_switch_authorized=False, collection_resume_authorized=False))
    original = final._load(path/final.STATE)
    database = dict(cluster="1234", oid=123, name="owned", allow_connections=True)
    calls = []
    def control(container, sql):
        assert container == "d"*64
        if "ALTER DATABASE" in sql:
            receipt = final._load(path/final.STATE)
            assert receipt["phase"] == "login_closing"
            assert receipt["login_gate"]["database"]["allow_connections"] is True
            assert receipt["deadline"] == original["deadline"] and receipt["switch"] == original["switch"]
            calls.append(sql)
            database["allow_connections"] = False
            if fault == "sql_reply":raise TimeoutError("lost gate reply")
            if fault == "expiry":clock.update(wall=1061, boot=161)
        return final.json.dumps(database)
    monkeypatch.setattr(host_boundary, "maintenance_query", control)
    sequence = [1]
    def exchange(operation, **kw):
        assert kw["deadline"] == deadline
        sequence[0] += 1
        if operation == "final_session_check" and fault == "worker_reply":raise EOFError("worker lost")
        if operation == "final_session_quiesce":
            assert final._load(path/final.STATE)["phase"] == "login_closing"
            assert not database["allow_connections"]
            if fault == "jobs_reply":raise EOFError("job stop reply lost")
        observed = dict(database)
        if fault == "identity":observed["oid"] += 1
        return dict(controller_id=CONTROLLER, operation=operation, state="background", bound_final_deadline=deadline,
            last_sequence=sequence[0], final_switch_authorized=False, collection_resume_authorized=False,
            result=dict(database=observed,capture=state["binding"]["capture"],backend_pid=2,owner_pid=1,
                database_jobs_stopped=fault != "jobs_unconfirmed",job_definitions_preserved=True,
                builtin_jobs_admitted=fault != "builtins",
                database_switch_authorized=False,collection_resume_authorized=False,runtime_activation_authorized=False))
    save = host_boundary.save_receipt
    def save_reply(*args, **kwargs):
        save(*args, **kwargs)
        if args[1]["phase"] == "login_closing":raise OSError("lost saved intent reply")
    if fault == "save_reply":monkeypatch.setattr(host_boundary,"save_receipt",save_reply)
    if fault:
        with pytest.raises((RuntimeError, TimeoutError, EOFError, OSError)):
            final.close_database_logins_locked(path, exchange=exchange)
    else:
        result = final.close_database_logins_locked(path, exchange=exchange)
        assert result["new_logins_closed"] and not result["database_switch_authorized"]
    receipt = final._load(path/final.STATE)
    assert receipt["deadline"] == original["deadline"] and receipt["switch"] == original["switch"]
    assert len(calls) == (0 if fault in {"save_reply","identity","builtins"} else 1)
    if fault not in {"identity", "builtins"}:
        assert receipt["phase"] == ("login_closed" if fault is None else "login_closing")
        before = (path/final.STATE).read_bytes()
        with pytest.raises(RuntimeError,match="login_switch_intent_required"):
            final.close_database_logins_locked(path, exchange=exchange)
        with pytest.raises(RuntimeError,match="resume_fence_reply_invalid" if fault is None else "resume_switch_intent_required"):
            final.resume_online_source_locked(path, exchange=exchange)
        assert (path/final.STATE).read_bytes() == before
    assert all("ALLOW_CONNECTIONS true" not in call for call in calls)


@pytest.mark.parametrize("fault", [None, "gate", "source", "sequence", "session", "deadline"])
def test_gated_residual_host_retains_intent_and_refuses_drift(pause_setup, monkeypatch, fault):
    path, rows, clock, state, stop = pause_setup
    state["binding"]["capture"] = {"original": "capture"}
    stop()
    deadline = final.time.monotonic()+30
    final.record_switch_entry_locked(path, deadline=deadline, observe_worker=lambda **kw: dict(
        controller_id=CONTROLLER, operation="status", state="background", bound_final_deadline=deadline,
        last_sequence=1, migration_ready=False, final_switch_authorized=False, collection_resume_authorized=False))
    saved = final._load(path/final.STATE)
    database = dict(cluster="1234", oid=123, name="owned", allow_connections=True)
    saved.update(phase="login_closed",login_gate=dict(database=database,requested_at=1000.,
        closed_at=1001.,database_jobs_stopped=True))
    host_boundary.save_receipt(path/final.STATE,saved,initial=False)
    before = (path/final.STATE).read_bytes()
    observe = final._observe
    def current(root, **args):
        assert args.pop("session")["database"]["allow_connections"] is False
        if fault == "source":state["binding"]["runtime"] = 2
        return observe(root, **args)
    monkeypatch.setattr(final,"_observe",current)
    calls = []
    def exchange(operation, **kw):
        calls.append(operation)
        assert kw["deadline"] == deadline
        reply = dict(controller_id=CONTROLLER, operation=operation, state="background",
            bound_final_deadline=deadline,last_sequence=len(calls)+1,
            final_switch_authorized=False,collection_resume_authorized=False)
        if operation == "final_session_check":
            reply["result"] = dict(database={**database,"allow_connections":fault == "gate"},
                capture=state["binding"]["capture"],backend_pid=3 if fault == "session" and len(calls)>1 else 2,
                owner_pid=1,builtin_jobs_admitted=True,database_switch_authorized=False,collection_resume_authorized=False,
                runtime_activation_authorized=False)
        else:
            assert operation == "final_delta"
            if fault == "sequence":reply["last_sequence"] = 1
            if fault == "deadline":clock.update(wall=1061,boot=161)
            reply["result"] = dict(sql={"outcome":"both_tails_observed_empty"},archives=[
                dict(family=f,captured_tail_empty_at_observation=True) for f in
                ("fact_archive_manifests","raw_archive_manifests","book_checkpoint_manifests")],
                migration_ready=False,final_switch_authorized=False,collection_resume_authorized=False)
        return reply
    if fault:
        with pytest.raises(RuntimeError):
            final.copy_final_delta_locked(path,exchange=exchange,deadline=deadline,max_rounds=2)
    else:
        result = final.copy_final_delta_locked(path,exchange=exchange,deadline=deadline,max_rounds=2)
        assert result["rounds"] == 1 and not result["publisher_drain_authorized"]
        assert calls == ["final_session_check","final_delta","final_session_check"]
    assert (path/final.STATE).read_bytes() == before
    assert all(not row["running"] for row in rows.values())


@pytest.mark.parametrize("fault", [None, "logins_reply", "jobs_reply", "fence_loss", "expiry", "gate_reclose"])
def test_gated_abort_journals_restoration_under_same_live_fence(resume_setup, monkeypatch, fault):
    path, rows, clock, state, stop, old_exchange, calls, starts, deadline = resume_setup
    saved = final._load(path/final.STATE)
    capture = {"original":"capture"}
    state["binding"]["capture"] = capture
    saved["binding"]["capture"] = capture
    database = dict(cluster="1234",oid=123,name="owned",allow_connections=True)
    saved.update(phase="login_closed",login_gate=dict(database=database,requested_at=1000.,
        closed_at=1001.,database_jobs_stopped=True))
    host_boundary.save_receipt(path/final.STATE,saved,initial=False)
    opened = [False]
    observed = final._observe
    def observe(root, **args):
        session = args.pop("session")
        assert session["capture"] == capture
        binding,prep,clients,limits = observed(root,**args)
        return binding,prep,{**clients,"tsdb":{"id":"d"*64}},limits
    monkeypatch.setattr(final,"_observe",observe)
    lost = [False]
    def exchange(operation, **kwargs):
        if lost[0]:raise EOFError("owned fence lost")
        reply = old_exchange(operation,**kwargs)
        if operation == "final_session_check":reply["state"] = "background"
        reply["result"].update(database={**database,"allow_connections":opened[0]},
            capture=capture,backend_pid=2,owner_pid=1,builtin_jobs_admitted=True,database_switch_authorized=False)
        return reply
    actions = []
    def action(arguments, *, deadline, check, input):
        check()
        record = final._load(path/final.STATE)
        pending = record["resume"]["gate_restore"]["inflight"]
        assert pending == ("logins" if not actions else "jobs")
        assert record["switch"] == saved["switch"] and record["login_gate"] == saved["login_gate"]
        assert not starts
        actions.append(pending)
        if pending == "logins":opened[0] = True
        if fault == pending+"_reply":raise TimeoutError("lost action reply")
        if fault == "gate_reclose" and pending == "jobs":opened[0] = False
        if fault == "fence_loss":lost[0] = True
        if fault == "expiry":clock.update(wall=1061,boot=161)
        check()
    monkeypatch.setattr(host_boundary,"supervised_source_action",action)
    if fault:
        with pytest.raises((RuntimeError,EOFError,TimeoutError)):
            final.resume_online_source_locked(path,exchange=exchange)
        result=final._load(path/final.STATE)
        assert result["phase"] == "source_resuming" and not starts
        assert result["resume"]["gate_restore"]["inflight"] == actions[-1]
        with pytest.raises(RuntimeError,match="resume_switch_intent_required"):
            final.resume_online_source_locked(path,exchange=exchange)
    else:
        result=final.resume_online_source_locked(path,exchange=exchange)
        assert result["original_source_resumed"] and result["original_login_gate_restored"]
        assert result["database_jobs_restart_requested"]
        record=final._load(path/final.STATE)
        assert record["phase"] == "source_resumed" and record["login_gate"] == saved["login_gate"]
        assert record["resume"]["gate_restore"] == {"completed":["logins","jobs"],"inflight":None}
        assert actions == ["logins","jobs"] and len(starts)==len(host_boundary.STOP)-1


@pytest.fixture
def guarded_source(pause_setup, monkeypatch):
    import stat
    path, rows, clock, state, stop = pause_setup
    root = path / "source"
    root.mkdir(mode=0o750)
    (root / "objects").mkdir(mode=0o700)
    roots = {str(p): [p.stat().st_dev, p.stat().st_ino, p.stat().st_uid,
                      p.stat().st_gid, stat.S_IMODE(p.stat().st_mode)]
             for p in [root, root / "objects"]}
    observe = final._observe
    def with_roots(*args, **kwargs):
        binding, preparation, current, limits = observe(*args, **kwargs)
        return binding, dict(preparation, source_roots=roots), current, limits
    monkeypatch.setattr(final, "_observe", with_roots)
    image = "sha256:" + "d" * 64
    target = "/app/logs/market-structure"
    details = {rows[service]["id"]: dict(image=image,
        config=dict(Entrypoint=None, Cmd=["python", "-m", module], Env=[
            "QT_IMAGE_SOURCE_REVISION="+REVISION, "QT_STORAGE_SOURCE_FENCE_ROOT="+target,
            "MARKET_STRUCTURE_STORAGE_ROOT="+target]),
        mounts=[dict(Type="bind", Source=str(root), Destination=target, RW=True)])
        for service, module in final._SOURCE_WRITERS.items()}
    monkeypatch.setattr(host_boundary, "database_details", lambda identity: details[identity])
    stop()
    return path, rows, clock, root, image, details


def test_final_source_hold_is_kernel_owned_and_cannot_be_reused(guarded_source):
    import fcntl
    import os
    path, rows, clock, root, image, details = guarded_source
    before = (path/final.STATE).read_bytes()
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="namespace_busy"):
            with final.held_source_writers_locked(path, source_image=image):
                pytest.fail("active source admitted")
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        with final.held_source_writers_locked(path, source_image=image) as check:
            check()
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
            assert (path/final.STATE).read_bytes() == before
        with pytest.raises(RuntimeError, match="source_hold_closed"):
            check()
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        assert (path/final.STATE).read_bytes() == before
    finally:
        os.close(descriptor)


@pytest.mark.parametrize("drift", ["image", "command", "guard", "code_mount", "running"])
def test_final_source_hold_refuses_unqualified_layout_before_lock(guarded_source, drift):
    path, rows, clock, root, image, details = guarded_source
    backend = details[rows['backend']['id']]
    if drift == "image": backend['image'] = 'sha256:'+'e'*64
    elif drift == "command": backend['config']['Cmd'] = ['python','-c','pass']
    elif drift == "guard": backend['config']['Env'] = [v for v in backend['config']['Env'] if not v.startswith('QT_STORAGE_SOURCE_FENCE_ROOT=')]
    elif drift == "code_mount": backend['mounts'].append(dict(Destination='/app/src/override.py'))
    else: rows['backend']['running'] = True
    with pytest.raises(RuntimeError, match="guarded_source_contract_required|source_hold_changed"):
        with final.held_source_writers_locked(path, source_image=image):
            pytest.fail("unqualified source admitted")


def test_final_source_hold_retains_original_window_and_metadata(guarded_source):
    path, rows, clock, root, image, details = guarded_source
    before = (path/final.STATE).read_bytes()
    with pytest.raises(RuntimeError, match="deadline_expired"):
        with final.held_source_writers_locked(path, source_image=image) as check:
            clock['boot'] = 161
            check()
    assert (path/final.STATE).read_bytes() == before
    clock['boot'] = 100
    with pytest.raises(RuntimeError, match="metadata_changed"):
        with final.held_source_writers_locked(path, source_image=image) as check:
            (root/'objects').chmod(0o750)
            check()
    assert (path/final.STATE).read_bytes() == before


@pytest.mark.parametrize("fault", [None, "lost_reply", "bad_reply", "unknown_reply", "uncommitted", "gate", "missing_policy"])
def test_host_commit_intent_is_once_only_and_never_grants_runtime(guarded_source, monkeypatch, fault):
    import json
    path, rows, clock, root, image, _ = guarded_source
    capture = {"original": "capture"}
    saved = final._load(path/final.STATE)
    saved["binding"]["capture"] = capture
    host_boundary.save_receipt(path/final.STATE, saved, initial=False)
    original_observe = final._observe
    def observe(*args, **kwargs):
        kwargs.pop("session", None)
        binding, prep, clients, limits = original_observe(*args, **kwargs)
        return dict(binding, capture=capture), prep, {**clients, "tsdb": {"id": "e"*64}}, limits
    monkeypatch.setattr(final, "_observe", observe)
    database = dict(cluster="1234", oid=123, name="owned", allow_connections=True)
    monkeypatch.setattr(host_boundary, "maintenance_query", lambda *a: json.dumps(
        {**database, "allow_connections": fault == "gate"}))
    calls = []
    with final.held_source_writers_locked(path, source_image=image):
        deadline = final.time.monotonic()+30
        final.record_switch_entry_locked(path, deadline=deadline, observe_worker=lambda **kw: dict(
            controller_id=CONTROLLER, operation="status", state="background", bound_final_deadline=deadline,
            last_sequence=1, migration_ready=False, final_switch_authorized=False, collection_resume_authorized=False))
        saved = final._load(path/final.STATE)
        saved.update(phase="login_closed", login_gate=dict(database=database, requested_at=1000.,
            closed_at=1001., database_jobs_stopped=True))
        host_boundary.save_receipt(path/final.STATE, saved, initial=False)
        clock.update(wall=1002., boot=102.)
        def exchange(operation, **kwargs):
            calls.append(operation)
            assert kwargs["deadline"] <= deadline
            reply = dict(controller_id=CONTROLLER, operation=operation, bound_final_deadline=deadline,
                last_sequence=len(calls)+1, state="background", final_switch_authorized=False,
                collection_resume_authorized=False)
            result = dict(collection_resume_authorized=False, runtime_activation_authorized=False)
            if operation == "final_session_check":
                result.update(database={**database, "allow_connections": False}, capture=capture,
                    backend_pid=2, owner_pid=1, builtin_jobs_admitted=True, database_switch_authorized=False)
            else:
                receipt = final._load(path/final.STATE)
                assert receipt["phase"] == "commit_dispatching"
                assert receipt["commit"]["worker_sequence"] == 3
                assert receipt["deadline"] == saved["deadline"] and receipt["switch"] == saved["switch"]
                reply["state"] = "commit_unknown" if fault in {"unknown_reply", "uncommitted"} else "committed"
                if operation == "commit_database":
                    if fault == "lost_reply": raise EOFError("unread commit frame")
                    if fault == "bad_reply": reply["controller_id"] = "f"*32
                    result["database_handoff_committed"] = None if reply["state"] == "commit_unknown" else True
                    result["initial_policy_activated"] = result["database_handoff_committed"]
                else:
                    assert operation == "inspect_outcome"
                    result.update(outcome="uncommitted" if fault == "uncommitted" else "committed",
                        database_handoff_committed=fault != "uncommitted",
                        initial_policy_activated=fault not in {"uncommitted", "missing_policy"},
                        confirmed_plan_id="handoff-"+"e"*32)
            reply["result"] = result
            return reply
        if fault in {"lost_reply", "bad_reply", "gate", "missing_policy"}:
            with pytest.raises((EOFError, RuntimeError)):
                final.commit_online_handoff_locked(path, exchange=exchange)
        else:
            result = final.commit_online_handoff_locked(path, exchange=exchange)
            assert not result["collection_resume_authorized"] and not result["runtime_activation_authorized"]
        receipt = final._load(path/final.STATE)
        assert receipt["phase"] == ("login_closed" if fault == "gate" else
            "commit_dispatching" if fault in {"lost_reply", "bad_reply", "uncommitted", "missing_policy"} else "committed")
        if fault != "gate":
            before = (path/final.STATE).read_bytes()
            with pytest.raises(RuntimeError, match="commit_closed_gate_required"):
                final.commit_online_handoff_locked(path, exchange=exchange)
            with pytest.raises(RuntimeError, match="resume_switch_intent_required"):
                final.resume_online_source_locked(path, exchange=exchange)
            assert (path/final.STATE).read_bytes() == before
        assert calls.count("commit_database") == (0 if fault == "gate" else 1)
        assert calls.count("inspect_outcome") == (0 if fault in {"lost_reply", "bad_reply", "gate"} else 1)
