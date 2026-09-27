"""Durable final stop clocks, interruption and independent deployment exclusion."""
from types import SimpleNamespace

import pytest

from scripts.automation import storage_online_final as final

PROJECT, REVISION, WORKER, CONTROLLER = "qt-owned", "a"*40, "b"*64, "c"*32


@pytest.fixture
def pause_setup(tmp_path, monkeypatch):
    rows = {name: dict(id=str(i+1)*64, running=name != "initialize",
                      status="exited" if name == "initialize" else "running", exit_code=0)
            for i, name in enumerate(final.held.STOP)}
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
    monkeypatch.setattr(final.held, "_docker", docker)
    def stop(**changed):
        return final.stop_online_source_locked(tmp_path, **{
            "project": PROJECT, "source_revision": REVISION, "worker_id": WORKER,
            "controller_id": CONTROLLER, "max_duration_seconds": 60, **changed})
    return tmp_path, rows, clock, state, stop


def test_final_stop_persists_intent_and_never_restarts_initializer(pause_setup):
    path, rows, clock, state, stop = pause_setup
    receipt = stop()
    assert receipt["phase"] == "paused"
    assert len(state["stops"]) == len(final.held.STOP)-1
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
    assert len(state["stops"]) == len(set(state["stops"])) == len(final.held.STOP)-1


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
    held = final.held
    clock = [10.]
    timeouts = []
    monkeypatch.setattr(held.time, "monotonic", lambda: clock[0])
    def run(args, **kwargs):
        timeouts.append(kwargs["timeout"])
        clock[0] += 2
        return SimpleNamespace(returncode=0, stdout="ok")
    monkeypatch.setattr(held.subprocess, "run", run)
    with held._docker_deadline(15):
        assert held._docker("inspect", "owned", timeout=30) == "ok"
        with held._docker_deadline(99):
            assert held._docker("inspect", "owned", timeout=30) == "ok"
        with pytest.raises(RuntimeError, match="host_deadline_expired"):
            held._docker("stop", "--timeout", "-1", "owned", timeout=30)
    assert timeouts == [5,3,1]
    assert held._DOCKER_DEADLINE.get() is None


def test_source_drain_rechecks_held_source_and_original_deadline(pause_setup):
    path, rows, clock, state, stop = pause_setup
    saved = stop()
    final.held._save(path/"storage-online-request.json", {"command_seconds": 30}, initial=True)
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
    original = final.held._save
    def save_then_lose(*args, **kwargs):
        original(*args, **kwargs)
        raise TimeoutError("lost durable write response")
    monkeypatch.setattr(final.held, "_save", save_then_lose)
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
    final.held._save(path/"storage-online-request.json", {"command_seconds": 1}, initial=True)
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
