"""Canonical forward request ownership and shared UTC final-window behavior."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.automation import storage_online_operation as operation
from scripts.automation import storage_online_forward as forward
from scripts.automation import storage_online_deadline as amendment
from scripts.automation import storage_online_terminal as terminal
from tests.test_storage_online_deadline import attempt as base_attempt, package_attempt, write
from tests.test_storage_online_forward import prepared


@pytest.fixture
def attempt(base_attempt):
    a = base_attempt
    a.plan["limits"] = vars(operation.OperationLimits(30, 120, 30, 60, 1024, 64, 1, 1024, 1, 1))
    a.plan.update(descriptor_limit=1024, memory_bytes=1024**3, source_image="sha256:"+"b"*64)
    a.plan["request"]["resource_limits"] = {"movement_timeout_seconds":180}
    request_path = a.root/amendment.REQUEST
    original_request = operation.host.load_receipt(request_path)
    original_request["resource_limits"] = a.plan["request"]["resource_limits"]
    request_path.write_bytes(amendment.request_bytes(original_request))
    a.worker["binding"]["request_sha256"] = amendment._sha(request_path.read_bytes())
    write(a.root/operation.launch._STATE, a.worker)
    write(a.path, a.plan)
    return a


@pytest.fixture
def published(prepared, monkeypatch):
    a = prepared
    a.run()
    a.publication = forward.inspect_published_operation(a.root)
    a.forward_path = Path(a.manifest["forward_plan_path"])
    a.observations = []
    a.retired = []
    monkeypatch.setattr(amendment, "_retired", lambda saved: a.retired.append(deepcopy(saved)))
    def inspect(*args, **kw):
        a.observations.append(kw)
        return {"verified": True}
    monkeypatch.setattr(operation, "inspect_prepared_operation", inspect)
    monkeypatch.setattr(operation.initial, "prepare_online_source_locked", lambda *a, **k: pytest.fail("old preparation replayed"))
    monkeypatch.setattr(operation.initial, "_load", lambda *a: pytest.fail("old initial clock read"))
    a.dispatches = []
    def run(*args, **kw):
        a.dispatches.append(kw)
        return {"ordinary_relaunch_authorized": False}
    monkeypatch.setattr(operation, "run_prepared_operation_locked", run)
    return a


@pytest.mark.parametrize("execute", [False, True])
def test_verified_separate_plan_uses_effective_request_without_rewriting_base(published, execute):
    a = published
    before = {p:p.read_bytes() for p in (a.path, a.forward_path, a.root/terminal.STATE)}
    result = operation.run_operation_plan(a.forward_path, execute=execute)
    assert result["phase"] == ("runtime_ready" if execute else "forward_inspected")
    assert a.retired == [a.publication["old_worker"]]
    assert a.observations[0]["request"] == a.publication["new_request"]
    assert len(a.dispatches) == int(execute)
    if execute:
        assert a.dispatches[0]["request"] == a.publication["new_request"]
        assert "capture_preparation" not in a.dispatches[0]["request"]
    assert all(p.read_bytes() == data for p,data in before.items())
    with pytest.raises(RuntimeError, match="terminal_intent"):
        amendment.require_settled(a.root)


@pytest.mark.parametrize("fault", ["original_path", "request", "publication", "retirement", "plan_during_preflight"])
def test_foreign_forward_dispatch_refuses_before_worker(published, monkeypatch, fault):
    a = published
    path = a.forward_path
    if fault == "original_path": path = a.path
    elif fault == "request": write(a.root/amendment.REQUEST, {"foreign": True})
    elif fault == "publication": write(a.root/forward.STATE, {"phase": "complete"})
    elif fault == "retirement": write(a.root/terminal.FORWARD_STATE, {"phase": "dispatched"})
    else:
        def changed(*args, **kw):
            write(a.forward_path, {"foreign": True})
            return {"verified": True}
        monkeypatch.setattr(operation, "inspect_prepared_operation", changed)
    with pytest.raises((RuntimeError, KeyError, ValueError)):
        operation.run_operation_plan(path, execute=True)
    assert not a.dispatches


@pytest.fixture
def utc_clock(monkeypatch):
    boundary = datetime(2026, 10, 4, tzinfo=timezone.utc).timestamp()
    clock = [boundary-100, 10.]
    monkeypatch.setattr(operation.time, "time", lambda: clock[0])
    monkeypatch.setattr(operation.time, "monotonic", lambda: clock[1])
    sleeps = []
    def sleep(seconds):
        sleeps.append(seconds)
        clock[:] = [x+seconds for x in clock]
    monkeypatch.setattr(operation.time, "sleep", sleep)
    return SimpleNamespace(boundary=boundary, clock=clock, sleeps=sleeps, intent={"end_day":"2026-10-04"})


def test_boundary_wait_keeps_source_serving_and_rollover_uses_original_final_clock(utc_clock, monkeypatch):
    a = utc_clock
    calls = []
    def exchange(command):
        calls.append(command)
        return {"result": (dict(phase="catch_up", outcome="both_tails_observed_empty") if command=="sql_copy"
            else dict(family="fact_archive_manifests", captured_tail_empty_at_observation=True))}
    operation._await_forward_boundary(exchange, intent=a.intent, worker=SimpleNamespace(poll=lambda:None),
        deadline=1010., capture_deadline=a.boundary+900, final_seconds=600)
    assert a.clock[0] == a.boundary-30 and set(calls) == {"sql_copy", "archive_copy"}
    assert max(a.sleeps) <= 30
    paused = dict(duration_seconds=600, deadline=a.clock[0]+600, original="retain")
    before = deepcopy(paused)
    monkeypatch.setattr(operation.final, "_remaining", lambda saved: saved["deadline"]-a.clock[0])
    operation._enter_forward_day(a.intent, paused)
    assert a.clock[0] == a.boundary and paused == before
    assert operation.final._remaining(paused) == 570


@pytest.mark.parametrize("fault", ["insufficient_capture", "worker_exit", "expired", "missed_day", "long_pause"])
def test_boundary_refusal_never_dispatches_final_or_renews_clocks(utc_clock, fault):
    a = utc_clock
    deadline = 1010.; capture = a.boundary+900; seconds = 600; status = None
    if fault == "insufficient_capture": capture = a.boundary+599
    elif fault == "worker_exit": status = 1
    elif fault == "expired": deadline = 10.
    elif fault == "missed_day": a.clock[0] = a.boundary+86400
    else: seconds = 601
    with pytest.raises((RuntimeError, ValueError)):
        operation._await_forward_boundary(lambda *a:pytest.fail("unexpected SQL dispatch"), intent=a.intent,
            worker=SimpleNamespace(poll=lambda:status), deadline=deadline, capture_deadline=capture, final_seconds=seconds)
    assert not a.sleeps


def test_stop_refuses_early_boundary_and_final_wait_cannot_renew_expired_pause(utc_clock, monkeypatch):
    a = utc_clock
    with pytest.raises(RuntimeError, match="stop_too_early"):
        operation._admit_forward_stop(a.intent, final_seconds=600)
    a.clock[0] = a.boundary-10
    monkeypatch.setattr(operation.final, "_remaining", lambda saved: (_ for _ in ()).throw(RuntimeError("original final expired")))
    with pytest.raises(RuntimeError, match="original final expired"):
        operation._enter_forward_day(a.intent, dict(duration_seconds=600))
    assert not a.sleeps


def test_prepared_forward_driver_rechecks_publication_before_source_stop(prepared, monkeypatch):
    from contextlib import contextmanager
    a = prepared
    a.run()
    publication = forward.inspect_published_operation(a.root)
    plan = publication["new_plan"]
    request = publication["new_request"]
    events = []
    actual_inspect = forward.inspect_published_operation
    def inspect(*args, **kw):
        events.append("publication")
        return actual_inspect(*args, **kw)
    monkeypatch.setattr(forward, "inspect_published_operation", inspect)
    monkeypatch.setattr(operation, "inspect_prepared_operation", lambda *a, **kw: events.append("preflight") or {})
    @contextmanager
    def launch(*args, **kw):
        assert kw["request"] == request
        events.append("launch")
        try:
            yield SimpleNamespace(poll=lambda:None), dict(container_id="worker", deadline=operation.time.time()+900)
        finally:
            events.append("retired")
    monkeypatch.setattr(operation.launch, "launched_online_worker_locked", launch)
    monkeypatch.setattr(operation.host, "OnlineWorkerChannel", lambda *a, **kw:SimpleNamespace(
        greeting={"controller_id":"controller"}, exchange=lambda *a, **kw:None))
    def background(*args, **kw):
        assert kw == dict(preparation_seconds=plan["limits"]["preparation_seconds"], forward=True)
        events.append("background")
    monkeypatch.setattr(operation, "prepare_background", background)
    monkeypatch.setattr(operation, "_await_forward_boundary", lambda *a, **kw:events.append("serving_wait"))
    monkeypatch.setattr(operation, "_admit_forward_stop", lambda *a, **kw:events.append("stop_admission"))
    def refuse(*args, **kw):
        events.append("stop")
        raise RuntimeError("injected before source mutation")
    monkeypatch.setattr(operation.final, "stop_online_source_locked", refuse)
    arguments={k:plan[k] for k in ("project", "source_revision", "source_image", "image", "inventory_path",
        "descriptor_limit", "memory_bytes", "keys_root", "socket_volume", "spool_destination")}
    with pytest.raises(RuntimeError, match="injected before source mutation"):
        operation.run_prepared_operation_locked(a.root, **arguments, request=request,
            limits=operation.OperationLimits(**plan["limits"]))
    assert events == ["publication", "preflight", "launch", "background", "serving_wait", "preflight",
        "publication", "stop_admission", "stop", "retired"]
