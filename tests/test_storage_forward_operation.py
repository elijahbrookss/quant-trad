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


@pytest.fixture(autouse=True)
def fixture_day(monkeypatch):
    # The shared publication fixture binds an immutable 2026-10-04 boundary.
    # Freeze before dependent fixtures create receipts; real UTC must not age
    # those fixtures past their cutover day. Boundary tests override this clock.
    now = datetime(2026, 10, 3, 23, tzinfo=timezone.utc).timestamp()
    monkeypatch.setattr(operation.time, "time", lambda: now)


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
@pytest.mark.parametrize("prepare_only", [False, True])
def test_verified_separate_plan_uses_effective_request_without_rewriting_base(published, execute, prepare_only):
    a = published
    before = {p:p.read_bytes() for p in (a.path, a.forward_path, a.root/terminal.STATE)}
    result = operation.run_operation_plan(a.forward_path, execute=execute, prepare_forward_only=prepare_only)
    assert result["phase"] == (("forward_background_prepared" if prepare_only else "runtime_ready")
                               if execute else "forward_inspected")
    assert a.retired == [a.publication["old_worker"]]
    assert a.observations[0]["request"] == a.publication["new_request"]
    assert len(a.dispatches) == int(execute)
    if execute:
        assert a.dispatches[0]["request"] == a.publication["new_request"]
        assert "capture_preparation" not in a.dispatches[0]["request"]
        assert a.dispatches[0].get("prepare_forward_only", False) is prepare_only
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


@pytest.mark.parametrize("delay", [0, 1, 569])
def test_late_stop_refuses_but_an_existing_pause_can_finish_drain(utc_clock, monkeypatch, delay):
    a = utc_clock
    a.clock[0] = a.boundary+delay
    with pytest.raises(RuntimeError, match="stop_window_missed"):
        operation._admit_forward_stop(a.intent, final_seconds=600)
    checks = []
    monkeypatch.setattr(operation.final, "_remaining", lambda saved: checks.append(saved) or 1.)
    paused = dict(duration_seconds=600, deadline=a.boundary+570)
    operation._enter_forward_day(a.intent, paused)
    assert checks == [paused] and not a.sleeps


@pytest.fixture
def prepared_driver(prepared, monkeypatch):
    from contextlib import contextmanager
    a = prepared
    actual_stop_admission = operation._admit_forward_stop
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
        return dict(reference_count=3, tail_rounds=1, final_switch_authorized=False)
    monkeypatch.setattr(operation, "prepare_background", background)
    monkeypatch.setattr(operation, "_await_forward_boundary", lambda *a, **kw:events.append("serving_wait"))
    monkeypatch.setattr(operation, "_admit_forward_stop", lambda *a, **kw:events.append("stop_admission"))
    def refuse(*args, **kw):
        events.append("stop")
        raise RuntimeError("injected before source mutation")
    monkeypatch.setattr(operation.final, "stop_online_source_locked", refuse)
    arguments={k:plan[k] for k in ("project", "source_revision", "source_image", "image", "inventory_path",
        "descriptor_limit", "memory_bytes", "keys_root", "socket_volume", "spool_destination")}
    return SimpleNamespace(attempt=a, plan=plan, request=request, arguments=arguments, events=events,
                           actual_stop_admission=actual_stop_admission)


def test_prepared_forward_driver_rechecks_publication_before_source_stop(prepared_driver):
    d = prepared_driver
    with pytest.raises(RuntimeError, match="injected before source mutation"):
        operation.run_prepared_operation_locked(d.attempt.root, **d.arguments, request=d.request,
            limits=operation.OperationLimits(**d.plan["limits"]))
    assert d.events == ["publication", "preflight", "launch", "background", "serving_wait", "preflight",
        "publication", "stop_admission", "stop", "retired"]


def test_prepared_forward_driver_missed_midnight_retires_without_source_stop(prepared_driver, monkeypatch):
    d = prepared_driver
    boundary = datetime.fromisoformat(d.request["forward"]["end_day"]).replace(tzinfo=timezone.utc).timestamp()
    monkeypatch.setattr(operation.time, "time", lambda: boundary+1)
    monkeypatch.setattr(operation, "_admit_forward_stop", d.actual_stop_admission)
    with pytest.raises(RuntimeError, match="stop_window_missed"):
        operation.run_prepared_operation_locked(d.attempt.root, **d.arguments, request=d.request,
            limits=operation.OperationLimits(**d.plan["limits"]))
    assert d.events == ["publication", "preflight", "launch", "background", "serving_wait", "preflight",
                       "publication", "retired"]


@pytest.mark.parametrize("fault", [None, "background", "preflight", "close", "retirement", "expired"])
def test_preparation_only_retires_without_entering_final_and_preserves_failure(prepared_driver, monkeypatch, fault):
    from contextlib import contextmanager
    d = prepared_driver
    deadline = operation.time.time()+900
    before = {p:p.read_bytes() for p in (d.attempt.path, Path(d.attempt.manifest["forward_plan_path"]),
        d.attempt.root/terminal.STATE, d.attempt.root/amendment.REQUEST, d.attempt.root/forward.STATE)}

    @contextmanager
    def launch(*args, **kw):
        d.events.append("launch")
        try:
            yield SimpleNamespace(poll=lambda:None), dict(container_id="worker", deadline=deadline)
        finally:
            d.events.append("retired")
            if fault == "retirement":
                raise RuntimeError("retirement uncertain")

    def exchange(command):
        assert command == "close"
        d.events.append("close")
        if fault == "close":
            raise RuntimeError("close uncertain")

    monkeypatch.setattr(operation.launch, "launched_online_worker_locked", launch)
    monkeypatch.setattr(operation.host, "OnlineWorkerChannel", lambda *a, **kw:SimpleNamespace(exchange=exchange))
    if fault == "background":
        def fail(*args, **kw):
            raise RuntimeError("background uncertain")
        monkeypatch.setattr(operation, "prepare_background", fail)
    elif fault == "preflight":
        def changed(*args, **kw):
            d.events.append("preflight")
            return {"changed": "operator_id" in kw}
        monkeypatch.setattr(operation, "inspect_prepared_operation", changed)
    elif fault == "expired":
        deadline = operation.time.time()-1

    def run():
        return operation.run_prepared_operation_locked(d.attempt.root, **d.arguments, request=d.request,
            limits=operation.OperationLimits(**d.plan["limits"]), prepare_forward_only=True)

    if fault:
        with pytest.raises(RuntimeError):
            run()
    else:
        result = run()
        assert result["adoption_deadline"] == deadline
        assert result["end_day"] == d.request["forward"]["end_day"]
        assert result["adoption_active"] is True
        assert all(result[k] is False for k in ("source_stopped", "final_switch_authorized",
                                               "runtime_activated", "ordinary_relaunch_authorized"))
        assert d.events == ["publication", "preflight", "launch", "background", "preflight",
                            "publication", "close", "retired"]
    assert d.events[-1] == "retired"
    assert not set(d.events).intersection({"serving_wait", "stop_admission", "stop"})
    assert all(p.read_bytes() == value for p,value in before.items())


@pytest.mark.parametrize("argument", ["extend_attempt_seconds", "capacity_file", "replacement_package_file",
    "cancel_attempt_file", "forward_package_file", "prepare_forward_keys_file", "reschedule_forward_file"])
def test_preparation_only_refuses_combined_phase_before_reading_plan(tmp_path, argument):
    with pytest.raises(ValueError, match="preparation_must_be_separate"):
        operation.run_operation_plan(tmp_path/"missing.json", execute=True, prepare_forward_only=True,
            **{argument: 30 if argument == "extend_attempt_seconds" else "other.json"})


def test_preparation_only_refuses_final_state_before_any_recovery_or_release(published):
    write(published.root/operation.final.STATE, {"foreign": True})
    with pytest.raises(ValueError, match="preparation_requires_pre_final_publication"):
        operation.run_operation_plan(published.forward_path, execute=True, prepare_forward_only=True)
    assert not published.dispatches


@pytest.mark.parametrize("flag", [True, 1, "true"])
def test_preparation_only_rejects_legacy_driver_before_launch(prepared_driver, flag):
    d = prepared_driver
    request = {k:v for k,v in d.request.items() if k != "forward"}
    with pytest.raises(ValueError, match="preparation_requires_forward_operation"):
        operation.run_prepared_operation_locked(d.attempt.root, **d.arguments, request=request,
            limits=operation.OperationLimits(**d.plan["limits"]), prepare_forward_only=flag)
    assert not d.events


def test_preparation_only_cli_uses_local_operation_without_http(monkeypatch, tmp_path):
    from cli import main
    monkeypatch.setattr(main, "_client", lambda _:pytest.fail("HTTP client opened"))
    calls = []
    monkeypatch.setattr(operation, "run_operation_plan", lambda path, **kw:calls.append((path,kw)) or {})
    path = tmp_path/"forward.json"
    for execute in (False, True):
        args = main.build_parser().parse_args(["storage", "migrate", "--operation-file", str(path),
            "--prepare-forward-only", *(["--execute"] if execute else [])])
        assert args.func(args) == 0
    assert calls == [(str(path), dict(execute=e, prepare_forward_only=True)) for e in (False, True)]



def test_unamended_completed_publication_keeps_original_evidence(published, monkeypatch):
    a = published
    model = deepcopy(a.publication["new_runtime"])
    model["services"]["tsdb"]["image"] = "sha256:"+"d"*64
    write(a.root/forward.runtime.RUNTIME_RECIPE, model)
    final = dict(phase="recovery_runtime_ready", runtime=dict(admission=dict(
        recipe_sha256=operation.host.digest(model))))
    monkeypatch.setattr(operation.final, "_load", lambda path: final)
    before = {p:p.read_bytes() for p in a.root.iterdir() if p.is_file()}
    assert forward.inspect_published_operation(a.root, operation_path=a.forward_path,
        completed_runtime=True) == a.publication
    with pytest.raises(RuntimeError, match="published_file_changed"):
        forward.inspect_published_operation(a.root, operation_path=a.forward_path)
    assert all(p.read_bytes() == data for p, data in before.items())
    assert not a.dispatches
