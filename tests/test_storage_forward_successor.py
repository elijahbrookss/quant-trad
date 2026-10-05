"""Successor host publication preserves retired owners and original clocks."""
from copy import deepcopy
from pathlib import Path

import pytest

from scripts.automation import storage_online_forward as forward
from scripts.automation import storage_online_terminal as terminal
from scripts.automation import storage_online_deadline as publication
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_host_boundary as host
from tests.test_storage_online_deadline import attempt as base_attempt, package_attempt, write
from tests.test_storage_forward_operation import attempt
from tests.test_storage_online_forward import prepared
from tests.test_storage_forward_launch import owned, _rows, _advance
from tests.test_storage_forward_retirement import retiring


@pytest.fixture
def successor(retiring, monkeypatch):
    a = retiring
    from scripts.automation import storage_online_keys as lookup_owner
    a.require_lookups_finished = lookup_owner.require_lookups_finished
    monkeypatch.setattr(lookup_owner, "require_lookups_finished", lambda *args:None)
    a.retire()
    a.predecessor = forward.inspect_published_operation(a.root)
    a.retired = host.load_receipt(a.root/terminal.FORWARD_STATE)
    a.previous_path = a.forward_path
    a.successor_manifest = dict(schema_version="qt.storage_online_forward_package.v2",
        plan_sha256=publication._sha(a.previous_path.read_bytes()), image="sha256:"+"9"*64,
        source_revision="9"*40, source_tree_hash="8"*64,
        forward_plan_path=str(a.path.parent/"successor-operation.json"), end_day="2026-10-05",
        predecessor_operation_sha256=a.request["forward"]["operation_sha256"],
        predecessor_terminal_sha256=a.committed["terminal_sha256"], lookup_operation_sha256="d"*64,
        predecessor_terminal_file=terminal.FORWARD_STATE,
        predecessor_terminal_file_sha256=publication._sha((a.root/terminal.FORWARD_STATE).read_bytes()))
    a.successor_file = a.path.parent/"successor-package.json"
    write(a.successor_file, a.successor_manifest)
    a.protected = {p:p.read_bytes() for p in (a.path, a.previous_path, a.root/terminal.STATE,
        a.root/terminal.FORWARD_STATE, a.root/forward.STATE, a.root/forward.LAUNCH_STATE)}
    a.name = "/"+a.plan["project"]+"-storage-online"
    monkeypatch.setattr(publication, "_retired", lambda saved: dict(config=dict(Env=[
        "PG_DSN=fixture-only", "QT_DISABLE_DOTENV=1", "QT_ARCHIVE_SHARED_GROUP_ID=1000",
        "QT_LOGGING_LOKI_URL=", "QT_STORAGE_UDEV_ROOT=/run/qt-online/udev"])))
    def docker(*args, **kwargs):
        if args[0] == "inspect": return a.name
        assert args[0] == "rename" and args[1] == a.current["container_id"]
        a.name = "/"+args[2]
        return ""
    monkeypatch.setattr(host, "docker", docker)
    a.lookups = []
    monkeypatch.setattr(forward, "observe_lookup_placement", lambda db,package:a.lookups.append((db,deepcopy(package))))
    def reconcile(root, plan, saved, package, **kwargs):
        assert kwargs["action"] == "reconcile"
        assert kwargs["original_request"] == a.predecessor["new_request"]
        assert kwargs["probe_operation_sha256"] != a.successor_manifest["predecessor_operation_sha256"]
        assert package["image"] == a.successor_manifest["image"]
        return deepcopy(a.committed)
    monkeypatch.setattr(terminal, "_probe", reconcile)
    a.publish_successor = lambda execute=True:forward.publish_package(a.previous_path,
        package_file=a.successor_file, execute=execute)
    return a


def test_successor_publication_and_launch_keep_retired_owners(successor):
    a = successor
    assert a.publish_successor(False)["storage_mutations_performed"] is False
    assert a.publish_successor()["forward_worker_authorized"] is False
    request = host.load_receipt(a.root/publication.REQUEST)
    selected = forward.inspect_published_operation(a.root, request=request,
        operation_path=a.successor_manifest["forward_plan_path"])
    assert selected["forward"]["schema_version"] == "qt.storage_online_forward_intent.v2"
    assert a.lookups
    assert a.publish_successor()["phase"] == "forward_package_recorded"
    with pytest.raises(RuntimeError, match="operation_path_changed"):
        forward.inspect_published_operation(a.root, operation_path=a.previous_path)
    intent = forward.launch_intent(a.root, selected, selected["new_worker"]["binding"])
    assert forward.load_launch(a.root, request=request) == intent
    assert forward.load_launch(a.root) != intent
    assert intent["successor_operation_sha256"] == request["forward"]["operation_sha256"]
    assert all(p.read_bytes() == before for p,before in a.protected.items())
    a.request, a.intent = request, intent
    keys, initial, capture = _rows(a, initial_offset=1, complete=True)
    _advance(a, 3)
    forward.admit_startup(a.root, intent, request, keys=keys, initialization=initial, capture=capture)
    worker = deepcopy(intent["worker"])
    worker.update(container_id="7"*64, contract={"fixture":"successor"}, deadline=intent["deadline"])
    forward.save_launched_worker(a.root, intent, worker)
    forward.admit_launched_adoption(a.root, request, worker, initialization=initial, capture=capture)
    assert all(p.read_bytes() == before for p,before in a.protected.items())


@pytest.mark.parametrize("boundary", ["intent", "rename", publication.runtime.RUNTIME_RECIPE,
    publication.REQUEST, launch._STATE, "plan", "complete"])
def test_successor_partial_publication_reuses_original_intent(successor, monkeypatch, boundary):
    a = successor
    replace, docker, save, publish = publication._replace, host.docker, host.save_receipt, forward._publish_plan
    def lost_replace(path, before, after):
        replace(path, before, after)
        if path.name == boundary: raise TimeoutError("lost replacement reply")
    def lost_docker(*args, **kwargs):
        result = docker(*args, **kwargs)
        if args[0] == boundary: raise TimeoutError("lost rename reply")
        return result
    def lost_save(path, value, **kwargs):
        save(path, value, **kwargs)
        if path.name.startswith("storage-online-forward-") and value.get("package") == a.successor_manifest:
            if boundary == "intent" and value["phase"] == "prepared" or boundary == "complete" and value["phase"] == "complete":
                raise TimeoutError("lost intent reply")
    def lost_plan(*args):
        publish(*args)
        if boundary == "plan": raise TimeoutError("lost plan reply")
    with monkeypatch.context() as lost:
        lost.setattr(publication, "_replace", lost_replace)
        lost.setattr(host, "docker", lost_docker)
        lost.setattr(host, "save_receipt", lost_save)
        lost.setattr(forward, "_publish_plan", lost_plan)
        with pytest.raises(TimeoutError): a.publish_successor()
    records = [p for p in a.root.glob("storage-online-forward-*.json")
        if host.load_receipt(p, max_bytes=forward._MAX_BYTES).get("package") == a.successor_manifest]
    assert len(records) == 1
    before = host.load_receipt(records[0], max_bytes=forward._MAX_BYTES)
    assert a.publish_successor()["phase"] in ("forward_package_published", "forward_package_recorded")
    after = host.load_receipt(records[0], max_bytes=forward._MAX_BYTES)
    assert {k:v for k,v in before.items() if k != "phase"} == {k:v for k,v in after.items() if k != "phase"}
    assert all(p.read_bytes() == data for p,data in a.protected.items())


@pytest.mark.parametrize("fault", ["terminal", "launch", "publication", "lookup", "worker", "clock"])
def test_successor_refuses_drift_without_resetting_old_owners(successor, monkeypatch, fault):
    a = successor
    if fault == "lookup":
        monkeypatch.setattr(forward, "observe_lookup_placement", lambda *args: (_ for _ in ()).throw(RuntimeError("lookup incomplete")))
    elif fault == "clock":
        with monkeypatch.context() as lost:
            lost.setattr(publication, "_replace", lambda *args: (_ for _ in ()).throw(TimeoutError()))
            with pytest.raises(TimeoutError): a.publish_successor()
        _advance(a, 301)
    else:
        target = {"terminal":terminal.FORWARD_STATE, "launch":forward.LAUNCH_STATE,
            "publication":forward.STATE, "worker":launch._STATE}[fault]
        write(a.root/target, {"foreign":"preserve"})
    before = {p:p.read_bytes() for p in a.root.iterdir() if p.is_file()}
    with pytest.raises((RuntimeError, KeyError)): a.publish_successor()
    assert all(p.read_bytes() == data for p,data in before.items())


def test_successor_journal_names_and_probe_names_are_explicit():
    operation = "b"*64
    assert forward.operation_file(forward.STATE) == forward.STATE
    assert forward.operation_file(forward.STATE, operation_sha256=operation).endswith(operation+".json")
    assert terminal._probe_name("qt-test", forward=True, operation_sha256=operation).endswith(operation)
    with pytest.raises(ValueError): forward.operation_file(forward.STATE, operation_sha256="../foreign")


def test_successor_terminal_route_keeps_previous_terminal_files(successor, monkeypatch):
    from scripts.automation import storage_online_operation as operation
    from scripts.automation.storage_online_forward_worker import capture_binding
    a = successor
    a.publish_successor()
    published = forward.inspect_published_operation(a.root)
    a.request = published["new_request"]
    a.intent = forward.launch_intent(a.root, published, published["new_worker"]["binding"])
    keys, initial, capture = _rows(a, initial_offset=1, complete=True)
    _advance(a, 3)
    forward.admit_startup(a.root, a.intent, a.request, keys=keys, initialization=initial, capture=capture)
    a.current = deepcopy(a.intent["worker"])
    a.current.update(container_id="7"*64, contract={"fixture":"successor"}, deadline=a.intent["deadline"])
    forward.save_launched_worker(a.root, a.intent, a.current)
    path = Path(a.successor_manifest["forward_plan_path"])
    package = dict(schema_version="qt.storage_online_terminal.v1", plan_sha256=publication._sha(path.read_bytes()),
        **{k:a.successor_manifest[k] for k in ("image", "source_revision", "source_tree_hash")})
    file = a.path.parent/"successor-terminal-package.json"
    write(file, package)
    journal = a.root/forward.operation_file(terminal.FORWARD_STATE, request=a.request)
    observed = dict(schema_version="qt.storage_forward_terminal.v1", capture=capture_binding(capture),
        request_sha256=host.digest(a.request), retired=False, terminal_sha256=None,
        source_retained=True, partial_copies_retained=True, migration_ready=False, final_switch_authorized=False)
    actions = []
    def probe(*args, **kwargs):
        actions.append(kwargs["action"])
        if kwargs["action"] == "apply":
            saved = host.load_receipt(journal)
            assert saved["phase"] == "dispatched" and saved["intent_sha256"] == kwargs["intent_sha256"]
            observed.update(retired=True, terminal_sha256="f"*64)
            raise TimeoutError("lost successor retirement reply")
        return deepcopy(observed)
    monkeypatch.setattr(terminal, "_probe", probe)
    with pytest.raises(TimeoutError): operation.run_operation_plan(path, cancel_attempt_file=file, execute=True)
    saved = host.load_receipt(journal)
    _advance(a, 301)
    assert operation.run_operation_plan(path, cancel_attempt_file=file, execute=True)["phase"] == "forward_retired"
    complete = host.load_receipt(journal)
    assert complete["intent_sha256"] == saved["intent_sha256"] and complete["wall_deadline"] == saved["wall_deadline"]
    assert actions.count("apply") == 1
    assert all(p.read_bytes() == data for p,data in a.protected.items())


@pytest.fixture
def lookup_operation(successor, monkeypatch):
    from scripts.automation import storage_online_keys as owner
    from scripts.automation import storage_online_operation as operation
    a = successor
    package = {k:v for k,v in a.successor_manifest.items() if k not in ("forward_plan_path", "end_day", "lookup_operation_sha256")}
    package.update(schema_version="qt.storage_online_lookup_package.v1", operation_sha256="d"*64)
    file = a.path.parent/"lookup-package.json"
    write(file, package)
    a.lookup_package, a.lookup_file = package, file
    a.lookup_state = a.root/forward.operation_file(owner.LOOKUP_STATE, operation_sha256=package["operation_sha256"])
    a.lookup_proof, a.lookup_actions = None, []
    def probe(root, plan, saved, supplied, **kwargs):
        assert supplied == package and saved == a.retired["worker"]
        a.lookup_actions.append(kwargs["action"])
        if kwargs["action"] == "place_lookups":
            state = host.load_receipt(a.lookup_state)
            assert state["phase"] == "dispatched" and state["wall_deadline"] == kwargs["wall_deadline"]
            a.lookup_proof = dict(operation_sha256=package["operation_sha256"], binding_sha256="1"*64,
                started_at="2026-10-05T01:00:00+00:00", expires_at="2026-10-05T02:00:00+00:00",
                duration_seconds=3600, complete=True, completion_sha256="2"*64)
        return deepcopy(a.lookup_proof)
    a.lookup_probe = probe
    monkeypatch.setattr(terminal, "_probe", probe)
    a.place = lambda execute=True:operation.run_operation_plan(a.previous_path,
        place_forward_lookups_file=file, execute=execute)
    return a


def test_lookup_host_owns_physical_phase_without_publishing_or_launching(lookup_operation):
    a = lookup_operation
    before = {p:p.read_bytes() for p in a.root.iterdir() if p.is_file()}
    assert a.place(False)["storage_mutations_performed"] is False
    assert not a.lookup_state.exists()
    assert a.place()["indexes_placed"] is True
    assert a.place(False)["indexes_placed"] is True
    assert a.lookup_actions.count("place_lookups") == 1
    assert host.load_receipt(a.lookup_state)["phase"] == "complete"
    assert all(p.read_bytes() == data for p,data in before.items())


def test_lookup_host_lost_commit_reconciles_after_expiry_without_moving_twice(lookup_operation, monkeypatch):
    a = lookup_operation
    def lost(*args, **kwargs):
        result = a.lookup_probe(*args, **kwargs)
        if kwargs["action"] == "place_lookups": raise TimeoutError("lost committed reply")
        return result
    monkeypatch.setattr(terminal, "_probe", lost)
    with pytest.raises(TimeoutError): a.place()
    before = host.load_receipt(a.lookup_state)
    _advance(a, 3601)
    monkeypatch.setattr(terminal, "_probe", a.lookup_probe)
    assert a.place()["indexes_placed"] is True
    after = host.load_receipt(a.lookup_state)
    assert after["intent_sha256"] == before["intent_sha256"] and after["wall_deadline"] == before["wall_deadline"]
    assert a.lookup_actions.count("place_lookups") == 1


def test_lookup_host_unfinished_expiry_keeps_intent_and_refuses_move(lookup_operation, monkeypatch):
    a = lookup_operation
    def interrupted(*args, **kwargs):
        if kwargs["action"] == "place_lookups": raise TimeoutError("lost before SQL intent")
        return a.lookup_probe(*args, **kwargs)
    monkeypatch.setattr(terminal, "_probe", interrupted)
    with pytest.raises(TimeoutError): a.place()
    before = a.lookup_state.read_bytes()
    _advance(a, 3601)
    with pytest.raises(RuntimeError, match="expired"): a.place()
    assert a.lookup_state.read_bytes() == before
    assert "place_lookups" not in a.lookup_actions


def test_lookup_cli_and_phase_exclusion(lookup_operation, monkeypatch):
    from cli import main
    from scripts.automation import storage_online_keys as owner
    from scripts.automation import storage_online_operation as operation
    a = lookup_operation
    calls = []
    monkeypatch.setattr(owner, "place_lookups", lambda path,**kw:calls.append((path,kw)) or {"phase":"inspected"})
    args = main.build_parser().parse_args(["storage", "migrate", "--operation-file", str(a.previous_path),
        "--place-forward-lookups-file", str(a.lookup_file)])
    assert args.func(args) == 0 and calls == [(str(a.previous_path),dict(package_file=str(a.lookup_file), execute=False))]
    for option in ("forward_package_file", "prepare_forward_keys_file", "cancel_attempt_file", "replacement_package_file", "extend_attempt_seconds", "capacity_file"):
        with pytest.raises(ValueError, match="must_be_separate"):
            operation.run_operation_plan(a.previous_path, place_forward_lookups_file=a.lookup_file, **{option:1})


@pytest.mark.parametrize("fault", [None, "unfinished", "lineage", "proof", "probe", "alive"])
def test_lookup_publication_requires_completed_host_owner_and_retired_worker(lookup_operation, monkeypatch, fault):
    a = lookup_operation
    a.place()
    path = a.root/forward.operation_file(terminal.LOOKUP_PROBE, operation_sha256=a.lookup_package["operation_sha256"])
    probe = dict(name=terminal._probe_name(a.plan["project"], lookups=True, operation_sha256=a.lookup_package["operation_sha256"]),
        binding=dict(project=a.plan["project"]), retired=True, container_id="f"*64,
        owner=dict(package=a.lookup_package))
    if fault == "probe": probe["retired"] = False
    write(path, probe)
    monkeypatch.setattr(host, "docker", lambda *args,**kw: "f"*64 if fault == "alive" else "")
    if fault in ("unfinished", "lineage", "proof"):
        state = host.load_receipt(a.lookup_state)
        if fault == "unfinished": state["phase"] = "dispatched"
        elif fault == "lineage": state["binding"]["package"]["predecessor_operation_sha256"] = "b"*64
        else: state["proof"]["operation_sha256"] = "b"*64
        state["intent_sha256"] = host.digest({k:v for k,v in state.items() if k not in ("intent_sha256", "phase", "proof")})
        write(a.lookup_state, state)
    if fault is None:
        a.require_lookups_finished(a.root, a.successor_manifest)
    else:
        with pytest.raises(RuntimeError): a.require_lookups_finished(a.root, a.successor_manifest)


@pytest.mark.parametrize("execute", [False, True])
@pytest.mark.parametrize("prepare_only", [False, True])
@pytest.mark.parametrize("retirement", [None, "dispatched", "complete"])
def test_successor_dispatch_selects_own_retirement(successor, monkeypatch, execute, prepare_only, retirement):
    from scripts.automation import storage_online_operation as operation
    a = successor
    a.publish_successor()
    selected = forward.inspect_published_operation(a.root)
    request = selected["new_request"]
    path = Path(a.successor_manifest["forward_plan_path"])
    if retirement is not None:
        write(a.root/forward.operation_file(terminal.FORWARD_STATE, request=request), {"phase":retirement})
    # Freeze only this dispatch's UTC admission; publication/retirement evidence
    # remains real private files validated by the normal entry point.
    monkeypatch.setattr(operation, "_forward_cutover_day", lambda *args,**kwargs:None)
    calls = []
    def dispatch(*args, **kwargs):
        calls.append(kwargs)
        return {"adoption_active":True}
    monkeypatch.setattr(operation, "run_prepared_operation_locked", dispatch)
    if retirement is not None:
        with pytest.raises(RuntimeError, match="retirement_requires_reconciliation"):
            operation.run_operation_plan(path, execute=execute, prepare_forward_only=prepare_only)
        assert not calls
    else:
        result = operation.run_operation_plan(path, execute=execute, prepare_forward_only=prepare_only)
        assert result["phase"] == (("forward_background_prepared" if prepare_only else "runtime_ready")
                                   if execute else "forward_inspected")
        assert len(calls) == int(execute)
        if execute:
            assert calls[0]["request"] == request
            assert calls[0].get("prepare_forward_only", False) is prepare_only
    assert all(p.read_bytes() == data for p,data in a.protected.items())
