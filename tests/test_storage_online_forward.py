"""Forward package publication with real private files and synthetic host peers."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from scripts.automation import storage_online_forward as forward
from scripts.automation import storage_online_terminal as terminal
from scripts.automation import storage_online_deadline as publication
from scripts.automation import storage_online_operation as operation
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_host_boundary as host
from tests.test_storage_online_deadline import attempt, package_attempt, write


@pytest.fixture
def prepared(package_attempt, monkeypatch):
    a = package_attempt
    inventory = Path(a.plan["inventory_path"]); inventory.write_text("fixture-only")
    a.worker["binding"].update(inventory_sha256=publication._sha(inventory.read_bytes()), clients={"fixture":"source"})
    write(a.root/launch._STATE, a.worker)
    a.canceled = dict(operation_path=str(a.path), package={"original":"terminal-package"}, operator_sha256="f"*64,
        worker=deepcopy(a.worker), worker_sha256=publication._sha((a.root/launch._STATE).read_bytes()),
        original_capture=deepcopy(a.capture), observation={"source":"preserved"},
        wall_deadline=1., monotonic_deadline=1., boot_id="old-boot")
    a.canceled["intent_sha256"] = host.digest(a.canceled)
    a.receipt = dict(schema_version="qt.fact_header_cancel.v2", intent_sha256=a.canceled["intent_sha256"],
        capture=deepcopy(a.capture), source_retained=True, partial_copies_retained=True,
        migration_ready=False, final_switch_authorized=False)
    a.canceled.update(phase="complete", receipt=deepcopy(a.receipt))
    write(a.root/terminal.STATE, a.canceled)
    a.original_bytes = a.path.read_bytes()
    a.terminal_bytes = (a.root/terminal.STATE).read_bytes()
    a.manifest.update(schema_version="qt.storage_online_forward_package.v1",
        forward_plan_path=str(a.path.parent/"forward-operation.json"), end_day="2026-10-04")
    write(a.package_path, a.manifest)
    a.preflights = []
    def preflight(*args, **kwargs):
        a.preflights.append(deepcopy(kwargs))
        assert kwargs["proposed_runtime_recipe"]["services"]["backend"]["image"] == kwargs["image"]
        return {"verified": True}
    monkeypatch.setattr(operation, "inspect_prepared_operation", preflight)
    monkeypatch.setattr(host, "inventory", lambda *args, **kwargs:{"running":"source"})
    monkeypatch.setattr(host, "identities", lambda rows: {"fixture":"source"})
    monkeypatch.setattr(host, "source_clients_serving", lambda rows: True)
    a.probes = []
    def probe(root, plan, saved, package, **kwargs):
        assert kwargs["action"] == "reconcile"
        assert kwargs["expected_capture"] == a.capture
        assert kwargs["intent_sha256"] == a.canceled["intent_sha256"]
        assert publication._sha(publication.request_bytes(kwargs["original_request"])) == a.worker["binding"]["request_sha256"]
        a.probes.append(kwargs)
        return deepcopy(a.receipt)
    monkeypatch.setattr(terminal, "_probe", probe)
    a.run = lambda execute=True: forward.publish_package(a.path, package_file=a.package_path, execute=execute)
    return a


def assert_complete(a):
    journal = host.load_receipt(a.root/forward.STATE, max_bytes=forward._MAX_BYTES)
    assert journal["phase"] == "complete"
    assert a.path.read_bytes() == a.original_bytes
    assert (a.root/terminal.STATE).read_bytes() == a.terminal_bytes
    assert journal["old_worker"] == a.worker
    assert journal["forward"]["original_capture"] == a.capture
    assert journal["forward"]["cancellation_intent_sha256"] == a.canceled["intent_sha256"]
    assert journal["forward"]["operation_sha256"] != a.canceled["intent_sha256"]
    assert journal["wall_deadline"] == journal["started_at"]+300
    assert journal["monotonic_deadline"] == journal["started_monotonic"]+300
    assert a.name.startswith("/qt-test-storage-online-forward-before-")
    request = host.load_receipt(a.root/publication.REQUEST)
    assert "capture_preparation" not in request
    assert request["forward"] == journal["forward"]
    assert request["source_revision"] == a.manifest["source_revision"]
    worker = host.load_receipt(a.root/launch._STATE)
    assert worker["deadline"] is None and worker["container_id"] is None and worker["contract"] is None
    assert "capture" not in worker
    assert worker["binding"]["request_sha256"] == publication._sha(publication.request_bytes(request))
    assert host.load_receipt(Path(a.manifest["forward_plan_path"])) == journal["new_plan"]
    with pytest.raises(RuntimeError, match="terminal_intent"):
        publication.require_settled(a.root)
    return journal


def test_forward_inspect_publish_keeps_original_and_does_not_enable_worker(prepared):
    a = prepared
    paths = [a.path, a.root/terminal.STATE, a.root/publication.REQUEST, a.root/launch._STATE,
             a.root/publication.runtime.RUNTIME_RECIPE]
    original = {p:p.read_bytes() for p in paths}
    assert a.run(False)["storage_mutations_performed"] is False
    assert all(p.read_bytes() == b for p,b in original.items())
    assert not (a.root/forward.STATE).exists()
    assert a.run()["forward_worker_authorized"] is False
    assert len(a.preflights) == 4 and len(a.probes) == 2
    assert_complete(a)
    calls = len(a.probes)
    assert a.run()["current_source_verified"] is False
    assert len(a.probes) == calls


@pytest.mark.parametrize("boundary", ["intent", "rename", publication.runtime.RUNTIME_RECIPE,
    publication.REQUEST, launch._STATE, "plan", "complete"])
def test_forward_interruption_converges_with_same_intent_and_clock(prepared, monkeypatch, boundary):
    a = prepared
    replace, docker, save, publish_plan = publication._replace, host.docker, host.save_receipt, forward._publish_plan
    def lose_replace(path, before, after):
        replace(path, before, after)
        if path.name == boundary: raise TimeoutError("publication reply lost")
    def lose_docker(*args, **kwargs):
        value = docker(*args, **kwargs)
        if args[0] == boundary: raise TimeoutError("rename reply lost")
        return value
    def lose_save(path, value, **kwargs):
        save(path, value, **kwargs)
        if path.name == forward.STATE and ((boundary == "intent" and value["phase"] == "prepared")
                or (boundary == "complete" and value["phase"] == "complete")):
            raise TimeoutError("journal reply lost")
    def lose_plan(*args):
        publish_plan(*args)
        if boundary == "plan": raise TimeoutError("plan reply lost")
    with monkeypatch.context() as lost:
        lost.setattr(publication, "_replace", lose_replace); lost.setattr(host, "docker", lose_docker)
        lost.setattr(host, "save_receipt", lose_save); lost.setattr(forward, "_publish_plan", lose_plan)
        with pytest.raises(TimeoutError): a.run()
    before = host.load_receipt(a.root/forward.STATE, max_bytes=forward._MAX_BYTES)
    a.run()
    after = assert_complete(a)
    assert {k:v for k,v in after.items() if k != "phase"} == {k:v for k,v in before.items() if k != "phase"}


@pytest.mark.parametrize("fault", ["wall", "monotonic", "reboot", "clock_reversal", "proof", "original", "terminal", "runtime", "request", "worker", "plan"])
def test_forward_partial_intent_refuses_drift_and_never_overwrites_it(prepared, monkeypatch, fault):
    a = prepared
    with monkeypatch.context() as lost:
        lost.setattr(publication, "_replace", lambda *args: (_ for _ in ()).throw(TimeoutError()))
        with pytest.raises(TimeoutError): a.run()
    before = host.load_receipt(a.root/forward.STATE, max_bytes=forward._MAX_BYTES)
    changed = None
    if fault == "wall": monkeypatch.setattr(forward.time, "time", lambda: before["wall_deadline"]+1)
    elif fault == "monotonic": monkeypatch.setattr(forward.time, "monotonic", lambda: before["monotonic_deadline"]+1)
    elif fault == "reboot": monkeypatch.setattr(terminal, "_boot", lambda:"changed")
    elif fault == "clock_reversal": monkeypatch.setattr(forward.time,"time",lambda:before["started_at"]-1)
    elif fault == "proof": a.receipt["foreign"] = True
    else:
        changed = {"original":a.path, "terminal":a.root/terminal.STATE,
            "runtime":a.root/publication.runtime.RUNTIME_RECIPE, "request":a.root/publication.REQUEST,
            "worker":a.root/launch._STATE,"plan":Path(a.manifest["forward_plan_path"])}[fault]
        write(changed,{"foreign":"preserved"})
    all_paths = [a.path, a.root/terminal.STATE, a.root/publication.REQUEST, a.root/launch._STATE,
        a.root/publication.runtime.RUNTIME_RECIPE, Path(a.manifest["forward_plan_path"])]
    preimages = {p: p.read_bytes() if p.exists() else None for p in all_paths}
    old_name = a.name
    with pytest.raises((RuntimeError, KeyError)): a.run()
    assert a.name == old_name
    assert all((p.read_bytes() if p.exists() else None) == data for p, data in preimages.items())
    assert host.load_receipt(a.root/forward.STATE, max_bytes=forward._MAX_BYTES) == before
    if changed: assert host.load_receipt(changed) == {"foreign":"preserved"}


def test_forward_requires_both_runtime_preflights_before_private_intent(prepared, monkeypatch):
    a = prepared
    def reject(*args, **kwargs):
        if kwargs["image"] == a.manifest["image"]: raise RuntimeError("new runtime refused")
    monkeypatch.setattr(operation,"inspect_prepared_operation",reject)
    with pytest.raises(RuntimeError, match="new runtime refused"): a.run()
    assert not (a.root/forward.STATE).exists() and not a.probes


def test_forward_cli_uses_canonical_operator_and_keeps_amendments_separate(prepared, monkeypatch):
    from cli import main
    a = prepared; calls=[]
    monkeypatch.setattr(forward,"publish_package",lambda path,**kwargs:calls.append((path,kwargs)) or {"phase":"inspected"})
    args=main.build_parser().parse_args(["storage","migrate","--operation-file",str(a.path),
        "--forward-package-file",str(a.package_path)])
    assert args.func(args)==0
    assert calls==[(str(a.path),dict(package_file=str(a.package_path),execute=False))]
    with pytest.raises(ValueError,match="must_be_separate"):
        operation.run_operation_plan(a.path, forward_package_file=a.package_path, cancel_attempt_file=a.package_path)


@pytest.mark.parametrize("action,changed", [("apply",False),("inspect",False),("reconcile",True)])
def test_original_request_override_refuses_before_peer_actions(package_attempt, monkeypatch, action, changed):
    a=package_attempt
    request=host.load_receipt(a.root/publication.REQUEST)
    if changed: request["foreign"]=True
    monkeypatch.setattr(terminal,"_retire_previous_probe",lambda *args: pytest.fail("peer action before validation"))
    with pytest.raises(RuntimeError,match="original_request_changed"):
        terminal._probe(a.root,a.plan,a.worker,{},action=action,wall_deadline=1,original_request=request)


@pytest.mark.parametrize("target", [terminal.STATE, publication.REQUEST, launch._STATE, publication.runtime.RUNTIME_RECIPE])
def test_initial_preflight_drift_refuses_before_intent_or_rename(prepared, monkeypatch, target):
    a=prepared
    actual=operation.inspect_prepared_operation
    def changed(*args,**kwargs):
        result=actual(*args,**kwargs)
        if kwargs["image"]==a.manifest["image"]:
            write(a.root/target,{"foreign":"preserved"})
        return result
    monkeypatch.setattr(operation,"inspect_prepared_operation",changed)
    old_name=a.name
    with pytest.raises(RuntimeError): a.run()
    assert a.name==old_name and not (a.root/forward.STATE).exists()
    assert host.load_receipt(a.root/target)=={"foreign":"preserved"}



def test_forward_preserves_large_private_runtime_configuration(prepared):
    a=prepared
    path=a.root/publication.runtime.RUNTIME_RECIPE
    recipe=host.load_receipt(path)
    recipe["services"]["backend"]["environment"]={"FIXTURE_ONLY":"x"*70000}
    write(path,recipe)
    a.run()
    after=host.load_receipt(path,max_bytes=524288)
    assert after["services"]["backend"]["environment"]==recipe["services"]["backend"]["environment"]
    assert after["services"]["backend"]["image"]==a.manifest["image"]



def test_completed_forward_publication_inspection_binds_original_and_effective_request(prepared):
    a = prepared
    a.run()
    journal = assert_complete(a)
    paths = [a.path, a.root/terminal.STATE, a.root/forward.STATE,
             a.root/publication.REQUEST, a.root/launch._STATE,
             a.root/publication.runtime.RUNTIME_RECIPE, Path(a.manifest["forward_plan_path"])]
    before = {p:p.read_bytes() for p in paths}
    observed = forward.inspect_published_operation(a.root, request=journal["new_request"],
        operation_path=a.manifest["forward_plan_path"])
    assert observed == journal
    assert all(p.read_bytes() == value for p,value in before.items())
    with pytest.raises(RuntimeError, match="operation_path_changed"):
        forward.inspect_published_operation(a.root, operation_path=a.path)
    with pytest.raises(RuntimeError, match="publication_binding_changed"):
        forward.inspect_published_operation(a.root, request=journal["new_plan"]["request"])


@pytest.mark.parametrize("fault", ["request", "plan", "runtime", "original", "terminal", "inventory", "journal"])
def test_completed_forward_inspection_refuses_foreign_files_without_writes(prepared, fault):
    a = prepared
    a.run()
    paths = dict(request=a.root/publication.REQUEST, plan=Path(a.manifest["forward_plan_path"]),
        runtime=a.root/publication.runtime.RUNTIME_RECIPE, original=a.path,
        terminal=a.root/terminal.STATE, inventory=Path(a.plan["inventory_path"]), journal=a.root/forward.STATE)
    paths[fault].write_text(paths[fault].read_text()+" ")
    # JSON whitespace is also a foreign preimage except inventory, which has a
    # separate exact digest. Publication journals pin their content digest.
    if fault == "journal":
        value=host.load_receipt(paths[fault], max_bytes=forward._MAX_BYTES)
        value["phase"]="publishing"
        write(paths[fault],value)
    before={name:p.read_bytes() for name,p in paths.items()}
    with pytest.raises(RuntimeError):
        forward.inspect_published_operation(a.root)
    assert before == {name:p.read_bytes() for name,p in paths.items()}


@pytest.mark.parametrize("fault", ["runtime", "worker", "old_request"])
def test_forward_inspection_refuses_rehashed_journal_with_changed_original_binding(prepared, fault):
    a=prepared
    a.run()
    journal=host.load_receipt(a.root/forward.STATE, max_bytes=forward._MAX_BYTES)
    if fault=="runtime":
        journal["new_runtime"]["services"]["backend"]["image"]="sha256:"+"d"*64
        write(a.root/publication.runtime.RUNTIME_RECIPE, journal["new_runtime"])
    elif fault=="worker":
        journal["new_worker"]["binding"]["memory_bytes"]=123
    else:
        original=json.loads(journal["old_request_bytes"])
        original["source_inode"]=123
        journal["old_request_bytes"]=publication.request_bytes(original).decode()
    journal["intent_sha256"]=host.digest({k:v for k,v in journal.items() if k not in {"intent_sha256","phase"}})
    write(a.root/forward.STATE,journal)
    with pytest.raises(RuntimeError, match="publication_binding_changed"):
        forward.inspect_published_operation(a.root)
