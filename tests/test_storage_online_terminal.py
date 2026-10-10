"""Terminal host fault injection using durable private files and fake peers."""
from copy import deepcopy
import json
import time

import pytest
from scripts.automation import storage_online_terminal as terminal
from scripts.automation import storage_online_deadline as amendment
from scripts.automation import storage_online_operation as operation
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_host_boundary as host
from tests.test_storage_online_deadline import attempt, write
from tests.test_storage_online_launch import _contract_fixture

_REAL_RETIRE=terminal._retire_previous_probe


@pytest.fixture
def terminal_attempt(attempt, monkeypatch):
    a = attempt
    inventory = a.path.parent/"inventory"; inventory.write_text("{}")
    a.worker["binding"].update(image=a.plan["image"], source_revision=a.plan["source_revision"],
                               project=a.plan["project"], inventory_sha256=amendment._sha(inventory.read_bytes()))
    a.rows = {"market-data-collector":dict(id="e"*64,image="old",restart="no",running=True)}
    a.worker["binding"]["clients"] = host.identities(a.rows)
    write(a.root/launch._STATE, a.worker)
    a.package = dict(schema_version="qt.storage_online_terminal.v1", plan_sha256=amendment._sha(a.path.read_bytes()),
                     image="sha256:"+"d"*64, source_revision="e"*40, source_tree_hash="f"*64)
    a.package_path = a.path.parent/"terminal.json"; write(a.package_path,a.package)
    a.terminal_receipt = None
    a.calls.clear()
    def probe(root, plan, saved, package, *, action, wall_deadline, expected_capture=None, intent_sha256=None):
        a.calls.append(action)
        if action == "inspect":
            return deepcopy(a.capture)
        if action == "apply":
            journal=host.load_receipt(root/terminal.STATE)
            assert journal["phase"] == "dispatched" and journal["intent_sha256"] == intent_sha256
            a.terminal_receipt=dict(schema_version="qt.fact_header_cancel.v2", capture=deepcopy(expected_capture),
                intent_sha256=intent_sha256,source_retained=True,partial_copies_retained=True,
                migration_ready=False,final_switch_authorized=False)
        return deepcopy(a.terminal_receipt)
    a.terminal_probe=probe
    monkeypatch.setattr(terminal,"_probe",probe)
    monkeypatch.setattr(terminal,"_retire_previous_probe",lambda *args:None)
    monkeypatch.setattr(host,"inventory",lambda *args,**kwargs:a.rows)
    monkeypatch.setattr(host,"source_clients_serving",lambda rows:all(x["running"] for x in rows.values()))
    monkeypatch.setattr(launch,"inspect_candidate_image",lambda image,request:{})
    a.terminal_run=lambda execute=True:terminal.cancel_operation(a.path,package_file=a.package_path,execute=execute)
    a.original={p:p.read_bytes() for p in (a.path,a.root/launch._STATE,a.root/amendment.REQUEST)}
    return a


def assert_preserved(a):
    assert all(p.read_bytes()==value for p,value in a.original.items())


def test_default_inspection_and_exact_terminal_completion_preserve_original_inputs(terminal_attempt):
    a=terminal_attempt
    assert a.terminal_run(False)["storage_mutations_performed"] is False
    assert not (a.root/terminal.STATE).exists()
    assert_preserved(a)
    assert a.terminal_run()["phase"] == "attempt_cancelled"
    assert a.calls.count("apply")==1
    assert_preserved(a)
    assert a.terminal_run(False)["phase"] == "attempt_cancelled"
    assert a.calls.count("apply")==1
    assert host.load_receipt(a.root/terminal.STATE)["phase"]=="complete"


def test_lost_commit_reconciles_after_reboot_without_new_dispatch(terminal_attempt,monkeypatch):
    a=terminal_attempt
    def lost(*args,**kwargs):
        result=a.terminal_probe(*args,**kwargs)
        if kwargs["action"]=="apply":raise TimeoutError("lost committed reply")
        return result
    monkeypatch.setattr(terminal,"_probe",lost)
    with pytest.raises(TimeoutError):a.terminal_run()
    old=host.load_receipt(a.root/terminal.STATE)
    monkeypatch.setattr(terminal,"_probe",a.terminal_probe)
    monkeypatch.setattr(terminal,"_boot",lambda:"different boot")
    assert a.terminal_run(False)["phase"]=="attempt_cancelled"
    new=host.load_receipt(a.root/terminal.STATE)
    assert (new["wall_deadline"],new["monotonic_deadline"],new["boot_id"]) == (old["wall_deadline"],old["monotonic_deadline"],old["boot_id"])
    assert a.calls.count("apply")==1
    assert_preserved(a)


def test_unknown_outcome_never_replays_or_changes_original_inputs(terminal_attempt,monkeypatch):
    a=terminal_attempt
    def lost(*args,**kwargs):
        if kwargs["action"]=="apply":
            a.calls.append("apply")
            raise TimeoutError("unknown outcome")
        return a.terminal_probe(*args,**kwargs)
    monkeypatch.setattr(terminal,"_probe",lost)
    with pytest.raises(TimeoutError):a.terminal_run()
    monkeypatch.setattr(terminal,"_probe",a.terminal_probe)
    with pytest.raises(RuntimeError,match="unresolved_no_replay"):a.terminal_run()
    assert a.calls.count("apply")==1
    assert_preserved(a)


@pytest.mark.parametrize("change",["boot","clock"])
def test_prepared_intent_cannot_gain_new_dispatch_window(terminal_attempt,monkeypatch,change):
    a=terminal_attempt;save=host.save_receipt
    def interrupt(path,value,**kwargs):
        save(path,value,**kwargs)
        if path.name==terminal.STATE and value["phase"]=="prepared":raise RuntimeError("prepared interruption")
    monkeypatch.setattr(host,"save_receipt",interrupt)
    with pytest.raises(RuntimeError,match="prepared interruption"):a.terminal_run()
    monkeypatch.setattr(host,"save_receipt",save)
    journal=host.load_receipt(a.root/terminal.STATE)
    if change=="boot":monkeypatch.setattr(terminal,"_boot",lambda:"new boot")
    else:monkeypatch.setattr(terminal.time,"time",lambda:journal["wall_deadline"]+1)
    with pytest.raises(RuntimeError,match="no_dispatch"):a.terminal_run()
    assert "apply" not in a.calls
    assert_preserved(a)


@pytest.mark.parametrize("change",["plan","request","worker","fleet","running","final","inventory"])
def test_foreign_ownership_changes_refuse_before_sql(terminal_attempt,monkeypatch,change):
    a=terminal_attempt
    if change=="plan":write(a.path,{**a.plan,"spool_destination":"foreign"})
    elif change=="request":write(a.root/amendment.REQUEST,{"foreign":True})
    elif change=="inventory":(a.path.parent/"inventory").write_text("foreign")
    elif change=="worker":a.worker["binding"]["source_revision"]="f"*40
    elif change=="fleet":a.rows["market-data-collector"]["id"]="f"*64
    elif change=="running":
        def running(saved):raise RuntimeError("worker_must_be_retired")
        monkeypatch.setattr(amendment,"_retired",running)
    else:write(a.root/"storage-online-final.json",{"uncertain":True})
    with pytest.raises(RuntimeError):a.terminal_run()
    assert not a.calls


def test_terminal_intent_blocks_normal_migration_and_amendments(terminal_attempt):
    a=terminal_attempt
    a.terminal_run()
    with pytest.raises(RuntimeError,match="terminal_owner"):amendment.require_settled(a.root)
    with pytest.raises(RuntimeError,match="terminal_owner"):a.run()


def test_cli_terminal_option_is_separate_and_read_only_by_default(terminal_attempt,monkeypatch):
    from cli import main
    a=terminal_attempt;calls=[]
    monkeypatch.setattr(terminal,"cancel_operation",lambda path,**kw:calls.append((path,kw)) or {"phase":"inspected"})
    args=main.build_parser().parse_args(["storage","migrate","--operation-file",str(a.path),"--cancel-attempt-file",str(a.package_path)])
    assert args.func(args)==0
    assert calls==[(str(a.path),dict(package_file=str(a.package_path),execute=False))]
    with pytest.raises(ValueError,match="must_be_separate"):
        operation.run_operation_plan(a.path,cancel_attempt_file=a.package_path,replacement_package_file=a.package_path)


@pytest.mark.parametrize("drift",["command","writable","capability","keys"])
def test_terminal_container_contract_has_no_extra_authority(tmp_path,monkeypatch,drift):
    identity,binding,details=_contract_fixture(tmp_path)
    details["config"]["Cmd"]=terminal.COMMAND
    for m in binding["mounts"].values():m["readonly"]=True
    for m in details["mounts"]:m["RW"]=False
    monkeypatch.setattr(host,"database_details",lambda identity:details)
    original=launch._admit(identity,binding,command=terminal.COMMAND)
    if drift=="command":details["config"]["Cmd"]=launch._COMMAND
    elif drift=="writable":details["mounts"][0]["RW"]=True
    elif drift=="capability":details["host"].update(CapAdd=[*details["host"]["CapAdd"],"DAC_OVERRIDE"])
    else:details["mounts"].append(dict(Type="bind",Source="/private/keys",Destination="/keys",RW=False))
    with pytest.raises(RuntimeError,match="binding_changed"):
        launch._admit(identity,binding,original,command=terminal.COMMAND)


@pytest.mark.parametrize("failure", [None, "running", "foreign"])
def test_exact_transient_retirement_never_removes_a_live_or_foreign_worker(terminal_attempt, monkeypatch, failure):
    a=terminal_attempt
    monkeypatch.setattr(terminal,"_retire_previous_probe",_REAL_RETIRE)
    identity="9"*64
    binding={"project":a.plan["project"]}
    record=dict(name=a.plan["project"]+"-storage-terminal",binding=binding,container_id=identity,
                contract="fixed",retired=False,owner=dict(root=str(a.root),worker=a.worker["container_id"],package=a.package))
    if failure=="foreign":record["owner"]["worker"]="8"*64
    write(a.root/terminal.PROBE,record)
    state=dict(Running=True,Pid=77,Status="running",Paused=False,Restarting=False,Dead=False)
    actions=[]
    monkeypatch.setattr(launch,"_admit",lambda *args,**kwargs:"fixed")
    def docker(*args,**kwargs):
        actions.append(args[0])
        if args[0]=="ps":return identity
        if args[0]=="inspect":return json.dumps(state)
        if args[0]=="stop":
            if failure!="running":state.update(Running=False,Pid=0,Status="exited")
            return ""
        assert args==("rm",identity)
        assert host.load_receipt(a.root/terminal.PROBE)["retired"] is True
        return ""
    monkeypatch.setattr(host,"docker",docker)
    if failure:
        with pytest.raises(RuntimeError):terminal._retire_previous_probe(a.root,a.worker,a.package)
        assert "rm" not in actions
    else:
        terminal._retire_previous_probe(a.root,a.worker,a.package)
        assert actions[-1]=="rm"


def test_retired_key_inspection_can_admit_another_candidate_without_losing_evidence(tmp_path, monkeypatch):
    original = {"container_id": "a" * 64, "binding": {"project": "qt-test"}}
    old_package = {"schema_version": "qt.storage_online_forward_package.v1", "plan_sha256": "b" * 64,
                   "image": "sha256:" + "c" * 64}
    new_package = {**old_package, "image": "sha256:" + "d" * 64}
    saved = dict(name="qt-test-storage-key-preparation", container_id="e" * 64,
                 retired=True, owner=dict(root=str(tmp_path), worker=original["container_id"], package=old_package))
    path = tmp_path / terminal.KEY_PROBE
    write(path, saved)
    before = path.read_bytes()
    calls = []
    def absent(*args, **kwargs):
        calls.append(args)
        assert args[0] == "ps"
        return ""
    monkeypatch.setattr(host, "docker", absent)
    terminal._retire_previous_probe(tmp_path, original, new_package, keys=True)
    assert path.read_bytes() == before
    retained = list(tmp_path.glob("storage-online-key-preparation-worker.retired-*.json"))
    assert len(retained) == 1 and retained[0].read_bytes() == before
    assert any("id=" + saved["container_id"] in args for args in calls)
    # Interruption before the next probe publishes its receipt is retryable.
    terminal._retire_previous_probe(tmp_path, original, new_package, keys=True)
    assert len(list(tmp_path.glob("storage-online-key-preparation-worker.retired-*.json"))) == 1
    host.save_receipt(path, {**saved, "owner": {**saved["owner"], "package": new_package}}, initial=False)
    assert retained[0].read_bytes() == before and path.read_bytes() != before


@pytest.mark.parametrize("drift", ["worker", "root", "name", "plan", "schema", "unretired", "missing_id",
                                   "key_intent", "same_name_container", "renamed_container", "terminal", "forward"])
def test_key_candidate_change_never_adopts_uncertain_or_foreign_ownership(tmp_path, monkeypatch, drift):
    from scripts.automation.storage_online_keys import STATE
    original = {"container_id": "a" * 64, "binding": {"project": "qt-test"}}
    previous = {"schema_version": "qt.storage_online_forward_package.v1", "plan_sha256": "b" * 64,
                "image": "sha256:" + "c" * 64}
    proposed = {**previous, "image": "sha256:" + "d" * 64}
    saved = dict(name="qt-test-storage-key-preparation", container_id="e" * 64, retired=True,
                 owner=dict(root=str(tmp_path), worker=original["container_id"], package=previous))
    options = {"keys": True}
    name = terminal.KEY_PROBE
    if drift == "worker": saved["owner"]["worker"] = "f" * 64
    elif drift == "root": saved["owner"]["root"] = str(tmp_path / "foreign")
    elif drift == "name": saved["name"] = "foreign"
    elif drift == "plan": proposed["plan_sha256"] = "f" * 64
    elif drift == "schema": previous["schema_version"] = "foreign"
    elif drift == "unretired": saved["retired"] = False
    elif drift == "missing_id": saved["container_id"] = None
    elif drift == "key_intent": write(tmp_path / STATE, {"phase": "prepared"})
    elif drift == "terminal":
        options = {}; name = terminal.PROBE; saved["name"] = "qt-test-storage-terminal"
    elif drift == "forward":
        options = {"forward": True}; name = terminal.FORWARD_PROBE; saved["name"] = "qt-test-storage-forward-terminal"
    path = tmp_path / name
    write(path, saved)
    before = path.read_bytes()
    def docker(*args, **kwargs):
        assert drift in {"same_name_container", "renamed_container"} and args[0] == "ps"
        return saved["container_id"] if drift == "same_name_container" or args[-1].startswith("id=") else ""
    monkeypatch.setattr(host, "docker", docker)
    with pytest.raises(RuntimeError, match="probe_owner_changed|retirement_unproven"):
        terminal._retire_previous_probe(tmp_path, original, proposed, **options)
    assert path.read_bytes() == before
    assert not list(tmp_path.glob("*.retired-*.json"))


@pytest.mark.parametrize("failure", ["after_link", "conflicting_receipt"])
def test_retired_key_receipt_publication_failure_preserves_active_owner(tmp_path, monkeypatch, failure):
    original = {"container_id": "a" * 64, "binding": {"project": "qt-test"}}
    previous = {"schema_version": "qt.storage_online_forward_package.v1", "plan_sha256": "b" * 64,
                "image": "sha256:" + "c" * 64}
    proposed = {**previous, "image": "sha256:" + "d" * 64}
    saved = dict(name="qt-test-storage-key-preparation", container_id="e" * 64, retired=True,
                 owner=dict(root=str(tmp_path), worker=original["container_id"], package=previous))
    path = tmp_path / terminal.KEY_PROBE
    write(path, saved); before = path.read_bytes()
    retained = path.with_name(path.stem + ".retired-" + host.digest(saved) + ".json")
    monkeypatch.setattr(host, "docker", lambda *args, **kw: "")
    if failure == "conflicting_receipt":
        write(retained, {"foreign": True})
        with pytest.raises(RuntimeError, match="retained_receipt_changed"):
            terminal._retire_previous_probe(tmp_path, original, proposed, keys=True)
    else:
        with monkeypatch.context() as patch:
            def interrupted(_): raise TimeoutError("after durable receipt link")
            patch.setattr(host, "sync_directory", interrupted)
            with pytest.raises(TimeoutError):
                terminal._retire_previous_probe(tmp_path, original, proposed, keys=True)
        assert retained.read_bytes() == before
        terminal._retire_previous_probe(tmp_path, original, proposed, keys=True)
    assert path.read_bytes() == before
