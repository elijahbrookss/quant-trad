"""Preserving host forward retirement: exact ownership and no uncertain replay."""
from copy import deepcopy
from pathlib import Path

import pytest
from scripts.automation import storage_online_terminal as terminal
from scripts.automation import storage_online_forward as forward
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_operation as operation
from scripts.automation import storage_online_deadline as amendment
from scripts.automation import storage_host_boundary as host
from scripts.automation.storage_online_forward_worker import capture_binding
from tests.test_storage_online_deadline import attempt, package_attempt, write
from tests.test_storage_online_forward import prepared
from tests.test_storage_forward_launch import owned, _rows, _advance


@pytest.fixture
def retiring(owned,monkeypatch):
    a=owned;keys,initial,capture=_rows(a,complete=True);_advance(a,3502)
    forward.admit_startup(a.root,a.intent,a.request,keys=keys,initialization=initial,capture=capture)
    a.current=deepcopy(a.intent["worker"])
    a.current.update(container_id="e"*64,contract={"fixture":"contract"},deadline=a.intent["deadline"])
    forward.save_launched_worker(a.root,a.intent,a.current)
    a.forward_path=Path(a.manifest["forward_plan_path"])
    a.retirement_package=dict(schema_version="qt.storage_online_terminal.v1",plan_sha256=amendment._sha(a.forward_path.read_bytes()),
        image=a.manifest["image"],source_revision=a.manifest["source_revision"],source_tree_hash=a.manifest["source_tree_hash"])
    a.retirement_file=a.path.parent/"forward-retirement.json";write(a.retirement_file,a.retirement_package)
    a.actions=[];a.committed=None
    a.observed=dict(schema_version="qt.storage_forward_terminal.v1",capture=capture_binding(capture),
        request_sha256=host.digest(a.request),retired=False,terminal_sha256=None,
        source_retained=True,partial_copies_retained=True,migration_ready=False,final_switch_authorized=False)
    def probe(root,plan,saved,package,**kw):
        a.actions.append(kw["action"])
        if kw["action"]=="inspect":return deepcopy(a.observed)
        if kw["action"]=="apply":
            state=host.load_receipt(root/terminal.FORWARD_STATE)
            assert state["phase"]=="dispatched" and state["intent_sha256"]==kw["intent_sha256"]
            a.committed={**a.observed,"retired":True,"terminal_sha256":"a"*64}
        return deepcopy(a.committed)
    a.probe=probe
    monkeypatch.setattr(terminal,"_probe",probe)
    monkeypatch.setattr(terminal,"_retire_previous_probe",lambda *a,**k:None)
    monkeypatch.setattr(launch,"observe_owned_worker",lambda *a:(deepcopy(owned.current),None,[owned.current["container_id"]]))
    monkeypatch.setattr(amendment,"_retired",lambda saved:None)
    monkeypatch.setattr(operation,"inspect_prepared_operation",lambda *a,**k:{"verified":True})
    a.retire=lambda execute=True:operation.run_operation_plan(a.forward_path,cancel_attempt_file=a.retirement_file,execute=execute)
    a.preserved={p:p.read_bytes() for p in (a.path,a.root/terminal.STATE,a.forward_path,a.root/forward.STATE,a.root/forward.LAUNCH_STATE,a.root/launch._STATE,a.root/amendment.REQUEST)}
    return a


def test_forward_retirement_inspection_then_complete_preserves_original_owners(retiring):
    a=retiring
    assert not a.retire(False)["storage_mutations_performed"]
    assert not (a.root/terminal.FORWARD_STATE).exists()
    assert a.retire()["phase"]=="forward_retired"
    assert a.retire(False)["phase"]=="forward_retired"
    assert a.actions.count("apply")==1
    assert all(p.read_bytes()==before for p,before in a.preserved.items())


def test_forward_retirement_lost_commit_reconciles_after_reboot_without_replay(retiring,monkeypatch):
    a=retiring
    def lost(*args,**kw):
        result=a.probe(*args,**kw)
        if kw["action"]=="apply":raise TimeoutError("lost SQL COMMIT reply")
        return result
    monkeypatch.setattr(terminal,"_probe",lost)
    with pytest.raises(TimeoutError):a.retire()
    before=host.load_receipt(a.root/terminal.FORWARD_STATE)
    monkeypatch.setattr(terminal,"_probe",a.probe);monkeypatch.setattr(terminal,"_boot",lambda:"new boot")
    _advance(a,400)
    assert a.retire()["phase"]=="forward_retired"
    after=host.load_receipt(a.root/terminal.FORWARD_STATE)
    assert all(after[k]==before[k] for k in ("wall_deadline","monotonic_deadline","boot_id","intent_sha256"))
    assert a.actions.count("apply")==1


def test_forward_unknown_commit_never_dispatches_again(retiring,monkeypatch):
    a=retiring
    def lost(*args,**kw):
        if kw["action"]=="apply":a.actions.append("apply");raise TimeoutError("unknown")
        return a.probe(*args,**kw)
    monkeypatch.setattr(terminal,"_probe",lost)
    with pytest.raises(TimeoutError):a.retire()
    monkeypatch.setattr(terminal,"_probe",a.probe)
    with pytest.raises(RuntimeError,match="unresolved_no_replay"):a.retire()
    assert a.actions.count("apply")==1


@pytest.mark.parametrize("fault",["clock","boot"])
def test_forward_prepared_retirement_keeps_original_dispatch_clock(retiring,monkeypatch,fault):
    a=retiring;save=host.save_receipt
    def fail(path,value,**kw):
        save(path,value,**kw)
        if path.name==terminal.FORWARD_STATE and value["phase"]=="prepared":raise TimeoutError("prepared")
    monkeypatch.setattr(host,"save_receipt",fail)
    with pytest.raises(TimeoutError):a.retire()
    monkeypatch.setattr(host,"save_receipt",save)
    if fault=="boot":monkeypatch.setattr(terminal,"_boot",lambda:"changed")
    else:_advance(a,301)
    with pytest.raises(RuntimeError,match="no_dispatch"):a.retire()
    assert "apply" not in a.actions


@pytest.mark.parametrize("fault",["original","request","launch","worker","publication","final","fleet","running"])
def test_forward_foreign_owner_refuses_before_terminal_sql(retiring,monkeypatch,fault):
    a=retiring
    paths={"original":a.path,"request":a.root/amendment.REQUEST,"launch":a.root/forward.LAUNCH_STATE,
        "worker":a.root/launch._STATE,"publication":a.root/forward.STATE,"final":a.root/"storage-online-final.json"}
    if fault in paths:write(paths[fault],{"foreign":"retain"})
    elif fault=="fleet":monkeypatch.setattr(host,"source_clients_serving",lambda rows:False)
    else:monkeypatch.setattr(amendment,"_retired",lambda saved:(_ for _ in ()).throw(RuntimeError("live worker")))
    with pytest.raises((RuntimeError,KeyError)):a.retire()
    assert not a.actions


@pytest.mark.parametrize("read_only_namespace", [False, True])
def test_retirement_checks_exact_namespace_before_any_dependency_change(monkeypatch, read_only_namespace):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from scripts.db import fact_header_forward_adoption as adoption
    from scripts.db import archive_root_v2_online as archives
    state = dict(operation_sha256="a"*64, terminal=None,
                 binding={"old_headers": {"placement": {"exact": "saved"}}})
    statements = []
    conn = SimpleNamespace(exec_driver_sql=statements.append, scalar=lambda *a: 1)
    monkeypatch.setattr(adoption, "_step", lambda *a: nullcontext())
    monkeypatch.setattr(adoption, "_state", lambda *a: state)
    monkeypatch.setattr(adoption, "_snapshot", lambda *a: state["binding"])
    def operation_schema(actual, operation):
        assert actual is conn and operation == state["operation_sha256"]
        return adoption.keys.SCHEMA
    monkeypatch.setattr(adoption, "operation_schema", operation_schema)
    def verify(actual, saved, *, read_only_namespace=False):
        assert actual is conn and saved == {"exact": "saved"}
        assert read_only_namespace is expected_namespace
        raise RuntimeError("database target read only")
    expected_namespace = read_only_namespace
    monkeypatch.setattr(archives.archives.physical, "verify", verify)
    with pytest.raises(RuntimeError, match="database target read only"):
        adoption.retire_adoption(conn, operation_sha256=state["operation_sha256"],
                                 read_only_namespace=read_only_namespace)
    assert statements and all(statement.startswith("LOCK TABLE ") for statement in statements)


def test_retirement_rejects_ambiguous_namespace_before_sql():
    from scripts.db import fact_header_forward_adoption as adoption
    with pytest.raises(ValueError, match="namespace_mode_invalid"):
        adoption.retire_adoption(None, operation_sha256="a"*64, read_only_namespace=1)


@pytest.fixture
def recovery_attempt(retiring, monkeypatch):
    a = retiring
    def failed(*args, **kw):
        if kw["action"] == "apply":
            raise RuntimeError("conflicting writer rolled back terminal transaction")
        return a.probe(*args, **kw)
    monkeypatch.setattr(terminal, "_probe", failed)
    with pytest.raises(RuntimeError, match="conflicting writer"):
        a.retire()
    a.previous_bytes = (a.root/terminal.FORWARD_STATE).read_bytes()
    a.previous = host.load_receipt(a.root/terminal.FORWARD_STATE)
    a.recovery_package = {**a.retirement_package, "schema_version": terminal.RECOVERY_SCHEMA,
        "previous_terminal_sha256": amendment._sha(a.previous_bytes)}
    a.recovery_file = a.path.parent/"terminal-recovery-package.json"
    write(a.recovery_file, a.recovery_package)
    a.recovery_actions = []
    def probe(root, plan, saved, package, **kw):
        a.recovery_actions.append(kw["action"])
        if kw["action"] == "inspect":
            return deepcopy(a.committed or a.observed)
        if kw["action"] == "apply":
            journal = host.load_receipt(root/terminal.FORWARD_RECOVERY_STATE)
            assert journal["phase"] == "dispatched"
            assert kw["expected_capture"] == a.previous["original_capture"]
            a.committed = {**a.observed, "retired": True, "terminal_sha256": "c"*64}
        return deepcopy(a.committed)
    a.recovery_probe = probe
    monkeypatch.setattr(terminal, "_probe", probe)
    a.recover = lambda execute=True: operation.run_operation_plan(a.forward_path,
        cancel_attempt_file=a.recovery_file, execute=execute)
    return a


def test_explicit_recovery_preserves_failed_intent_and_uses_separate_clock(recovery_attempt):
    a = recovery_attempt
    _advance(a, 400)
    assert a.recover(False)["phase"] == "forward_retirement_inspected"
    assert not (a.root/terminal.FORWARD_RECOVERY_STATE).exists()
    assert a.recover()["phase"] == "forward_retired"
    assert a.recover(False)["phase"] == "forward_retired"
    assert a.recovery_actions.count("apply") == 1
    assert (a.root/terminal.FORWARD_STATE).read_bytes() == a.previous_bytes
    recovery = host.load_receipt(a.root/terminal.FORWARD_RECOVERY_STATE)
    assert recovery["wall_deadline"] > a.previous["wall_deadline"]
    assert recovery["original_capture"] == a.previous["original_capture"]
    assert all(p.read_bytes() == before for p, before in a.preserved.items())


def test_recovery_lost_commit_reconciles_without_redispatch(recovery_attempt, monkeypatch):
    a = recovery_attempt
    def lost(*args, **kw):
        value = a.recovery_probe(*args, **kw)
        if kw["action"] == "apply":
            raise TimeoutError("lost recovery reply")
        return value
    monkeypatch.setattr(terminal, "_probe", lost)
    with pytest.raises(TimeoutError, match="lost recovery reply"):
        a.recover()
    before = (a.root/terminal.FORWARD_RECOVERY_STATE).read_bytes()
    _advance(a, 400)
    monkeypatch.setattr(terminal, "_probe", a.recovery_probe)
    assert a.recover(False)["phase"] == "forward_retired"
    assert a.recovery_actions.count("apply") == 1
    after = host.load_receipt(a.root/terminal.FORWARD_RECOVERY_STATE)
    import json
    assert after["wall_deadline"] == json.loads(before)["wall_deadline"]
    assert (a.root/terminal.FORWARD_STATE).read_bytes() == a.previous_bytes


@pytest.mark.parametrize("fault", ["hash", "intent", "worker", "completed", "receipt"])
def test_recovery_refuses_changed_or_completed_predecessor(recovery_attempt, fault):
    a = recovery_attempt
    previous = deepcopy(a.previous)
    if fault == "hash":
        a.recovery_package["previous_terminal_sha256"] = "0"*64
    else:
        if fault == "intent": previous["intent_sha256"] = "0"*64
        if fault == "worker": previous["worker"]["container_id"] = "f"*64
        if fault == "completed": previous["phase"] = "complete"
        if fault == "receipt": previous["receipt"] = {"foreign": True}
        write(a.root/terminal.FORWARD_STATE, previous)
        a.recovery_package["previous_terminal_sha256"] = amendment._sha((a.root/terminal.FORWARD_STATE).read_bytes())
    write(a.recovery_file, a.recovery_package)
    with pytest.raises(RuntimeError, match="predecessor_changed"):
        a.recover()
    assert not a.recovery_actions
    assert not (a.root/terminal.FORWARD_RECOVERY_STATE).exists()


def test_recovery_committed_predecessor_is_read_only_no_new_dispatch(recovery_attempt):
    a = recovery_attempt
    a.committed = {**a.observed, "retired": True, "terminal_sha256": "c"*64}
    assert a.recover(False)["already_retired"]
    with pytest.raises(RuntimeError, match="unowned_retired_result"):
        a.recover()
    assert "apply" not in a.recovery_actions
    assert not (a.root/terminal.FORWARD_RECOVERY_STATE).exists()


def test_recovery_refuses_unretired_original_probe(recovery_attempt, monkeypatch):
    a = recovery_attempt
    def live(*args, **kw):
        raise RuntimeError("original terminal worker still alive")
    monkeypatch.setattr(terminal, "_retire_previous_probe", live)
    with pytest.raises(RuntimeError, match="still alive"):
        a.recover()
    assert not a.recovery_actions
