"""Host amendment fault injection using real durable files and bounded fake peers."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from types import SimpleNamespace
import time

import pytest

from scripts.automation import storage_online_deadline as deadline
from scripts.automation import storage_online_operation as operation
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_host_boundary as host


def write(path, value):
    path.write_text(json.dumps(value, sort_keys=True)+"\n")
    path.chmod(0o600)


@pytest.fixture
def attempt(tmp_path, monkeypatch):
    root = tmp_path/"state"; root.mkdir()
    source = tmp_path/"source"; source.mkdir()
    history = tmp_path/"history"; history.mkdir()
    started = datetime.fromtimestamp(time.time()-3600, timezone.utc).isoformat()
    request = dict(source_revision="a"*40, policy={"reserve_percent":20})
    old_request = {**request, "capture_preparation":dict(requested_at=time.time()-3602,
        deadline=time.time()-3500, history_before="2026-09-01", attempt_seconds=60*3600)}
    old_deadline = datetime.fromisoformat(started).timestamp()+60*3600
    plan = dict(state_root=str(root), project="qt-test", source_revision="b"*40, source_image="old",
        image="candidate", request=request, inventory_path=str(tmp_path/"inventory"), keys_root=str(tmp_path/"keys"),
        socket_volume="socket", spool_destination=str(tmp_path/"spool"), attempt_seconds=60*3600,
        limits=dict(spool_reserve_bytes=1, recent_free_bytes=1, repository_reserve_bytes=1))
    path = tmp_path/"operation.json"; write(path,plan)
    request_path = root/deadline.REQUEST
    request_path.write_bytes(deadline.request_bytes(old_request));request_path.chmod(0o600)
    worker = dict(container_id="c"*64, contract="contract", deadline=old_deadline,
        capture=dict(started_at=started,seconds=60*3600),binding=dict(request_sha256=deadline._sha(request_path.read_bytes()),
        mounts={"/app/logs/market-structure":{"host_source":str(source)},"/qt-history":{"host_source":str(history)}},
        environment_sha256="old"))
    write(root/launch._STATE,worker)
    capture = dict(id=1, prepared_at=started, attempt_seconds=60*3600, source_oid=1, queue_oid=2,
                   database_oid=3, cluster_id="123")
    data = dict(capture=deepcopy(capture), audit=None)
    calls = []
    def probe(plan, saved, *, action, arguments=None):
        calls.append(action)
        if action == "apply":
            assert data["capture"] == arguments["expected_capture"]
            after = {**data["capture"],"attempt_seconds":arguments["attempt_seconds"]}
            data["audit"] = dict(schema_version="qt.fact_header_deadline_amendment.v1",
                intent_sha256=arguments["intent_sha256"],before=deepcopy(data["capture"]),after=after)
            data["capture"] = after
            return deepcopy(data["audit"])
        return deepcopy(data)
    state = SimpleNamespace(root=root,path=path,plan=plan,worker=worker,capture=capture,data=data,calls=calls,
                            probe=probe,name="/qt-test-storage-online")
    monkeypatch.setattr(operation,"load_operation_plan",lambda p:host.load_receipt(p))
    monkeypatch.setattr(operation,"inspect_prepared_operation",lambda *a,**k:{"checked":True})
    monkeypatch.setattr(launch,"observe_owned_worker",lambda *a:(worker,"qt-test-storage-online",[worker["container_id"]]))
    monkeypatch.setattr(deadline,"_retired",lambda saved:{"config":{"Env":["PG_DSN=private","QT_ONLINE_REQUEST_SHA256=old"]}})
    monkeypatch.setattr(deadline,"_probe",probe)
    def docker(*args, **kwargs):
        if args[0] == "inspect":return state.name
        assert args[0] == "rename" and args[1] == worker["container_id"]
        state.name="/"+args[2]
        return ""
    monkeypatch.setattr(host,"docker",docker)
    capacity_path=tmp_path/"capacity.json"
    value=dict(schema_version="qt.storage_deadline_capacity.v1",plan_sha256=deadline._sha(path.read_bytes()),
        attempt_seconds=96*3600,through_epoch=datetime.fromisoformat(started).timestamp()+96*3600,
        observed_at=time.time(),filesystems=[])
    for p in (source,history):
        space=os.statvfs(p)
        value["filesystems"].append(dict(path=str(p),device=p.stat().st_dev,
            reserve_bytes=(space.f_blocks*space.f_frsize*20+99)//100,
            remaining_peak_bytes={k:1 for k in ("targets","growth","queue","wal","temporary","maintenance","recovery")},
            evidence_sha256="d"*64))
    write(capacity_path,value);state.capacity_path=capacity_path
    state.run=lambda execute=True:deadline.amend_operation(path,attempt_seconds=96*3600,capacity_file=capacity_path,execute=execute)
    return state


def assert_complete(attempt):
    saved=host.load_receipt(attempt.root/launch._STATE)
    request=host.load_receipt(attempt.root/deadline.REQUEST)
    plan=host.load_receipt(attempt.path)
    journal=host.load_receipt(attempt.root/deadline.STATE)
    assert journal["phase"] == "complete"
    assert saved["capture"] == {**attempt.worker["capture"],"seconds":96*3600}
    assert saved["deadline"] == attempt.worker["deadline"]+36*3600
    assert saved["container_id"] is None and saved["contract"] is None
    assert saved["binding"]["request_sha256"] == deadline._sha(deadline.request_bytes(request))
    assert request["capture_preparation"]["attempt_seconds"] == plan["attempt_seconds"] == 96*3600
    assert {**plan,"attempt_seconds":60*3600} == attempt.plan
    assert journal["old_worker"] == attempt.worker
    assert json.loads(journal["old_plan_bytes"]) == attempt.plan
    assert attempt.calls.count("apply") == 1
    deadline.require_settled(attempt.root)


def test_inspection_does_not_mutate_and_explicit_amendment_keeps_original_start(attempt):
    before={p:p.read_bytes() for p in (attempt.path,attempt.root/launch._STATE,attempt.root/deadline.REQUEST)}
    assert attempt.run(False)["storage_mutations_performed"] is False
    assert all(p.read_bytes()==value for p,value in before.items())
    assert not (attempt.root/deadline.STATE).exists()
    assert attempt.run()["migration_started"] is False
    assert_complete(attempt)
    assert attempt.run()["current_capture_verified"] is False
    assert attempt.calls.count("apply") == 1


def test_lost_database_reply_reconciles_exact_audit_without_replay(attempt,monkeypatch):
    def lost(*args,**kwargs):
        result=attempt.probe(*args,**kwargs)
        if kwargs["action"]=="apply":raise TimeoutError("lost committed reply")
        return result
    monkeypatch.setattr(deadline,"_probe",lost)
    with pytest.raises(TimeoutError):attempt.run()
    with pytest.raises(RuntimeError,match="requires_reconciliation"):deadline.require_settled(attempt.root)
    monkeypatch.setattr(deadline,"_probe",attempt.probe)
    attempt.run()
    assert_complete(attempt)


def test_absent_audit_after_uncertain_dispatch_never_replays(attempt,monkeypatch):
    def lost(*args,**kwargs):
        if kwargs["action"]=="apply":
            attempt.calls.append("apply")
            raise TimeoutError("outcome unavailable")
        return attempt.probe(*args,**kwargs)
    monkeypatch.setattr(deadline,"_probe",lost)
    with pytest.raises(TimeoutError):attempt.run()
    monkeypatch.setattr(deadline,"_probe",attempt.probe)
    with pytest.raises(RuntimeError,match="outcome_unresolved_no_replay"):attempt.run()
    assert attempt.calls.count("apply")==1
    assert host.load_receipt(attempt.path)==attempt.plan
    assert host.load_receipt(attempt.root/launch._STATE)==attempt.worker


@pytest.mark.parametrize("boundary",[deadline.REQUEST,launch._STATE,"operation.json"])
def test_interrupted_file_publication_converges_without_database_replay(attempt,monkeypatch,boundary):
    original=deadline._replace
    def interrupted(path,before,after):
        original(path,before,after)
        if path.name==boundary:raise RuntimeError("interrupted publication")
    monkeypatch.setattr(deadline,"_replace",interrupted)
    with pytest.raises(RuntimeError,match="interrupted publication"):attempt.run()
    monkeypatch.setattr(deadline,"_replace",original)
    attempt.run()
    assert_complete(attempt)


def test_foreign_file_edit_is_preserved_and_blocks_reentry(attempt,monkeypatch):
    original=deadline._replace
    def foreign(path,before,after):
        if path.name==deadline.REQUEST:
            write(path,{"foreign":True})
        original(path,before,after)
    monkeypatch.setattr(deadline,"_replace",foreign)
    with pytest.raises(RuntimeError,match="file_changed"):attempt.run()
    assert host.load_receipt(attempt.root/deadline.REQUEST)=={"foreign":True}
    with pytest.raises(RuntimeError,match="requires_reconciliation"):deadline.require_settled(attempt.root)


def test_live_worker_and_final_marker_refuse_before_any_database_action(attempt,monkeypatch):
    def running(saved):raise RuntimeError("worker_must_be_retired")
    monkeypatch.setattr(deadline,"_retired",running)
    with pytest.raises(RuntimeError,match="must_be_retired"):attempt.run()
    assert not attempt.calls
    write(attempt.root/"storage-online-final.json",{"intent":"retained"})
    with pytest.raises(RuntimeError,match="pre_final_serving"):attempt.run()
    assert not attempt.calls


@pytest.mark.parametrize("change,error",[("age","stale_or_unbound"),("horizon","stale_or_unbound"),("reserve","insufficient"),("space","insufficient"),("device","insufficient")])
def test_capacity_cannot_use_stale_shortened_or_reduced_reserve_evidence(attempt,change,error):
    value=host.load_receipt(attempt.capacity_path)
    if change=="age":value["observed_at"]-=601
    if change=="horizon":value["through_epoch"]-=1
    if change=="reserve":value["filesystems"][0]["reserve_bytes"]=1
    if change=="space":value["filesystems"][0]["remaining_peak_bytes"]["targets"]=2**63
    if change=="device":value["filesystems"][0]["device"]+=1
    write(attempt.capacity_path,value)
    with pytest.raises(RuntimeError,match=error):attempt.run()
    assert "apply" not in attempt.calls
    assert not (attempt.root/deadline.STATE).exists()


def test_expired_amendment_journal_never_renews_or_replays(attempt,monkeypatch):
    def lost(*args,**kwargs):
        result=attempt.probe(*args,**kwargs)
        if kwargs["action"]=="apply":raise TimeoutError("lost committed reply")
        return result
    monkeypatch.setattr(deadline,"_probe",lost)
    with pytest.raises(TimeoutError):attempt.run()
    original=host.load_receipt(attempt.root/deadline.STATE)
    now=time.time(); monotonic=time.monotonic()
    monkeypatch.setattr(deadline.time,"time",lambda:now+301)
    monkeypatch.setattr(deadline.time,"monotonic",lambda:monotonic+301)
    monkeypatch.setattr(deadline,"_probe",attempt.probe)
    with pytest.raises(RuntimeError,match="expired_requires_reconciliation"):attempt.run()
    assert host.load_receipt(attempt.root/deadline.STATE)==original
    assert attempt.calls.count("apply")==1


def test_cli_amendment_is_explicit(attempt,monkeypatch):
    from cli import main
    calls=[]
    monkeypatch.setattr(operation,"run_operation_plan",lambda path,**kw:calls.append((path,kw)) or {"phase":"inspected"})
    args=main.build_parser().parse_args(["storage","migrate","--operation-file",str(attempt.path),
        "--extend-attempt-seconds","345600","--capacity-file",str(attempt.capacity_path)])
    assert args.func(args)==0
    assert calls==[(str(attempt.path),dict(execute=False,extend_attempt_seconds=345600,capacity_file=str(attempt.capacity_path)))]


@pytest.mark.parametrize("state",[
    dict(Running=True,Pid=5,Status="running"),
    dict(Running=False,Pid=5,Status="exited"),
    dict(Running=False,Pid=0,Status="exited",OOMKilled=True),
    dict(Running=False,Pid=False,Status="exited"),
])
def test_worker_retirement_requires_daemon_pid_zero_without_oom(monkeypatch,state):
    state={**dict(Paused=False,Restarting=False,Dead=False,OOMKilled=False),**state}
    monkeypatch.setattr(launch,"_admit",lambda *a:None)
    monkeypatch.setattr(host,"docker",lambda *a,**k:json.dumps(state))
    with pytest.raises(RuntimeError,match="worker_must_be_retired"):
        deadline._retired(dict(container_id="a"*64,contract="bound",binding={}))


@pytest.mark.parametrize("entry",["operation","worker"])
def test_partial_amendment_blocks_real_entry_before_peer_actions(attempt,monkeypatch,entry):
    write(attempt.root/deadline.STATE,{"phase":"database_dispatched"})
    monkeypatch.setattr(operation,"OperationLimits",lambda **kw:None)
    def forbidden(*args,**kwargs):
        raise AssertionError("partial amendment reached a peer")
    monkeypatch.setattr(host,"docker",forbidden)
    monkeypatch.setattr(operation,"inspect_prepared_operation",forbidden)
    with pytest.raises(RuntimeError,match="deadline_amendment_requires_reconciliation"):
        if entry=="operation":
            operation.run_operation_plan(attempt.path,execute=True)
        else:
            with launch.launched_online_worker(attempt.root,project="qt-test",source_revision="a"*40,
                    image="candidate",request={},inventory_path=attempt.root,descriptor_limit=2048,memory_bytes=1024**3):
                raise AssertionError("partial amendment launched a worker")


def test_wall_clock_jump_during_preflight_prevents_dispatch(attempt,monkeypatch):
    original=deadline._capacity
    wall_now=time.time()
    def capacity_then_jump(*args,**kwargs):
        result=original(*args,**kwargs)
        monkeypatch.setattr(deadline.time,"time",lambda:wall_now+301)
        return result
    monkeypatch.setattr(deadline,"_capacity",capacity_then_jump)
    with pytest.raises(RuntimeError,match="expired_requires_reconciliation"):
        attempt.run()
    assert "apply" not in attempt.calls
    assert not (attempt.root/deadline.STATE).exists()


def test_wall_clock_jump_after_commit_preserves_uncertain_journal(attempt,monkeypatch):
    wall_now=time.time()
    def committed_then_jump(*args,**kwargs):
        result=attempt.probe(*args,**kwargs)
        if kwargs["action"]=="apply":
            monkeypatch.setattr(deadline.time,"time",lambda:wall_now+301)
        return result
    monkeypatch.setattr(deadline,"_probe",committed_then_jump)
    with pytest.raises(RuntimeError,match="expired_requires_reconciliation"):
        attempt.run()
    assert attempt.calls.count("apply")==1
    assert host.load_receipt(attempt.root/deadline.STATE)["phase"]=="database_dispatched"
    assert host.load_receipt(attempt.path)==attempt.plan
    assert host.load_receipt(attempt.root/launch._STATE)==attempt.worker
