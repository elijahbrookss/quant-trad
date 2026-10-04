"""Preserving recovery phase: durable daemon intent and no uncertain replay."""
from copy import deepcopy
import json
from types import SimpleNamespace
import time

import pytest

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_final as final
from scripts.automation import storage_online_recovery as recovery


@pytest.fixture
def transition(tmp_path, monkeypatch):
    keys=tmp_path/"keys"; keys.mkdir(mode=0o700)
    history=tmp_path/"history"; history.mkdir()
    old,new,reader="a"*64,"b"*64,"c"*64
    image="sha256:"+"d"*64
    rows={n:dict(id=str(i+1)*64,running=False,pid=0,status="exited",exit_code=0)
          for i,n in enumerate(host.STOP)}
    rows["tsdb"]=dict(id=old,running=True,pid=12,status="running",exit_code=0)
    base=dict(services=dict(tsdb=dict(volumes=[])),volumes={})
    prep=dict(recipe_sha256=host.digest(base),history_uuid="owned-hdd",cluster="1234",clients={})
    worker=dict(container_id=reader,binding={},contract="owned",deadline=time.time()+300)
    host.save_receipt(tmp_path/recovery.launch._STATE,worker,initial=True)
    now=time.time(); mono=time.monotonic(); boot=final._boot_seconds()
    binding=dict(worker_id=reader,worker_started_at="original-start",project="qt-owned",
                 worker_sha256=host.digest(worker),preparation_sha256=host.digest(prep))
    saved=dict(schema=final.SCHEMA,phase="committed",binding=binding,
        started_at=now-5,deadline=now+55,boot_id=final._boot_id(),started_boot=boot-5,
        deadline_boot=boot+55,duration_seconds=60,paused_at=now-4,
        switch=dict(entered_at=now-3,deadline_monotonic=mono+50,worker_sequence=1),
        login_gate=dict(database=dict(cluster="1234",oid=123,name="owned",allow_connections=True),
                        requested_at=now-2,closed_at=now-1.5,database_jobs_stopped=True),
        commit=dict(requested_at=now-1,worker_sequence=3,source_image=image,confirmed_at=now,initial_policy_activated=True))
    host.save_receipt(tmp_path/final.STATE,saved,initial=True)
    mounts=[dict(Destination="/var/lib/postgresql/data",Type="volume",Name="owned-pg",RW=True),
            dict(Destination="/qt-history",Type="bind",Source=str(history),RW=True)]
    original=dict(image=image,mounts=mounts,contract="same",networks={})
    candidate=deepcopy(original)
    candidate["mounts"] += [dict(Destination="/run/quanttrad/recovery",Type="bind",Source=str(keys),RW=False,Propagation="rprivate"),
        dict(Destination="/var/run/postgresql",Type="volume",Name="owned-socket",RW=True)]
    state=dict(fault=None,actions=[],reader_running=False,reaped=True)
    monkeypatch.setattr(recovery.initial,"_load",lambda root:prep)
    monkeypatch.setattr(recovery.initial,"_admit_source",lambda *a,**kw:deepcopy(rows))
    monkeypatch.setattr(recovery.initial,"_admit_clients",lambda *a:None)
    monkeypatch.setattr(recovery.initial,"_recipe",lambda *a:host.digest(base))
    monkeypatch.setattr(recovery.preserving,"_database_recipe",lambda *a:(deepcopy(base),str(history)))
    monkeypatch.setattr(recovery.preserving,"_history_filesystem",lambda *a:None)
    monkeypatch.setattr(recovery.launch,"_admit",lambda *a:None)
    monkeypatch.setattr(host,"inventory",lambda *a,**kw:deepcopy(rows))
    monkeypatch.setattr(host,"database_details",lambda identity:deepcopy(original if identity==old else candidate))
    monkeypatch.setattr(host,"database_contract",lambda value:value["contract"])
    monkeypatch.setattr(host,"database_networks",lambda value:{})
    monkeypatch.setattr(host,"same_database_networks",lambda *a:True)
    monkeypatch.setattr(host,"cluster_identifier",lambda *a,**kw:"9999" if state["fault"]=="cluster" else "1234")
    monkeypatch.setattr(final,"_admit_mount_writers",lambda *a,**kw:None)
    def docker(action,*args,**kw):
        if action=="volume":return json.dumps(dict(Name="owned-socket",Driver="local",Options=None,Scope="local"))
        assert action=="inspect" and args[-1]==reader
        return json.dumps(dict(Running=state["reader_running"],Paused=False,Restarting=False,Dead=False,
            Pid=33 if state["reader_running"] else 0,Status="exited",StartedAt="original-start"))
    monkeypatch.setattr(host,"docker",docker)
    def query(identity,sql):
        if sql==final._GATE_OBSERVE:
            return json.dumps({**saved["login_gate"]["database"],"allow_connections":state["fault"]=="gate"})
        return json.dumps(dict(backends=1 if state["fault"]=="backend" else 0,prepared=0))
    monkeypatch.setattr(host,"maintenance_query",query)
    def action(args,*,deadline,check):
        current=final._load(tmp_path/final.STATE)
        name=current["recovery"]["inflight"]
        assert name==recovery._ACTIONS[len(state["actions"])]
        assert deadline<=saved["switch"]["deadline_monotonic"]
        check();state["actions"].append(name)
        if name=="stop":rows["tsdb"].update(running=False,pid=0,status="exited")
        elif name=="remove":rows.pop("tsdb")
        elif name=="create":rows["tsdb"]=dict(id=new,running=False,pid=0,status="created",exit_code=0)
        else:rows["tsdb"].update(running=True,pid=14,status="running")
        if state["fault"]==name:raise EOFError("completed daemon request lost reply")
        check()
    monkeypatch.setattr(host,"supervised_source_action",action)
    process=SimpleNamespace(args=["docker","start","--attach","--interactive",reader],
                            poll=lambda:0 if state["reaped"] else None)
    def source_check():
        final._remaining(final._load(tmp_path/final.STATE))
    token=final._SOURCE_HOLD.set((tmp_path,binding,image,source_check))
    try:
        yield tmp_path,state,rows,saved,lambda:final.prepare_recovery_database_locked(tmp_path,
            worker_process=process,keys_root=keys,socket_volume="owned-socket",max_duration_seconds=30)
    finally:
        final._SOURCE_HOLD.reset(token)


def test_preserving_recovery_retains_clocks_and_refuses_reentry(transition):
    path,state,rows,original,run=transition
    result=run()
    saved=final._load(path/final.STATE)
    assert saved["phase"]=="recovery_database_ready"
    assert state["actions"]==list(recovery._ACTIONS)
    assert all(saved[k]==original[k] for k in ("binding","commit","switch","deadline","deadline_boot","login_gate"))
    assert not result["runtime_activation_authorized"] and not result["collection_resume_authorized"]
    before=(path/final.STATE).read_bytes()
    with pytest.raises(RuntimeError,match="committed_live_hold_required"):run()
    assert before==(path/final.STATE).read_bytes()


@pytest.mark.parametrize("fault",["stop","remove","create","start"])
def test_lost_daemon_reply_retains_inflight_and_never_dispatches_again(transition,fault):
    path,state,rows,original,run=transition
    state["fault"]=fault
    with pytest.raises(EOFError):run()
    saved=final._load(path/final.STATE)
    assert saved["phase"]=="recovery_preparing"
    assert saved["recovery"]["inflight"]==fault
    assert state["actions"]==list(recovery._ACTIONS[:recovery._ACTIONS.index(fault)+1])
    with pytest.raises(RuntimeError,match="committed_live_hold_required"):run()


@pytest.mark.parametrize("fault",["reader","attach","gate","backend","source"])
def test_recovery_refuses_before_any_daemon_mutation(transition,fault):
    path,state,rows,original,run=transition
    if fault=="reader":state["reader_running"]=True
    elif fault=="attach":state["reaped"]=False
    elif fault=="source":rows["backend"]["running"]=True
    else:state["fault"]=fault
    before=(path/final.STATE).read_bytes()
    with pytest.raises(RuntimeError):run()
    assert not state["actions"] and before==(path/final.STATE).read_bytes()
    assert not (path/recovery.RECIPE).exists()


def test_wrong_cluster_after_restart_stays_unresolved(transition):
    path,state,rows,original,run=transition
    state["fault"]="cluster"
    with pytest.raises(RuntimeError,match="cluster_changed"):run()
    saved=final._load(path/final.STATE)
    assert saved["phase"]=="recovery_preparing" and saved["recovery"]["finished_at"] is None
    with pytest.raises(RuntimeError,match="committed_live_hold_required"):run()


@pytest.mark.parametrize("drift",[None,"wrong_id","exit","pid","running","not_preparing"])
def test_inventory_only_admits_exact_journaled_clean_database_removal(monkeypatch,drift):
    identity="a"*64
    rows=[dict(id=identity if name=="tsdb" else str(i+1)*64,image="sha256:"+"d"*64,
        project="qt-owned",service=name,oneoff="False",restart="no",running=False,
        restarting=False,paused=False,pid=0,status="removing" if name=="tsdb" else "exited",
        exit_code=0,oom=False) for i,name in enumerate((*host.STOP,"tsdb"))]
    database=rows[-1]
    if drift=="exit":database["exit_code"]=143
    elif drift=="pid":database["pid"]=1
    elif drift=="running":database["running"]=True
    def docker(action,*args):
        return "\n".join(r["id"] for r in rows) if action=="ps" else "\n".join(json.dumps(r) for r in rows)
    monkeypatch.setattr(host,"docker",docker)
    kwargs=dict(database_preparing=drift!="not_preparing",removing_database_id="b"*64 if drift=="wrong_id" else identity)
    if drift is None:
        assert host.inventory("qt-owned",**kwargs)["tsdb"]["status"]=="removing"
        with pytest.raises(RuntimeError):host.inventory("qt-owned",database_preparing=True)
    else:
        with pytest.raises((RuntimeError,ValueError)):host.inventory("qt-owned",**kwargs)


@pytest.mark.parametrize("value", [None, False, 1])
def test_committed_receipt_requires_confirmed_initial_policy_before_recovery(transition, value):
    path, state, _, saved, run = transition
    if value is None:
        del saved["commit"]["initial_policy_activated"]
    else:
        saved["commit"]["initial_policy_activated"] = value
    host.save_receipt(path/final.STATE, saved, initial=False)
    before = (path/final.STATE).read_bytes()
    with pytest.raises(RuntimeError, match="commit_receipt_invalid"):
        run()
    assert state["actions"] == [] and (path/final.STATE).read_bytes() == before
