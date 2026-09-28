"""Fixed operation ownership and uncertainty behavior; real integration is native."""
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
import time

import pytest
from scripts.automation import storage_online_operation as operation


def limits():
    return operation.OperationLimits(30,120,30,60,1024,64,1,1024,1,1)


def test_background_finishes_finite_preparation_before_chasing_live_tail():
    calls=[]
    replies=iter([
        dict(outcome="identity_relocation_required",phase="identity_relocation_required"),
        dict(outcome="raw_relocation_required",phase="raw_relocation_required"),
        dict(outcome="page_budget_reached",phase="catch_up"),
        *[dict(outcome="both_tails_observed_empty",phase="catch_up")]*3])
    family=iter(sorted(operation._FAMILIES));proved=set()
    def exchange(op,**kw):
        calls.append((op,kw))
        if op=="prepare_step":return dict(result=dict(committed=True))
        if op=="sql_copy":return dict(result=next(replies))
        if op=="inspect_references":return dict(result=dict(references=[dict(relation="market.ref")],catalogs=list(operation._CATALOGS),next_after=None))
        if op=="archive_copy":
            name=next(family);proved.add(name)
            return dict(result=dict(family=name,baseline_complete=True))
        if op=="reprove":return dict(reproved_families_at_observation=sorted(proved))
        raise AssertionError(op)
    result=operation.prepare_background(exchange,preparation_seconds=30)
    assert result==dict(reference_count=1,tail_rounds=3,final_switch_authorized=False)
    steps=[kw['step'] for op,kw in calls if op=='prepare_step']
    assert steps==['catalog_history','identity_history','raw_history','identity_capture',
                   'reference_prepare','reference_validate','reference_adopt','catalog_history','catalog_history']
    assert all(kw['max_duration_seconds']==30 for op,kw in calls if op=='prepare_step')


@pytest.mark.parametrize('failure',[None,'commit','retirement','repositories','runtime'])
def test_operation_keeps_holds_and_never_advances_after_uncertainty(monkeypatch,tmp_path,failure):
    events=[];held=set();worker=object()
    @contextmanager
    def lock(path):
        held.add('deployment')
        try:yield
        finally:held.remove('deployment');events.append('deployment_released')
    @contextmanager
    def launch(path,**kw):
        assert held=={'deployment'}
        try:yield worker,dict(container_id='worker',deadline=time.time()+180)
        finally:
            assert held=={'deployment','source'}
            events.append('reader_retired')
            if failure=='retirement':raise RuntimeError('uncertain_retirement')
    @contextmanager
    def source(path,**kw):
        held.add('source')
        try:yield
        finally:held.remove('source');events.append('source_released')
    monkeypatch.setattr(operation,'inspect_prepared_operation',lambda *a,**kw: {})
    monkeypatch.setattr(operation.host,'deployment_lock',lock)
    monkeypatch.setattr(operation.launch,'launched_online_worker_locked',launch)
    monkeypatch.setattr(operation.final,'held_source_writers_locked',source)
    monkeypatch.setattr(operation,'prepare_background',lambda *a,**kw:dict(final_switch_authorized=False))
    def exchange(op,**kw):events.append(op);return {}
    monkeypatch.setattr(operation.host,'OnlineWorkerChannel',lambda *a,**kw:SimpleNamespace(exchange=exchange,greeting={'controller_id':'controller'}))
    monkeypatch.setattr(operation.final,'stop_online_source_locked',lambda *a,**kw:{})
    monkeypatch.setattr(operation.final,'_remaining',lambda _:120)
    for name in ['observe_source_drain_locked','copy_final_delta_locked','record_switch_entry_locked','close_database_logins_locked']:
        monkeypatch.setattr(operation.final,name,lambda *a,**kw:None)
    def stage(label,result):
        def call(*a,**kw):
            assert held=={'deployment','source'}
            if label!='commit':assert 'reader_retired' in events
            events.append(label)
            if failure==label:raise RuntimeError('uncertain_'+label)
            return result
        return call
    monkeypatch.setattr(operation.final,'commit_online_handoff_locked',stage('commit',dict(outcome='committed',initial_policy_activated=True)))
    for name,label in [('prepare_recovery_database_locked','database'),('prepare_online_repositories_locked','repositories'),('prepare_online_runtime_spool_locked','spool'),('activate_online_runtime_locked','runtime')]:
        monkeypatch.setattr(operation.final,name,stage(label,dict(collector_process_healthy=True)))
    operation.host.save_receipt(tmp_path/operation.runtime_owner.RUNTIME_RECIPE,{},initial=True)
    args=dict(project='test',source_revision='a'*40,source_image='sha256:'+'b'*64,image='candidate',
        request={'resource_limits':{'movement_timeout_seconds':180}},inventory_path=tmp_path/'inventory',
        descriptor_limit=1024,memory_bytes=1024**3,limits=limits(),keys_root=tmp_path/'keys',
        socket_volume='socket',spool_destination=tmp_path/'spool')
    if failure:
        with pytest.raises(RuntimeError,match='uncertain_'+failure):operation.run_prepared_operation(tmp_path,**args)
    else:
        result=operation.run_prepared_operation(tmp_path,**args)
        assert result['ordinary_relaunch_authorized'] is False and result['complete_backup_confirmed'] is False
    assert not held
    assert events[-2:]==['source_released','deployment_released']
    expected=['commit','close','reader_retired','database','repositories','spool','runtime']
    if failure:expected=expected[:expected.index('reader_retired' if failure=='retirement' else failure)+1]
    if failure=='commit':expected+=['reader_retired']
    assert events[:-2]==expected


def test_invalid_limits_refuse_before_host_dispatch(monkeypatch,tmp_path):
    monkeypatch.setattr(operation.host,'deployment_lock',lambda _:pytest.fail('host lock entered'))
    with pytest.raises(ValueError,match='limits_invalid'):
        operation.run_prepared_operation(tmp_path,project='x',source_revision='x',source_image='x',image='x',
            request={'resource_limits':{'movement_timeout_seconds':120}},inventory_path=tmp_path,
            descriptor_limit=1024,memory_bytes=1024**3,limits=replace(limits(),spool_max_entries=4097),
            keys_root=tmp_path,socket_volume='x',spool_destination=tmp_path)


@pytest.mark.parametrize("failure", ["initial", "changed", "expired"])
def test_preflight_refusal_never_pauses_source(monkeypatch,tmp_path,failure):
    events=[]
    @contextmanager
    def lock(*a,**kw):
        yield
    @contextmanager
    def launch(*a,**kw):
        events.append("launch")
        try:yield object(),dict(container_id="worker",deadline=time.time()+180)
        finally:events.append("retired")
    def inspect(*a,**kw):
        events.append("inspect")
        if failure=="initial" or failure=="expired" and "launch" in events:
            raise RuntimeError("preflight_refused")
        return {"recipe": "changed" if "launch" in events else "original"}
    monkeypatch.setattr(operation.host,"deployment_lock",lock)
    monkeypatch.setattr(operation.launch,"launched_online_worker_locked",launch)
    monkeypatch.setattr(operation,"inspect_prepared_operation",inspect)
    monkeypatch.setattr(operation,"prepare_background",lambda *a,**kw: events.append("background"))
    monkeypatch.setattr(operation.host,"OnlineWorkerChannel",lambda *a,**kw:SimpleNamespace(exchange=lambda *a,**kw:None))
    monkeypatch.setattr(operation.final,"stop_online_source_locked",lambda *a,**kw:pytest.fail("source paused"))
    with pytest.raises(RuntimeError,match="preflight"):
        operation.run_prepared_operation(tmp_path,project="test",source_revision="a"*40,
            source_image="sha256:"+"b"*64,image="candidate",request={"resource_limits":{"movement_timeout_seconds":180}},
            inventory_path=tmp_path/"inventory",descriptor_limit=1024,memory_bytes=1024**3,
            limits=limits(),keys_root=tmp_path/"keys",socket_volume="socket",spool_destination=tmp_path/"spool")
    assert events==(["inspect"] if failure=="initial" else ["inspect","launch","background","inspect","retired"])


@pytest.mark.parametrize("journal_id", [None, "a"*64, "b"*64])
def test_preflight_preserves_named_background_worker_admission(monkeypatch,tmp_path,journal_id):
    found="a"*64
    if journal_id is not None:
        operation.host.save_receipt(tmp_path/operation.launch._STATE,{"container_id":journal_id},initial=True)
    monkeypatch.setattr(operation.host,"docker",lambda *a,**kw:found)
    seen=[]
    def source(*a,**kw):
        seen.append(kw["operator_id"])
        raise RuntimeError("serving_boundary_reached")
    monkeypatch.setattr(operation.initial,"admit_serving_source",source)
    with pytest.raises(RuntimeError,match="serving_boundary_reached" if journal_id==found else "unowned_container"):
        operation.inspect_prepared_operation(tmp_path,project="fixture",source_revision="r",
            source_image="source",image="candidate",request={},inventory_path=tmp_path,
            keys_root=tmp_path,socket_volume="socket",spool_destination=tmp_path,deadline=time.monotonic()+30)
    assert seen==([found] if journal_id==found else [])


@pytest.fixture
def operation_file(tmp_path):
    request = dict(schema_version="qt.storage_online_worker.v1", source_revision="c"*40,
        source_tree_hash="d"*64, database_identity="123/456", source_device=1, source_inode=2,
        expected_started_at=None, policy={}, resource_limits={"movement_timeout_seconds":180},
        max_page_bytes=1024, max_objects=128, max_bytes=1024, page_rows=2, command_seconds=30)
    for name in ("keys", "spool"):
        (tmp_path/name).mkdir()
    (tmp_path/"inventory").write_text("fixture")
    plan = dict(schema_version="qt.storage_online_operation.v1", state_root=str(tmp_path),
        project="fixture", source_revision="a"*40, source_image="sha256:"+"b"*64,
        image="sha256:"+"e"*64, history_uuid="uuid-hdd", history_before="2026-01-01",
        attempt_seconds=180, request=request, inventory_path=str(tmp_path/"inventory"),
        descriptor_limit=1024, memory_bytes=1024**3, limits=vars(limits()),
        keys_root=str(tmp_path/"keys"), socket_volume="fixture-socket", spool_destination=str(tmp_path/"spool"))
    path=tmp_path/"operation.json"
    operation.host.save_receipt(path,plan,initial=True)
    return path,plan


@pytest.mark.parametrize("fault", ["clock", "capture", "deadline", "schema", "fields", "permissions", "symlink", "inventory_alias"])
def test_private_plan_rejects_invented_clocks_or_ambiguous_inputs(operation_file,fault):
    import json
    path,plan=operation_file
    if fault=="clock": plan["request"]["expected_started_at"]="2026-01-01T00:00:00+00:00"
    elif fault=="capture": plan["request"]["capture_preparation"]=None
    elif fault=="deadline": plan["attempt_seconds"]=96*3600+1
    elif fault=="schema": plan["schema_version"]="unknown"
    elif fault=="fields": plan["replay"]=True
    elif fault=="inventory_alias": plan["inventory_path"]+="/../inventory"
    path.write_text(json.dumps(plan))
    if fault=="permissions": path.chmod(0o644)
    elif fault=="symlink":
        target=path.with_name("target.json");path.rename(target);path.symlink_to(target)
    with pytest.raises((ValueError,RuntimeError,OSError)):
        operation.load_operation_plan(path)


@pytest.mark.parametrize("execute", [False,True])
def test_full_operator_admits_before_initial_pause_under_one_lock(monkeypatch,operation_file,execute):
    path,plan=operation_file
    events=[];held=[]
    @contextmanager
    def lock(root):
        assert not held
        held.append(root)
        try: yield
        finally: held.pop()
    def inspect(root,**kw):
        assert held==[root]
        events.append("inspect")
        return {"observed":True}
    preparation=dict(phase="serving",history_uuid="uuid-hdd",started_at=1000.,completed_at=1010.,deadline=1600.)
    def prepare(root,**kw):
        assert held==[root] and events==["inspect"]
        events.append("prepare")
        return preparation
    def run(root,**kw):
        assert held==[root] and events==["inspect","prepare"]
        assert kw["request"]["capture_preparation"]==dict(requested_at=1010.,deadline=1600.,history_before="2026-01-01",attempt_seconds=180)
        events.append("driver")
        return {"ordinary_relaunch_authorized":False,"complete_backup_confirmed":False}
    monkeypatch.setattr(operation.host,"deployment_lock",lock)
    monkeypatch.setattr(operation,"inspect_initial_operation",inspect)
    monkeypatch.setattr(operation.initial,"prepare_online_source_locked",prepare)
    monkeypatch.setattr(operation,"run_prepared_operation_locked",run)
    result=operation.run_operation_plan(path,execute=execute)
    assert events==(["inspect","prepare","driver"] if execute else ["inspect"])
    assert result["phase"]==("runtime_ready" if execute else "inspected")
    assert operation.load_operation_plan(path)==plan and not held


@pytest.mark.parametrize("failure", ["preflight", "plan_changed", "initial_uncertain", "final_intent"])
def test_full_operator_never_advances_or_replays_uncertain_initial_work(monkeypatch,operation_file,failure):
    path,plan=operation_file
    calls=[]
    def inspect(root,**kw):
        calls.append("inspect")
        if failure=="preflight":raise RuntimeError("preflight_refused")
        if failure=="plan_changed":
            changed=dict(plan,attempt_seconds=179)
            operation.host.save_receipt(path,changed,initial=False)
        return {}
    def prepare(root,**kw):
        calls.append("prepare")
        raise RuntimeError("initial_uncertain")
    monkeypatch.setattr(operation,"inspect_initial_operation",inspect)
    monkeypatch.setattr(operation.initial,"prepare_online_source_locked",prepare)
    monkeypatch.setattr(operation,"run_prepared_operation_locked",lambda *a,**kw:pytest.fail("driver dispatched"))
    if failure=="final_intent":(path.parent/"storage-online-final.json").write_text("unreadable retained intent")
    with pytest.raises(RuntimeError):operation.run_operation_plan(path,execute=True)
    assert calls==([] if failure=="final_intent" else ["inspect","prepare"] if failure=="initial_uncertain" else ["inspect"])


def test_full_operator_reentry_preserves_original_preparation_deadline(monkeypatch,operation_file):
    path,plan=operation_file
    (path.parent/operation.initial.STATE).write_text("owned fixture")
    saved=dict(phase="serving",history_uuid=plan["history_uuid"],started_at=1000.,completed_at=1010.,deadline=1600.)
    monkeypatch.setattr(operation.initial,"_load",lambda _:saved)
    monkeypatch.setattr(operation,"inspect_prepared_operation",lambda *a,**kw:{})
    monkeypatch.setattr(operation.initial,"prepare_online_source_locked",lambda *a,**kw:pytest.fail("initial phase restarted"))
    def driver(root,**kw):
        assert kw["request"]["capture_preparation"]["deadline"]==1600.
        assert kw["request"]["capture_preparation"]["requested_at"]==1010.
        raise RuntimeError("existing worker owns expiry admission")
    monkeypatch.setattr(operation,"run_prepared_operation_locked",driver)
    with pytest.raises(RuntimeError,match="existing worker owns"):
        operation.run_operation_plan(path,execute=True)


def test_cli_migrate_uses_local_owner_without_api_client(monkeypatch,operation_file,capsys):
    from cli import main
    path,_=operation_file
    parser=main.build_parser()
    monkeypatch.setattr(main,"_client",lambda _:pytest.fail("HTTP client opened"))
    calls=[]
    def run(path,**kw):calls.append((path,kw));return {"phase":"inspected"}
    monkeypatch.setattr(operation,"run_operation_plan",run)
    for tail in ([],["--execute"]):
        args=parser.parse_args(["storage","migrate","--operation-file",str(path),*tail])
        assert args.func(args)==0
    assert calls==[(str(path),{"execute":False}),(str(path),{"execute":True})]
    assert 'inspected' in capsys.readouterr().out
