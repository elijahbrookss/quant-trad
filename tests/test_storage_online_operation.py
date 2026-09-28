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
