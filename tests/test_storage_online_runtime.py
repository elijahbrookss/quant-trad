"""Post-commit preserving spool admission, clocks and private proof binding."""
from copy import deepcopy
import hashlib
import json
import os
import time
from pathlib import Path

import pytest

from scripts.automation import storage_online_runtime as runtime
from scripts.automation import storage_online_final as final
from tests.test_storage_online_recovery import transition


def spool_receipt():
    now=time.time();deadline=time.monotonic()+30
    return dict(phase="recovery_spool_preparing",deadline=now+30,
        repositories=dict(finished_at=now-1),switch=dict(deadline_monotonic=deadline),
        runtime_spool=dict(source="/original",destination="/candidate",source_identity=[1,2],
            destination_identity=[1,3],started_at=now,deadline_monotonic=deadline,
            completed=[],inflight="create",helper_id=None,helper_contract=None,
            helper_retirement=None,report=None,finished_at=None,arguments_sha256="a"*64))


@pytest.mark.parametrize("completed,inflight",[([],"copy"),(["copy"],None),(["create"],"create")])
def test_spool_journal_refuses_skipped_or_replayed_action(completed,inflight):
    value=spool_receipt();value['runtime_spool'].update(completed=completed,inflight=inflight)
    with pytest.raises(RuntimeError,match="journal_invalid"):runtime.validate_spool_journal(value)


def test_spool_completion_cannot_extend_deadline_or_skip_retirement():
    saved=spool_receipt();runtime.validate_spool_journal(saved)
    saved['phase']='recovery_spool_ready'
    with pytest.raises(RuntimeError,match="completion_invalid"):runtime.validate_spool_journal(saved)
    value=saved['runtime_spool']
    value.update(completed=['create','copy'],inflight=None,helper_id='b'*64,
        helper_contract='c'*64,helper_retirement='d'*64,report={},finished_at=time.time())
    runtime.validate_spool_journal(saved)
    value['deadline_monotonic']+=1
    with pytest.raises(RuntimeError,match="journal_invalid"):runtime.validate_spool_journal(saved)


def test_spool_entry_requires_repository_completion_and_live_source_hold(transition,monkeypatch):
    path,state,rows,original,mounts=transition
    calls=[];monkeypatch.setattr(runtime,'prepare_spool',lambda *a,**kw:calls.append(kw))
    kwargs=dict(worker_process=object(),destination=path/'new',max_bytes=1024,max_entries=10,reserve_bytes=0,max_duration_seconds=5)
    with pytest.raises(RuntimeError,match="live_transition_required"):
        final.prepare_online_runtime_spool_locked(path,**kwargs)
    mounts()
    with pytest.raises(RuntimeError,match="live_transition_required"):
        final.prepare_online_runtime_spool_locked(path,**kwargs)
    assert not calls
    token=final._SOURCE_HOLD.set(None)
    try:
        with pytest.raises(RuntimeError,match="live_source_hold_required"):
            final.prepare_online_runtime_spool_locked(path,**kwargs)
    finally:final._SOURCE_HOLD.reset(token)


def test_copy_manifest_is_bound_private_and_resists_substitution(tmp_path,monkeypatch):
    # Native test proves UID1000. Unit input models that owner only when CI differs.
    path=tmp_path/'.qt-recovery-copy.json';raw=json.dumps({'copied_files':[{'path':'spool/a.sealed','sha256':'a'*64}]}).encode()
    path.write_bytes(raw);path.chmod(0o600);digest=hashlib.sha256(raw).hexdigest()
    if os.getuid()!=1000:
        with pytest.raises(RuntimeError,match='manifest_invalid'):
            runtime._copy_report(tmp_path,digest)
        return  # Exact wrong owner refuses; native UID1000 proof covers success.
    assert runtime._copy_report(tmp_path,digest)==json.loads(raw)
    path.write_bytes(raw+b' ')
    with pytest.raises(RuntimeError,match="manifest_changed"):runtime._copy_report(tmp_path,digest)
    path.write_bytes(raw);path.chmod(0o644)
    with pytest.raises(RuntimeError,match="manifest_invalid"):runtime._copy_report(tmp_path,digest)
    path.unlink();path.symlink_to(tmp_path/'other')
    with pytest.raises(OSError):runtime._copy_report(tmp_path,digest)


def test_spool_helper_checks_space_before_opening_source(monkeypatch):
    from types import SimpleNamespace
    from scripts.automation import storage_online_drain
    called=[]
    def copy(*args,check,**kwargs):
        check()
        called.append(True)
        raise AssertionError('copy proceeded below the preserved reserve')
    monkeypatch.setattr(storage_online_drain,'prepare_recovery_spool',copy)
    monkeypatch.setattr(os,'statvfs',lambda _:SimpleNamespace(f_bavail=1,f_frsize=1))
    monkeypatch.setattr(__import__('sys'),'argv',['python',json.dumps(dict(deadline=time.monotonic()+30,
        max_entries=10,max_bytes=100,reserve_bytes=2))])
    with pytest.raises(RuntimeError,match='reserve_exhausted'):
        exec(runtime._COPY,{})
    assert not called


@pytest.mark.parametrize("phase",["committed","recovery_wal_ready","recovery_runtime_starting","recovery_runtime_ready"])
def test_runtime_entry_refuses_wrong_phase_without_dispatch(tmp_path,monkeypatch,phase):
    saved=dict(phase=phase,binding={},commit=dict(source_image="image"))
    monkeypatch.setattr(final,"_load",lambda _:saved)
    calls=[];monkeypatch.setattr(runtime,"activate_runtime",lambda *a,**kw:calls.append(kw))
    token=final._SOURCE_HOLD.set((tmp_path,{},"image",lambda:None))
    try:
        with pytest.raises(RuntimeError,match="runtime_live_transition_required"):
            final.activate_online_runtime_locked(tmp_path,worker_process=object(),max_duration_seconds=30)
    finally:final._SOURCE_HOLD.reset(token)
    assert not calls


def test_runtime_journal_never_accepts_reordered_dispatch_or_unconfirmed_completion():
    now=time.time();deadline=time.monotonic()+30
    saved=dict(phase="recovery_runtime_starting",deadline=now+30,switch=dict(deadline_monotonic=deadline),
        runtime_spool=dict(finished_at=now-1),runtime=dict(started_at=now,deadline_monotonic=deadline,
        completed=[],inflight="remove:initialize",finished_at=None,admission={},compose_hashes={},candidate_ids={}))
    runtime.validate_runtime_journal(saved)
    saved['runtime']['inflight']='start:market-data-collector'
    with pytest.raises(RuntimeError,match='journal_invalid'):runtime.validate_runtime_journal(saved)
    saved['runtime']['inflight']=None;saved['phase']='recovery_runtime_ready'
    with pytest.raises(RuntimeError,match='completion_invalid'):runtime.validate_runtime_journal(saved)
    saved['runtime']['deadline_monotonic']+=1
    with pytest.raises(RuntimeError,match='journal_invalid'):runtime.validate_runtime_journal(saved)


def test_runtime_inventory_extension_requires_explicit_activation():
    with pytest.raises(ValueError,match='invalid_client_transition'):
        runtime.host.inventory('fixture',runtime_maintenance=True)
    with pytest.raises(ValueError,match='invalid_client_transition'):
        runtime.host.inventory('fixture',activating=True,removing_client_id='a'*64)


@pytest.fixture
def split_recipe(tmp_path,monkeypatch):
    import stat
    h=runtime.host
    image="sha256:"+"a"*64
    root=tmp_path/'hdd';root.mkdir();archive=root/'archives';archive.mkdir()
    real_stat=Path.stat
    def fixture_stat(path,*args,**kwargs):
        value=real_stat(path,*args,**kwargs)
        if path==archive:
            fields=list(value);fields[0]=stat.S_IFDIR|0o2770;fields[5]=70
            return os.stat_result(fields)
        return value
    monkeypatch.setattr(Path,'stat',fixture_stat)
    inventory=tmp_path/'inventory.json'
    targets=[dict(medium='ssd',target_id='ssd',filesystem_uuid='SSD'),dict(medium='hdd',target_id='hdd',filesystem_uuid='HDD')]
    inventory.write_text(json.dumps(dict(targets=targets)))
    limits=tmp_path/'limits.json';limits.write_text('{}')
    def mount(source,target,readonly=False):return dict(type='bind',source=str(source),target=target,read_only=readonly,bind=dict(create_host_path=False))
    mounts=[mount(root,'/qt-history'),dict(type='volume',source='postgres-data',target='/var/lib/postgresql/data'),
        mount(tmp_path/'keys','/run/quanttrad/recovery',True),dict(type='volume',source='storage-recovery-socket',target='/var/run/postgresql')]
    dbmodel=dict(name='fixture',services=dict(tsdb=dict(volumes=mounts)),networks=dict(quanttrad={}),volumes={})
    h.save_receipt(tmp_path/runtime.recovery.RECIPE,dbmodel,initial=True)
    request=dict(archive_shared_group_id=70,source_revision='b'*40,source_tree_hash='c'*64)
    raw=json.dumps(request).encode();(tmp_path/'storage-online-request.json').write_bytes(raw)
    worker=dict(binding=dict(image=image,request_sha256=hashlib.sha256(raw).hexdigest(),inventory_sha256=hashlib.sha256(inventory.read_bytes()).hexdigest(),
        mounts={'/run/qt-online/inventory.json':dict(source=str(inventory)),'/run/qt-online/udev':dict(source=str(tmp_path/'udev'/'data'))}))
    saved=dict(binding=dict(project='fixture'),recovery=dict(recipe_sha256=h.digest(dbmodel),replacement_id='db'),runtime_spool=dict(destination=str(tmp_path/'new')))
    rows={n:dict(id=n) for n in runtime._APPLICATIONS if n!='storage-maintenance'}
    monkeypatch.setattr(h,'database_details',lambda _:dict(mounts=[],config=dict(Env=['PG_DSN=fixture-dsn'])))
    monkeypatch.setattr(runtime.launch,'_dsn',lambda *a:'fixture-dsn')
    monkeypatch.setattr(runtime.preserving,'_validate_runtime_maintenance',lambda *a:None)
    monkeypatch.setattr(h,'docker',lambda *a:json.dumps(dict(Id=image,Config=dict(Env=['QT_IMAGE_SOURCE_REVISION='+request['source_revision'],'QT_IMAGE_SOURCE_TREE_HASH='+request['source_tree_hash']]))))
    model=deepcopy(dbmodel)
    bytarget={m['target']:m for m in mounts}
    for n,module in runtime._APPLICATIONS.items():
        maintenance=n=='storage-maintenance'
        env=dict(PG_DSN='fixture-dsn',QT_DISABLE_DOTENV='1',QT_STORAGE_MAINTENANCE_OWNER='dedicated',QT_ARCHIVE_SHARED_GROUP_ID='70',
            MARKET_STRUCTURE_STORAGE_ROOT='/qt-history/archives',MARKET_STRUCTURE_WORKING_ROOT='/qt-history/archives' if maintenance else '/app/logs/market-structure',
            QT_MARKET_DATA_EXPECTED_UUID='HDD',QT_MARKET_DATA_WORKING_EXPECTED_UUID='HDD' if maintenance else 'SSD',
            QT_STORAGE_INVENTORY_PATH='/run/quanttrad/storage-inventory.json',QT_STORAGE_UDEV_ROOT='/run/qt-host-udev/data')
        entries=[bytarget['/qt-history'],mount(inventory,'/run/quanttrad/storage-inventory.json',True),mount(tmp_path/'udev'/'data','/run/qt-host-udev/data',True)]
        service=dict(image=image,pull_policy='never',user='70:70' if maintenance else '1000:1000',command=['python','-m',module],
            init=True,cap_drop=['ALL'],security_opt=['no-new-privileges:true'],restart='no',group_add=['70'],networks={'quanttrad':{}},environment=env,volumes=entries)
        if maintenance:
            service['pid']='container:db'
            entries.extend(bytarget[k] for k in ('/var/lib/postgresql/data','/run/quanttrad/recovery','/var/run/postgresql'))
            entries.append(mount(limits,'/run/quanttrad/storage-maintenance.json',True))
            env.update(QT_MARKET_DATA_LIFECYCLE_ENABLED='true',QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED='true',QT_MARKET_DATA_LIFECYCLE_CANONICAL_EXECUTION_ENABLED='true',QT_STORAGE_MAINTENANCE_LIMITS_PATH='/run/quanttrad/storage-maintenance.json')
        else:
            entries.append(mount(tmp_path/'new','/app/logs/market-structure'))
            if n=='backend':entries.append(bytarget['/var/lib/postgresql/data']);env['QT_MARKET_DATA_ROOT']=str(root/'archives')
        if n!='initialize':service['healthcheck']=dict(test=runtime._APPLICATION_HEALTH[n])
        model['services'][n]=service
    def admit():
        h.save_receipt(tmp_path/runtime.RUNTIME_RECIPE,model,initial=not (tmp_path/runtime.RUNTIME_RECIPE).exists())
        return runtime.admit_runtime_recipe(tmp_path,saved,worker,{},rows)
    return model,admit


def test_split_recipe_binds_existing_private_spool_and_single_owner(split_recipe):
    model,admit=split_recipe
    admitted,proof=admit()
    assert admitted==model and set(proof['images'])==set(runtime._APPLICATIONS)
    assert len(proof['files'])==2


@pytest.mark.parametrize('fault',['source-fence','collector-pid','collector-uid','maintenance-spool','maintenance-key-write',
    'collector-owner','group','capability','db-definition','foreign-service','foreign-pid-service'])
def test_split_recipe_refuses_ownership_and_mount_regressions(split_recipe,fault):
    model,admit=split_recipe
    collector=model['services']['market-data-collector'];maintenance=model['services']['storage-maintenance']
    if fault=='source-fence':collector['environment']['QT_STORAGE_SOURCE_FENCE_ROOT']=''
    elif fault=='collector-pid':collector['pid']='service:tsdb'
    elif fault=='collector-uid':collector['user']='70:70'
    elif fault=='maintenance-spool':maintenance['volumes'].append(deepcopy(next(m for m in collector['volumes'] if m['target']=='/app/logs/market-structure')))
    elif fault=='maintenance-key-write':next(m for m in maintenance['volumes'] if m['target']=='/run/quanttrad/recovery')['read_only']=False
    elif fault=='collector-owner':collector['environment']['QT_STORAGE_MAINTENANCE_OWNER']='collector'
    elif fault=='group':collector['group_add']=[]
    elif fault=='capability':collector['cap_add']=['DAC_READ_SEARCH']
    elif fault=='db-definition':model['services']['tsdb']['user']='0:0'
    elif fault=='foreign-pid-service':maintenance['pid']='service:other-database'
    else:model['services']['another-maintenance']={}
    with pytest.raises(RuntimeError,match='storage_online_runtime_'):admit()


def test_runtime_health_admission_matches_existing_public_compositions():
    import yaml
    base=yaml.safe_load(Path('docker/docker-compose.server.yml').read_text())
    overlay=yaml.safe_load(Path('docker/docker-compose.storage-server.yml').read_text())
    for name,probe in runtime._APPLICATION_HEALTH.items():
        owner=overlay if name=='storage-maintenance' else base
        assert owner['services'][name]['healthcheck']['test']==probe


@pytest.mark.parametrize('fault',[None,'other-id','running','pid','unclean','ordinary'])
def test_inventory_only_admits_exact_stopped_application_removal(monkeypatch,fault):
    h=runtime.host
    rows=[dict(id='a'*64,image='sha256:'+'c'*64,project='fixture',service='tsdb',oneoff='False',restart='no',
        running=True,restarting=False,paused=False,pid=1,status='running',exit_code=0,oom=False),
        dict(id='b'*64,image='sha256:'+'c'*64,project='fixture',service='backend',oneoff='False',restart='no',
        running=False,restarting=False,paused=False,pid=0,status='removing',exit_code=0,oom=False)]
    if fault=='running':rows[1]['running']=True
    if fault=='pid':rows[1]['pid']=2
    if fault=='unclean':rows[1]['exit_code']=137
    def docker(action,*args):
        return '\n'.join(r['id'] for r in rows) if action=='ps' else '\n'.join(json.dumps(r) for r in rows)
    monkeypatch.setattr(h,'docker',docker)
    kwargs=dict(activating=True,runtime_maintenance=True,removing_client_id='d'*64 if fault=='other-id' else 'b'*64)
    if fault=='ordinary':kwargs={}
    if fault is None:assert h.inventory('fixture',**kwargs)['backend']['status']=='removing'
    else:
        with pytest.raises(RuntimeError):h.inventory('fixture',**kwargs)


def test_prepared_recipe_accepts_only_database_service_pid_reference(split_recipe):
    model,admit=split_recipe
    model['services']['storage-maintenance']['pid']='service:tsdb'
    admitted,_=admit()
    assert admitted['services']['storage-maintenance']['pid']=='service:tsdb'


def test_preflight_and_activation_share_same_recipe_checks(split_recipe,monkeypatch,tmp_path):
    model,admit=split_recipe
    expected,proof=admit()
    worker=runtime.host.load_receipt(tmp_path/runtime.recovery.RECIPE)
    request=json.loads((tmp_path/"storage-online-request.json").read_text())
    rows={n:dict(id=n) for n in runtime._APPLICATIONS if n!="storage-maintenance"}
    args=dict(database_model=worker,image_id="sha256:"+"a"*64,request=request,
        inventory=tmp_path/"inventory.json",udev_root=tmp_path/"udev"/"data",
        destination=str(tmp_path/"new"),rows=rows,database_id="db")
    # This uses no committed receipt or replacement: it cannot supply authority.
    assert runtime.inspect_runtime_configuration(tmp_path,**args)==(expected,proof)
    model["services"]["market-data-collector"]["cap_add"]=["DAC_READ_SEARCH"]
    runtime.host.save_receipt(tmp_path/runtime.RUNTIME_RECIPE,model,initial=False)
    with pytest.raises(RuntimeError,match="application_contract_changed"):
        runtime.inspect_runtime_configuration(tmp_path,**args)


@pytest.mark.parametrize("fault", ["project", "inventory"])
def test_activation_keeps_original_project_and_consumed_inventory_binding(split_recipe,monkeypatch,tmp_path,fault):
    model,admit=split_recipe
    original=runtime.inspect_runtime_configuration
    if fault=="project":
        load=runtime.host.load_receipt
        def changed(path,**kw):
            value=load(path,**kw)
            if path==tmp_path/runtime.recovery.RECIPE:value["name"]="foreign"
            return value
        monkeypatch.setattr(runtime.host,"load_receipt",changed)
    else:
        def changed(*a,**kw):
            result,proof=original(*a,**kw)
            proof["files"][str(kw["inventory"]) ]="f"*64
            return result,proof
        monkeypatch.setattr(runtime,"inspect_runtime_configuration",changed)
    with pytest.raises(RuntimeError,match="fixed_composition_required" if fault=="project" else "inventory_changed"):
        admit()


@pytest.mark.parametrize("phase",["login_closing","commit_dispatching","committed","recovery_runtime_starting"])
def test_completion_never_replays_or_inspects_an_uncertain_action(tmp_path,monkeypatch,phase):
    monkeypatch.setattr(final,"_load",lambda _:dict(phase=phase))
    monkeypatch.setattr(runtime,"inspect_completed_runtime",lambda *a,**kw:pytest.fail("uncertain operation admitted"))
    with pytest.raises(RuntimeError,match="runtime_not_confirmed"):
        final.inspect_runtime_completion_locked(tmp_path)


@pytest.mark.parametrize("fault",[None,"reboot","backwards"])
def test_completion_observation_after_expiry_preserves_original_clocks(tmp_path,monkeypatch,fault):
    saved=dict(phase="recovery_runtime_ready",boot_id="original",started_boot=10,
        deadline=80,deadline_boot=70,runtime=dict(finished_at=75))
    original=deepcopy(saved)
    monkeypatch.setattr(final,"_load",lambda _:saved)
    monkeypatch.setattr(final,"_boot_id",lambda:"different" if fault=="reboot" else "original")
    monkeypatch.setattr(final,"_boot_seconds",lambda:200)
    monkeypatch.setattr(final.time,"time",lambda:74 if fault=="backwards" else 300)
    calls=[]
    def observe(root,**kwargs):
        calls.append(kwargs)
        assert kwargs["saved"]==original
        return dict(ready=True,ordinary_relaunch_authorized=False)
    monkeypatch.setattr(runtime,"inspect_completed_runtime",observe)
    if fault:
        with pytest.raises(RuntimeError,match="boot_changed|clock_moved_backwards"):
            final.inspect_runtime_completion_locked(tmp_path)
        assert not calls
    else:
        assert final.inspect_runtime_completion_locked(tmp_path)["ready"]
        assert len(calls)==1
    assert saved==original


@pytest.mark.parametrize("change",["missing","other_plan","changed_policy","not_ready","source_not_retained"])
def test_published_runtime_certificate_cannot_be_substituted(monkeypatch,change):
    from types import SimpleNamespace
    from scripts.db import fact_header_v2_handoff as handoff
    receipt=dict(schema_version=handoff.RECEIPT_VERSION,initial_policy_required=True,
                 policy_fingerprint="original")
    expected=handoff._policy_plan_id(receipt)
    row=dict(state="ready",evidence=dict(source_retained=True,handoff=receipt))
    if change=="other_plan":expected="handoff-"+"0"*32
    if change=="changed_policy":receipt["policy_fingerprint"]="changed"
    if change=="not_ready":row["state"]="preparing"
    if change=="source_not_retained":row["evidence"]["source_retained"]=False
    class Connection:
        def scalar(self,*a,**kw):return True
        def execute(self,*a,**kw):return self
        def mappings(self):return self
        def one_or_none(self):return None if change=="missing" else row
    monkeypatch.setattr(handoff,"assert_fact_storage_contract",lambda *_:pytest.fail("substituted certificate admitted"))
    with pytest.raises(RuntimeError,match="certificate_changed"):
        handoff._inspect_published_runtime_policy(Connection(),policy=SimpleNamespace(fingerprint="original"),plan_id=expected)
