from __future__ import annotations
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import zipfile
from datetime import datetime,timedelta,timezone
import pytest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts/automation'))
import routine_promotion as routine

def test_routine_scope_is_smaller_than_ci_selection():
    routine.eligible(['docs/index.md',next(iter(routine.PRESENTATION))])
    for path in ['portal/backend/service/research/metrics.py','src/research_science/check.py',
                 'portal/frontend/package-lock.json','portal/frontend/src/api/Client.jsx',
                 'portal/frontend/src/auth/Login.jsx',
                 'portal/frontend/src/components/ChartComponent/CredentialsModal.jsx',
                 'portal/frontend/src/components/ChartComponent/ChartComponent.jsx',
                 'portal/frontend/src/App.jsx','scripts/db/change.sql','docker/compose.yml']:
        with pytest.raises(ValueError):
            routine.eligible(['docs/index.md',path])

def fixture(tmp_path,monkeypatch):
    manifest={'schema_version':'qt.release_package.v1','revision':'a'*40,'source_tree_hash':'b'*64,
              'run_id':10,'attempt':2,'images':{name:{'tag':f'quanttrad-{name}:'+('a'*40),'id':'sha256:'+('c'*64)} for name in ('backend','frontend','frontend-v2')},
              'archive_sha256':hashlib.sha256(b'fixture').hexdigest()}
    package=tmp_path/'package.zip'
    with zipfile.ZipFile(package,'w') as archive:
        archive.writestr('manifest.json',json.dumps(manifest))
        archive.writestr('images.tar',b'fixture')
    item={'name':'routine-release-package','expired':False,'digest':'sha256:'+hashlib.sha256(package.read_bytes()).hexdigest(),
          'created_at':'2026-10-15T01:01:00Z','workflow_run':{'id':10,'head_sha':'a'*40}}
    job={'name':'deployment-contract','started_at':'2026-10-15T01:00:00Z','completed_at':'2026-10-15T01:02:00Z',
         'steps':[{'name':n,'conclusion':'success'} for n in ('Export qualified routine images','Preserve qualified routine images')]}
    monkeypatch.setattr(routine,'api',lambda path:{'total_count':1,'artifacts':[item]} if '/artifacts?' in path else {'jobs':[job]})
    return package,item,job,{'run_id':10,'attempt':2}

def test_package_is_bound_to_full_attempt_and_artifact_bytes(tmp_path,monkeypatch):
    package,item,job,receipt=fixture(tmp_path,monkeypatch)
    assert routine.verify_package(package,'a'*40,receipt)['revision']=='a'*40
    for key,value in [('expired',True),('digest','sha256:'+'0'*64),('created_at','2026-10-14T01:00:00Z')]:
        original=item[key];item[key]=value
        with pytest.raises(ValueError): routine.verify_package(package,'a'*40,receipt)
        item[key]=original
    job['steps'][0]['conclusion']='skipped'
    with pytest.raises(ValueError): routine.verify_package(package,'a'*40,receipt)

def test_private_restore_evidence_is_source_bound_and_expires(tmp_path):
    now=datetime.now(timezone.utc)
    backup=tmp_path/'backup.json';backup.write_text('backup evidence')
    restore=tmp_path/'restore.json';restore.write_text('restoration evidence')
    receipt={'schema_version':'qt.routine_recovery.v1','revision':'a'*40,'restore_result':'passed',
             'verified_at':(now-timedelta(minutes=1)).isoformat(),'expires_at':(now+timedelta(hours=1)).isoformat()}
    for key,path in [('backup_manifest',backup),('restore_receipt',restore)]:
        receipt[key]=str(path);receipt[key+'_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    path=tmp_path/'routine-recovery.json';path.write_text(json.dumps(receipt));path.chmod(0o600)
    routine.recovery_admission(tmp_path,'a'*40)
    with pytest.raises(ValueError): routine.recovery_admission(tmp_path,'b'*40)
    backup.write_text('changed')
    with pytest.raises(ValueError): routine.recovery_admission(tmp_path,'a'*40)

def test_candidate_pins_infrastructure_and_never_builds_or_pulls(monkeypatch):
    monkeypatch.setattr(routine,'output',lambda *args:'container' if 'ps' in args else 'sha256:'+'d'*64)
    manifest={'images':{'backend':{'tag':'quanttrad-backend:candidate','id':'sha256:'+'c'*64}}}
    model={'name':'qt','services':{'backend':{'image':'quanttrad-backend:candidate','build':'fixture'},'tsdb':{'image':'database:mutable','build':'fixture'}}}
    pinned=routine.pin_candidate(model,manifest)
    assert pinned['services']['backend']['image']=='sha256:'+'c'*64
    assert pinned['services']['tsdb']['image']=='sha256:'+'d'*64
    assert all(s['pull_policy']=='never' and 'build' not in s for s in pinned['services'].values())


@pytest.mark.parametrize("fault", ["expired", "cluster", "schema", "public"])
def test_recovery_preflight_refuses_stale_or_different_live_state(tmp_path,fault):
    now=datetime.now(timezone.utc)
    backup=tmp_path/"backup.json";backup.write_text('qualified backup')
    restore=tmp_path/"restore.json";restore.write_text('actual restore proof')
    receipt={"schema_version":"qt.routine_recovery.v1","revision":"a"*40,"restore_result":"passed",
             "verified_at":(now-timedelta(minutes=5)).isoformat(),"expires_at":(now+timedelta(hours=1)).isoformat(),
             "schema_sha256":"d"*64,"cluster_identifier":"123"}
    for key,evidence in [("backup_manifest",backup),("restore_receipt",restore)]:
        receipt[key]=str(evidence);receipt[key+"_sha256"]=hashlib.sha256(evidence.read_bytes()).hexdigest()
    if fault=="expired":receipt["expires_at"]=(now-timedelta(minutes=1)).isoformat()
    path=tmp_path/"routine-recovery.json";path.write_text(json.dumps(receipt));path.chmod(0o644 if fault=="public" else 0o600)
    schema={"schema_version":"qt.routine_schema.v1","schema_sha256":"d"*64,"cluster_identifier":"123"}
    if fault=="cluster":schema["cluster_identifier"]="456"
    if fault=="schema":schema["schema_sha256"]="e"*64
    with pytest.raises(ValueError): routine.recovery_admission(tmp_path,"a"*40,schema)


def test_export_refuses_image_replacement_during_runtime_proofs(tmp_path,monkeypatch):
    import release_package
    manifest={"revision":"a"*40,"images":{"backend":{"id":"sha256:"+"b"*64}}}
    (tmp_path/"manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(release_package,"image_ids",lambda revision:{"backend":{"id":"sha256:"+"c"*64}})
    monkeypatch.setattr(sys,"argv",["release_package","export","--root",str(tmp_path)])
    with pytest.raises(ValueError,match="changed during proofs"):release_package.main()
    assert not (tmp_path/"images.tar").exists()



def test_schema_probe_host_modes_require_no_application_or_database_imports(monkeypatch,capsys):
    import routine_schema_probe as probe
    monkeypatch.setitem(sys.modules,'sqlalchemy',None)
    monkeypatch.setattr(sys,'argv',['probe','--sql'])
    probe.main()
    sql=capsys.readouterr().out
    assert 'REPEATABLE READ READ ONLY' in sql and 'statement_timeout=10000' in sql
    assert 'pg_control_system' in sql
    assert probe.fingerprint([['column','public','table','field','text','false','']],123)['cluster_identifier']=='123'
    with pytest.raises(ValueError):probe.fingerprint([],123)
