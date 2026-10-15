from __future__ import annotations
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest
import yaml

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('routine_rehearsal',ROOT/'scripts/ci/test_server_promotion.py')
rehearsal=importlib.util.module_from_spec(spec)
spec.loader.exec_module(rehearsal)

@pytest.mark.parametrize('changes',[
    {'GITHUB_ACTIONS':'false'}, {'RUNNER_ENVIRONMENT':'self-hosted'},
    {'RUNNER_TEMP':''}, {'DOCKER_HOST':'ssh://production'},
    {'DOCKER_CONTEXT':'production'},
])
def test_routine_rehearsal_rejects_unapproved_routes(changes):
    env={'GITHUB_ACTIONS':'true','RUNNER_ENVIRONMENT':'github-hosted','RUNNER_TEMP':'/tmp/owned',**changes}
    with pytest.raises(RuntimeError):rehearsal.require_hosted_routine_runner(env)

def test_local_routine_entry_refuses_before_docker(monkeypatch):
    monkeypatch.delenv('GITHUB_ACTIONS',raising=False)
    monkeypatch.setattr(sys,'argv',['rehearsal','--routine'])
    def forbidden(*args,**kwargs):raise AssertionError('Docker boundary reached')
    monkeypatch.setattr(rehearsal,'run',forbidden)
    with pytest.raises(RuntimeError,match='GitHub-hosted'):rehearsal.main()

def test_synthetic_package_api_matches_actual_package_verifier(tmp_path,monkeypatch):
    # Exercises the local admission fixture, not actual GitHub release authority.
    sys.path.insert(0,str(ROOT/'scripts/automation'))
    import routine_promotion
    import check_release_ci
    sha='a'*40
    content=b'tiny image archive fixture'
    manifest={'schema_version':'qt.release_package.v1','revision':sha,'source_tree_hash':'b'*64,
              'run_id':1,'attempt':1,'archive_sha256':hashlib.sha256(content).hexdigest(),
              'images':{name:{'tag':f'quanttrad-{name}:{sha}','id':'sha256:'+'c'*64} for name in ('backend','frontend','frontend-v2')}}
    package=tmp_path/'package.zip'
    with zipfile.ZipFile(package,'w') as bundle:
        bundle.writestr('manifest.json',json.dumps(manifest));bundle.writestr('images.tar',content)
    fake=tmp_path/'gh';fake.write_text(rehearsal.GITHUB_API_FIXTURE)
    def api(path):
        r=subprocess.run([sys.executable,str(fake),'api',path],capture_output=True,text=True,check=True,timeout=10,
                         env={**os.environ,'QT_FIXTURE_CI':'success','QT_FIXTURE_PACKAGE':str(package)})
        return json.loads(r.stdout)
    monkeypatch.setattr(check_release_ci,'api',api)
    monkeypatch.setattr(routine_promotion,'api',api)
    receipt=check_release_ci.qualify(sha)
    assert routine_promotion.verify_package(package,sha,receipt)==manifest

def test_workflow_keeps_generated_environment_private_and_runs_routine_path():
    workflow=yaml.safe_load((ROOT/'.github/workflows/test.yaml').read_text())
    job=workflow['jobs']['deployment-contract']
    setup=next(s for s in job['steps'] if s.get('name')=='Generate disposable operator environment')
    path=setup['env']['QT_SINGLE_NODE_ENV_FILE']
    assert setup['env']['QT_SERVER_ENV_FILE']==path
    assert 'GITHUB_ENV' in setup['run']
    assert all('runner.' not in str(j.get('env',{})) for j in workflow['jobs'].values())
    assert path.startswith('${{ runner.temp }}/') and 'secrets.env' not in path
    commands='\n'.join(s.get('run','') for s in job['steps'])
    assert 'install -d -m 0700' in commands
    assert '--env-file "$QT_SINGLE_NODE_ENV_FILE"' in commands
    assert '--env-file secrets.env' not in commands
    rehearsal_steps=workflow['jobs']['deployment-rehearsal']['steps']
    assert any('--routine --evidence-root' in s.get('run','') for s in rehearsal_steps)
    artifacts=[s for s in rehearsal_steps if s.get('uses')=='actions/upload-artifact@v4']
    assert artifacts and artifacts[0]['with']['path'].endswith('/qualification.json')


def test_cleanup_timeout_does_not_prevent_other_owned_cleanup():
    attempted=[]
    def timeout():
        attempted.append('compose')
        raise subprocess.TimeoutExpired(['docker','compose','down'],90)
    def remove():
        attempted.append('restore')
        return subprocess.CompletedProcess([],0,'','')
    errors=rehearsal.cleanup_owned_operations([
        ('compose',timeout,None),('restore',remove,None),
    ])
    assert attempted==['compose','restore']
    assert errors==['compose: TimeoutExpired']


def test_cleanup_distinguishes_missing_resource_from_daemon_failure():
    def result(message):return lambda: subprocess.CompletedProcess([],1,'',message)
    errors=rehearsal.cleanup_owned_operations([
        ('absent image',result('Error response from daemon: No such image: owned:tag'),'No such image:'),
        ('restore volume',result('Cannot connect to the Docker daemon'),'no such volume'),
        ('later cleanup',result('Error response from daemon: permission denied'),None),
    ])
    assert len(errors)==2
    assert errors[0].startswith('restore volume: Cannot connect')
    assert errors[1].startswith('later cleanup:')
