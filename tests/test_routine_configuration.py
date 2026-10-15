from __future__ import annotations
import copy,json,sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts/automation'))
import routine_configuration as config

@pytest.fixture
def deployment(tmp_path,monkeypatch):
    env=tmp_path/'operator.env';env.write_text('PRIVATE_CREDENTIAL=approved\n');env.chmod(0o600)
    source='a'*40;tree='b'*64
    model={'name':'owned','services':{'backend':{'image':'quanttrad-backend:'+source,
        'build':{'args':{'QT_SOURCE_REVISION':source}},'ports':[{'host_ip':'127.0.0.1','published':'8000','target':8000}],
        'environment':{'SOURCE_REVISION':source,'SOURCE_TREE_HASH':tree,'QT_BOT_RUNTIME_IMAGE':'quanttrad-backend:'+source,'PRIVATE_CREDENTIAL':'approved'}}}}
    runtime={'Id':'container','Image':'sha256:old','Config':{'Env':['PRIVATE_CREDENTIAL=approved']},'HostConfig':{'RestartPolicy':{'Name':'unless-stopped'}},'Mounts':[]}
    def output(*args):return 'container' if 'ps' in args else json.dumps([runtime])
    monkeypatch.setattr(config,'output',output)
    identity=config.configuration_identity(model,source,tree,env)
    baseline={'schema_version':'qt.routine_configuration.v1','revision':source,**identity}
    return model,env,source,tree,runtime,baseline


def test_qualified_source_identity_may_change_without_configuration_drift(deployment):
    model,env,previous,tree,runtime,baseline=deployment
    candidate='c'*40;new_tree='d'*64
    service=model['services']['backend'];service['image']='quanttrad-backend:'+candidate
    service['build']['args']['QT_SOURCE_REVISION']=candidate
    service['environment'].update(SOURCE_REVISION=candidate,SOURCE_TREE_HASH=new_tree,QT_BOT_RUNTIME_IMAGE='quanttrad-backend:'+candidate)
    config.admit(baseline,config.configuration_identity(model,candidate,new_tree,env),previous)


@pytest.mark.parametrize('change',['private_file','caller_port','caller_credential','runtime_restart','runtime_credential','runtime_replacement'])
def test_unreviewed_settings_or_running_runtime_drift_refuse(deployment,change):
    model,env,previous,tree,runtime,baseline=deployment
    if change=='private_file':env.write_text('PRIVATE_CREDENTIAL=edited\n')
    if change=='caller_port':model['services']['backend']['ports'][0]['host_ip']='0.0.0.0'
    if change=='caller_credential':model['services']['backend']['environment']['PRIVATE_CREDENTIAL']='edited'
    if change=='runtime_restart':runtime['HostConfig']['RestartPolicy']['Name']='always'
    if change=='runtime_credential':runtime['Config']['Env']=['PRIVATE_CREDENTIAL=edited']
    if change=='runtime_replacement':runtime['Id']='unknown-container'
    with pytest.raises(ValueError,match='configuration drift'):
        config.admit(baseline,config.configuration_identity(model,previous,tree,env),previous)


def test_mounted_configuration_bytes_are_bound_but_data_can_grow(deployment,tmp_path):
    model,env,previous,tree,runtime,baseline=deployment
    settings=tmp_path/'settings';settings.mkdir();(settings/'mail.yml').write_text('approved')
    data=tmp_path/'data';data.mkdir()
    model['services']['backend']['volumes']=[{'type':'bind','source':str(settings),'target':'/etc/application'},
                                          {'type':'bind','source':str(data),'target':'/app/logs/market-structure'}]
    baseline.update(config.configuration_identity(model,previous,tree,env))
    (data/'new-row').write_text('expected changing data')
    config.admit(baseline,config.configuration_identity(model,previous,tree,env),previous)
    (settings/'mail.yml').write_text('edited')
    with pytest.raises(ValueError,match='configuration drift'):
        config.admit(baseline,config.configuration_identity(model,previous,tree,env),previous)


def test_recovery_keeps_settings_guard_when_candidate_changed_containers(deployment):
    model,env,previous,tree,runtime,baseline=deployment
    runtime['Id']='partially-replaced-candidate'
    config.admit(baseline,config.configuration_identity(model,previous,tree,env,running=False),previous)
    env.write_text('PRIVATE_CREDENTIAL=edited\n')
    with pytest.raises(ValueError,match='configuration drift'):
        config.admit(baseline,config.configuration_identity(model,previous,tree,env,running=False),previous)


def test_missing_or_wrong_release_baseline_refuses(deployment):
    model,env,previous,tree,runtime,baseline=deployment
    for value in ({},{**baseline,'revision':'c'*40}):
        with pytest.raises(ValueError,match='baseline absent'):
            config.admit(value,config.configuration_identity(model,previous,tree,env),previous)


def test_child_configuration_permissions_are_bound(deployment,tmp_path):
    model,env,previous,tree,runtime,baseline=deployment
    settings=tmp_path/'settings';settings.mkdir();key=settings/'recovery-key';key.write_text('private');key.chmod(0o600)
    model['services']['backend']['volumes']=[{'type':'bind','source':str(settings),'target':'/etc/application'}]
    baseline.update(config.configuration_identity(model,previous,tree,env))
    key.chmod(0o644)
    with pytest.raises(ValueError,match='configuration drift'):
        config.admit(baseline,config.configuration_identity(model,previous,tree,env),previous)


@pytest.mark.parametrize('mechanism',['configs','secrets'])
@pytest.mark.parametrize('location',['model','service'])
@pytest.mark.parametrize('running',[False,True])
def test_compose_configs_and_secrets_refuse_before_file_or_runtime_observation(deployment,monkeypatch,mechanism,location,running):
    model,env,previous,tree,runtime,baseline=deployment
    if location=='model':model[mechanism]={'private':{'file':'/must-not-be-read'}}
    else:model['services']['backend'][mechanism]=[{'source':'private','target':'/run/private'}]
    def unexpected(*args,**kwargs):raise AssertionError('unsupported model must refuse before private or Docker observations')
    monkeypatch.setattr(config,'binding_identity',unexpected)
    monkeypatch.setattr(config,'running_identity',unexpected)
    with pytest.raises(ValueError,match='unsupported Compose configs/secrets'):
        config.configuration_identity(model,previous,tree,env,running=running)
