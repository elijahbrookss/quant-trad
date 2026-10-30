"""Actual inventory/Docker boundary with a deterministic disposable CLI adapter.

No Docker process runs here. Successful interleavings do not diagnose the prior
hosted failure, whose daemon diagnostics were intentionally not retained.
"""
from copy import deepcopy
import json
import subprocess

import pytest
from scripts.automation import storage_host_boundary as host

OLD = 'a'*64
OPERATOR = 'e'*64


@pytest.fixture
def daemon(monkeypatch):
    registry = {identity:dict(id=identity,image='sha256:'+'d'*64,project='qt-owned',
        service=name,oneoff='False',restart='no',running=False,restarting=False,
        paused=False,pid=0,status='removing' if name=='tsdb' else 'exited',exit_code=0,oom=False)
        for index,name in enumerate((*host.STOP,'tsdb'))
        for identity in [OLD if name=='tsdb' else f'{index+1:064x}']}
    state = dict(case='race',ps=0,inspect=0,calls=[],clock=0.0)
    client = next(identity for identity,row in registry.items() if row['service']=='backend')

    def run(args,**kwargs):
        assert args[0]=='docker'
        action=args[1];state['calls'].append(action)
        assert action in ('ps','inspect'), 'inventory cannot dispatch mutation'
        if action=='ps':
            state['ps']+=1
            listed='\n'.join([*registry,OPERATOR])
            if state['ps']==2 and state['case']!='still_present':
                registry.pop(OLD,None)
                if state['case']=='unrelated_missing':registry.pop(client)
            return subprocess.CompletedProcess(args,0,listed,'')
        state['inspect']+=1
        requested=args[4:]
        assert args[2:4]==['--format',host.INSPECT]
        assert OPERATOR not in requested
        missing=set(requested)-registry.keys()
        rows=[deepcopy(registry[identity]) for identity in requested if identity in registry]
        error='\n'.join('Error: No such object: '+identity for identity in sorted(missing))
        code=1 if missing else 0
        case=state['case']
        if state['inspect']==1 and case!='still_present':
            if case=='daemon_failure':error='Cannot connect to Docker daemon PRIVATE';rows=[]
            if case=='mixed_diagnostics':error+='\nCannot connect PRIVATE'
            if case=='wrong_missing_id':error='Error: No such object: '+'b'*64
            if case=='duplicate_diagnostic':error+='\n'+error
            if case=='short_missing_id':error='Error: No such object: '+OLD[:12]
            if case=='duplicate_row':rows.append(deepcopy(rows[0]))
            if case=='partial_rows':rows.pop()
            if case=='unsafe_partial':rows[0]['paused']=True
            if case=='unknown_partial':rows[0]['service']='unexpected'
            if case=='reappeared':registry[OLD]=dict(rows[0],id=OLD,service='tsdb')
            if case=='new_client':registry['b'*64]=dict(rows[0],id='b'*64,service='new')
            if case=='client_disappears_after_inspect':registry.pop(client)
            if case=='deadline_first':state['clock']=11.0
        elif state['inspect']==2:
            if case=='second_inspect_failure':code=1;error='Cannot connect PRIVATE'
            if case=='second_missing':rows.pop();code=1;error='Error: No such object: '+client
            if case=='unsafe_fresh':rows[0]['restart']='always'
            if case=='deadline_fresh':state['clock']=11.0
        out='\n'.join(json.dumps(row) for row in rows)
        if state['inspect']==1 and case=='malformed_rows':out='{malformed'
        if state['inspect']==1 and case=='oversized_rows':out='x'*524289
        return subprocess.CompletedProcess(args,code,out,error)

    monkeypatch.setattr(host.subprocess,'run',run)
    monkeypatch.setattr(host.time,'monotonic',lambda:state['clock'])
    return registry,state


def observe():
    return host.inventory('qt-owned',database_preparing=True,operator_id=OPERATOR,
                          removing_database_id=OLD)


def test_exact_journaled_disappearance_reobserves_every_survivor_once(daemon,caplog):
    registry,state=daemon
    result=observe()
    assert set(result)==set(host.STOP)
    assert {row['id'] for row in result.values()}==set(registry)
    assert state['calls']==['ps','ps','inspect','ps','ps','inspect']
    assert 'storage_pause_journaled_database_disappearance_observed' in caplog.text
    assert 'PRIVATE' not in caplog.text


@pytest.mark.parametrize('case',['still_present','already_absent'])
def test_existing_removal_observations_need_no_reconciliation(daemon,case,caplog):
    registry,state=daemon;state['case']=case
    if case=='already_absent':registry.pop(OLD)
    result=observe()
    assert ('tsdb' in result)==(case=='still_present')
    assert state['calls']==['ps','ps','inspect']
    assert 'disappearance_observed' not in caplog.text


@pytest.mark.parametrize('case',[
    'unrelated_missing','daemon_failure','mixed_diagnostics','wrong_missing_id',
    'duplicate_diagnostic','short_missing_id','duplicate_row','partial_rows',
    'unsafe_partial','unknown_partial','reappeared','new_client',
    'client_disappears_after_inspect','second_inspect_failure','second_missing',
    'unsafe_fresh','malformed_rows','oversized_rows',
])
def test_uncertain_or_unsafe_inventory_never_reconciles(daemon,case,caplog):
    registry,state=daemon;state['case']=case
    with pytest.raises((RuntimeError,ValueError)) as error:observe()
    assert 'PRIVATE' not in str(error.value) and 'PRIVATE' not in caplog.text
    assert 'disappearance_observed' not in caplog.text
    assert state['inspect']<=2 and state['ps']<=4


@pytest.mark.parametrize('case',['deadline_first','deadline_fresh'])
def test_original_deadline_bounds_initial_and_fresh_observation(daemon,case):
    registry,state=daemon;state['case']=case
    with host.docker_deadline(10.0):
        with pytest.raises(RuntimeError,match='host_deadline_expired'):observe()
    assert state['inspect']<=2 and state['ps']<=4


def test_missing_database_without_journaled_removal_still_refuses(daemon):
    with pytest.raises(RuntimeError):
        host.inventory('qt-owned',database_preparing=True,operator_id=OPERATOR)


@pytest.mark.parametrize('arguments,code,diagnostic',[
    (('inspect','--format','{{json .Config}}',OLD),1,'Error: No such object: '+OLD),
    (('inspect','--format',host.INSPECT,OLD),2,'Error: No such object: '+OLD),
    (('inspect','--format',host.INSPECT,OLD[:12]),1,'Error: No such object: '+OLD[:12]),
    (('inspect','--format',host.INSPECT,OLD),1,'Error: No such object: '+OLD+'\nPRIVATE daemon failure'),
    (('inspect','--format',host.INSPECT,OLD),1,'x'*16385),
])
def test_missing_classification_is_fixed_format_bounded_and_redacted(monkeypatch,arguments,code,diagnostic):
    monkeypatch.setattr(host.subprocess,'run',lambda *a,**kw:subprocess.CompletedProcess(a,code,'',diagnostic))
    with pytest.raises(RuntimeError) as error:host.docker(*arguments)
    assert type(error.value) is RuntimeError
    assert 'PRIVATE' not in str(error.value)
