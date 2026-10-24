"""Run-proven operator gaps: real persisted envelopes and observation-only jobs."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import research as routes
from portal.backend.service.research import operator_reads as ops
from portal.backend.service.research.async_dispatch import research_request_fingerprint, JOB_TYPE_RESEARCH_CHECK_RUN
from tests.test_portal.test_saved_selection_measurements import synthetic, planned
from portal.backend.db.models import ResearchItemRecord


class Rows:
    def __init__(self, rows): self.rows=rows
    def mappings(self): return self
    def all(self): return self.rows


class Session:
    def __init__(self, item=None, owner=False):
        self.record=ResearchItemRecord(**{k:deepcopy(item[k]) for k in
            ('id','kind','status','instrument_id','timeframe','payload')},
            created_at=datetime(2025,1,1),updated_at=datetime(2025,1,1)) if item else None
        self.owner=owner; self.statements=[]
    def get(self, model, key):
        assert model is ResearchItemRecord and key==self.record.id
        return self.record
    def execute(self, statement, *args):
        self.statements.append(str(statement))
        if str(statement).startswith('SET '):return None
        assert statement.compile().params['param_1']==3  # limit+1, not an unbounded scan
        return Rows([{'id':str(i),'status':('failed' if 'request_fingerprint =' in str(statement) else 'running'),
            'request_fingerprint':'saved','attempts':1,'max_attempts':1} for i in range(3)])
    def scalar(self, statement, *args):
        self.statements.append(str(statement))
        return datetime(2026,1,1,tzinfo=timezone.utc) if 'clock_timestamp' in str(statement) else self.owner


def bind(monkeypatch, session):
    @contextmanager
    def factory(): yield session
    monkeypatch.setattr(ops,'db',SimpleNamespace(session=factory))


def test_prepare_uses_real_repository_envelope_and_existing_validation(monkeypatch):
    item,ref,ctx,_=synthetic(); before=deepcopy(item)
    session=Session(item);bind(monkeypatch,session)
    prepared=ops.prepare_saved_selection_measurement_request(item['id'],expected_hashes={'result_hash':ref['result_hash']})
    assert prepared['selection_reference']==ref and prepared['frozen_context']==ctx
    assert prepared['read_only'] and not prepared['scientific_execution_performed'] and item==before
    definition,request,plan=planned(prepared['request'])
    assert definition.evaluator_version=='18' and request.dataset_id==ctx['dataset_id']
    assert plan.outcome_tail['seconds']==0
    assert all(s.startswith('SET ') for s in session.statements)
    assert session.statements[0]=='SET TRANSACTION READ ONLY'


@pytest.mark.parametrize('change',[
    lambda item:item['payload'].update(replayable=False),
    lambda item:item['payload'].update(observation_eligible=False),
    lambda item:item.update(status='draft'),
    lambda item:item['payload']['evidence'].update(code_revision='changed'),
])
def test_prepare_cannot_bypass_saved_contract(monkeypatch,change):
    item,_,_,_=synthetic();change(item);bind(monkeypatch,Session(item))
    with pytest.raises(ValueError):ops.prepare_saved_selection_measurement_request(item['id'])


def test_prepare_expected_hash_conflict_and_unknown_profile_fail(monkeypatch):
    item,_,_,_=synthetic();bind(monkeypatch,Session(item))
    with pytest.raises(ValueError,match='expected saved hashes differ'):
        ops.prepare_saved_selection_measurement_request(item['id'],expected_hashes={'result_hash':'f'*64})
    with pytest.raises(ValueError,match='profile_unsupported'):
        ops.prepare_saved_selection_measurement_request(item['id'],measurement_profile='13')
    with pytest.raises(ValueError,match='expected_hashes_invalid'):
        ops.prepare_saved_selection_measurement_request(item['id'],expected_hashes={'dataset_id':'replacement'})


@pytest.mark.parametrize('held',[False,True])
def test_snapshot_exact_fingerprint_truncation_and_existing_status(monkeypatch,held):
    session=Session(owner=held);bind(monkeypatch,session)
    status={'schema_version':'research_job_status.v1','job_id':'known','status':'failed','attempts':1,'max_attempts':1}
    monkeypatch.setattr(ops,'get_research_job_status',lambda key:status if key=='known' else None)
    request={'mode':'evidence','scope':{},'detector':{}}
    result=ops.execution_snapshot(request=request,job_id='known',limit=2)
    assert result['research_owner_held'] is held and result['requested_job'] is status
    assert result['request_fingerprint']==research_request_fingerprint(job_type=JOB_TYPE_RESEARCH_CHECK_RUN,request=request)
    assert len(result['matching_jobs'])==2 and result['matching_jobs'][0]['status']=='failed'
    assert result['truncated']=={'global_inflight_jobs':True,'matching_jobs':True}
    assert result['read_only'] and result['admission_granted'] is False
    assert any("mode='ExclusiveLock'" in s and 'pg_locks' in s for s in session.statements)
    assert any('database=(SELECT oid FROM pg_database WHERE datname=current_database())' in s for s in session.statements)
    assert all(not any(word in s.upper() for word in ['INSERT ','UPDATE ','DELETE ','PG_TRY_ADVISORY']) for s in session.statements)


@pytest.mark.parametrize('limit',[0,101,True,'2'])
def test_snapshot_rejects_invalid_bound_before_opening_session(limit):
    with pytest.raises(ValueError,match='limit_invalid'):ops.execution_snapshot(limit=limit)


def test_http_snapshot_uses_dispatch_defaults_and_read_only_routes(monkeypatch):
    app=FastAPI();app.include_router(routes.router,prefix='/api/research');client=TestClient(app)
    seen={}
    def snapshot(**kwargs):seen.update(kwargs);return {'observed_at':'now','admission_granted':False}
    monkeypatch.setattr(ops,'execution_snapshot',snapshot)
    request={'mode':'evidence','scope':{'instrument_id':'x'},'detector':{}}
    response=client.post('/api/research/jobs/snapshot',json={'request':request,'job_id':'known','limit':2})
    assert response.status_code==200 and response.json()['admission_granted'] is False
    assert seen['request']==routes._model_payload(routes.ResearchCheckRunRequest(**request))
    assert seen['job_id']=='known' and seen['limit']==2
    assert client.post('/api/research/jobs/snapshot',json={'limit':True}).status_code==422
    assert client.post('/api/research/jobs/snapshot',json={'sql':'SELECT secret'}).status_code==422
    monkeypatch.setattr(ops,'prepare_saved_selection_measurement_request',lambda *a,**k:{'request':{'saved':True}})
    assert client.post('/api/research/checks/saved-selection/request',json={'source_check_id':'saved'}).json()=={'request':{'saved':True}}


def test_snapshot_without_request_does_not_invent_duplicate_matches(monkeypatch):
    session=Session();bind(monkeypatch,session)
    result=ops.execution_snapshot(limit=2)
    assert result['request_fingerprint'] is None and result['matching_jobs']==[]
    assert result['truncated']['matching_jobs'] is False and result['requested_job'] is None
    assert not any('request_fingerprint =' in s for s in session.statements)


@pytest.mark.parametrize("version", ["12", "14"])
def test_prepare_rejects_retired_selection_without_new_measurement(version, monkeypatch):
    import json
    from pathlib import Path
    frozen = json.loads((Path(__file__).parent / f"fixtures/historical_saved_selection_v{version}.json").read_text())
    before = deepcopy(frozen)
    bind(monkeypatch, Session(frozen["item"]))
    with pytest.raises(ValueError, match="unavailable|eligible"):
        ops.prepare_saved_selection_measurement_request(frozen["item"]["id"])
    assert frozen == before
