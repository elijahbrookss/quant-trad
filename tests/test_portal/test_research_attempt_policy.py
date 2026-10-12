from copy import deepcopy

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import research as controller
from portal.backend.service.async_jobs import EnqueuedJob
from portal.backend.service.research import async_dispatch as service


def test_single_attempt_preserves_scientific_identity_and_reports_actual_claim_count(monkeypatch):
    request = {"mode":"evidence", "scope":{"instrument_id":"test"}, "detector":{}}
    before = deepcopy(request); enqueued=[]
    monkeypatch.setattr(service, "enqueue_or_reuse_job", lambda **kw: (enqueued.append(kw) or EnqueuedJob(id="job-1",status="queued",reused=True)))
    monkeypatch.setattr(service, "get_job", lambda ident: {"id":ident,"status":"running","attempts":1,"max_attempts":1})
    result=service.dispatch_research_check_run(request,max_attempts=1)
    assert request == before and enqueued[0]["payload"]["request"] == before
    assert enqueued[0]["max_attempts"] == 1
    assert result["attempts"] == result["max_attempts"] == 1
    assert result["status"] == "running" and result["reused"] is True
    assert result["request_fingerprint"] == service.research_request_fingerprint(job_type=service.JOB_TYPE_RESEARCH_CHECK_RUN,request=request)


@pytest.mark.parametrize("requested,persisted", [(1,2),(2,1)])
def test_policy_mismatch_reuses_identity_then_fails_without_second_enqueue(monkeypatch,requested,persisted):
    calls=[]
    monkeypatch.setattr(service,"enqueue_or_reuse_job",lambda **kw: (calls.append(kw) or EnqueuedJob(id="existing",status="running",reused=True)))
    monkeypatch.setattr(service,"get_job",lambda ident: {"id":ident,"status":"running","attempts":1,"max_attempts":persisted})
    with pytest.raises(ValueError,match=f"job_id=existing requested={requested} persisted={persisted}"):
        service.dispatch_research_check_run({"mode":"evidence"},max_attempts=requested)
    assert len(calls)==1


@pytest.mark.parametrize("limit", [0,3,True,1.0,"1",None])
def test_invalid_attempt_limit_never_enqueues(monkeypatch,limit):
    monkeypatch.setattr(service,"enqueue_or_reuse_job",lambda **kw: pytest.fail("must reject before enqueue"))
    with pytest.raises(ValueError,match="attempt_policy_invalid"):
        service.dispatch_research_check_run({"mode":"evidence"},max_attempts=limit)


def test_default_retry_budget_remains_two(monkeypatch):
    seen=[]
    monkeypatch.setattr(service,"enqueue_or_reuse_job",lambda **kw: (seen.append(kw) or EnqueuedJob(id="job",status="queued",reused=False)))
    monkeypatch.setattr(service,"get_job",lambda ident: {"id":ident,"status":"queued","attempts":0,"max_attempts":2})
    result=service.dispatch_research_check_run({"mode":"evidence"})
    assert seen[0]["max_attempts"] == result["max_attempts"] == 2


def test_explicit_route_applies_one_attempt_without_altering_scientific_payload(monkeypatch):
    calls=[]
    def dispatch(request,*,max_attempts=2):
        calls.append((request,max_attempts))
        return {"job_id":"job","status":"queued","attempts":0,"max_attempts":max_attempts}
    monkeypatch.setattr(controller.research_async_dispatch,"dispatch_research_check_run",dispatch)
    app=FastAPI(); app.include_router(controller.router,prefix="/api/research")
    client=TestClient(app); request={"mode":"evidence","scope":{},"detector":{}}
    first=client.post("/api/research/jobs/checks/run-once",json=request)
    second=client.post("/api/research/jobs/checks/run",json=request)
    assert first.status_code == second.status_code == 202
    assert calls[0][0] == calls[1][0]
    assert [r[1] for r in calls] == [1,2]


@pytest.mark.parametrize("readback", [None,"error"])
def test_uncertain_enqueue_exposes_job_identity_without_retry(monkeypatch,readback):
    calls=[]
    monkeypatch.setattr(service,"enqueue_or_reuse_job",lambda **kw:(calls.append(kw) or EnqueuedJob(id="reconcile-me",status="queued",reused=False)))
    def get(ident):
        if readback == "error": raise RuntimeError("private database detail")
        return None
    monkeypatch.setattr(service,"get_job",get)
    app=FastAPI(); app.include_router(controller.router,prefix="/api/research")
    response=TestClient(app).post("/api/research/jobs/checks/run-once",json={"mode":"evidence","scope":{},"detector":{}})
    assert response.status_code == 503
    assert "job_id=reconcile-me" in response.json()["detail"]
    assert "private database detail" not in response.text
    assert len(calls)==1
