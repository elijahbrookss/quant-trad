"""Actual forward clock shape; no source-stop or launch authority."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta

import pytest
from scripts.automation import storage_online_forward as forward
from scripts.automation.storage_host_boundary import digest
from tests.test_storage_online_forward_worker import _request


def _observations():
    request = _request({"attempt_seconds":3600, "prepared_at":"2026-09-01T00:00:00+00:00"})
    intent=request["forward"]
    start=datetime(2026,10,3,10,tzinfo=timezone.utc)
    initial=dict(binding=dict(request_sha256=digest(request), operation_sha256=intent["operation_sha256"],
        cancellation_intent_sha256=intent["cancellation_intent_sha256"], key_seconds=3600,
        initial_seconds=600, attempt_seconds=3600), started_at=start.isoformat(),
        expires_at=(start+timedelta(seconds=600)).isoformat(), duration_seconds=600, complete=True)
    adopted=start+timedelta(seconds=10)
    capture=dict(operation_sha256=intent["operation_sha256"], started_at=adopted.isoformat(),
        expires_at=(adopted+timedelta(seconds=3600)).isoformat(), attempt_seconds=3600,
        placement={"fixture":"retained"}, cancellation={"receipt":dict(schema_version="qt.fact_header_cancel.v2",
            capture=intent["original_capture"], intent_sha256=intent["cancellation_intent_sha256"],
            source_retained=True,partial_copies_retained=True,migration_ready=False,final_switch_authorized=False)})
    return request,initial,capture,(start+timedelta(seconds=700)).timestamp()


def test_forward_observation_uses_actual_adoption_not_expired_capture_or_new_initial_clock():
    request,initial,capture,now=_observations()
    before=deepcopy((request,initial,capture))
    owner,deadline=forward.admit_adoption_observation(request,initialization=initial,capture=capture,now=now)
    assert owner["started_at"]==capture["started_at"] and owner["end_day"]==request["forward"]["end_day"]
    assert deadline==datetime.fromisoformat(capture["expires_at"]).timestamp()
    assert (request,initial,capture)==before


@pytest.mark.parametrize("fault", ["incomplete","initial_duration","initial_expiry","request","operation",
    "cancel","old_capture","preservation","attempt","adoption_expiry","expiry","future","naive","range"])
def test_forward_observation_refuses_changed_proof_or_original_clock(fault):
    request,initial,capture,now=_observations()
    if fault=="incomplete":initial["complete"]=False
    elif fault=="initial_duration":initial["duration_seconds"]=True
    elif fault=="initial_expiry":initial["expires_at"]=capture["expires_at"]
    elif fault=="request":initial["binding"]["request_sha256"]="f"*64
    elif fault=="operation":capture["operation_sha256"]="f"*64
    elif fault=="cancel":capture["cancellation"]["receipt"]["intent_sha256"]="f"*64
    elif fault=="old_capture":capture["cancellation"]["receipt"]["capture"]={}
    elif fault=="preservation":capture["cancellation"]["receipt"]["source_retained"]=False
    elif fault=="attempt":capture["attempt_seconds"]=True
    elif fault=="adoption_expiry":capture["expires_at"]=initial["expires_at"]
    elif fault=="expiry":now=datetime.fromisoformat(capture["expires_at"]).timestamp()
    elif fault=="future":now=datetime.fromisoformat(capture["started_at"]).timestamp()-1
    elif fault=="naive":initial["started_at"]=initial["started_at"][:-6]
    else:initial["started_at"]=capture["expires_at"]
    with pytest.raises((RuntimeError,ValueError)):
        forward.admit_adoption_observation(request,initialization=initial,capture=capture,now=now)


@pytest.mark.parametrize("fault", ["missing", "duplicate", "empty", "foreign_id", "retired"])
def test_host_sql_observation_refuses_absent_ambiguous_or_retired_adoption(monkeypatch, fault):
    request, initial, capture, now = _observations()
    import json
    rows = dict(initialization=[dict(id=1, **initial)], adoption=[dict(id=1, terminal=None, **capture)])
    if fault == "duplicate": rows["adoption"] *= 2
    elif fault == "empty": rows["initialization"] = None
    elif fault == "foreign_id": rows["adoption"][0]["id"] = 2
    elif fault == "retired": rows["adoption"][0]["terminal"] = {"source_authoritative":True}
    values=iter([[True, False] if fault == "missing" else [True, True], rows])
    monkeypatch.setattr(forward.host,"database_query",lambda *args, **kwargs:json.dumps(next(values)))
    with pytest.raises(RuntimeError, match="active_adoption_required"):
        forward.observe_adoption("fixture")


def test_host_adoption_transport_sets_server_read_only_bound_without_changing_default(monkeypatch):
    calls=[]
    monkeypatch.setattr(forward.host,"docker",lambda *args,**kwargs:calls.append((args,kwargs)) or "result")
    forward.host.database_query("fixture","SELECT 1",read_only_seconds=5)
    args,kwargs=calls.pop()
    assert args[:4]==("exec","--env","PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000","fixture")
    assert args[-1]=="SELECT 1" and not kwargs
    forward.host.database_query("fixture","SELECT 1")
    assert calls.pop()[0][:2]==("exec","fixture")
    for invalid in (True,0,31,5.0):
        with pytest.raises(ValueError,match="read_bound_invalid"):
            forward.host.database_query("fixture","SELECT 1",read_only_seconds=invalid)
    assert not calls


def test_forward_initialization_binding_requires_integer_phase_bounds():
    request,initial,capture,now=_observations()
    initial["binding"]["key_seconds"]=3600.0
    with pytest.raises(RuntimeError,match="initialization_binding_changed"):
        forward.admit_adoption_observation(request,initialization=initial,capture=capture,now=now)
