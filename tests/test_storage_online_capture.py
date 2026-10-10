"""Original capture and preparation clocks at the host/worker boundary."""
from datetime import datetime, timezone
import pytest
from scripts.automation import storage_online_worker as worker


def request():
    return {"expected_started_at": None, "capture_preparation": {
        "requested_at":1000., "deadline":1500., "history_before":"2026-01-01", "attempt_seconds":96*3600}}


def observed(start=1100., seconds=96*3600):
    return {"started_at":datetime.fromtimestamp(start, timezone.utc).isoformat(), "seconds":seconds}


def test_original_capture_survives_preparation_expiry_without_renewal(monkeypatch):
    monkeypatch.setattr(worker.time, "time", lambda:2000.)
    assert worker.admit_capture(request(), observed()) == 1100+96*3600
    for changed in (observed(999), observed(1501), observed(seconds=96*3600-1)):
        with pytest.raises(RuntimeError, match="original_attempt_changed"):
            worker.admit_capture(request(), changed)
    monkeypatch.setattr(worker.time, "time", lambda:1100+96*3600)
    with pytest.raises(RuntimeError, match="original_attempt_expired"):
        worker.admit_capture(request(), observed())


@pytest.mark.parametrize("field,value", [
    ("deadline",1601.), ("deadline",float("inf")), ("requested_at",float("nan")),
    ("requested_at",True), ("attempt_seconds",96*3600+1), ("attempt_seconds",True),
    ("history_before","20260101"), ("history_before","2026-13-01"), ("extra",1)])
def test_invalid_initial_capture_binding_refuses(field, value):
    value_request=request();value_request["capture_preparation"][field]=value
    with pytest.raises(ValueError):
        worker.capture_preparation(value_request)


def test_prepared_attempt_cannot_silently_change_to_initial_preparation():
    value=request();value["expected_started_at"]=observed()["started_at"]
    with pytest.raises(ValueError, match="capture_preparation_invalid"):
        worker.capture_preparation(value)


@pytest.mark.parametrize("missing", [(), ("portal_storage_targets",), ("portal_storage_policy",)])
def test_preflight_requires_existing_storage_tables_without_schema_mutation(missing):
    class Connection:
        def __init__(self): self.relations=[]
        def scalar(self, statement, parameters):
            assert str(statement)=="SELECT to_regclass(:relation)"
            relation=parameters["relation"]
            self.relations.append(relation)
            return None if relation.removeprefix("public.") in missing else relation
    conn=Connection()
    if missing:
        with pytest.raises(RuntimeError,match="storage_schema_missing: tables="+missing[0]):
            worker.inspect_storage_schema(conn)
    else:
        worker.inspect_storage_schema(conn)
    assert len(conn.relations)==7 and len(set(conn.relations))==7
    assert all(name.startswith("public.portal_storage_") for name in conn.relations)
