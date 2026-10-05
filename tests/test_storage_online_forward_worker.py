"""Forward worker intent validation; no host or production authority."""
from copy import deepcopy

import pytest

from scripts.automation import storage_online_forward_worker as forward
from scripts.automation import storage_online_worker as worker
from scripts.automation.storage_host_boundary import digest


def _request(original=None):
    value = dict(schema_version="qt.storage_online_worker.v1", source_revision="a"*40,
        source_tree_hash="b"*64, database_identity="fixture/1", source_device=1, source_inode=2,
        expected_started_at=None, policy={}, resource_limits={}, max_page_bytes=1024**2,
        max_objects=100, max_bytes=1024**2, page_rows=128, command_seconds=30)
    intent = dict(schema_version="qt.storage_online_forward_intent.v1",
        original_plan_sha256="c"*64, cancellation_intent_sha256="d"*64,
        original_capture=original or {"attempt_seconds":600}, end_day="2026-10-04",
        candidate_revision=value["source_revision"], candidate_source_hash=value["source_tree_hash"])
    intent["operation_sha256"] = digest(intent)
    value["forward"] = intent
    return value


def test_forward_intent_matches_published_digest_and_keeps_legacy_shape():
    request = _request()
    worker.validate_request_shape(request)
    assert forward.request_binding(request) == request["forward"]
    legacy = deepcopy(request)
    del legacy["forward"]
    worker.validate_request_shape(legacy)
    assert forward.request_binding(legacy) is None


@pytest.mark.parametrize("mutation", [
    lambda r:r.update(forward=None),
    lambda r:r.update(capture_preparation={}),
    lambda r:r.update(source_revision="e"*40),
    lambda r:r["forward"].update(operation_sha256="f"*64),
    lambda r:r["forward"].update(end_day="20261004"),
    lambda r:r["forward"].update(original_capture={}),
    lambda r:r["forward"].update(extra=True),
])
def test_forward_foreign_or_mixed_intent_refuses(mutation):
    request = _request()
    mutation(request)
    with pytest.raises(ValueError):
        forward.request_binding(request)


@pytest.mark.parametrize("field,value", [("key_seconds",3601),("initial_seconds",601),
    ("key_seconds",True),("initial_seconds",29)])
def test_forward_phase_bounds_refuse_before_sql(field, value):
    with pytest.raises(ValueError, match="phase_bounds_invalid"):
        forward.prepare_forward(None, _request(), targets=(), policy=None, limits=None,
            source=None, destination=None, **{field:value})


def test_forward_missing_duration_is_not_replaced_with_a_default():
    request = _request({"id":1})
    with pytest.raises(ValueError, match="adoption_bound_invalid"):
        forward.prepare_forward(None, request, targets=(), policy=None, limits=None,
            source=None, destination=None)


def _successor_request(original=None):
    request = _request(original)
    intent = request["forward"]
    intent.update(schema_version="qt.storage_online_forward_intent.v2",
        predecessor_operation_sha256="e"*64, predecessor_terminal_sha256="f"*64,
        lookup_operation_sha256="1"*64)
    intent["operation_sha256"] = digest({k:v for k,v in intent.items() if k != "operation_sha256"})
    return request


def _rescheduled(request, *, end_day="2026-10-05"):
    return {**deepcopy(request), "source_revision":"2"*40, "source_tree_hash":"3"*64,
        "max_objects":request["max_objects"]+10,
        "forward_reschedule":dict(schema_version="qt.storage_online_forward_reschedule.v1",
            original_request_sha256=digest(request), original_max_objects=request["max_objects"], end_day=end_day)}


def test_reschedule_preserves_hashed_operation_and_has_separate_effective_schedule():
    original = _successor_request()
    request = _rescheduled(original)
    before = deepcopy(request)
    worker.validate_request_shape(request)
    assert forward.original_request(request) == original
    assert forward.request_binding(request) == original["forward"]
    assert forward.execution_intent(request) == {**original["forward"], "end_day":"2026-10-05"}
    old = forward.initialization_binding(original)
    new = forward.initialization_binding(request)
    assert {k:v for k,v in old.items() if k != "request_sha256"} == {
        k:v for k,v in new.items() if k not in {"request_sha256", "reschedule"}}
    assert new["reschedule"] == request["forward_reschedule"]
    assert new["request_sha256"] == digest(request)
    assert request == before


@pytest.mark.parametrize("mutation", [
    lambda r:r.update(forward_reschedule=None),
    lambda r:r["forward_reschedule"].update(extra=True),
    lambda r:r["forward_reschedule"].update(end_day="2026-10-04"),
    lambda r:r["forward_reschedule"].update(end_day="20261005"),
    lambda r:r["forward_reschedule"].update(original_request_sha256="f"*64),
    lambda r:r["forward_reschedule"].update(original_max_objects=True),
    lambda r:r.update(max_objects=99),
    lambda r:r.update(max_objects=1000001),
    lambda r:r.update(max_objects=True),
    lambda r:r.update(source_revision="wrong"),
    lambda r:r.update(source_inode=999),
    lambda r:r.update(max_bytes=999),
    lambda r:r.update(page_rows=1),
    lambda r:r["forward"].update(end_day="2026-10-03"),
])
def test_reschedule_refuses_hidden_scope_changes_or_unbounded_inputs(mutation):
    request = _rescheduled(_successor_request())
    mutation(request)
    with pytest.raises(ValueError):
        worker.validate_request_shape(request)


def test_reschedule_cannot_adopt_legacy_or_another_reschedule():
    with pytest.raises(ValueError, match="successor_required"):
        forward.request_binding(_rescheduled(_request()))
    request = _rescheduled(_rescheduled(_successor_request()), end_day="2026-10-06")
    with pytest.raises(ValueError, match="original_request_changed"):
        forward.request_binding(request)


def test_successor_names_are_explicit_and_legacy_names_are_preserved():
    request = _successor_request()
    worker.validate_request_shape(request)
    intent = forward.request_binding(request)
    name = forward.initialization_relation(intent)
    assert name == "qt_fact_header_forward_v2.initialization_" + intent["operation_sha256"][:48]
    assert len(name.split(".")[1]) <= 63
    assert forward.initialization_relation() == forward.INITIAL
    assert forward.initialization_relation(_request()["forward"]) == forward.INITIAL
    from scripts.automation import storage_online_forward as host
    from scripts.db import fact_header_forward_adoption as adoption
    assert host._observation_names(request)[1] == (name, adoption.successor_schema(intent["operation_sha256"])+".adoption")


@pytest.mark.parametrize("field,value", [
    ("lookup_operation_sha256", "wrong"), ("predecessor_operation_sha256", "1"*64),
    ("predecessor_terminal_sha256", None), ("schema_version", "qt.storage_online_forward_intent.v1"),
    ("schema_version", []),
])
def test_successor_foreign_or_ambiguous_bindings_refuse(field, value):
    request = _successor_request()
    request["forward"][field] = value
    request["forward"]["operation_sha256"] = digest({k:v for k,v in request["forward"].items() if k != "operation_sha256"})
    with pytest.raises(ValueError):
        forward.request_binding(request)
