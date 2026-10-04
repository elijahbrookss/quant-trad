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
