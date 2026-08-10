from __future__ import annotations

from types import SimpleNamespace
import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from portal.backend.controller import market_data as controller


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(controller.router, prefix="/api/market-data")
    return TestClient(app)


def test_collector_snapshot_route_is_bounded_and_observed(monkeypatch) -> None:
    observed = {}

    def fake_snapshot(*, attempt_limit: int):
        observed["attempt_limit"] = attempt_limit
        return {
            "schema_version": "market_collector_snapshot.v1",
            "observed_at": "2026-08-02T12:00:00+00:00",
            "worker_health": {"status": "alive"},
            "workers": [],
            "collectors": [],
        }

    monkeypatch.setattr(
        controller.market_data_collector, "collector_snapshot", fake_snapshot
    )
    response = _client().get(
        "/api/market-data/collectors/snapshot", params={"attempt_limit": 7}
    )

    assert response.status_code == 200
    assert observed["attempt_limit"] == 7
    assert response.json()["worker_health"]["status"] == "alive"


def test_collector_fact_history_route_clamps_window_and_limit(monkeypatch) -> None:
    observed = {}

    def fake_fact_history(*, definition_id: str, hours: int, limit: int):
        observed.update(
            definition_id=definition_id,
            hours=hours,
            limit=limit,
        )
        return {
            "schema_version": "market_collector_fact_history.v1",
            "definition_id": definition_id,
            "facts": [],
        }

    monkeypatch.setattr(
        controller.market_data_collector, "fact_history", fake_fact_history
    )
    response = _client().get(
        "/api/market-data/collectors/definition-a/facts",
        params={"hours": 999, "limit": 9999},
    )

    assert response.status_code == 200
    assert observed == {
        "definition_id": "definition-a",
        "hours": 168,
        "limit": 1000,
    }


def test_structured_collector_route_never_enables_by_default(monkeypatch) -> None:
    observed = {}

    def fake_create(**kwargs):
        observed.update(kwargs)
        return {"id": "mcd_structured", "enabled": kwargs["enabled"]}

    monkeypatch.setattr(
        controller.market_data_collector,
        "create_structured_fact_definition",
        fake_create,
    )
    response = _client().post(
        "/api/market-data/collectors/structured",
        json={
            "manifest_path": "config/market-data/structured.json",
            "binding_id": "reserve-feed",
        },
    )

    assert response.status_code == 200
    assert response.json()["definition"]["enabled"] is False
    assert observed == {
        "manifest_path": "config/market-data/structured.json",
        "binding_id": "reserve-feed",
        "max_attempts": 3,
        "minimum_spacing_seconds": 1.0,
        "enabled": False,
    }


def test_collector_stream_fingerprint_ignores_observation_clock_only() -> None:
    first = {
        "observed_at": "2026-08-02T12:00:00+00:00",
        "worker_health": {"status": "alive", "observed_at": "first"},
        "workers": [{"worker_id": "worker-a", "heartbeat_at": "same"}],
        "collectors": [],
    }
    second = {
        **first,
        "observed_at": "2026-08-02T12:00:02+00:00",
        "worker_health": {"status": "alive", "observed_at": "second"},
        "workers": [{"worker_id": "worker-a", "heartbeat_at": "same"}],
    }
    assert controller._collector_fingerprint(first) == controller._collector_fingerprint(second)
    second["workers"][0]["heartbeat_at"] = "new"
    assert controller._collector_fingerprint(first) != controller._collector_fingerprint(second)


def test_market_structure_operator_snapshot_is_consolidated(monkeypatch) -> None:
    monkeypatch.setattr(
        controller.market_structure_repository,
        "list_stream_definitions",
        lambda: [{"id": "definition-a"}],
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "list_sessions",
        lambda **kwargs: [{"session_id": "session-a", "limit": kwargs["limit"]}],
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "list_archive_status_summaries",
        lambda: {"definition-a": {"manifest_count": 1}},
    )
    monkeypatch.setattr(
        controller.market_normalization_service,
        "list_specs",
        lambda: [{"spec_id": "spec-a"}],
    )

    response = _client().get(
        "/api/market-data/market-structure/snapshot",
        params={"session_limit": 17},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "market_structure_operator_snapshot.v1"
    assert payload["sessions"][0]["limit"] == 17
    assert payload["status_by_definition"]["definition-a"]["manifest_count"] == 1
    assert payload["observed_at"]
    assert payload["component_errors"] == {}


def test_market_structure_snapshot_isolates_normalization_integrity_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        controller.market_structure_repository,
        "list_stream_definitions",
        lambda: [{"id": "definition-a"}],
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "list_sessions",
        lambda **kwargs: [{"session_id": "session-a"}],
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "list_archive_status_summaries",
        lambda: {"definition-a": {"manifest_count": 1}},
    )

    def corrupt_specs():
        raise RuntimeError("market_normalization_spec_storage_corrupt: hash mismatch")

    monkeypatch.setattr(
        controller.market_normalization_service, "list_specs", corrupt_specs
    )
    response = _client().get("/api/market-data/market-structure/snapshot")

    assert response.status_code == 200
    payload = response.json()
    assert payload["definitions"] == [{"id": "definition-a"}]
    assert payload["sessions"] == [{"session_id": "session-a"}]
    assert payload["status_by_definition"]["definition-a"]["manifest_count"] == 1
    assert payload["normalization_specs"] == []
    error = payload["component_errors"]["normalization_specs"]
    assert error["code"] == "market_structure_normalization_specs_unavailable"
    assert "hash mismatch" in error["details"]


def test_market_structure_enrollment_route_applies_manifest(monkeypatch) -> None:
    observed = {}

    def fake_enroll(**kwargs):
        observed.update(kwargs)
        return {"fleet_id": "coinbase_perpetual_trades"}

    monkeypatch.setattr(
        controller.market_structure_service,
        "apply_stream_enrollment_manifest",
        fake_enroll,
    )
    response = _client().post(
        "/api/market-data/market-structure/enrollments/apply",
        json={"manifest_path": "config/fleet.json"},
    )
    assert response.status_code == 200
    assert response.json()["fleet_id"] == "coinbase_perpetual_trades"
    assert str(observed["manifest_path"]) == "config/fleet.json"


def test_market_structure_operator_routes_preserve_typed_boundaries(
    monkeypatch,
) -> None:
    observed_book_replay = {}

    async def fake_capture(**kwargs):
        assert kwargs["definition_id"] == "definition-a"
        assert kwargs["duration_seconds"] == 12.0
        return {"schema_version": "market_structure_bounded_capture.v1"}

    def fake_book_replay(**kwargs):
        observed_book_replay.update(kwargs)
        return {
            "schema_version": "market_structure_book_replay.v1",
            "definition_id": kwargs["definition_id"],
            "session_id": kwargs["session_id"],
            "checkpoint_delta_equal": True,
        }

    monkeypatch.setattr(
        controller.market_structure_service, "capture_bounded", fake_capture
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "replay_manifest",
        lambda **kwargs: {
            "schema_version": "market_structure_manifest_replay.v1",
            "manifest_id": kwargs["manifest_id"],
        },
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "replay_book_session",
        fake_book_replay,
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "compact_session_archives",
        lambda **kwargs: {
            "schema_version": "market.raw_archive_compaction.v1",
            "source_session_id": kwargs["source_session_id"],
            "replacement_manifest_id": "manifest-compact",
        },
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "reconcile_recent_trades",
        lambda **kwargs: {
            "schema_version": "market.recent_trade_reconciliation.v1",
            "rest_limit": kwargs["limit"],
            "historical_completeness_claim": "none",
        },
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "archive_status",
        lambda **_kwargs: {
            "schema_version": "market.stream_archive_status.v1",
            "archive_mapping_lag_records": 0,
            "continuous_enabled": False,
        },
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "append_archive_retention_pin_version",
        lambda **_kwargs: "pin-version-a",
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "archive_retention_status",
        lambda **kwargs: {
            "schema_version": "market.archive_retention_status.v1",
            "target_id": kwargs["target_id"],
            "pinned": True,
        },
    )
    client = _client()
    capture = client.post(
        "/api/market-data/market-structure/definitions/definition-a/capture",
        json={"duration_seconds": 12},
    )
    status = client.get(
        "/api/market-data/market-structure/definitions/definition-a/status"
    )
    replay = client.post(
        "/api/market-data/market-structure/manifests/manifest-a/replay",
        json={},
    )
    book_replay = client.post(
        "/api/market-data/market-structure/definitions/definition-a/sessions/session-a/replay-book",
        json={"execution_instrument_id": "instrument-btc"},
    )
    compact = client.post(
        "/api/market-data/market-structure/definitions/definition-a/sessions/session-a/compact",
        json={"source_manifest_ids": ["manifest-a", "manifest-b"]},
    )
    pin = client.post(
        "/api/market-data/market-structure/archive-retention/raw_manifest/manifest-a/pin",
        json={
            "owner_kind": "operator",
            "owner_id": "test",
            "active": True,
            "reason": "test hold",
        },
    )
    retention = client.get(
        "/api/market-data/market-structure/archive-retention/raw_manifest/manifest-a"
    )
    recent = client.post(
        "/api/market-data/market-structure/definitions/definition-a/reconcile-recent",
        params={"limit": 25},
    )
    assert capture.status_code == status.status_code == 200
    assert replay.status_code == book_replay.status_code == compact.status_code == 200
    assert pin.status_code == retention.status_code == recent.status_code == 200
    assert status.json()["archive_mapping_lag_records"] == 0
    assert replay.json()["manifest_id"] == "manifest-a"
    assert book_replay.json()["checkpoint_delta_equal"] is True
    assert book_replay.json()["session_id"] == "session-a"
    assert observed_book_replay["execution_instrument_id"] == "instrument-btc"
    assert compact.json()["replacement_manifest_id"] == "manifest-compact"
    assert pin.json()["version_id"] == "pin-version-a"
    assert retention.json()["pinned"] is True
    assert recent.json()["historical_completeness_claim"] == "none"
    assert recent.json()["rest_limit"] == 25


def test_market_normalization_routes_preserve_causal_request(monkeypatch) -> None:
    observed = {}

    monkeypatch.setattr(
        controller.market_normalization_service,
        "install_builtin_specs",
        lambda **kwargs: [{"approved_by": kwargs["approved_by"], "spec_id": "nsp-a"}],
    )
    monkeypatch.setattr(
        controller.market_normalization_service,
        "list_specs",
        lambda: [{"spec_id": "nsp-a"}],
    )

    def fake_materialize(**kwargs):
        observed["materialize"] = kwargs
        return {"schema_version": "market.normalization_materialization.v1"}

    def fake_compare(**kwargs):
        observed["compare"] = kwargs
        return {"persisted_equal": True, "provider_call_performed": False}

    monkeypatch.setattr(
        controller.market_normalization_service, "materialize", fake_materialize
    )
    monkeypatch.setattr(
        controller.market_normalization_service, "compare_persisted", fake_compare
    )
    client = _client()
    install = client.post(
        "/api/market-data/market-structure/normalization/specs/install",
        json={"approved_by": "operator-a"},
    )
    specs = client.get(
        "/api/market-data/market-structure/normalization/specs"
    )
    payload = {
        "spec_id": "nsp-a",
        "source_series_id": 41,
        "start": "2026-08-02T12:00:00Z",
        "end": "2026-08-02T12:02:00Z",
        "known_at": "2026-08-02T12:03:00Z",
        "as_of_commit_seq": 77,
    }
    materialize = client.post(
        "/api/market-data/market-structure/normalization/materialize",
        json=payload,
    )
    compare = client.post(
        "/api/market-data/market-structure/normalization/compare",
        json=payload,
    )

    assert install.status_code == specs.status_code == 200
    assert materialize.status_code == compare.status_code == 200
    assert install.json()["specs"][0]["approved_by"] == "operator-a"
    assert compare.json()["persisted_equal"] is True
    assert observed["materialize"]["source_series_id"] == 41
    assert observed["materialize"]["as_of_commit_seq"] == 77
    assert observed["compare"]["known_at"].isoformat() == "2026-08-02T12:03:00+00:00"


def test_continuous_collector_control_routes_are_non_blocking_and_typed(
    monkeypatch,
) -> None:
    observed = {}

    def record(name, result):
        def inner(**kwargs):
            observed[name] = kwargs
            return result

        return inner

    monkeypatch.setattr(
        controller.market_structure_service,
        "start_continuous_validation",
        record("validate", {"mode": "validation"}),
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "start_continuous",
        record("start", {"mode": "continuous"}),
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "stop_continuous",
        record("stop", {"mode": "stopped"}),
    )
    monkeypatch.setattr(
        controller.market_structure_service,
        "set_safety_halt",
        record("halt", {"event_type": "halted"}),
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "continuous_validation_evidence",
        record("evidence", {"continuous_capture_completed": True}),
    )
    client = _client()
    validate = client.post(
        "/api/market-data/market-structure/definitions/definition-a/continuous/validate",
        json={
            "duration_seconds": 86400,
            "requested_by": "operator-a",
            "policy": {"max_inflight_segments": 3},
        },
    )
    start = client.post(
        "/api/market-data/market-structure/definitions/definition-a/continuous/start",
        json={"requested_by": "operator-a", "policy": None},
    )
    stop = client.post(
        "/api/market-data/market-structure/definitions/definition-a/continuous/stop",
        json={"requested_by": "operator-a"},
    )
    halt = client.post(
        "/api/market-data/market-structure/safety/halt",
        json={
            "request_id": "request-a",
            "scope_type": "stream",
            "scope_id": "definition-a",
            "requested_by": "operator-a",
            "reason": "operator test",
            "policy_hash": "abc123",
        },
    )
    evidence = client.get(
        "/api/market-data/market-structure/definitions/definition-a/continuous/validation/session-a"
    )

    assert all(
        response.status_code == 200
        for response in (validate, start, stop, halt, evidence)
    )
    assert observed["validate"]["duration_seconds"] == 86400.0
    assert observed["validate"]["policy"] == {"max_inflight_segments": 3}
    assert observed["start"]["requested_by"] == "operator-a"
    assert observed["stop"]["definition_id"] == "definition-a"
    assert observed["halt"]["scope_id"] == "definition-a"
    assert observed["halt"]["evidence"] is None
    assert observed["evidence"] == {
        "definition_id": "definition-a",
        "session_id": "session-a",
    }


def test_market_storage_lifecycle_routes_are_dry_run_first(monkeypatch) -> None:
    policy = object()
    observed = {}
    monkeypatch.setattr(
        controller,
        "get_settings",
        lambda: SimpleNamespace(market_data_lifecycle=policy),
    )

    def fake_plan(*, policy):
        observed["plan_policy"] = policy
        return {
            "schema_version": "market.storage_lifecycle_plan.v1",
            "summary": {"eligible_count": 2},
        }

    def fake_run(**kwargs):
        observed["run"] = kwargs
        return {
            "schema_version": "market.storage_lifecycle_run.v1",
            "status": "dry_run",
        }

    monkeypatch.setattr(
        controller.market_storage_lifecycle_service,
        "plan",
        fake_plan,
    )
    monkeypatch.setattr(
        controller.market_storage_lifecycle_service,
        "run",
        fake_run,
    )
    monkeypatch.setattr(
        controller.market_storage_lifecycle_repository,
        "list_recent_events",
        lambda **kwargs: [{"id": "event-a", "limit": kwargs["limit"]}],
    )

    client = _client()
    plan = client.get("/api/market-data/market-structure/storage-lifecycle/plan")
    run = client.post(
        "/api/market-data/market-structure/storage-lifecycle/run",
        json={"storage_root": "/portable/market-data"},
    )
    events = client.get(
        "/api/market-data/market-structure/storage-lifecycle/events",
        params={"limit": 17},
    )

    assert plan.status_code == run.status_code == events.status_code == 200
    assert observed["plan_policy"] is policy
    assert observed["run"]["policy"] is policy
    assert observed["run"]["execute"] is False
    assert str(observed["run"]["storage_root"]) == "/portable/market-data"
    assert events.json()["events"] == [{"id": "event-a", "limit": 17}]
