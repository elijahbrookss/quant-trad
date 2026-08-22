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


def test_canonical_collector_fleet_route_projects_both_implementations(
    monkeypatch,
) -> None:
    observed = {}

    def fake_snapshot(*, attempt_limit: int):
        observed["attempt_limit"] = attempt_limit
        return {
            "schema_version": "market.collector_operational_snapshot.v1",
            "observed_at": "2026-08-10T12:00:00+00:00",
            "fleet": {"collector_count": 2},
            "worker_fleet": {"alive_count": 1},
            "collectors": [
                {"collector_kind": "scheduled_fact", "collector_id": "poll-1"},
                {
                    "collector_kind": "continuous_stream",
                    "collector_id": "stream-1",
                },
            ],
        }

    monkeypatch.setattr(
        controller.collector_operations_service,
        "fleet_snapshot",
        fake_snapshot,
    )
    response = _client().get(
        "/api/market-data/operations/collectors/snapshot",
        params={"attempt_limit": 9},
    )

    assert response.status_code == 200
    assert observed["attempt_limit"] == 9
    assert response.json()["fleet"]["collector_count"] == 2


def test_canonical_collector_action_forwards_audited_operator_context(
    monkeypatch,
) -> None:
    observed = {}

    def fake_execute(**kwargs):
        observed.update(kwargs)
        return {
            "schema_version": "market.collector_operation.v1",
            "mutated": True,
            "operation": {"status": "succeeded"},
        }

    monkeypatch.setattr(
        controller.collector_operations_service,
        "execute_action",
        fake_execute,
    )
    response = _client().post(
        "/api/market-data/operations/collectors/continuous_stream/stream-1/actions/restart",
        json={
            "request_id": "operator-request-1",
            "actor_id": "frontend-v2:local-operator",
            "requested_at": "2026-08-10T12:00:00Z",
            "confirmation": "continuous_stream:stream-1:restart",
            "context": {"surface": "collector-console"},
        },
    )

    assert response.status_code == 200
    assert observed["collector_kind"] == "continuous_stream"
    assert observed["collector_id"] == "stream-1"
    assert observed["action"] == "restart"
    assert observed["actor_id"] == "frontend-v2:local-operator"
    assert observed["context"] == {"surface": "collector-console"}


def test_retired_collector_mutation_routes_are_not_part_of_the_runtime_api() -> None:
    paths = set(_client().app.openapi()["paths"])

    assert "/api/market-data/collectors" not in paths
    assert "/api/market-data/collectors/structured" not in paths
    assert "/api/market-data/collectors/{definition_id}/enabled" not in paths
    assert (
        "/api/market-data/market-structure/definitions/{definition_id}/continuous/start"
        not in paths
    )
    assert (
        "/api/market-data/market-structure/definitions/{definition_id}/continuous/stop"
        not in paths
    )


def test_operational_collector_stream_fingerprint_ignores_observation_clock_only() -> None:
    first = {
        "observed_at": "2026-08-02T12:00:00+00:00",
        "worker_fleet": {
            "workers": [{"worker_id": "worker-a", "heartbeat_at": "same"}]
        },
        "collectors": [],
    }
    second = {
        **first,
        "observed_at": "2026-08-02T12:00:02+00:00",
        "worker_fleet": {
            "workers": [{"worker_id": "worker-a", "heartbeat_at": "same"}]
        },
    }
    assert controller._operational_collector_fingerprint(
        first
    ) == controller._operational_collector_fingerprint(second)
    second["worker_fleet"]["workers"][0]["heartbeat_at"] = "new"
    assert controller._operational_collector_fingerprint(
        first
    ) != controller._operational_collector_fingerprint(second)


def test_provider_summary_stream_ignores_derived_freshness_age() -> None:
    first = {
        "observed_at": "2026-08-10T12:00:00+00:00",
        "fleet": {"collector_count": 1},
        "providers": [
            {
                "provider": "COINBASE",
                "health_status": "HEALTHY",
                "freshness_seconds": 10.0,
                "last_accepted_fact_at": "2026-08-10T11:59:50+00:00",
            }
        ],
    }
    second = {
        **first,
        "observed_at": "2026-08-10T12:00:10+00:00",
        "providers": [{**first["providers"][0], "freshness_seconds": 20.0}],
    }

    assert controller._provider_summary_fingerprint(
        first
    ) == controller._provider_summary_fingerprint(second)
    second["providers"][0]["health_status"] = "DELAYED"
    assert controller._provider_summary_fingerprint(
        first
    ) != controller._provider_summary_fingerprint(second)


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


def test_product_collector_enrollment_route_preserves_admin_context(
    monkeypatch,
) -> None:
    observed = {}

    def fake_enroll(**kwargs):
        observed.update(kwargs)
        return {
            "schema_version": "market.collector_product_enrollment.v1",
            "status": "enrolled",
            "product_id": kwargs["product_id"],
        }

    monkeypatch.setattr(
        controller.collector_definition_enrollment_service,
        "enroll_product",
        fake_enroll,
    )
    response = _client().post(
        "/api/market-data/definitions/enroll-product",
        json={
            "provider": "COINBASE",
            "venue": "COINBASE_DIRECT",
            "product_id": "LNP-20DEC30-CDE",
            "collector_types": [
                "open_interest",
                "funding_rate",
                "market_trades",
                "level2",
            ],
            "poll_interval_seconds": 60,
            "request_id": "operator-link-1",
            "actor_id": "operator:test",
            "reason": "Add LINK market-data coverage",
            "confirmation": "COINBASE:COINBASE_DIRECT:LNP-20DEC30-CDE:enroll",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "enrolled"
    assert observed["collector_types"] == [
        "open_interest",
        "funding_rate",
        "market_trades",
        "level2",
    ]
    assert observed["actor_id"] == "operator:test"
    assert observed["reason"] == "Add LINK market-data coverage"


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


def test_collector_safety_and_historical_validation_evidence_remain_inspectable(
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
        "set_safety_halt",
        record("halt", {"event_type": "halted"}),
    )
    monkeypatch.setattr(
        controller.market_structure_repository,
        "continuous_validation_evidence",
        record("evidence", {"continuous_capture_completed": True}),
    )
    client = _client()
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

    assert halt.status_code == evidence.status_code == 200
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
