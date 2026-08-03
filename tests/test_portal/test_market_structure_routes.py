from __future__ import annotations

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


def test_market_structure_pair_route_never_implies_production(monkeypatch) -> None:
    observed = {}

    def fake_configure(**kwargs):
        observed.update(kwargs)
        return {"pair_id": kwargs["pair_id"], "production_admitted": False}

    monkeypatch.setattr(
        controller.market_structure_service, "configure_pair", fake_configure
    )
    response = _client().post(
        "/api/market-data/market-structure/pairs",
        json={"pair_id": "bip_btc", "auth_mode": "authenticated"},
    )
    assert response.status_code == 200
    assert response.json()["production_admitted"] is False
    assert observed["enable_production"] is False


def test_market_structure_operator_routes_preserve_typed_boundaries(
    monkeypatch,
) -> None:
    async def fake_capture(**kwargs):
        assert kwargs["definition_id"] == "definition-a"
        assert kwargs["duration_seconds"] == 12.0
        return {"schema_version": "market_structure_bounded_capture.v1"}

    monkeypatch.setattr(
        controller.market_structure_service,
        "materialize_pair_features",
        lambda **kwargs: {
            "schema_version": "market.cross_stream_materialization.v1",
            "pair_id": kwargs["pair_id"],
            "basis_count": 2,
            "source_commit_seq": 42,
        },
    )
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
        lambda **kwargs: {
            "schema_version": "market_structure_book_replay.v1",
            "definition_id": kwargs["definition_id"],
            "session_id": kwargs["session_id"],
            "checkpoint_delta_equal": True,
        },
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
            "production_admitted": False,
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
    materialize = client.post(
        "/api/market-data/market-structure/pairs/bip_btc/materialize",
        json={
            "start": "2026-08-02T14:00:00Z",
            "end": "2026-08-02T14:01:00Z",
            "known_at": "2026-08-02T14:02:00Z",
        },
    )
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
        json={},
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
    assert materialize.status_code == 200
    assert materialize.json()["source_commit_seq"] == 42
    assert replay.status_code == book_replay.status_code == compact.status_code == 200
    assert pin.status_code == retention.status_code == recent.status_code == 200
    assert status.json()["archive_mapping_lag_records"] == 0
    assert replay.json()["manifest_id"] == "manifest-a"
    assert book_replay.json()["checkpoint_delta_equal"] is True
    assert book_replay.json()["session_id"] == "session-a"
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
