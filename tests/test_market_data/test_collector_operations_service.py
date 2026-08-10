from datetime import UTC, datetime, timedelta

from market_data.collector_operations import (
    CollectorAction,
    CollectorActualState,
    CollectorConfiguredState,
    CollectorDesiredState,
    CollectorKind,
)
from portal.backend.service.market.collector_operations_service import (
    CollectorOperationsService,
)
from portal.backend.service.market.collector_supervisor import (
    CollectorAdapterRegistry,
)


NOW = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)


class _CollectionRepository:
    def list_definitions(self):
        return [
            {
                "id": "coinbase-oi",
                "source_id": 1,
                "series_id": 11,
                "enabled": True,
                "desired_state": "running",
                "control_generation": 2,
                "provider": "COINBASE",
                "venue": "COINBASE_DIRECT",
                "source_kind": "poll",
                "adapter_version": (
                    "coinbase_advanced_trade.open_interest.public_poll.v1"
                ),
                "instrument_id": "instrument-oi",
                "instrument_symbol": "BIP-20DEC30-CDE",
                "instrument_type": "future",
                "fact_type": "derivatives.open_interest",
                "contract_version": "derivatives.open_interest.v1",
                "poll_interval_seconds": 60,
                "max_attempts": 3,
                "next_scheduled_at": NOW + timedelta(seconds=30),
                "available_at": NOW,
                "consecutive_failures": 0,
                "lease_current": False,
                "lease_generation": 2,
                "last_attempt_at": NOW - timedelta(seconds=10),
                "last_success_at": NOW - timedelta(seconds=10),
                "last_error": None,
                "config": {"schema_version": "market_collection_definition.v1"},
            }
        ]

    def list_worker_states(self):
        return [
            {
                "worker_id": "worker-1",
                "worker_role": "scheduled_market_fact_collector",
                "worker_version": "market_data_collector.v3",
                "state": "idle",
                "alive": True,
                "started_at": NOW - timedelta(hours=1),
                "heartbeat_at": NOW - timedelta(seconds=2),
                "active_definition_id": None,
                "context": {
                    "continuous_collectors": {
                        "state": "running",
                        "tasks": {
                            "coinbase-trades": {
                                "adapter_id": "fixture.trades.v1",
                                "control_generation": 4,
                                "started_at": (NOW - timedelta(minutes=5)).isoformat(),
                                "restart_count": 1,
                                "last_error": None,
                            }
                        },
                        "errors": {},
                    }
                },
            }
        ]

    def list_recent_attempts(self, *, limit_per_definition):
        assert limit_per_definition == 5
        return [
            {
                "id": "attempt-1",
                "definition_id": "coinbase-oi",
                "status": "succeeded",
                "scheduled_for": NOW - timedelta(seconds=10),
                "error": None,
            }
        ]


class _StreamRepository:
    def list_stream_definitions(self):
        return [
            {
                "id": "coinbase-trades",
                "source_id": 2,
                "series_id": 21,
                "enabled": True,
                "desired_state": "running",
                "control_generation": 4,
                "provider": "COINBASE",
                "venue": "COINBASE_DIRECT",
                "source_kind": "stream",
                "adapter_version": "coinbase_advanced_trade.market_trades.v1",
                "instrument_id": "instrument-trades",
                "instrument_symbol": "BIP-20DEC30-CDE",
                "instrument_type": "future",
                "provider_product_id": "BIP-20DEC30-CDE",
                "channels": ("market_trades", "heartbeats"),
                "series_fact_type": "market.trade",
                "contract_version": "market.trade.v1",
                "lease_current": True,
                "owner_id": "worker-1:coinbase-trades",
                "lease_generation": 8,
                "expires_at": NOW + timedelta(seconds=30),
                "config": {
                    "schema_version": "market.stream_runtime_config.v1",
                    "product_definition_version_id": "product.v1",
                    "aggregate_series_ids": {"1": 22, "60": 23},
                    "flow_feature_series_ids": {"1": 24, "60": 25},
                    "runtime_policy": {},
                },
            }
        ]


class _OperationsRepository:
    def __init__(self):
        self.last_action = None
        self.idempotent_replay = False

    def fact_series_telemetry(self, *, series_ids):
        assert set(series_ids) == {11, 21, 22, 23, 24, 25}
        return {
            11: {
                "last_observation_time": NOW - timedelta(seconds=12),
                "last_accepted_at": NOW - timedelta(seconds=10),
                "accepted_last_minute": 1,
                "accepted_last_five_minutes": 5,
            },
            21: {
                "last_observation_time": NOW - timedelta(seconds=1),
                "last_accepted_at": NOW - timedelta(seconds=1),
                "accepted_last_minute": 120,
                "accepted_last_five_minutes": 600,
            },
        }

    def apply_lifecycle_action(self, **kwargs):
        self.last_action = dict(kwargs)
        error = kwargs.get("precondition_error")
        return {
            "status": "failed" if error else "succeeded",
            "error": error,
            "idempotent_replay": self.idempotent_replay,
        }


class _StreamAdapter:
    adapter_id = "fixture.trades.v1"

    def supports(self, definition):
        return definition.get("provider") == "COINBASE"


def _service() -> CollectorOperationsService:
    return CollectorOperationsService(
        collection_repository=_CollectionRepository(),
        stream_repository=_StreamRepository(),
        operations_repository=_OperationsRepository(),
        stream_registry=CollectorAdapterRegistry((_StreamAdapter(),)),
        clock=lambda: NOW,
    )


def test_fleet_snapshot_projects_both_collector_families_without_provider_ui_logic():
    snapshot = _service().fleet_snapshot()

    assert snapshot["fleet"]["collector_count"] == 2
    assert snapshot["fleet"]["unregistered_definition_count"] == 0
    assert snapshot["fleet"]["accepted_last_minute"] == 121
    assert {item["collector_kind"] for item in snapshot["collectors"]} == {
        "scheduled_fact",
        "continuous_stream",
    }
    assert {item["actual_state"] for item in snapshot["collectors"]} == {
        "HEALTHY"
    }
    stream = next(
        item
        for item in snapshot["collectors"]
        if item["collector_kind"] == "continuous_stream"
    )
    assert {item["fact_type"] for item in stream["fact_schemas"]} == {
        "market.trade",
        "market.trade_flow",
        "market.trade_flow_feature",
    }
    assert stream["runtime"]["restart_count"] == 1
    assert "health_probe" in stream["capabilities"]["actions"]
    assert all(
        "health_probe" in item["capabilities"]["actions"]
        for item in snapshot["collectors"]
    )


def test_actual_state_keeps_configured_desired_and_runtime_state_distinct():
    derive = CollectorOperationsService._actual_state

    assert derive(
        configured_state=CollectorConfiguredState.DISABLED,
        desired_state=CollectorDesiredState.RUNNING,
        worker_alive=True,
        active=False,
        retrying=False,
        recovering=False,
        has_error=False,
        has_accepted_fact=True,
        freshness_ok=True,
    ) == CollectorActualState.DISABLED
    assert derive(
        configured_state=CollectorConfiguredState.INVALID,
        desired_state=CollectorDesiredState.STOPPED,
        worker_alive=True,
        active=False,
        retrying=False,
        recovering=False,
        has_error=False,
        has_accepted_fact=False,
        freshness_ok=None,
    ) == CollectorActualState.STOPPED
    assert derive(
        configured_state=CollectorConfiguredState.ENABLED,
        desired_state=CollectorDesiredState.STOPPED,
        worker_alive=True,
        active=True,
        retrying=False,
        recovering=False,
        has_error=False,
        has_accepted_fact=True,
        freshness_ok=True,
    ) == CollectorActualState.STOPPING
    assert derive(
        configured_state=CollectorConfiguredState.ENABLED,
        desired_state=CollectorDesiredState.RUNNING,
        worker_alive=True,
        active=False,
        retrying=True,
        recovering=False,
        has_error=True,
        has_accepted_fact=True,
        freshness_ok=False,
    ) == CollectorActualState.RETRYING


def test_disabled_registration_preserves_diagnostics_without_failing_the_fleet():
    configured_state, errors = CollectorOperationsService._scheduled_registration(
        {
            "enabled": False,
            "adapter_version": "retired.adapter.v1",
            "fact_type": "unknown",
            "contract_version": "unknown.v1",
            "config": {},
        }
    )

    assert configured_state == CollectorConfiguredState.DISABLED
    assert errors == ["adapter_not_registered", "definition_schema_unsupported"]
    assert CollectorOperationsService._lifecycle_capabilities(
        configured_state=configured_state,
        registration_errors=errors,
        desired_state=CollectorDesiredState.STOPPED,
        active=False,
    ) == ["health_probe"]


def test_mutation_precondition_failures_enter_the_audit_command_path():
    operations = _OperationsRepository()
    service = CollectorOperationsService(
        collection_repository=_CollectionRepository(),
        stream_repository=_StreamRepository(),
        operations_repository=operations,
        stream_registry=CollectorAdapterRegistry((_StreamAdapter(),)),
        clock=lambda: NOW,
    )

    result = service.execute_action(
        request_id="request-1",
        collector_kind=CollectorKind.CONTINUOUS_STREAM,
        collector_id="coinbase-trades",
        action=CollectorAction.RESTART,
        requested_at=NOW,
        actor_id="test-operator",
        confirmation="incorrect",
        context={"surface": "test"},
    )

    assert result["mutated"] is False
    assert result["operation"]["status"] == "failed"
    assert operations.last_action["precondition_error"].startswith(
        "collector_operation_confirmation_required"
    )


def test_idempotent_operation_replay_does_not_claim_a_second_mutation():
    operations = _OperationsRepository()
    operations.idempotent_replay = True
    service = CollectorOperationsService(
        collection_repository=_CollectionRepository(),
        stream_repository=_StreamRepository(),
        operations_repository=operations,
        stream_registry=CollectorAdapterRegistry((_StreamAdapter(),)),
        clock=lambda: NOW,
    )

    result = service.execute_action(
        request_id="request-1",
        collector_kind=CollectorKind.CONTINUOUS_STREAM,
        collector_id="coinbase-trades",
        action=CollectorAction.RESTART,
        requested_at=NOW,
        actor_id="test-operator",
        confirmation="continuous_stream:coinbase-trades:restart",
        context={"surface": "test"},
    )

    assert result["operation"]["idempotent_replay"] is True
    assert result["mutated"] is False


def test_data_plane_snapshot_reuses_canonical_fleet_metrics():
    snapshot = _service().data_plane_snapshot()

    assert snapshot["ingestion_rate_per_minute"] == 121
    assert snapshot["active_schema_count"] == 4
    assert snapshot["collector_health"] == {"HEALTHY": 2}
