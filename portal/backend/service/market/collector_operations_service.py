"""Provider-neutral collector fleet projection, diagnostics, and safe control."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import Any, Callable, Mapping

from data_providers.streams.runtime import ContinuousStreamPolicy
from market_data.collector_operations import (
    COLLECTOR_DETAIL_VERSION,
    COLLECTOR_DIAGNOSTIC_VERSION,
    COLLECTOR_EVENT_CATALOG_VERSION,
    COLLECTOR_GAP_CATALOG_VERSION,
    COLLECTOR_OPERATION_VERSION,
    COLLECTOR_OPERATIONAL_SNAPSHOT_VERSION,
    COLLECTOR_PAGE_VERSION,
    COLLECTOR_PROVIDER_SUMMARY_VERSION,
    MARKET_DATA_PLANE_OPERATIONAL_VERSION,
    CollectorAction,
    CollectorActualState,
    CollectorHealthStatus,
    CollectorOperationalState,
    CollectorConfiguredState,
    CollectorDesiredState,
    CollectorDiagnosticBoundary,
    CollectorDiagnosticStatus,
    CollectorKind,
)

from ..storage.repos.collector_operations import (
    PostgresCollectorOperationsRepository,
    collector_operations_repository,
)
from ..storage.repos.market_collection import (
    PostgresMarketCollectionRepository,
    market_collection_repo,
)
from ..storage.repos.market_structure import (
    PostgresMarketStructureRepository,
    market_structure_repository,
)
from .collector_supervisor import (
    CoinbaseLevel2CollectorAdapter,
    CoinbaseMarketTradeCollectorAdapter,
    CollectorAdapterRegistry,
)


_SCHEDULED_ADAPTERS = {
    "coinbase_advanced_trade.open_interest.public_poll.v1": (
        "derivatives.open_interest",
        "derivatives.open_interest.v1",
    ),
    "coinbase_advanced_trade.funding_rate.public_poll.v1": (
        "derivatives.funding_rate",
        "derivatives.funding_rate.v1",
    ),
    "chainlink_mvr_bundle.v1": (
        "asset.reserve_state",
        "asset.reserve_state.v1",
    ),
}
_SCHEDULED_DEFINITION_VERSION = "market_collection_definition.v1"
_CONTINUOUS_DERIVED_SCHEMAS = (
    ("market.trade_flow", "market.trade_flow.v1", "aggregate_series_ids"),
    (
        "market.trade_flow_feature",
        "market.trade_flow_feature.v1",
        "flow_feature_series_ids",
    ),
)


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: Any) -> str | None:
    parsed = _as_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def _seconds_since(value: Any, *, now: datetime) -> float | None:
    parsed = _as_datetime(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed).total_seconds())


def _series_ids(definition: Mapping[str, Any], kind: CollectorKind) -> list[int]:
    values = [int(definition["series_id"])]
    if kind == CollectorKind.CONTINUOUS_STREAM:
        config = dict(definition.get("config") or {})
        output_series = config.get("output_series")
        if isinstance(output_series, list) and output_series:
            values.extend(
                int(row["series_id"])
                for row in output_series
                if isinstance(row, Mapping) and row.get("series_id") is not None
            )
        else:
            for key in ("aggregate_series_ids", "flow_feature_series_ids"):
                values.extend(
                    int(value) for value in dict(config.get(key) or {}).values()
                )
    return sorted(set(values))


class CollectorOperationsService:
    """Project both implementations into one operator-facing domain contract."""

    def __init__(
        self,
        *,
        collection_repository: PostgresMarketCollectionRepository = (
            market_collection_repo
        ),
        stream_repository: PostgresMarketStructureRepository = (
            market_structure_repository
        ),
        operations_repository: PostgresCollectorOperationsRepository = (
            collector_operations_repository
        ),
        stream_registry: CollectorAdapterRegistry | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.collection_repository = collection_repository
        self.stream_repository = stream_repository
        self.operations_repository = operations_repository
        self.stream_registry = stream_registry or CollectorAdapterRegistry(
            (
                CoinbaseMarketTradeCollectorAdapter(),
                CoinbaseLevel2CollectorAdapter(),
            )
        )
        self.clock = clock

    @staticmethod
    def _scheduled_registration(
        definition: Mapping[str, Any],
    ) -> tuple[CollectorConfiguredState, list[str]]:
        reasons: list[str] = []
        config = dict(definition.get("config") or {})
        adapter_version = str(definition.get("adapter_version") or "")
        expected = _SCHEDULED_ADAPTERS.get(adapter_version)
        if expected is None:
            reasons.append("adapter_not_registered")
        elif (
            str(definition.get("fact_type") or ""),
            str(definition.get("contract_version") or ""),
        ) != expected:
            reasons.append("adapter_fact_schema_mismatch")
        if config.get("schema_version") != _SCHEDULED_DEFINITION_VERSION:
            reasons.append("definition_schema_unsupported")
        if not bool(definition.get("enabled")):
            return CollectorConfiguredState.DISABLED, reasons
        if reasons:
            return CollectorConfiguredState.INVALID, reasons
        return CollectorConfiguredState.ENABLED, []

    def _is_operationally_registered(
        self, definition: Mapping[str, Any], kind: CollectorKind
    ) -> bool:
        config_version = str(
            dict(definition.get("config") or {}).get("schema_version") or ""
        )
        if kind == CollectorKind.SCHEDULED_FACT:
            return (
                str(definition.get("adapter_version") or "")
                in _SCHEDULED_ADAPTERS
                or config_version == _SCHEDULED_DEFINITION_VERSION
            )
        try:
            self.stream_registry.resolve(definition)
        except ValueError:
            return False
        return True

    def _continuous_registration(
        self, definition: Mapping[str, Any]
    ) -> tuple[CollectorConfiguredState, list[str]]:
        reasons: list[str] = []
        try:
            adapter = self.stream_registry.resolve(definition)
        except ValueError:
            reasons.append("adapter_not_registered")
        else:
            reasons.extend(adapter.registration_errors(definition))
        if not bool(definition.get("enabled")):
            return CollectorConfiguredState.DISABLED, reasons
        if reasons:
            return CollectorConfiguredState.INVALID, reasons
        return CollectorConfiguredState.ENABLED, []

    @staticmethod
    def _actual_state(
        *,
        configured_state: CollectorConfiguredState,
        desired_state: CollectorDesiredState,
        worker_alive: bool,
        active: bool,
        retrying: bool,
        recovering: bool,
        has_error: bool,
        has_acquisition_evidence: bool,
        freshness_ok: bool | None,
    ) -> CollectorActualState:
        if configured_state == CollectorConfiguredState.DISABLED:
            return CollectorActualState.DISABLED
        if desired_state == CollectorDesiredState.STOPPED:
            return (
                CollectorActualState.STOPPING
                if active
                else CollectorActualState.STOPPED
            )
        if desired_state == CollectorDesiredState.PAUSED:
            return (
                CollectorActualState.STOPPING
                if active
                else CollectorActualState.PAUSED
            )
        if configured_state == CollectorConfiguredState.INVALID:
            return CollectorActualState.FAILED
        if recovering:
            return CollectorActualState.RECOVERING
        if retrying:
            return CollectorActualState.RETRYING
        if not worker_alive:
            return CollectorActualState.DEGRADED
        if has_error:
            return CollectorActualState.DEGRADED
        if not has_acquisition_evidence:
            return CollectorActualState.STARTING
        if freshness_ok is False:
            return CollectorActualState.DEGRADED
        return CollectorActualState.HEALTHY

    @staticmethod
    def _operational_state(
        collector: Mapping[str, Any],
    ) -> CollectorOperationalState:
        configured = str(collector.get("configured_state") or "")
        desired = str(collector.get("desired_state") or "")
        actual = str(collector.get("actual_state") or "")
        if configured == CollectorConfiguredState.DISABLED.value:
            return CollectorOperationalState.DISABLED
        if actual == CollectorActualState.STOPPING.value:
            return CollectorOperationalState.STOPPING
        if desired == CollectorDesiredState.STOPPED.value:
            return CollectorOperationalState.STOPPED
        if desired == CollectorDesiredState.PAUSED.value:
            return CollectorOperationalState.PAUSED
        return CollectorOperationalState.RUNNING

    @staticmethod
    def _health_status(
        collector: Mapping[str, Any],
        operational_state: CollectorOperationalState,
    ) -> CollectorHealthStatus:
        if operational_state in {
            CollectorOperationalState.DISABLED,
            CollectorOperationalState.STOPPED,
            CollectorOperationalState.PAUSED,
            CollectorOperationalState.STOPPING,
        }:
            return CollectorHealthStatus.NOT_APPLICABLE
        actual = str(collector.get("actual_state") or "")
        if (
            collector.get("registration_errors")
            or actual == CollectorActualState.FAILED.value
            or bool(dict(collector.get("error") or {}).get("active"))
        ):
            return CollectorHealthStatus.FAILED
        if actual in {
            CollectorActualState.DEGRADED.value,
            CollectorActualState.RETRYING.value,
            CollectorActualState.RECOVERING.value,
        }:
            return CollectorHealthStatus.DELAYED
        if actual == CollectorActualState.HEALTHY.value:
            return CollectorHealthStatus.HEALTHY
        return CollectorHealthStatus.UNKNOWN

    @classmethod
    def _attach_operator_projection(
        cls, collector: dict[str, Any]
    ) -> dict[str, Any]:
        operational_state = cls._operational_state(collector)
        health_status = cls._health_status(collector, operational_state)
        attention_reason = None
        if operational_state == CollectorOperationalState.RUNNING:
            if collector.get("registration_errors"):
                attention_reason = "registration_invalid"
            elif health_status == CollectorHealthStatus.FAILED:
                attention_reason = "acquisition_failed"
            elif health_status == CollectorHealthStatus.DELAYED:
                attention_reason = "acquisition_delayed"
        collector["operational_state"] = operational_state.value
        collector["health_status"] = health_status.value
        collector["needs_attention"] = attention_reason is not None
        collector["attention_reason"] = attention_reason
        return collector

    @staticmethod
    def _lifecycle_capabilities(
        *,
        configured_state: CollectorConfiguredState,
        registration_errors: list[str],
        desired_state: CollectorDesiredState,
        active: bool,
    ) -> list[str]:
        """Expose only commands the canonical mutation path can safely honor."""

        actions = ["health_probe"]
        if registration_errors:
            if active or desired_state != CollectorDesiredState.STOPPED:
                actions.append("stop")
            return actions
        if configured_state != CollectorConfiguredState.ENABLED:
            return actions
        if desired_state == CollectorDesiredState.STOPPED:
            actions.append("start")
        elif desired_state == CollectorDesiredState.PAUSED:
            actions.extend(["resume", "stop"])
        else:
            actions.extend(["stop", "pause", "restart"])
        return actions

    @staticmethod
    def _worker_projection(
        worker: Mapping[str, Any] | None,
        *,
        now: datetime,
        active_definition_id: str | None = None,
    ) -> dict[str, Any]:
        if worker is None:
            return {
                "identity": None,
                "state": "unavailable",
                "alive": False,
                "heartbeat_at": None,
                "heartbeat_age_seconds": None,
                "uptime_seconds": None,
                "active": False,
            }
        return {
            "identity": worker.get("worker_id"),
            "role": worker.get("worker_role"),
            "version": worker.get("worker_version"),
            "state": worker.get("state"),
            "alive": bool(worker.get("alive")),
            "heartbeat_at": _iso(worker.get("heartbeat_at")),
            "heartbeat_age_seconds": _seconds_since(
                worker.get("heartbeat_at"), now=now
            ),
            "uptime_seconds": _seconds_since(worker.get("started_at"), now=now),
            "active": (
                str(worker.get("active_definition_id") or "")
                == str(active_definition_id or "")
                if active_definition_id
                else False
            ),
        }

    @staticmethod
    def _latest_worker(
        workers: list[Mapping[str, Any]],
    ) -> Mapping[str, Any] | None:
        alive = [item for item in workers if bool(item.get("alive"))]
        return (alive or workers or [None])[0]

    @staticmethod
    def _continuous_snapshot(
        workers: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        for worker in workers:
            if not bool(worker.get("alive")):
                continue
            context = dict(worker.get("context") or {})
            snapshot = context.get("continuous_collectors")
            if isinstance(snapshot, Mapping):
                return dict(snapshot)
        return {"state": "unavailable", "tasks": {}, "errors": {}}

    @staticmethod
    def _fact_schemas(
        definition: Mapping[str, Any], kind: CollectorKind
    ) -> list[dict[str, Any]]:
        schemas = [
            {
                "fact_type": str(
                    definition.get("fact_type")
                    or definition.get("series_fact_type")
                    or ""
                ),
                "schema_version": str(definition.get("contract_version") or ""),
                "series_id": int(definition["series_id"]),
            }
        ]
        if kind == CollectorKind.CONTINUOUS_STREAM:
            config = dict(definition.get("config") or {})
            output_series = config.get("output_series")
            if isinstance(output_series, list) and output_series:
                schemas = [
                    {
                        key: value
                        for key, value in dict(row).items()
                        if key
                        in {
                            "fact_type",
                            "schema_version",
                            "series_id",
                            "timeframe_seconds",
                        }
                    }
                    for row in output_series
                    if isinstance(row, Mapping)
                ]
            else:
                for fact_type, schema_version, config_key in _CONTINUOUS_DERIVED_SCHEMAS:
                    for timeframe, series_id in sorted(
                        dict(config.get(config_key) or {}).items(),
                        key=lambda item: int(item[0]),
                    ):
                        schemas.append(
                            {
                                "fact_type": fact_type,
                                "schema_version": schema_version,
                                "series_id": int(series_id),
                                "timeframe_seconds": int(timeframe),
                            }
                        )
        return schemas

    def _project_scheduled(
        self,
        *,
        definition: Mapping[str, Any],
        attempts: list[Mapping[str, Any]],
        worker: Mapping[str, Any] | None,
        telemetry: Mapping[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        configured_state, registration_errors = self._scheduled_registration(
            definition
        )
        desired_state = CollectorDesiredState(definition["desired_state"])
        cadence = int(definition["poll_interval_seconds"])
        last_accepted = telemetry.get("last_accepted_at") or definition.get(
            "last_success_at"
        )
        freshness_seconds = _seconds_since(last_accepted, now=now)
        freshness_ok = (
            freshness_seconds <= max(120.0, cadence * 3.0)
            if freshness_seconds is not None
            else None
        )
        latest_attempt = attempts[0] if attempts else None
        retrying = bool(
            int(definition.get("consecutive_failures") or 0) > 0
            and _as_datetime(definition.get("available_at"))
            and _as_datetime(definition.get("available_at")) > now
        )
        active = bool(definition.get("lease_current"))
        worker_projection = self._worker_projection(
            worker,
            now=now,
            active_definition_id=str(definition["id"]),
        )
        error = definition.get("last_error") or (
            latest_attempt.get("error") if latest_attempt else None
        )
        actual_state = self._actual_state(
            configured_state=configured_state,
            desired_state=desired_state,
            worker_alive=bool(worker_projection["alive"]),
            active=active,
            retrying=retrying,
            recovering=False,
            has_error=bool(error and not retrying),
            has_acquisition_evidence=last_accepted is not None,
            freshness_ok=freshness_ok,
        )
        return {
            "collector_id": str(definition["id"]),
            "collector_kind": CollectorKind.SCHEDULED_FACT.value,
            "collector_type": "scheduled_poll",
            "provider": str(definition.get("provider") or "").upper(),
            "venue": str(definition.get("venue") or "").upper(),
            "fact_schemas": self._fact_schemas(
                definition, CollectorKind.SCHEDULED_FACT
            ),
            "subjects": [
                {
                    "instrument_id": definition.get("instrument_id"),
                    "symbol": definition.get("instrument_symbol"),
                    "instrument_type": definition.get("instrument_type"),
                }
            ],
            "configured_state": configured_state.value,
            "registration_errors": registration_errors,
            "desired_state": desired_state.value,
            "actual_state": actual_state.value,
            "control_generation": int(definition.get("control_generation") or 0),
            "worker": worker_projection,
            "runtime": {
                "active": active,
                "lease_owner": definition.get("lease_owner"),
                "lease_generation": int(definition.get("lease_generation") or 0),
                "lease_expires_at": _iso(definition.get("lease_expires_at")),
                "restart_count": 0,
            },
            "acquisition": {
                "cadence_seconds": cadence,
                "next_scheduled_at": _iso(definition.get("next_scheduled_at")),
                "last_attempt_at": _iso(definition.get("last_attempt_at")),
                "last_provider_success_at": _iso(definition.get("last_success_at")),
                "last_accepted_fact_at": _iso(last_accepted),
                "last_observation_time": _iso(
                    telemetry.get("last_observation_time")
                ),
                "freshness_seconds": freshness_seconds,
                "freshness_ok": freshness_ok,
                "freshness_basis": "accepted_fact",
                "freshness_threshold_seconds": max(120.0, cadence * 3.0),
            },
            "throughput": {
                "accepted_last_minute": int(
                    telemetry.get("accepted_last_minute") or 0
                ),
                "accepted_last_five_minutes": int(
                    telemetry.get("accepted_last_five_minutes") or 0
                ),
                "rejected_recent": sum(
                    1 for item in attempts if str(item.get("status")) == "failed"
                ),
            },
            "retry": {
                "active": retrying,
                "consecutive_failures": int(
                    definition.get("consecutive_failures") or 0
                ),
                "available_at": _iso(definition.get("available_at")),
                "max_attempts": int(definition.get("max_attempts") or 0),
            },
            "gap": {
                "state": "recorded_in_gap_evidence",
                "active_count": None,
            },
            "error": {
                "active": bool(error),
                "message": str(error) if error else None,
            },
            "capabilities": {
                "actions": self._lifecycle_capabilities(
                    configured_state=configured_state,
                    registration_errors=registration_errors,
                    desired_state=desired_state,
                    active=active,
                ),
                "recovery": False,
                "historical_acquisition": bool(
                    dict(definition.get("config") or {}).get("historical_acquisition")
                ),
            },
        }

    def _project_continuous(
        self,
        *,
        definition: Mapping[str, Any],
        worker: Mapping[str, Any] | None,
        supervisor: Mapping[str, Any],
        telemetry: Mapping[str, Any],
        stream_telemetry: Mapping[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        configured_state, registration_errors = self._continuous_registration(
            definition
        )
        desired_state = CollectorDesiredState(definition["desired_state"])
        tasks = dict(supervisor.get("tasks") or {})
        errors = dict(supervisor.get("errors") or {})
        task = dict(tasks.get(str(definition["id"])) or {})
        error = errors.get(str(definition["id"])) or task.get("last_error")
        worker_projection = self._worker_projection(worker, now=now)
        last_accepted = telemetry.get("last_accepted_at")
        last_provider_activity = stream_telemetry.get("last_provider_activity_at")
        runtime_policy = ContinuousStreamPolicy.from_mapping(
            dict(definition.get("config") or {}).get("runtime_policy")
        )
        freshness_threshold = max(120.0, runtime_policy.segment_max_seconds * 2.0)
        freshness_seconds = _seconds_since(last_provider_activity, now=now)
        freshness_ok = (
            freshness_seconds <= freshness_threshold
            if freshness_seconds is not None
            else None
        )
        active = bool(task) or bool(definition.get("lease_current"))
        retrying = bool(error and not task and desired_state == CollectorDesiredState.RUNNING)
        actual_state = self._actual_state(
            configured_state=configured_state,
            desired_state=desired_state,
            worker_alive=bool(worker_projection["alive"]),
            active=active,
            retrying=retrying,
            recovering=False,
            has_error=bool(error and not retrying),
            has_acquisition_evidence=last_provider_activity is not None,
            freshness_ok=freshness_ok,
        )
        return {
            "collector_id": str(definition["id"]),
            "collector_kind": CollectorKind.CONTINUOUS_STREAM.value,
            "collector_type": "continuous_stream",
            "provider": str(definition.get("provider") or "").upper(),
            "venue": str(definition.get("venue") or "").upper(),
            "fact_schemas": self._fact_schemas(
                definition, CollectorKind.CONTINUOUS_STREAM
            ),
            "subjects": [
                {
                    "instrument_id": definition.get("instrument_id"),
                    "symbol": definition.get("instrument_symbol"),
                    "provider_product_id": definition.get("provider_product_id"),
                    "instrument_type": definition.get("instrument_type"),
                }
            ],
            "configured_state": configured_state.value,
            "registration_errors": registration_errors,
            "desired_state": desired_state.value,
            "actual_state": actual_state.value,
            "control_generation": int(definition.get("control_generation") or 0),
            "worker": worker_projection,
            "runtime": {
                "active": active,
                "adapter_id": task.get("adapter_id"),
                "started_at": task.get("started_at"),
                "uptime_seconds": _seconds_since(task.get("started_at"), now=now),
                "restart_count": int(task.get("restart_count") or 0),
                "lease_owner": definition.get("owner_id"),
                "lease_generation": int(definition.get("lease_generation") or 0),
                "lease_expires_at": _iso(definition.get("expires_at")),
            },
            "acquisition": {
                "cadence_seconds": None,
                "trigger": "provider_stream",
                "channels": list(definition.get("channels") or []),
                "last_provider_success_at": _iso(last_provider_activity),
                "last_accepted_fact_at": _iso(last_accepted),
                "last_observation_time": _iso(
                    telemetry.get("last_observation_time")
                ),
                "freshness_seconds": freshness_seconds,
                "freshness_ok": freshness_ok,
                "freshness_basis": "provider_activity",
                "freshness_threshold_seconds": freshness_threshold,
            },
            "throughput": {
                "accepted_last_minute": int(
                    telemetry.get("accepted_last_minute") or 0
                ),
                "accepted_last_five_minutes": int(
                    telemetry.get("accepted_last_five_minutes") or 0
                ),
                "rejected_recent": None,
            },
            "retry": {
                "active": retrying,
                "restart_count": int(task.get("restart_count") or 0),
                "error": error,
            },
            "gap": {
                "state": "stream_quality_and_gap_evidence",
                "active_count": None,
            },
            "error": {
                "active": bool(error),
                "message": str(error) if error else None,
            },
            "capabilities": {
                "actions": self._lifecycle_capabilities(
                    configured_state=configured_state,
                    registration_errors=registration_errors,
                    desired_state=desired_state,
                    active=active,
                ),
                "recovery": False,
                "recovery_scope": None,
                "historical_acquisition": False,
            },
        }

    def fleet_snapshot(self, *, attempt_limit: int = 5) -> dict[str, Any]:
        now = self.clock().astimezone(UTC)
        stored_scheduled = self.collection_repository.list_definitions()
        stored_continuous = self.stream_repository.list_stream_definitions()
        scheduled = [
            item
            for item in stored_scheduled
            if self._is_operationally_registered(
                item, CollectorKind.SCHEDULED_FACT
            )
        ]
        continuous = [
            item
            for item in stored_continuous
            if self._is_operationally_registered(
                item, CollectorKind.CONTINUOUS_STREAM
            )
        ]
        unregistered = [
            {
                "collector_id": str(item["id"]),
                "collector_kind": kind.value,
                "provider": str(item.get("provider") or "").upper(),
                "adapter_version": item.get("adapter_version"),
                "config_schema_version": dict(item.get("config") or {}).get(
                    "schema_version"
                ),
            }
            for kind, definitions in (
                (CollectorKind.SCHEDULED_FACT, stored_scheduled),
                (CollectorKind.CONTINUOUS_STREAM, stored_continuous),
            )
            for item in definitions
            if not self._is_operationally_registered(item, kind)
        ]
        workers = self.collection_repository.list_worker_states()
        worker = self._latest_worker(workers)
        supervisor = self._continuous_snapshot(workers)

        recent_attempts = self.collection_repository.list_recent_attempts(
            limit_per_definition=attempt_limit
        )
        attempts_by_definition: dict[str, list[Mapping[str, Any]]] = {
            str(item["id"]): [] for item in scheduled
        }
        for attempt in recent_attempts:
            attempts_by_definition.setdefault(
                str(attempt["definition_id"]), []
            ).append(attempt)

        all_series_ids = [
            series_id
            for definition in scheduled
            for series_id in _series_ids(definition, CollectorKind.SCHEDULED_FACT)
        ] + [
            series_id
            for definition in continuous
            for series_id in _series_ids(definition, CollectorKind.CONTINUOUS_STREAM)
        ]
        telemetry = self.operations_repository.fact_series_telemetry(
            series_ids=all_series_ids
        )
        stream_telemetry = self.operations_repository.continuous_stream_telemetry(
            definition_ids=[str(definition["id"]) for definition in continuous]
        )

        collectors = [
            self._project_scheduled(
                definition=definition,
                attempts=attempts_by_definition.get(str(definition["id"]), []),
                worker=worker,
                telemetry=telemetry.get(int(definition["series_id"]), {}),
                now=now,
            )
            for definition in scheduled
        ] + [
            self._project_continuous(
                definition=definition,
                worker=worker,
                supervisor=supervisor,
                telemetry=telemetry.get(int(definition["series_id"]), {}),
                stream_telemetry=stream_telemetry.get(str(definition["id"]), {}),
                now=now,
            )
            for definition in continuous
        ]
        collectors.sort(key=lambda item: (item["provider"], item["collector_id"]))
        for collector in collectors:
            self._attach_operator_projection(collector)
        state_counts = Counter(item["actual_state"] for item in collectors)
        operational_state_counts = Counter(
            item["operational_state"] for item in collectors
        )
        health_counts = Counter(item["health_status"] for item in collectors)
        provider_counts = Counter(item["provider"] for item in collectors)
        return {
            "schema_version": COLLECTOR_OPERATIONAL_SNAPSHOT_VERSION,
            "observed_at": now.isoformat(),
            "fleet": {
                "collector_count": len(collectors),
                "unregistered_definition_count": len(unregistered),
                "configured_enabled_count": sum(
                    item["configured_state"] == CollectorConfiguredState.ENABLED.value
                    for item in collectors
                ),
                "desired_running_count": sum(
                    item["desired_state"] == CollectorDesiredState.RUNNING.value
                    for item in collectors
                ),
                "operational_state_counts": dict(
                    sorted(operational_state_counts.items())
                ),
                "health_counts": dict(sorted(health_counts.items())),
                "attention_count": sum(
                    bool(item["needs_attention"]) for item in collectors
                ),
                "state_counts": dict(sorted(state_counts.items())),
                "provider_counts": dict(sorted(provider_counts.items())),
                "active_gap_count": None,
                "accepted_last_minute": sum(
                    int(item["throughput"]["accepted_last_minute"])
                    for item in collectors
                ),
            },
            "unregistered_definitions": unregistered,
            "worker_fleet": {
                "known_count": len(workers),
                "alive_count": sum(bool(item.get("alive")) for item in workers),
                "split_ownership_risk": sum(
                    bool(item.get("alive")) for item in workers
                ) > 1,
                "workers": [
                    self._worker_projection(item, now=now) for item in workers
                ],
                "continuous_supervisor_state": supervisor.get("state"),
            },
            "collectors": collectors,
        }

    @staticmethod
    def _collector_search_text(collector: Mapping[str, Any]) -> str:
        values: list[Any] = [
            collector.get("collector_id"),
            collector.get("collector_kind"),
            collector.get("provider"),
            collector.get("venue"),
            collector.get("operational_state"),
            collector.get("health_status"),
        ]
        for subject in collector.get("subjects") or []:
            values.extend(dict(subject).values())
        for schema in collector.get("fact_schemas") or []:
            values.extend(
                [schema.get("fact_type"), schema.get("schema_version")]
            )
        return " ".join(str(value or "") for value in values).lower()

    def provider_summary_snapshot(
        self, *, attempt_limit: int = 1
    ) -> dict[str, Any]:
        """Return the light fleet stream contract used by operator surfaces."""

        fleet = self.fleet_snapshot(attempt_limit=attempt_limit)
        providers: list[dict[str, Any]] = []
        grouped: dict[str, list[dict[str, Any]]] = {}
        for collector in fleet["collectors"]:
            grouped.setdefault(collector["provider"], []).append(collector)
        for provider, collectors in sorted(grouped.items()):
            operational_counts = Counter(
                item["operational_state"] for item in collectors
            )
            health_counts = Counter(item["health_status"] for item in collectors)
            running = [
                item
                for item in collectors
                if item["operational_state"] == CollectorOperationalState.RUNNING.value
            ]
            running_freshness = [
                float(item["acquisition"]["freshness_seconds"])
                for item in running
                if item["acquisition"].get("freshness_seconds") is not None
            ]
            accepted_times = [
                item["acquisition"].get("last_accepted_fact_at")
                for item in running
                if item["acquisition"].get("last_accepted_fact_at")
            ]
            attention = [item for item in collectors if item["needs_attention"]]
            if health_counts[CollectorHealthStatus.FAILED.value]:
                provider_health = CollectorHealthStatus.FAILED.value
            elif health_counts[CollectorHealthStatus.DELAYED.value]:
                provider_health = CollectorHealthStatus.DELAYED.value
            elif (
                running
                and health_counts[CollectorHealthStatus.HEALTHY.value] == len(running)
            ):
                provider_health = CollectorHealthStatus.HEALTHY.value
            elif running:
                provider_health = CollectorHealthStatus.UNKNOWN.value
            else:
                provider_health = CollectorHealthStatus.NOT_APPLICABLE.value
            schemas = {
                (schema["fact_type"], schema["schema_version"])
                for item in collectors
                for schema in item["fact_schemas"]
            }
            providers.append(
                {
                    "provider": provider,
                    "collector_count": len(collectors),
                    "operational_state_counts": dict(
                        sorted(operational_counts.items())
                    ),
                    "health_counts": dict(sorted(health_counts.items())),
                    "health_status": provider_health,
                    "attention_count": len(attention),
                    "accepted_last_minute": sum(
                        int(item["throughput"]["accepted_last_minute"])
                        for item in collectors
                    ),
                    "freshness_seconds": (
                        max(running_freshness) if running_freshness else None
                    ),
                    "last_accepted_fact_at": (
                        max(accepted_times) if accepted_times else None
                    ),
                    "fact_schema_count": len(schemas),
                    "attention_collectors": [
                        {
                            "collector_id": item["collector_id"],
                            "collector_kind": item["collector_kind"],
                            "display_subject": next(
                                (
                                    subject.get("provider_product_id")
                                    or subject.get("symbol")
                                    or subject.get("instrument_id")
                                    for subject in item["subjects"]
                                ),
                                item["collector_id"],
                            ),
                            "health_status": item["health_status"],
                            "attention_reason": item["attention_reason"],
                            "evidence_at": (
                                item["acquisition"].get("last_attempt_at")
                                or item["acquisition"].get("last_accepted_fact_at")
                                or item["worker"].get("heartbeat_at")
                            ),
                        }
                        for item in attention[:5]
                    ],
                }
            )
        schemas = {
            (schema["fact_type"], schema["schema_version"])
            for collector in fleet["collectors"]
            for schema in collector["fact_schemas"]
        }
        return {
            "schema_version": COLLECTOR_PROVIDER_SUMMARY_VERSION,
            "observed_at": fleet["observed_at"],
            "fleet": {
                **fleet["fleet"],
                "active_schema_count": len(schemas),
            },
            "worker_fleet": {
                key: value
                for key, value in fleet["worker_fleet"].items()
                if key != "workers"
            },
            "providers": providers,
        }

    def collector_page(
        self,
        *,
        provider: str | None = None,
        query: str | None = None,
        attention_only: bool = False,
        offset: int = 0,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Return one bounded page; detail telemetry remains lazy."""

        fleet = self.fleet_snapshot(attempt_limit=1)
        provider_key = str(provider or "").strip().upper()
        needle = str(query or "").strip().lower()
        rows = [
            item
            for item in fleet["collectors"]
            if (not provider_key or item["provider"] == provider_key)
            and (not attention_only or item["needs_attention"])
            and (not needle or needle in self._collector_search_text(item))
        ]
        bounded_offset = max(0, int(offset))
        bounded_limit = max(1, min(int(limit), 100))
        return {
            "schema_version": COLLECTOR_PAGE_VERSION,
            "observed_at": fleet["observed_at"],
            "provider": provider_key or None,
            "query": needle or None,
            "attention_only": bool(attention_only),
            "offset": bounded_offset,
            "limit": bounded_limit,
            "total": len(rows),
            "collectors": rows[
                bounded_offset : bounded_offset + bounded_limit
            ],
        }

    def _find_collector(
        self,
        *,
        collector_kind: CollectorKind | str,
        collector_id: str,
        snapshot: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        kind = CollectorKind(collector_kind)
        fleet = dict(snapshot or self.fleet_snapshot())
        matches = [
            dict(item)
            for item in fleet.get("collectors", [])
            if item["collector_kind"] == kind.value
            and item["collector_id"] == str(collector_id)
        ]
        if len(matches) != 1:
            raise ValueError(
                "collector_unknown: "
                f"collector_kind={kind.value} collector_id={collector_id}"
            )
        return matches[0]

    def detail(
        self,
        *,
        collector_kind: CollectorKind | str,
        collector_id: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        kind = CollectorKind(collector_kind)
        fleet = self.fleet_snapshot()
        collector = self._find_collector(
            collector_kind=kind,
            collector_id=collector_id,
            snapshot=fleet,
        )
        series_ids = [int(item["series_id"]) for item in collector["fact_schemas"]]
        facts = self.operations_repository.recent_facts(
            series_ids=series_ids, limit=limit
        )
        gaps = self.operations_repository.list_gap_evidence(
            series_ids=series_ids, limit=limit
        )
        if kind == CollectorKind.SCHEDULED_FACT:
            events = self.collection_repository.list_attempts(
                definition_id=collector_id, limit=limit
            )
            quality_events: list[dict[str, Any]] = []
            read_only_config = self.collection_repository.list_definitions(
                definition_id=collector_id
            )[0]
        else:
            events = self.operations_repository.list_stream_events(
                definition_id=collector_id, limit=limit
            )
            quality_events = self.operations_repository.list_stream_quality_events(
                definition_id=collector_id, limit=limit
            )
            read_only_config = self.stream_repository.list_stream_definitions(
                definition_id=collector_id
            )[0]
        operations = self.operations_repository.list_operations(
            collector_id=collector_id,
            collector_kind=kind,
            limit=limit,
        )
        return {
            "schema_version": COLLECTOR_DETAIL_VERSION,
            "observed_at": fleet["observed_at"],
            "collector": collector,
            "recent_facts": facts,
            "gaps": gaps,
            "quality_events": quality_events,
            "runtime_events": events,
            "operations": operations,
            "read_only_configuration": {
                key: value
                for key, value in read_only_config.items()
                if key
                not in {
                    "lease_token_hash",
                }
            },
        }

    @staticmethod
    def _diagnostic_item(
        boundary: CollectorDiagnosticBoundary,
        status: CollectorDiagnosticStatus,
        summary: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "boundary": boundary.value,
            "status": status.value,
            "summary": summary,
            "evidence": dict(evidence),
        }

    def diagnose(
        self,
        *,
        collector_kind: CollectorKind | str,
        collector_id: str,
    ) -> dict[str, Any]:
        detail = self.detail(
            collector_kind=collector_kind,
            collector_id=collector_id,
            limit=50,
        )
        collector = detail["collector"]
        boundaries: list[dict[str, Any]] = []
        registration_ok = not collector["registration_errors"]
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.REGISTRATION,
                CollectorDiagnosticStatus.PASS
                if registration_ok
                else CollectorDiagnosticStatus.FAIL,
                "registered collector contract is executable"
                if registration_ok
                else "registered collector contract is invalid",
                {
                    "configured_state": collector["configured_state"],
                    "errors": collector["registration_errors"],
                },
            )
        )
        worker_ok = bool(collector["worker"]["alive"])
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.WORKER,
                CollectorDiagnosticStatus.PASS
                if worker_ok
                else CollectorDiagnosticStatus.FAIL,
                "collector worker heartbeat is current"
                if worker_ok
                else "collector worker heartbeat is unavailable or stale",
                collector["worker"],
            )
        )
        ownership_ok = not bool(
            collector["runtime"].get("active")
            and not collector["runtime"].get("lease_owner")
            and collector["collector_kind"] == CollectorKind.CONTINUOUS_STREAM.value
        )
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.OWNERSHIP,
                CollectorDiagnosticStatus.PASS
                if ownership_ok
                else CollectorDiagnosticStatus.FAIL,
                "ownership evidence is coherent"
                if ownership_ok
                else "active collector has no matching durable lease owner",
                collector["runtime"],
            )
        )
        retry = collector["retry"]
        provider_status = (
            CollectorDiagnosticStatus.FAIL
            if collector["error"]["active"] and retry.get("active")
            else CollectorDiagnosticStatus.PASS
            if collector["acquisition"]["last_provider_success_at"]
            else CollectorDiagnosticStatus.UNKNOWN
        )
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.PROVIDER,
                provider_status,
                "provider acquisition has succeeded"
                if provider_status == CollectorDiagnosticStatus.PASS
                else "provider acquisition is failing"
                if provider_status == CollectorDiagnosticStatus.FAIL
                else "provider success has not yet been observed",
                {
                    "last_provider_success_at": collector["acquisition"][
                        "last_provider_success_at"
                    ],
                    "retry": retry,
                    "error": collector["error"],
                },
            )
        )
        reject_count = len(detail["quality_events"]) + int(
            collector["throughput"].get("rejected_recent") or 0
        )
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.CANONICALIZATION,
                CollectorDiagnosticStatus.WARNING
                if reject_count
                else CollectorDiagnosticStatus.PASS,
                "recent malformed or rejected observations exist"
                if reject_count
                else "no recent canonicalization rejection evidence",
                {
                    "recent_reject_count": reject_count,
                    "quality_event_count": len(detail["quality_events"]),
                },
            )
        )
        schema_rejects = [
            item
            for item in detail["quality_events"]
            if "schema" in str(item.get("classification") or "").lower()
        ]
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.SCHEMA,
                CollectorDiagnosticStatus.FAIL
                if schema_rejects
                else CollectorDiagnosticStatus.PASS,
                "schema rejection evidence exists"
                if schema_rejects
                else "registered schemas have no recent rejection evidence",
                {"schema_reject_count": len(schema_rejects)},
            )
        )
        persisted = bool(collector["acquisition"]["last_accepted_fact_at"])
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.PERSISTENCE,
                CollectorDiagnosticStatus.PASS
                if persisted
                else CollectorDiagnosticStatus.UNKNOWN,
                "canonical facts are durably present"
                if persisted
                else "no accepted canonical fact is yet visible",
                {
                    "last_accepted_fact_at": collector["acquisition"][
                        "last_accepted_fact_at"
                    ],
                    "recent_fact_count": len(detail["recent_facts"]),
                },
            )
        )
        freshness_ok = collector["acquisition"]["freshness_ok"]
        freshness_basis = str(
            collector["acquisition"].get("freshness_basis") or "accepted_fact"
        )
        freshness_subject = (
            "provider activity"
            if freshness_basis == "provider_activity"
            else "accepted facts"
        )
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.FRESHNESS,
                CollectorDiagnosticStatus.PASS
                if freshness_ok is True
                else CollectorDiagnosticStatus.FAIL
                if freshness_ok is False
                else CollectorDiagnosticStatus.UNKNOWN,
                f"{freshness_subject} is fresh"
                if freshness_ok is True
                else f"{freshness_subject} is stale"
                if freshness_ok is False
                else "freshness cannot be established yet",
                {
                    "freshness_seconds": collector["acquisition"][
                        "freshness_seconds"
                    ],
                    "freshness_ok": freshness_ok,
                    "freshness_basis": freshness_basis,
                    "freshness_threshold_seconds": collector["acquisition"].get(
                        "freshness_threshold_seconds"
                    ),
                },
            )
        )
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.GAPS_RECOVERY,
                CollectorDiagnosticStatus.WARNING
                if detail["gaps"]
                else CollectorDiagnosticStatus.PASS,
                "gap evidence requires inspection"
                if detail["gaps"]
                else "no recent gap evidence is recorded",
                {
                    "gap_evidence_count": len(detail["gaps"]),
                    "recovery_supported": collector["capabilities"]["recovery"],
                },
            )
        )
        scheduler_status = (
            CollectorDiagnosticStatus.PASS
            if collector["collector_kind"] == CollectorKind.SCHEDULED_FACT.value
            and collector["acquisition"].get("next_scheduled_at")
            else CollectorDiagnosticStatus.UNKNOWN
        )
        boundaries.append(
            self._diagnostic_item(
                CollectorDiagnosticBoundary.SCHEDULER,
                scheduler_status,
                "durable schedule is present"
                if scheduler_status == CollectorDiagnosticStatus.PASS
                else "collector is stream-triggered; no poll schedule applies",
                {
                    "next_scheduled_at": collector["acquisition"].get(
                        "next_scheduled_at"
                    ),
                    "trigger": collector["acquisition"].get("trigger"),
                },
            )
        )
        failures = [
            item
            for item in boundaries
            if item["status"] == CollectorDiagnosticStatus.FAIL.value
        ]
        warnings = [
            item
            for item in boundaries
            if item["status"] == CollectorDiagnosticStatus.WARNING.value
        ]
        likely_boundary = (failures or warnings or [None])[0]
        if not failures and not warnings:
            recommended_action = "no_action"
        elif likely_boundary["boundary"] == CollectorDiagnosticBoundary.WORKER.value:
            recommended_action = "restart_collector_worker"
        elif likely_boundary["boundary"] in {
            CollectorDiagnosticBoundary.PROVIDER.value,
            CollectorDiagnosticBoundary.FRESHNESS.value,
        }:
            recommended_action = "retry_health_probe"
        elif likely_boundary["boundary"] == CollectorDiagnosticBoundary.GAPS_RECOVERY.value:
            recommended_action = (
                "run_bounded_recovery"
                if collector["capabilities"]["recovery"]
                else "inspect_gap_evidence"
            )
        else:
            recommended_action = "investigate_evidence"
        return {
            "schema_version": COLLECTOR_DIAGNOSTIC_VERSION,
            "observed_at": detail["observed_at"],
            "collector_id": collector["collector_id"],
            "collector_kind": collector["collector_kind"],
            "actual_state": collector["actual_state"],
            "likely_failing_boundary": (
                likely_boundary["boundary"] if likely_boundary else None
            ),
            "recommended_action": recommended_action,
            "boundaries": boundaries,
        }

    def execute_action(
        self,
        *,
        request_id: str,
        collector_kind: CollectorKind | str,
        collector_id: str,
        action: CollectorAction | str,
        requested_at: datetime,
        actor_id: str,
        confirmation: str | None,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        kind = CollectorKind(collector_kind)
        normalized_action = CollectorAction(action)
        if not normalized_action.mutates:
            result = (
                self.diagnose(collector_kind=kind, collector_id=collector_id)
                if normalized_action == CollectorAction.DIAGNOSE
                else self._find_collector(
                    collector_kind=kind, collector_id=collector_id
                )
            )
            return {
                "schema_version": COLLECTOR_OPERATION_VERSION,
                "action": normalized_action.value,
                "mutated": False,
                "result": result,
            }
        expected_confirmation = (
            f"{kind.value}:{collector_id}:{normalized_action.value}"
        )
        confirmation_error = None
        if (
            normalized_action.requires_confirmation
            and confirmation != expected_confirmation
        ):
            confirmation_error = (
                "collector_operation_confirmation_required: "
                f"confirmation={expected_confirmation}"
            )
        try:
            current = self._find_collector(
                collector_kind=kind, collector_id=collector_id
            )
        except ValueError as exc:
            if not str(exc).startswith("collector_unknown:"):
                raise
            current = None
        registration_error = None
        if (
            current is not None
            and current["registration_errors"]
            and normalized_action != CollectorAction.STOP
        ):
            registration_error = (
                "collector_operation_registration_invalid: "
                "inspect registration diagnostics"
            )
        operation = self.operations_repository.apply_lifecycle_action(
            request_id=request_id,
            collector_id=collector_id,
            collector_kind=kind,
            action=normalized_action,
            requested_at=requested_at,
            actor_id=actor_id,
            context=context,
            precondition_error=confirmation_error or registration_error,
        )
        mutated = (
            operation["status"] == "succeeded"
            and not bool(operation.get("idempotent_replay"))
        )
        resulting_collector = current
        if mutated:
            resulting_collector = self._find_collector(
                collector_kind=kind, collector_id=collector_id
            )
        return {
            "schema_version": COLLECTOR_OPERATION_VERSION,
            "action": normalized_action.value,
            "mutated": mutated,
            "operation": operation,
            "collector": resulting_collector,
        }

    def event_catalog(
        self,
        *,
        collector_kind: CollectorKind | str,
        collector_id: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        detail = self.detail(
            collector_kind=collector_kind,
            collector_id=collector_id,
            limit=limit,
        )
        timeline: list[dict[str, Any]] = []
        for operation in detail["operations"]:
            timeline.append(
                {
                    "occurred_at": operation["requested_at"],
                    "event_type": f"operation.{operation['action']}",
                    "status": operation["status"],
                    "evidence": operation,
                }
            )
        for event in detail["runtime_events"]:
            timeline.append(
                {
                    "occurred_at": _iso(
                        event.get("occurred_at")
                        or event.get("started_at")
                        or event.get("scheduled_for")
                    ),
                    "event_type": str(
                        event.get("event_type")
                        or f"attempt.{event.get('status', 'unknown')}"
                    ),
                    "status": event.get("status"),
                    "evidence": event,
                }
            )
        for event in detail["quality_events"]:
            timeline.append(
                {
                    "occurred_at": _iso(event.get("detected_at")),
                    "event_type": f"quality.{event.get('classification')}",
                    "status": "warning",
                    "evidence": event,
                }
            )
        timeline.sort(
            key=lambda item: item.get("occurred_at") or "",
            reverse=True,
        )
        return {
            "schema_version": COLLECTOR_EVENT_CATALOG_VERSION,
            "collector_id": collector_id,
            "collector_kind": CollectorKind(collector_kind).value,
            "events": timeline[: max(1, min(int(limit), 500))],
        }

    def gap_catalog(
        self,
        *,
        collector_kind: CollectorKind | str,
        collector_id: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        detail = self.detail(
            collector_kind=collector_kind,
            collector_id=collector_id,
            limit=limit,
        )
        return {
            "schema_version": COLLECTOR_GAP_CATALOG_VERSION,
            "collector_id": collector_id,
            "collector_kind": CollectorKind(collector_kind).value,
            "gaps": detail["gaps"],
            "quality_events": detail["quality_events"],
        }

    def data_plane_snapshot(self) -> dict[str, Any]:
        fleet = self.fleet_snapshot()
        schemas = {
            (schema["fact_type"], schema["schema_version"])
            for collector in fleet["collectors"]
            for schema in collector["fact_schemas"]
        }
        return {
            "schema_version": MARKET_DATA_PLANE_OPERATIONAL_VERSION,
            "observed_at": fleet["observed_at"],
            "providers": fleet["fleet"]["provider_counts"],
            "collector_health": fleet["fleet"]["state_counts"],
            "ingestion_rate_per_minute": fleet["fleet"][
                "accepted_last_minute"
            ],
            "active_schema_count": len(schemas),
            "active_schemas": [
                {"fact_type": fact_type, "schema_version": schema_version}
                for fact_type, schema_version in sorted(schemas)
            ],
            "active_gap_count": fleet["fleet"]["active_gap_count"],
            "stale_stream_count": sum(
                collector["acquisition"]["freshness_ok"] is False
                for collector in fleet["collectors"]
            ),
            "database_write_latency": None,
            "storage_growth": None,
        }


collector_operations_service = CollectorOperationsService()


__all__ = ["CollectorOperationsService", "collector_operations_service"]
