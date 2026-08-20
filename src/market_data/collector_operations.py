"""Provider-neutral collector operational contract vocabulary."""

from __future__ import annotations

from enum import Enum


COLLECTOR_OPERATIONAL_SNAPSHOT_VERSION = "market.collector_operational_snapshot.v1"
COLLECTOR_PROVIDER_SUMMARY_VERSION = "market.collector_provider_summary.v1"
COLLECTOR_PAGE_VERSION = "market.collector_page.v1"
COLLECTOR_DETAIL_VERSION = "market.collector_operational_detail.v1"
COLLECTOR_DIAGNOSTIC_VERSION = "market.collector_diagnostic.v1"
COLLECTOR_OPERATION_VERSION = "market.collector_operation.v1"
COLLECTOR_EVENT_CATALOG_VERSION = "market.collector_event_catalog.v1"
COLLECTOR_GAP_CATALOG_VERSION = "market.collector_gap_catalog.v1"
MARKET_DATA_PLANE_OPERATIONAL_VERSION = "market.data_plane_operational_snapshot.v1"


class CollectorKind(str, Enum):
    SCHEDULED_FACT = "scheduled_fact"
    CONTINUOUS_STREAM = "continuous_stream"


class CollectorConfiguredState(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    INVALID = "invalid"


class CollectorDesiredState(str, Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    PAUSED = "paused"


class CollectorActualState(str, Enum):
    DISABLED = "DISABLED"
    STOPPED = "STOPPED"
    PAUSED = "PAUSED"
    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    RETRYING = "RETRYING"
    RECOVERING = "RECOVERING"
    FAILED = "FAILED"
    STOPPING = "STOPPING"


class CollectorOperationalState(str, Enum):
    """Operator intent/runtime state, independent of health."""

    DISABLED = "DISABLED"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"


class CollectorHealthStatus(str, Enum):
    """Health only has meaning while a collector is expected to run."""

    HEALTHY = "HEALTHY"
    DELAYED = "DELAYED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class CollectorAction(str, Enum):
    START = "start"
    STOP = "stop"
    RESTART = "restart"
    PAUSE = "pause"
    RESUME = "resume"
    HEALTH_PROBE = "health_probe"
    DIAGNOSE = "diagnose"
    RECOVER = "recover"

    @property
    def mutates(self) -> bool:
        return self in {
            CollectorAction.START,
            CollectorAction.STOP,
            CollectorAction.RESTART,
            CollectorAction.PAUSE,
            CollectorAction.RESUME,
            CollectorAction.RECOVER,
        }

    @property
    def requires_confirmation(self) -> bool:
        return self in {
            CollectorAction.STOP,
            CollectorAction.RESTART,
            CollectorAction.RECOVER,
        }


class CollectorDiagnosticBoundary(str, Enum):
    REGISTRATION = "registration"
    WORKER = "worker"
    SCHEDULER = "scheduler"
    OWNERSHIP = "ownership"
    PROVIDER = "provider"
    CANONICALIZATION = "canonicalization"
    SCHEMA = "schema"
    PERSISTENCE = "persistence"
    FRESHNESS = "freshness"
    GAPS_RECOVERY = "gaps_recovery"


class CollectorDiagnosticStatus(str, Enum):
    PASS = "pass"
    WARNING = "warning"
    FAIL = "fail"
    UNKNOWN = "unknown"


class CollectorOperationStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


__all__ = [
    "COLLECTOR_DETAIL_VERSION",
    "COLLECTOR_DIAGNOSTIC_VERSION",
    "COLLECTOR_EVENT_CATALOG_VERSION",
    "COLLECTOR_GAP_CATALOG_VERSION",
    "COLLECTOR_OPERATION_VERSION",
    "COLLECTOR_OPERATIONAL_SNAPSHOT_VERSION",
    "COLLECTOR_PROVIDER_SUMMARY_VERSION",
    "COLLECTOR_PAGE_VERSION",
    "MARKET_DATA_PLANE_OPERATIONAL_VERSION",
    "CollectorAction",
    "CollectorActualState",
    "CollectorOperationalState",
    "CollectorHealthStatus",
    "CollectorConfiguredState",
    "CollectorDesiredState",
    "CollectorDiagnosticBoundary",
    "CollectorDiagnosticStatus",
    "CollectorKind",
    "CollectorOperationStatus",
]
