"""Backend-owned bot startup lifecycle contract and shared checkpoint helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Dict, Mapping, MutableMapping, Optional


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def utc_now_iso() -> str:
    return utc_now().isoformat() + "Z"


class LifecycleOwner(str, Enum):
    BACKEND = "backend"
    CONTAINER = "container"
    RUNTIME = "runtime"
    WATCHDOG = "watchdog"


class BotLifecycleStatus(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    TELEMETRY_DEGRADED = "telemetry_degraded"
    STARTUP_FAILED = "startup_failed"
    CRASHED = "crashed"
    STOPPED = "stopped"
    COMPLETED = "completed"
    IDLE = "idle"


class BotLifecyclePhase(str, Enum):
    START_REQUESTED = "start_requested"
    VALIDATING_CONFIGURATION = "validating_configuration"
    RESOLVING_STRATEGY = "resolving_strategy"
    RESOLVING_RUNTIME_DEPENDENCIES = "resolving_runtime_dependencies"
    PREPARING_RUN = "preparing_run"
    STAMPING_STARTING_STATE = "stamping_starting_state"
    LAUNCHING_CONTAINER = "launching_container"
    CONTAINER_LAUNCHED = "container_launched"
    AWAITING_CONTAINER_BOOT = "awaiting_container_boot"
    CONTAINER_BOOTING = "container_booting"
    LOADING_BOT_CONFIG = "loading_bot_config"
    CLAIMING_RUN = "claiming_run"
    LOADING_STRATEGY_SNAPSHOT = "loading_strategy_snapshot"
    PREPARING_WALLET = "preparing_wallet"
    PLANNING_SERIES_WORKERS = "planning_series_workers"
    SPAWNING_SERIES_WORKERS = "spawning_series_workers"
    WAITING_FOR_SERIES_BOOTSTRAP = "waiting_for_series_bootstrap"
    WARMING_UP_RUNTIME = "warming_up_runtime"
    RUNTIME_SUBSCRIBING = "runtime_subscribing"
    AWAITING_FIRST_SNAPSHOT = "awaiting_first_snapshot"
    LIVE = "live"
    DEGRADED = "degraded"
    TELEMETRY_DEGRADED = "telemetry_degraded"
    STARTUP_FAILED = "startup_failed"
    CRASHED = "crashed"
    STOPPED = "stopped"
    COMPLETED = "completed"


BACKEND_OWNED_PHASES = frozenset(
    {
        BotLifecyclePhase.START_REQUESTED.value,
        BotLifecyclePhase.VALIDATING_CONFIGURATION.value,
        BotLifecyclePhase.RESOLVING_STRATEGY.value,
        BotLifecyclePhase.RESOLVING_RUNTIME_DEPENDENCIES.value,
        BotLifecyclePhase.PREPARING_RUN.value,
        BotLifecyclePhase.STAMPING_STARTING_STATE.value,
        BotLifecyclePhase.LAUNCHING_CONTAINER.value,
        BotLifecyclePhase.CONTAINER_LAUNCHED.value,
        BotLifecyclePhase.AWAITING_CONTAINER_BOOT.value,
    }
)

CONTAINER_REPORTED_PHASES = frozenset(
    {
        BotLifecyclePhase.CONTAINER_BOOTING.value,
        BotLifecyclePhase.LOADING_BOT_CONFIG.value,
        BotLifecyclePhase.CLAIMING_RUN.value,
        BotLifecyclePhase.LOADING_STRATEGY_SNAPSHOT.value,
        BotLifecyclePhase.PREPARING_WALLET.value,
        BotLifecyclePhase.PLANNING_SERIES_WORKERS.value,
        BotLifecyclePhase.SPAWNING_SERIES_WORKERS.value,
        BotLifecyclePhase.WAITING_FOR_SERIES_BOOTSTRAP.value,
        BotLifecyclePhase.WARMING_UP_RUNTIME.value,
        BotLifecyclePhase.RUNTIME_SUBSCRIBING.value,
        BotLifecyclePhase.AWAITING_FIRST_SNAPSHOT.value,
        BotLifecyclePhase.LIVE.value,
        BotLifecyclePhase.DEGRADED.value,
        BotLifecyclePhase.TELEMETRY_DEGRADED.value,
        BotLifecyclePhase.STARTUP_FAILED.value,
        BotLifecyclePhase.CRASHED.value,
        BotLifecyclePhase.STOPPED.value,
        BotLifecyclePhase.COMPLETED.value,
    }
)

TERMINAL_PHASES = frozenset(
    {
        BotLifecyclePhase.STARTUP_FAILED.value,
        BotLifecyclePhase.CRASHED.value,
        BotLifecyclePhase.STOPPED.value,
        BotLifecyclePhase.COMPLETED.value,
    }
)

ACTIVE_PHASES = frozenset(
    {
        BotLifecyclePhase.START_REQUESTED.value,
        BotLifecyclePhase.VALIDATING_CONFIGURATION.value,
        BotLifecyclePhase.RESOLVING_STRATEGY.value,
        BotLifecyclePhase.RESOLVING_RUNTIME_DEPENDENCIES.value,
        BotLifecyclePhase.PREPARING_RUN.value,
        BotLifecyclePhase.STAMPING_STARTING_STATE.value,
        BotLifecyclePhase.LAUNCHING_CONTAINER.value,
        BotLifecyclePhase.CONTAINER_LAUNCHED.value,
        BotLifecyclePhase.AWAITING_CONTAINER_BOOT.value,
        BotLifecyclePhase.CONTAINER_BOOTING.value,
        BotLifecyclePhase.LOADING_BOT_CONFIG.value,
        BotLifecyclePhase.CLAIMING_RUN.value,
        BotLifecyclePhase.LOADING_STRATEGY_SNAPSHOT.value,
        BotLifecyclePhase.PREPARING_WALLET.value,
        BotLifecyclePhase.PLANNING_SERIES_WORKERS.value,
        BotLifecyclePhase.SPAWNING_SERIES_WORKERS.value,
        BotLifecyclePhase.WAITING_FOR_SERIES_BOOTSTRAP.value,
        BotLifecyclePhase.WARMING_UP_RUNTIME.value,
        BotLifecyclePhase.RUNTIME_SUBSCRIBING.value,
        BotLifecyclePhase.AWAITING_FIRST_SNAPSHOT.value,
        BotLifecyclePhase.LIVE.value,
        BotLifecyclePhase.DEGRADED.value,
        BotLifecyclePhase.TELEMETRY_DEGRADED.value,
    }
)


def status_for_phase(phase: str) -> str:
    if phase == BotLifecyclePhase.LIVE.value:
        return BotLifecycleStatus.RUNNING.value
    if phase == BotLifecyclePhase.DEGRADED.value:
        return BotLifecycleStatus.DEGRADED.value
    if phase == BotLifecyclePhase.TELEMETRY_DEGRADED.value:
        return BotLifecycleStatus.TELEMETRY_DEGRADED.value
    if phase == BotLifecyclePhase.STARTUP_FAILED.value:
        return BotLifecycleStatus.STARTUP_FAILED.value
    if phase == BotLifecyclePhase.CRASHED.value:
        return BotLifecycleStatus.CRASHED.value
    if phase == BotLifecyclePhase.STOPPED.value:
        return BotLifecycleStatus.STOPPED.value
    if phase == BotLifecyclePhase.COMPLETED.value:
        return BotLifecycleStatus.COMPLETED.value
    if phase in ACTIVE_PHASES:
        return BotLifecycleStatus.STARTING.value
    return BotLifecycleStatus.IDLE.value


def terminal_status_after_supervision(
    *,
    startup_live_emitted: bool,
    degraded_symbols_count: int,
    telemetry_degraded: bool,
) -> tuple[str, str]:
    if int(degraded_symbols_count or 0) > 0:
        if startup_live_emitted:
            return BotLifecyclePhase.DEGRADED.value, BotLifecycleStatus.DEGRADED.value
        return BotLifecyclePhase.STARTUP_FAILED.value, BotLifecycleStatus.STARTUP_FAILED.value
    if telemetry_degraded:
        return BotLifecyclePhase.TELEMETRY_DEGRADED.value, BotLifecycleStatus.TELEMETRY_DEGRADED.value
    return BotLifecyclePhase.COMPLETED.value, BotLifecycleStatus.COMPLETED.value


def deep_merge_dict(base: Mapping[str, Any] | None, incoming: Mapping[str, Any] | None) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base or {})
    for key, value in dict(incoming or {}).items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[str(key)] = deep_merge_dict(existing, value)
            continue
        merged[str(key)] = value
    return merged


@dataclass
class BotStartupContext:
    bot_id: str
    bot_record: Dict[str, Any]
    run_id: str
    strategy_id: str
    strategy_snapshot: Any
    wallet_config: Dict[str, Any]
    runtime_readiness: Dict[str, Any]
    runtime_dependency_metadata: Dict[str, Any]
    started_at: str = field(default_factory=utc_now_iso)
    lifecycle_metadata: Dict[str, Any] = field(default_factory=dict)
    current_phase: str = BotLifecyclePhase.START_REQUESTED.value
    container_id: Optional[str] = None

    def update_metadata(self, payload: Mapping[str, Any] | None = None) -> Dict[str, Any]:
        self.lifecycle_metadata = deep_merge_dict(self.lifecycle_metadata, payload)
        return dict(self.lifecycle_metadata)


def build_failure_payload(
    *,
    phase: str,
    message: str,
    error_type: str | None = None,
    at: str | None = None,
    type: str | None = None,
    reason_code: str | None = None,
    owner: str | None = None,
    worker_id: str | None = None,
    symbol: str | None = None,
    exit_code: int | None = None,
    stderr_tail: str | None = None,
    exception_type: str | None = None,
    traceback: str | None = None,
    symbols: list[str] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "phase": str(phase or "").strip(),
        "message": str(message or "").strip(),
        "at": at or utc_now_iso(),
    }
    if error_type:
        payload["error_type"] = str(error_type)
    if type:
        payload["type"] = str(type)
    if reason_code:
        payload["reason_code"] = str(reason_code)
    if owner:
        payload["owner"] = str(owner)
    if worker_id:
        payload["worker_id"] = str(worker_id)
    if symbol:
        payload["symbol"] = str(symbol).strip().upper()
    if exit_code is not None:
        payload["exit_code"] = int(exit_code)
    if stderr_tail:
        payload["stderr_tail"] = str(stderr_tail)
    resolved_exception_type = exception_type or error_type
    if resolved_exception_type:
        payload["exception_type"] = str(resolved_exception_type)
    if traceback:
        payload["traceback"] = str(traceback)
    if symbols:
        normalized_symbols = [str(entry).strip().upper() for entry in symbols if str(entry).strip()]
        if normalized_symbols:
            payload["symbols"] = normalized_symbols
    return payload


def lifecycle_checkpoint_payload(
    *,
    bot_id: str,
    run_id: str,
    phase: str,
    owner: str,
    message: str,
    metadata: Mapping[str, Any] | None = None,
    failure: Mapping[str, Any] | None = None,
    checkpoint_at: str | None = None,
    status: str | None = None,
) -> Dict[str, Any]:
    resolved_phase = str(phase or "").strip()
    return {
        "bot_id": str(bot_id or "").strip(),
        "run_id": str(run_id or "").strip(),
        "phase": resolved_phase,
        "status": str(status or status_for_phase(resolved_phase)).strip(),
        "owner": str(owner or "").strip(),
        "message": str(message or "").strip(),
        "metadata": dict(metadata or {}),
        "failure": dict(failure or {}),
        "checkpoint_at": checkpoint_at or utc_now_iso(),
    }


def build_series_progress_metadata(
    *,
    total_series: int,
    workers_planned: int,
    workers_spawned: int,
    series: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    series_map = {str(key): dict(value or {}) for key, value in dict(series or {}).items()}
    bootstrapped = sorted(key for key, value in series_map.items() if str(value.get("status") or "") == "bootstrapped")
    warming = sorted(key for key, value in series_map.items() if str(value.get("status") or "") == "warming_up")
    live = sorted(key for key, value in series_map.items() if str(value.get("status") or "") == "live")
    failed = sorted(key for key, value in series_map.items() if str(value.get("status") or "") == "failed")
    awaiting_first_snapshot = sorted(
        key for key, value in series_map.items() if str(value.get("status") or "") == "awaiting_first_snapshot"
    )
    return {
        "series_progress": {
            "total_series": max(0, int(total_series or 0)),
            "workers_planned": max(0, int(workers_planned or 0)),
            "workers_spawned": max(0, int(workers_spawned or 0)),
            "bootstrapped_series": bootstrapped,
            "warming_series": warming,
            "awaiting_first_snapshot_series": awaiting_first_snapshot,
            "live_series": live,
            "failed_series": failed,
            "series": series_map,
        }
    }


__all__ = [
    "ACTIVE_PHASES",
    "BACKEND_OWNED_PHASES",
    "BotLifecyclePhase",
    "BotLifecycleStatus",
    "BotStartupContext",
    "CONTAINER_REPORTED_PHASES",
    "LifecycleOwner",
    "TERMINAL_PHASES",
    "build_failure_payload",
    "build_series_progress_metadata",
    "deep_merge_dict",
    "lifecycle_checkpoint_payload",
    "status_for_phase",
    "utc_now",
    "utc_now_iso",
]
