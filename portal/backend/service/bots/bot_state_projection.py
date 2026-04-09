"""Pure authoritative bot lifecycle projection for API and SSE consumers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .botlens_contract import projection_only
from .runner import DockerBotRunner
from .startup_lifecycle import ACTIVE_PHASES, BACKEND_OWNED_PHASES, TERMINAL_PHASES

_ACTIVE_STATUSES = {"starting", "running", "paused", "degraded", "telemetry_degraded"}
_TERMINAL_STATUSES = {"idle", "stopped", "completed", "error", "failed", "crashed", "startup_failed"}


def _normalize_status(value: Any, default: str = "idle") -> str:
    text = str(value or "").strip().lower()
    return text or default


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    return list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) else []


def _parse_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _heartbeat_projection(
    bot: Mapping[str, Any],
    *,
    claimed_active: bool,
    heartbeat_stale_ms: int,
) -> Dict[str, Any]:
    heartbeat_at = _parse_timestamp(bot.get("heartbeat_at"))
    runner_id = str(bot.get("runner_id") or "").strip() or None
    age_ms: Optional[int] = None
    if heartbeat_at is not None:
        age_ms = max(0, int((datetime.now(timezone.utc) - heartbeat_at).total_seconds() * 1000.0))
    if heartbeat_at is None:
        state = "missing" if claimed_active or runner_id else "inactive"
    elif age_ms is not None and age_ms > heartbeat_stale_ms:
        state = "stale"
    else:
        state = "fresh"
    return {
        "runner_id": runner_id,
        "at": bot.get("heartbeat_at"),
        "age_ms": age_ms,
        "state": state,
        "fresh": state == "fresh",
        "stale": state == "stale",
    }


def _default_container_state(bot_id: str) -> Dict[str, Any]:
    return {
        "name": DockerBotRunner.container_name_for(bot_id),
        "status": "missing",
        "running": False,
        "id": None,
        "started_at": None,
        "finished_at": None,
        "exit_code": None,
        "error": None,
    }


def _resolve_status(
    *,
    persisted_status: str,
    lifecycle_status: str,
    engine_status: str,
    container_status: str,
    container_running: bool,
    heartbeat_state: str,
) -> str:
    if lifecycle_status in _TERMINAL_STATUSES and container_running:
        if engine_status in _ACTIVE_STATUSES:
            return engine_status
        if persisted_status in _ACTIVE_STATUSES:
            return persisted_status
        return "degraded"
    if lifecycle_status in _ACTIVE_STATUSES | _TERMINAL_STATUSES:
        if lifecycle_status in {"starting", "running", "degraded", "telemetry_degraded"} and container_status in {"exited", "dead"}:
            return "crashed"
        return lifecycle_status
    if container_running and engine_status:
        return engine_status
    if container_status in {"exited", "dead"}:
        return "crashed"
    if heartbeat_state == "stale" and persisted_status in _ACTIVE_STATUSES:
        return "crashed"
    if persisted_status in _ACTIVE_STATUSES | _TERMINAL_STATUSES:
        return persisted_status
    return "idle"


def _default_phase_for_status(status: str, *, container_running: bool, has_view_state: bool) -> str:
    if status == "starting":
        return "container_booting" if container_running else "launching_container"
    if status in {"running", "paused"}:
        return "live" if has_view_state else "awaiting_first_snapshot"
    if status in {"degraded", "telemetry_degraded"}:
        return status
    if status in {"startup_failed", "crashed", "stopped", "completed"}:
        return status
    return "idle"


def _resolve_reason(
    *,
    status: str,
    phase: str,
    container_status: str,
    heartbeat_state: str,
    selected_run_id: Optional[str],
) -> str:
    if phase == "launching_container":
        return "container_start_pending"
    if phase in {"container_booting", "loading_bot_config", "claiming_run"}:
        return "runtime_booting"
    if phase in {"awaiting_first_snapshot", "waiting_for_series_bootstrap"}:
        return "awaiting_first_snapshot"
    if phase == "live":
        return "live_runtime"
    if phase in {"degraded", "telemetry_degraded"}:
        return "runtime_degraded"
    if phase == "startup_failed":
        return "startup_failed"
    if phase == "completed":
        return "run_completed"
    if phase == "stopped":
        return "run_stopped"
    if phase == "crashed":
        if container_status in {"exited", "dead"}:
            return "container_exited"
        if container_status == "missing" and selected_run_id:
            return "container_missing"
        if heartbeat_state == "stale":
            return "runner_stale"
        return "runtime_crashed"
    return status or "idle"


def _crash_summary(
    *,
    status: str,
    phase: str,
    reason: str,
    failure: Dict[str, Any],
) -> Optional[str]:
    if status not in {"crashed", "startup_failed"}:
        return None
    error_type = failure.get("error_type") or ""
    failed_at = failure.get("phase") or phase
    if status == "startup_failed":
        label = f"Startup failed at {failed_at}"
        return f"{label}: {error_type}" if error_type else label
    if reason == "container_exited":
        return f"Container exited during {failed_at}"
    if reason == "container_missing":
        return f"Container disappeared during {failed_at}"
    if reason == "runner_stale":
        return "Runner became unresponsive"
    return f"Runtime crashed during {failed_at}"


def _resolve_controls(*, status: str, phase: str, container_running: bool, has_run: bool) -> Dict[str, Any]:
    active = status in _ACTIVE_STATUSES
    can_start = not active or not container_running
    start_label = "Start"
    if status == "completed":
        start_label = "Rerun"
    elif status in {"crashed", "error", "failed", "startup_failed", "stopped", "degraded", "telemetry_degraded"}:
        start_label = "Restart"
    elif phase in ACTIVE_PHASES:
        start_label = "Starting"
    return {
        "can_start": bool(can_start),
        "can_stop": bool(container_running or active or phase in ACTIVE_PHASES),
        "can_open_lens": bool(has_run),
        "can_delete": not active and phase not in ACTIVE_PHASES,
        "start_label": start_label,
    }


def project_bot_state(
    bot: Mapping[str, Any],
    *,
    run: Mapping[str, Any] | None = None,
    lifecycle: Mapping[str, Any] | None = None,
    view_row: Mapping[str, Any] | None = None,
    container_state: Mapping[str, Any] | None = None,
    heartbeat_stale_ms: int = 30000,
) -> Dict[str, Any]:
    payload = dict(bot or {})
    bot_id = str(payload.get("id") or "").strip()
    if not bot_id:
        raise ValueError("bot id is required for projection")

    selected_run = _mapping(run)
    lifecycle_row = _mapping(lifecycle)
    selected_run_id = (
        str(lifecycle_row.get("run_id") or "").strip()
        or str(selected_run.get("run_id") or "").strip()
        or None
    )
    view_payload = projection_only(_mapping(view_row).get("payload"))
    snapshot_runtime = _mapping(view_payload.get("runtime"))
    warnings = _sequence(view_payload.get("warnings"))
    engine_status = _normalize_status(snapshot_runtime.get("status"), default="")
    persisted_status = _normalize_status(payload.get("status"))
    lifecycle_status = _normalize_status(lifecycle_row.get("status"), default="")
    claimed_active = bool(selected_run_id) and (
        persisted_status in _ACTIVE_STATUSES
        or lifecycle_status in _ACTIVE_STATUSES
        or bool(payload.get("runner_id"))
    )
    heartbeat = _heartbeat_projection(payload, claimed_active=claimed_active, heartbeat_stale_ms=heartbeat_stale_ms)
    container = dict(container_state or _default_container_state(bot_id))
    status = _resolve_status(
        persisted_status=persisted_status,
        lifecycle_status=lifecycle_status,
        engine_status=engine_status,
        container_status=str(container.get("status") or ""),
        container_running=bool(container.get("running")),
        heartbeat_state=str(heartbeat.get("state") or ""),
    )
    lifecycle_phase = str(lifecycle_row.get("phase") or "").strip()
    default_phase = _default_phase_for_status(
        status,
        container_running=bool(container.get("running")),
        has_view_state=bool(view_row),
    )
    phase = default_phase if lifecycle_phase in TERMINAL_PHASES and status in _ACTIVE_STATUSES else (lifecycle_phase or default_phase)
    reason = _resolve_reason(
        status=status,
        phase=phase,
        container_status=str(container.get("status") or ""),
        heartbeat_state=str(heartbeat.get("state") or ""),
        selected_run_id=selected_run_id,
    )

    runtime_stats = _mapping(snapshot_runtime.get("stats"))
    if not runtime_stats:
        runtime_stats = _mapping(selected_run.get("summary")) or _mapping(payload.get("last_stats"))
    telemetry = {
        "run_id": selected_run_id,
        "seq": int(_mapping(view_row).get("seq") or 0) if view_row else 0,
        "available": bool(view_row),
        "known_at": _mapping(view_row).get("known_at") if view_row else None,
        "last_snapshot_at": _mapping(view_row).get("event_time") if view_row else None,
        "warning_count": len(warnings),
        "series_count": len(_sequence(view_payload.get("series"))),
        "trade_count": len(_sequence(view_payload.get("trades"))),
        "engine_status": engine_status or None,
        "worker_count": int(snapshot_runtime.get("worker_count") or 0),
        "active_workers": int(snapshot_runtime.get("active_workers") or 0),
    }
    runtime = {
        **snapshot_runtime,
        "status": status,
        "phase": phase,
        "engine_status": engine_status or None,
        "run_id": selected_run_id,
        "seq": telemetry["seq"],
        "known_at": telemetry["known_at"],
        "last_snapshot_at": telemetry["last_snapshot_at"],
        "warnings": warnings,
        "stats": runtime_stats,
        "started_at": snapshot_runtime.get("started_at") or selected_run.get("started_at") or payload.get("last_run_at"),
        "ended_at": snapshot_runtime.get("ended_at") or selected_run.get("ended_at"),
    }
    lifecycle_payload = {
        "status": status,
        "phase": phase,
        "reason": reason,
        "crash_summary": _crash_summary(
            status=status,
            phase=phase,
            reason=reason,
            failure=_mapping(lifecycle_row.get("failure")),
        ),
        "owner": lifecycle_row.get("owner"),
        "message": lifecycle_row.get("message"),
        "metadata": _mapping(lifecycle_row.get("metadata")),
        "failure": _mapping(lifecycle_row.get("failure")),
        "checkpoint_at": lifecycle_row.get("checkpoint_at"),
        "updated_at": lifecycle_row.get("updated_at"),
        "backend_owned": phase in BACKEND_OWNED_PHASES,
        "terminal": phase in TERMINAL_PHASES,
        "container": container,
        "heartbeat": heartbeat,
        "telemetry": telemetry,
        "live": bool(container.get("running")) and status in {"running", "degraded", "telemetry_degraded"},
    }
    controls = _resolve_controls(
        status=status,
        phase=phase,
        container_running=bool(container.get("running")),
        has_run=bool(selected_run_id),
    )

    payload["status"] = status
    payload["runtime"] = runtime
    payload["lifecycle"] = lifecycle_payload
    payload["controls"] = controls
    payload["active_run_id"] = selected_run_id
    payload["run"] = dict(selected_run) if selected_run else None
    return payload


def project_bot_states(
    bots: Sequence[Mapping[str, Any]],
    *,
    runs_by_bot_id: Mapping[str, Mapping[str, Any]] | None = None,
    lifecycle_by_bot_id: Mapping[str, Mapping[str, Any]] | None = None,
    view_rows_by_bot_id: Mapping[str, Mapping[str, Any]] | None = None,
    container_states_by_bot_id: Mapping[str, Mapping[str, Any]] | None = None,
    heartbeat_stale_ms: int = 30000,
) -> list[Dict[str, Any]]:
    projected: list[Dict[str, Any]] = []
    run_lookup = {str(key): value for key, value in dict(runs_by_bot_id or {}).items()}
    lifecycle_lookup = {str(key): value for key, value in dict(lifecycle_by_bot_id or {}).items()}
    view_lookup = {str(key): value for key, value in dict(view_rows_by_bot_id or {}).items()}
    container_lookup = {str(key): value for key, value in dict(container_states_by_bot_id or {}).items()}
    for bot in bots or []:
        if not isinstance(bot, Mapping):
            continue
        bot_id = str(bot.get("id") or "").strip()
        projected.append(
            project_bot_state(
                bot,
                run=run_lookup.get(bot_id),
                lifecycle=lifecycle_lookup.get(bot_id),
                view_row=view_lookup.get(bot_id),
                container_state=container_lookup.get(bot_id),
                heartbeat_stale_ms=heartbeat_stale_ms,
            )
        )
    return projected


__all__ = ["project_bot_state", "project_bot_states"]
