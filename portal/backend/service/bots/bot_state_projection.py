"""Authoritative bot lifecycle projection for API and SSE consumers."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from core.settings import get_settings
from ..storage.storage import get_bot_run, get_latest_bot_run_view_state, get_latest_bot_runtime_run_id
from .runner import DockerBotRunner

logger = logging.getLogger(__name__)
_BOT_RUNTIME_SETTINGS = get_settings().bot_runtime

_ACTIVE_STATUSES = {"starting", "running", "paused", "degraded", "telemetry_degraded"}
_TERMINAL_STATUSES = {"idle", "stopped", "completed", "error", "failed", "crashed"}
_HEARTBEAT_STALE_MS = _BOT_RUNTIME_SETTINGS.status_heartbeat_stale_ms


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


def _heartbeat_projection(bot: Mapping[str, Any], *, claimed_active: bool) -> Dict[str, Any]:
    heartbeat_at = _parse_timestamp(bot.get("heartbeat_at"))
    runner_id = str(bot.get("runner_id") or "").strip() or None
    age_ms: Optional[int] = None
    if heartbeat_at is not None:
        age_ms = max(
            0,
            int((datetime.now(timezone.utc) - heartbeat_at).total_seconds() * 1000.0),
        )
    if heartbeat_at is None:
        state = "missing" if claimed_active or runner_id else "inactive"
    elif age_ms is not None and age_ms > _HEARTBEAT_STALE_MS:
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


def _container_projection(bot_id: str, *, should_inspect: bool, assumed_running: bool = False) -> Dict[str, Any]:
    if not should_inspect:
        return {
            "name": DockerBotRunner.container_name_for(bot_id),
            "status": "running" if assumed_running else "missing",
            "running": bool(assumed_running),
            "id": None,
            "started_at": None,
            "finished_at": None,
            "exit_code": None,
            "error": None,
        }
    try:
        return DockerBotRunner.inspect_bot_container(bot_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bot_container_inspect_failed | bot_id=%s | error=%s", bot_id, exc)
        return {
            "name": DockerBotRunner.container_name_for(bot_id),
            "status": "unknown",
            "running": False,
            "id": None,
            "started_at": None,
            "finished_at": None,
            "exit_code": None,
            "error": str(exc),
        }


def _resolve_status(
    *,
    persisted_status: str,
    run_status: str,
    engine_status: str,
    container_status: str,
    container_running: bool,
    has_view_state: bool,
    heartbeat_state: str,
    claimed_active: bool,
) -> str:
    if run_status == "telemetry_degraded" and (container_running or claimed_active):
        return "telemetry_degraded"
    if container_running:
        if not has_view_state:
            return "starting" if persisted_status == "starting" else "running"
        if engine_status in _ACTIVE_STATUSES | {"completed", "error", "failed", "crashed", "stopped"}:
            return engine_status
        return "running"
    if container_status in {"exited", "dead"}:
        if run_status in {"completed", "stopped"}:
            return run_status
        if persisted_status in _TERMINAL_STATUSES:
            return persisted_status
        return "crashed"
    if claimed_active and container_status == "missing":
        if heartbeat_state == "stale":
            return "crashed"
        return "starting" if persisted_status == "starting" else "crashed"
    if persisted_status in _TERMINAL_STATUSES:
        return persisted_status
    if run_status in {"completed", "stopped", "failed", "crashed"}:
        return run_status
    if has_view_state and engine_status:
        return engine_status
    if claimed_active:
        return persisted_status if persisted_status in _ACTIVE_STATUSES else "starting"
    return "idle"


def _resolve_phase(
    *,
    status: str,
    container_running: bool,
    has_view_state: bool,
) -> str:
    if status == "starting":
        return "booting_runtime" if container_running else "starting_container"
    if status in {"running", "paused"}:
        return "live" if has_view_state else "awaiting_snapshot"
    if status in {"degraded", "telemetry_degraded"}:
        return "degraded"
    if status == "completed":
        return "completed"
    if status == "stopped":
        return "stopped"
    if status in {"error", "failed", "crashed"}:
        return "failed"
    return "idle"


def _resolve_reason(
    *,
    status: str,
    phase: str,
    container_status: str,
    heartbeat_state: str,
    has_view_state: bool,
    selected_run_id: Optional[str],
) -> str:
    if phase == "starting_container":
        return "container_start_pending"
    if phase == "booting_runtime":
        return "runtime_booting"
    if phase == "awaiting_snapshot":
        return "awaiting_first_snapshot"
    if phase == "live":
        if heartbeat_state == "stale":
            return "runner_stale"
        return "live_runtime"
    if phase == "degraded":
        return "runtime_degraded"
    if phase == "completed":
        return "run_completed"
    if phase == "stopped":
        return "run_stopped"
    if phase == "failed":
        if container_status in {"exited", "dead"}:
            return "container_exited"
        if container_status == "missing" and selected_run_id:
            return "container_missing"
        if heartbeat_state == "stale":
            return "runner_stale"
        return "runtime_failed"
    if not selected_run_id and not has_view_state:
        return "idle"
    return status or "idle"


def _resolve_controls(*, status: str, phase: str, container_running: bool, has_run: bool) -> Dict[str, Any]:
    active = status in _ACTIVE_STATUSES
    can_start = not active or not container_running
    start_label = "Start"
    if status == "completed":
        start_label = "Rerun"
    elif status in {"crashed", "error", "failed", "stopped", "degraded", "telemetry_degraded"}:
        start_label = "Restart"
    elif phase in {"starting_container", "booting_runtime", "awaiting_snapshot"}:
        start_label = "Starting"
    return {
        "can_start": bool(can_start),
        "can_stop": bool(container_running or active or phase in {"starting_container", "booting_runtime"}),
        "can_open_lens": bool(has_run),
        "can_delete": not active and phase not in {"starting_container", "booting_runtime"},
        "start_label": start_label,
    }


def project_bot_state(bot: Mapping[str, Any], *, inspect_container: bool = True) -> Dict[str, Any]:
    payload = dict(bot or {})
    bot_id = str(payload.get("id") or "").strip()
    if not bot_id:
        raise ValueError("bot id is required for projection")

    persisted_status = _normalize_status(payload.get("status"))
    run_id = get_latest_bot_runtime_run_id(bot_id)
    run = get_bot_run(run_id) if run_id else None
    view_row = get_latest_bot_run_view_state(bot_id=bot_id, run_id=run_id) if run_id else None
    view_payload = _mapping(view_row.get("payload")) if isinstance(view_row, Mapping) else {}
    snapshot_runtime = _mapping(view_payload.get("runtime"))
    engine_status = _normalize_status(snapshot_runtime.get("status"), default="")
    run_status = _normalize_status((run or {}).get("status"), default="")
    claimed_active = persisted_status in _ACTIVE_STATUSES or run_status in _ACTIVE_STATUSES or bool(payload.get("runner_id"))
    heartbeat = _heartbeat_projection(payload, claimed_active=claimed_active)
    container = _container_projection(
        bot_id,
        should_inspect=inspect_container and (
            claimed_active or persisted_status in _ACTIVE_STATUSES or heartbeat["state"] != "inactive"
        ),
        assumed_running=claimed_active,
    )
    status = _resolve_status(
        persisted_status=persisted_status,
        run_status=run_status,
        engine_status=engine_status,
        container_status=str(container.get("status") or ""),
        container_running=bool(container.get("running")),
        has_view_state=bool(view_row),
        heartbeat_state=str(heartbeat.get("state") or ""),
        claimed_active=claimed_active,
    )
    phase = _resolve_phase(
        status=status,
        container_running=bool(container.get("running")),
        has_view_state=bool(view_row),
    )
    reason = _resolve_reason(
        status=status,
        phase=phase,
        container_status=str(container.get("status") or ""),
        heartbeat_state=str(heartbeat.get("state") or ""),
        has_view_state=bool(view_row),
        selected_run_id=run_id,
    )

    runtime_stats = _mapping(snapshot_runtime.get("stats"))
    if not runtime_stats:
        runtime_stats = _mapping((run or {}).get("summary")) or _mapping(payload.get("last_stats"))
    warnings = _sequence(view_payload.get("warnings"))
    telemetry = {
        "run_id": run_id,
        "seq": int(view_row.get("seq") or 0) if isinstance(view_row, Mapping) else 0,
        "available": bool(view_row),
        "known_at": view_row.get("known_at") if isinstance(view_row, Mapping) else None,
        "last_snapshot_at": view_row.get("event_time") if isinstance(view_row, Mapping) else None,
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
        "engine_status": engine_status or None,
        "run_id": run_id,
        "seq": telemetry["seq"],
        "known_at": telemetry["known_at"],
        "last_snapshot_at": telemetry["last_snapshot_at"],
        "warnings": warnings,
        "stats": runtime_stats,
        "started_at": snapshot_runtime.get("started_at") or (run or {}).get("started_at") or payload.get("last_run_at"),
        "ended_at": snapshot_runtime.get("ended_at") or (run or {}).get("ended_at"),
    }
    controls = _resolve_controls(
        status=status,
        phase=phase,
        container_running=bool(container.get("running")),
        has_run=bool(run_id),
    )
    lifecycle = {
        "status": status,
        "phase": phase,
        "reason": reason,
        "container": container,
        "heartbeat": heartbeat,
        "telemetry": telemetry,
        "live": bool(container.get("running")) and telemetry["available"],
    }
    payload["status"] = status
    payload["runtime"] = runtime
    payload["lifecycle"] = lifecycle
    payload["controls"] = controls
    payload["active_run_id"] = run_id
    payload["run"] = run or None
    return payload


def project_bot_states(
    bots: Sequence[Mapping[str, Any]],
    *,
    inspect_container: bool = True,
) -> list[Dict[str, Any]]:
    projected: list[Dict[str, Any]] = []
    for bot in bots or []:
        if not isinstance(bot, Mapping):
            continue
        projected.append(project_bot_state(bot, inspect_container=inspect_container))
    return projected
