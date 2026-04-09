"""Facade for bot services (config + runtime control)."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Mapping

from core.settings import get_settings

from .bot_state_projection import project_bot_state
from .runner import DockerBotRunner
from .runtime_composition import get_runtime_composition

logger = logging.getLogger(__name__)
_BOT_RUNTIME_SETTINGS = get_settings().bot_runtime


_WATCHDOG_CALLBACK_SET = False


def _composition():
    return get_runtime_composition()


def _ensure_watchdog_callback() -> None:
    global _WATCHDOG_CALLBACK_SET
    if _WATCHDOG_CALLBACK_SET:
        return
    _composition().watchdog.set_orphan_callback(_handle_watchdog_orphan)
    _WATCHDOG_CALLBACK_SET = True


def ensure_watchdog_stream_bridge() -> None:
    _ensure_watchdog_callback()


def _broadcast_bot_stream(event: str, payload: Dict[str, Any]) -> None:
    _composition().stream_manager.broadcast(event, payload)


def _load_projection_inputs(bot: Mapping[str, Any]) -> tuple[Optional[Mapping[str, Any]], Optional[Mapping[str, Any]], Optional[Mapping[str, Any]]]:
    bot_id = str(bot.get("id") or "").strip()
    lifecycle = _composition().storage.get_latest_bot_run_lifecycle(bot_id) if bot_id else None
    run_id = (
        str((lifecycle or {}).get("run_id") or "").strip()
        or _composition().storage.get_latest_bot_runtime_run_id(bot_id)
        if bot_id
        else None
    )
    run = _composition().storage.get_bot_run(run_id) if run_id else None
    view_row = _composition().storage.get_latest_bot_run_view_state(bot_id=bot_id, run_id=run_id) if run_id else None
    return run, lifecycle, view_row


def _container_state_for_bot(bot: Mapping[str, Any], lifecycle: Mapping[str, Any] | None, *, inspect_container: bool) -> Dict[str, Any]:
    bot_id = str(bot.get("id") or "").strip()
    default_state = {
        "name": DockerBotRunner.container_name_for(bot_id),
        "status": "missing",
        "running": False,
        "id": None,
        "started_at": None,
        "finished_at": None,
        "exit_code": None,
        "error": None,
    }
    if not inspect_container:
        return default_state
    lifecycle_status = str((lifecycle or {}).get("status") or "").strip().lower()
    persisted_status = str(bot.get("status") or "").strip().lower()
    should_inspect = bool(
        bot.get("runner_id")
        or bot.get("heartbeat_at")
        or lifecycle_status in {"starting", "running", "degraded", "telemetry_degraded"}
        or persisted_status in {"starting", "running", "degraded", "telemetry_degraded"}
    )
    if not should_inspect:
        return default_state
    try:
        return DockerBotRunner.inspect_bot_container(bot_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bot_container_inspect_failed | bot_id=%s | error=%s", bot_id, exc)
        return {**default_state, "status": "unknown", "error": str(exc)}


def _project_bot(bot: Mapping[str, Any], *, inspect_container: bool = True) -> Dict[str, Any]:
    run, lifecycle, view_row = _load_projection_inputs(bot)
    container_state = _container_state_for_bot(bot, lifecycle, inspect_container=inspect_container)
    return project_bot_state(
        bot,
        run=run,
        lifecycle=lifecycle,
        view_row=view_row,
        container_state=container_state,
        heartbeat_stale_ms=_BOT_RUNTIME_SETTINGS.status_heartbeat_stale_ms,
    )


def list_bots() -> List[Dict[str, object]]:
    return [_project_bot(bot) for bot in _composition().config_service.list_bots()]


def publish_projected_bot(bot_id: str, *, inspect_container: bool = True) -> None:
    try:
        bot = _composition().config_service.get_bot(bot_id)
    except KeyError:
        logger.warning("bot_stream_projection_missing | bot_id=%s", bot_id)
        return
    projected = _project_bot(bot, inspect_container=inspect_container)
    lifecycle = projected.get("lifecycle") if isinstance(projected.get("lifecycle"), Mapping) else {}
    logger.info(
        "bot_stream_projected_bot_published | bot_id=%s | run_id=%s | bot_status=%s | lifecycle_status=%s | lifecycle_phase=%s | inspect_container=%s",
        bot_id,
        str(projected.get("active_run_id") or "").strip(),
        str(projected.get("status") or "").strip(),
        str(lifecycle.get("status") or "").strip(),
        str(lifecycle.get("phase") or "").strip(),
        inspect_container,
    )
    _broadcast_bot_stream("bot", {"bot": projected})


def publish_runtime_update(bot_id: str, runtime: Mapping[str, Any]) -> None:
    _broadcast_bot_stream(
        "bot_runtime",
        {
            "bot_id": bot_id,
            "runtime": dict(runtime or {}),
        },
    )


def create_bot(name: str, **payload: object) -> Dict[str, object]:
    bot = _composition().config_service.create_bot(name, **payload)
    logger.info("[BotService] bot created", extra={"bot_id": bot.get("id"), "run_type": bot.get("run_type")})
    projected = _project_bot(bot, inspect_container=False)
    _broadcast_bot_stream("bot", {"bot": projected})
    return projected


def update_bot(bot_id: str, **payload: object) -> Dict[str, object]:
    bot = _composition().config_service.update_bot(bot_id, **payload)
    logger.info("[BotService] bot updated", extra={"bot_id": bot_id})
    projected = _project_bot(bot)
    _broadcast_bot_stream("bot", {"bot": projected})
    return projected


def delete_bot_record(bot_id: str) -> None:
    _composition().config_service.delete_bot_record(bot_id)
    logger.info("[BotService] bot deleted", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot_deleted", {"bot_id": bot_id})


def start_bot(bot_id: str) -> Dict[str, object]:
    _ensure_watchdog_callback()
    return _composition().runtime_control_service.start_bot(bot_id)


def stop_bot(bot_id: str) -> Dict[str, object]:
    return _composition().runtime_control_service.stop_bot(bot_id)


def get_bot(bot_id: str) -> Dict[str, object]:
    return _project_bot(_composition().config_service.get_bot(bot_id))


def list_bot_runs_for_bot(bot_id: str, *, limit: int = 25) -> Dict[str, Any]:
    current = get_bot(bot_id)
    active_run_id = str(current.get("active_run_id") or "").strip() or None
    rows = _composition().storage.list_bot_runs(bot_id=bot_id)

    def _sort_key(run: Mapping[str, Any]) -> tuple[str, str]:
        return (
            str(run.get("started_at") or run.get("updated_at") or run.get("created_at") or ""),
            str(run.get("run_id") or ""),
        )

    ordered = sorted(rows, key=_sort_key, reverse=True)
    selected = ordered[: max(1, int(limit or 25))]
    projected_runs: list[Dict[str, Any]] = []
    for run in selected:
        run_id = str(run.get("run_id") or "").strip()
        if not run_id:
            continue
        view_row = _composition().storage.get_latest_bot_run_view_state(bot_id=bot_id, run_id=run_id)
        view_payload = dict(view_row.get("payload") or {}) if isinstance(view_row, Mapping) else {}
        runtime_payload = dict(view_payload.get("runtime") or {}) if isinstance(view_payload.get("runtime"), Mapping) else {}
        summary = dict(run.get("summary") or {})
        if not summary and isinstance(runtime_payload.get("stats"), Mapping):
            summary = dict(runtime_payload.get("stats") or {})
        projected_runs.append(
            {
                **dict(run),
                "is_active": run_id == active_run_id,
                "runtime_status": str(runtime_payload.get("status") or run.get("status") or ""),
                "view_state_available": bool(view_row),
                "last_snapshot_at": view_row.get("event_time") if isinstance(view_row, Mapping) else None,
                "known_at": view_row.get("known_at") if isinstance(view_row, Mapping) else None,
                "seq": int(view_row.get("seq") or 0) if isinstance(view_row, Mapping) else 0,
                "summary": summary,
            }
        )
    return {
        "bot_id": bot_id,
        "active_run_id": active_run_id,
        "runs": projected_runs,
    }


def bots_stream():
    return _composition().runtime_control_service.bots_stream()


def watchdog_status() -> Dict[str, Any]:
    return _composition().runtime_control_service.watchdog_status()


def runtime_capacity() -> Dict[str, Any]:
    host_cpu_cores = max(1, int(os.cpu_count() or 1))
    active_statuses = {"running", "starting", "degraded", "telemetry_degraded"}
    workers_in_use = 0
    workers_requested = 0
    running_bots = 0

    for bot in _composition().config_service.list_bots():
        status = str(bot.get("status") or "").strip().lower()
        if status not in active_statuses:
            continue
        running_bots += 1
        runtime_payload: Mapping[str, Any] = {}
        view_row = _composition().storage.get_latest_bot_run_view_state(
            bot_id=str(bot.get("id") or ""),
            run_id=None,
            series_key=None,
        )
        if isinstance(view_row, Mapping):
            payload = view_row.get("payload")
            if isinstance(payload, Mapping):
                maybe_runtime = payload.get("runtime")
                if isinstance(maybe_runtime, Mapping):
                    runtime_payload = maybe_runtime
        try:
            active_workers = int(runtime_payload.get("active_workers") or 0)
        except (TypeError, ValueError):
            active_workers = 0
        try:
            requested_workers = int(runtime_payload.get("worker_count") or 0)
        except (TypeError, ValueError):
            requested_workers = 0
        if active_workers <= 0:
            active_workers = 1
        if requested_workers <= 0:
            requested_workers = active_workers
        workers_in_use += max(0, active_workers)
        workers_requested += max(requested_workers, active_workers)

    in_use_pct = min(100.0, round((workers_in_use / host_cpu_cores) * 100.0, 1)) if host_cpu_cores > 0 else 0.0
    return {
        "host_cpu_cores": host_cpu_cores,
        "workers_in_use": workers_in_use,
        "workers_requested": workers_requested,
        "running_bots": running_bots,
        "over_capacity_workers": max(0, workers_in_use - host_cpu_cores),
        "in_use_pct": in_use_pct,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


def bot_settings_catalog() -> Dict[str, Any]:
    return _composition().config_service.settings_catalog()


def _handle_watchdog_orphan(bot_id: str, _bot: Dict[str, Any]) -> None:
    publish_projected_bot(bot_id)


__all__ = [
    "create_bot",
    "delete_bot_record",
    "ensure_watchdog_stream_bridge",
    "get_bot",
    "list_bots",
    "start_bot",
    "stop_bot",
    "update_bot",
    "bots_stream",
    "runtime_capacity",
    "bot_settings_catalog",
    "list_bot_runs_for_bot",
    "publish_runtime_update",
    "publish_projected_bot",
    "watchdog_status",
]
