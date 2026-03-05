"""Facade for bot services (config + runtime control)."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Mapping

from .bot_stream import BotStreamManager
from .config_service import BotConfigService
from .runtime_control_service import BotRuntimeControlService
from ..storage.storage import get_latest_bot_run_view_state

logger = logging.getLogger(__name__)

_stream_manager = BotStreamManager()
_config_service = BotConfigService()
_runtime_service = BotRuntimeControlService(_config_service, _stream_manager)


def _broadcast_bot_stream(event: str, payload: Dict[str, Any]) -> None:
    _stream_manager.broadcast(event, payload)


def list_bots() -> List[Dict[str, object]]:
    return _config_service.list_bots()


def create_bot(name: str, **payload: object) -> Dict[str, object]:
    bot = _config_service.create_bot(name, **payload)
    logger.info("[BotService] bot created", extra={"bot_id": bot.get("id"), "run_type": bot.get("run_type")})
    _broadcast_bot_stream("bot", {"bot": bot})
    return bot


def update_bot(bot_id: str, **payload: object) -> Dict[str, object]:
    bot = _config_service.update_bot(bot_id, **payload)
    logger.info("[BotService] bot updated", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot", {"bot": bot})
    return bot


def delete_bot_record(bot_id: str) -> None:
    _config_service.delete_bot_record(bot_id)
    logger.info("[BotService] bot deleted", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot_deleted", {"bot_id": bot_id})


def start_bot(bot_id: str) -> Dict[str, object]:
    return _runtime_service.start_bot(bot_id)


def stop_bot(bot_id: str) -> Dict[str, object]:
    return _runtime_service.stop_bot(bot_id)


def get_bot(bot_id: str) -> Dict[str, object]:
    return _config_service.get_bot(bot_id)


def bots_stream():
    return _runtime_service.bots_stream()


def watchdog_status() -> Dict[str, Any]:
    return _runtime_service.watchdog_status()


def runtime_capacity() -> Dict[str, Any]:
    host_cpu_cores = max(1, int(os.cpu_count() or 1))
    active_statuses = {"running", "starting", "degraded", "telemetry_degraded"}
    workers_in_use = 0
    workers_requested = 0
    running_bots = 0

    for bot in _config_service.list_bots():
        status = str(bot.get("status") or "").strip().lower()
        if status not in active_statuses:
            continue
        running_bots += 1
        runtime_payload: Mapping[str, Any] = {}
        view_row = get_latest_bot_run_view_state(
            bot_id=str(bot.get("id") or ""),
            run_id=None,
            series_key="bot",
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
    return _config_service.settings_catalog()


__all__ = [
    "create_bot",
    "delete_bot_record",
    "get_bot",
    "list_bots",
    "start_bot",
    "stop_bot",
    "update_bot",
    "bots_stream",
    "runtime_capacity",
    "bot_settings_catalog",
    "watchdog_status",
]
