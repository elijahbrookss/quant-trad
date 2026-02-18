"""Facade for bot services (config + runtime control)."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from .bot_stream import BotStreamManager
from .config_service import BotConfigService
from .runtime_control_service import BotRuntimeControlService

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
    "bot_settings_catalog",
    "watchdog_status",
]
