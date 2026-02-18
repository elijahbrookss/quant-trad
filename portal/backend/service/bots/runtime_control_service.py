"""Bot runtime control service: start/stop/runner/watchdog boundaries."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Mapping

from .bot_stream import BotStreamManager
from .bot_watchdog import get_watchdog
from .config_service import BotConfigService
from .runner import DockerBotRunner

logger = logging.getLogger(__name__)


class BotRuntimeControlService:
    def __init__(self, config_service: BotConfigService, stream_manager: BotStreamManager) -> None:
        self._config = config_service
        self._stream_manager = stream_manager

    @staticmethod
    def _now_iso() -> str:
        return datetime.utcnow().isoformat() + "Z"

    def _broadcast(self, event: str, payload: Mapping[str, Any]) -> None:
        self._stream_manager.broadcast(event, payload)

    def start_bot(self, bot_id: str) -> Dict[str, object]:
        bots = {bot["id"]: bot for bot in self._config.list_bots()}
        if bot_id not in bots:
            raise KeyError(f"Bot {bot_id} was not found")
        bot = bots[bot_id]

        bot["wallet_config"] = self._config.validate_wallet_config(bot.get("wallet_config"))
        bot["strategy_id"] = self._config.validate_strategy_id(bot.get("strategy_id"))
        self._config.validate_backtest_window(bot)
        self._config.validate_strategy_existence(bot)
        self._config.validate_instrument_policy(bot)

        runner = DockerBotRunner.from_env()
        container_id = runner.start_bot(bot=bot)
        bot["status"] = "running"
        bot["runner_id"] = container_id
        bot["last_run_at"] = self._now_iso()
        from ..storage.storage import upsert_bot

        upsert_bot(bot)
        logger.info("bot_container_started | bot_id=%s | container_id=%s", bot_id, container_id)
        self._broadcast("bot", {"bot": bot})
        return bot

    def stop_bot(self, bot_id: str) -> Dict[str, object]:
        runner = DockerBotRunner.from_env()
        runner.stop_bot(bot_id=bot_id)
        get_watchdog().unregister_bot(bot_id)

        bots = {bot["id"]: bot for bot in self._config.list_bots()}
        if bot_id not in bots:
            raise KeyError(f"Bot {bot_id} was not found")
        bot = bots[bot_id]
        bot["status"] = "stopped"
        bot["runner_id"] = None
        from ..storage.storage import upsert_bot

        upsert_bot(bot)
        logger.info("bot_container_stopped | bot_id=%s", bot_id)
        self._broadcast("bot", {"bot": bot})
        return bot


    def bots_stream(self):
        return self._stream_manager.subscribe_all(self._config.list_bots)

    @staticmethod
    def watchdog_status() -> Dict[str, Any]:
        watchdog = get_watchdog()
        stale = watchdog.scan_stale_heartbeats()
        containers = watchdog.verify_container_ownership()
        status = watchdog.status()
        status["stale_marked_failed"] = stale
        status["container_marked_failed"] = containers
        return status
