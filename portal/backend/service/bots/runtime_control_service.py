"""Bot runtime control service: start/stop/runner/watchdog boundaries."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any, Callable, Dict, Mapping, Optional, Protocol

from .bot_stream import BotStreamManager
from .bot_state_projection import project_bot_state
from .bot_watchdog import get_watchdog
from .config_service import BotConfigService
from .runner import BotRunner
from .runner import DockerBotRunner

logger = logging.getLogger(__name__)


class BotControlStorage(Protocol):
    def upsert_bot(self, payload: Mapping[str, Any]) -> None: ...


def _default_upsert_bot(payload: Mapping[str, Any]) -> None:
    from ..storage.storage import upsert_bot

    upsert_bot(dict(payload))



class BotRuntimeControlService:
    def __init__(
        self,
        config_service: BotConfigService,
        stream_manager: BotStreamManager,
        *,
        storage: Optional[BotControlStorage] = None,
        watchdog: Optional[Any] = None,
        runner_factory: Optional[Callable[[], BotRunner]] = None,
    ) -> None:
        self._config = config_service
        self._stream_manager = stream_manager
        self._storage = storage
        self._watchdog = watchdog
        self._runner_factory = runner_factory

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def _broadcast(self, event: str, payload: Mapping[str, Any]) -> None:
        self._stream_manager.broadcast(event, payload)

    @staticmethod
    def _runner_target() -> str:
        return str(os.getenv("BOT_RUNTIME_TARGET", "docker") or "docker").strip().lower()

    def _resolve_runner(self) -> BotRunner:
        if self._runner_factory is not None:
            return self._runner_factory()
        target = self._runner_target()
        if target == "docker":
            return DockerBotRunner.from_env()
        raise RuntimeError(
            f"Unsupported bot runtime target: {target}. "
            "Set BOT_RUNTIME_TARGET=docker."
        )

    def _upsert_bot(self, payload: Mapping[str, Any]) -> None:
        if self._storage is not None:
            self._storage.upsert_bot(payload)
            return
        _default_upsert_bot(payload)

    def _watchdog_instance(self):
        return self._watchdog if self._watchdog is not None else get_watchdog()

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
        self._config.validate_runtime_readiness(bot)

        runner = self._resolve_runner()
        watchdog = self._watchdog_instance()
        bot["status"] = "starting"
        bot["runner_id"] = watchdog.runner_id
        bot["last_run_at"] = self._now_iso()
        self._upsert_bot(bot)
        self._broadcast("bot", {"bot": project_bot_state(bot)})

        try:
            container_id = runner.start_bot(bot=bot)
        except Exception as exc:
            now = self._now_iso()
            error_payload = {
                "message": str(exc),
                "phase": "container_start",
                "at": now,
            }
            bot["status"] = "error"
            bot["runner_id"] = None
            bot["last_run_artifact"] = {"error": error_payload}
            self._upsert_bot(bot)
            projected = project_bot_state(bot)
            projected["runtime"] = {
                **dict(projected.get("runtime") or {}),
                "status": "error",
                "error": error_payload,
            }
            self._broadcast(
                "bot",
                {
                    "bot": projected
                },
            )
            logger.error("bot_container_start_failed | bot_id=%s | error=%s", bot_id, exc)
            raise

        watchdog.register_bot(bot_id)
        refreshed = self._config.get_bot(bot_id)
        logger.info(
            "bot_container_started | bot_id=%s | container_id=%s | runner_id=%s",
            bot_id,
            container_id,
            watchdog.runner_id,
        )
        projected = project_bot_state(refreshed)
        self._broadcast("bot", {"bot": projected})
        return projected

    def stop_bot(self, bot_id: str) -> Dict[str, object]:
        runner = self._resolve_runner()
        runner.stop_bot(bot_id=bot_id)
        self._watchdog_instance().unregister_bot(bot_id)

        bots = {bot["id"]: bot for bot in self._config.list_bots()}
        if bot_id not in bots:
            raise KeyError(f"Bot {bot_id} was not found")
        bot = bots[bot_id]
        bot["status"] = "stopped"
        bot["runner_id"] = None
        self._upsert_bot(bot)
        logger.info("bot_container_stopped | bot_id=%s", bot_id)
        projected = project_bot_state(bot)
        self._broadcast("bot", {"bot": projected})
        return projected


    def bots_stream(self):
        return self._stream_manager.subscribe_all(self._config.list_bots)

    def watchdog_status(self) -> Dict[str, Any]:
        watchdog = self._watchdog_instance()
        stale = watchdog.scan_stale_heartbeats()
        containers = watchdog.verify_container_ownership()
        status = watchdog.status()
        status["stale_marked_failed"] = stale
        status["container_marked_failed"] = containers
        return status
