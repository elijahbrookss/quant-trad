"""Bot runtime control service: explicit backend-owned start/stop lifecycle orchestration."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Protocol

from core.settings import get_settings

from .botlens_lifecycle_bridge import emit_lifecycle_event
from .bot_state_projection import project_bot_state
from .bot_stream import BotStreamManager
from .bot_watchdog import get_watchdog
from .config_service import BotConfigService
from .runner import BotRunner, DockerBotRunner
from .startup_lifecycle import BotLifecyclePhase, BotLifecycleStatus, LifecycleOwner, lifecycle_checkpoint_payload
from .startup_service import BotStartupOrchestrator
from ..storage import storage as storage_module
from ..storage.storage import upsert_bot

logger = logging.getLogger(__name__)
_BOT_RUNTIME_SETTINGS = get_settings().bot_runtime


class BotControlStorage(Protocol):
    def upsert_bot(self, payload: Dict[str, Any]) -> None: ...
    def upsert_bot_run(self, payload: Dict[str, Any]) -> Dict[str, Any]: ...
    def get_bot_run(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    def get_latest_bot_runtime_run_id(self, bot_id: str) -> Optional[str]: ...
    def get_latest_bot_run_lifecycle(self, bot_id: str) -> Optional[Dict[str, Any]]: ...
    def get_latest_bot_run_view_state(
        self,
        *,
        bot_id: str,
        run_id: Optional[str] = None,
        series_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]: ...
    def record_bot_run_lifecycle_checkpoint(self, payload: Dict[str, Any]) -> Dict[str, Any]: ...
    def update_bot_runtime_status(self, *, bot_id: str, run_id: str, status: str, telemetry_degraded: bool = False) -> None: ...


def _default_upsert_bot(payload: Dict[str, Any]) -> None:
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

    def _broadcast(self, event: str, payload: Dict[str, Any]) -> None:
        self._stream_manager.broadcast(event, payload)

    @staticmethod
    def _runner_target() -> str:
        return str(_BOT_RUNTIME_SETTINGS.target or "docker").strip().lower()

    def _resolve_runner(self) -> BotRunner:
        if self._runner_factory is not None:
            return self._runner_factory()
        target = self._runner_target()
        if target == "docker":
            return DockerBotRunner.from_env()
        raise RuntimeError(
            f"Unsupported bot runtime target: {target}. "
            "Set QT_BOT_RUNTIME_TARGET=docker."
        )

    def _storage_gateway(self) -> BotControlStorage:
        if self._storage is not None:
            return self._storage

        class _DefaultStorage:
            def upsert_bot(self, payload: Dict[str, Any]) -> None:
                storage_module.upsert_bot(dict(payload))

            def upsert_bot_run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
                return storage_module.upsert_bot_run(dict(payload))

            def get_bot_run(self, run_id: str) -> Optional[Dict[str, Any]]:
                return storage_module.get_bot_run(str(run_id))

            def get_latest_bot_runtime_run_id(self, bot_id: str) -> Optional[str]:
                return storage_module.get_latest_bot_runtime_run_id(str(bot_id))

            def get_latest_bot_run_lifecycle(self, bot_id: str) -> Optional[Dict[str, Any]]:
                return storage_module.get_latest_bot_run_lifecycle(str(bot_id))

            def get_latest_bot_run_view_state(
                self,
                *,
                bot_id: str,
                run_id: Optional[str] = None,
                series_key: Optional[str] = None,
            ) -> Optional[Dict[str, Any]]:
                return storage_module.get_latest_bot_run_view_state(bot_id=bot_id, run_id=run_id, series_key=series_key)

            def record_bot_run_lifecycle_checkpoint(self, payload: Dict[str, Any]) -> Dict[str, Any]:
                return storage_module.record_bot_run_lifecycle_checkpoint(dict(payload))

            def update_bot_runtime_status(self, *, bot_id: str, run_id: str, status: str, telemetry_degraded: bool = False) -> None:
                storage_module.update_bot_runtime_status(
                    bot_id=bot_id,
                    run_id=run_id,
                    status=status,
                    telemetry_degraded=telemetry_degraded,
                )

        return _DefaultStorage()

    def _upsert_bot(self, payload: Dict[str, Any]) -> None:
        if self._storage is not None:
            self._storage.upsert_bot(payload)
            return
        _default_upsert_bot(payload)

    def _watchdog_instance(self):
        return self._watchdog if self._watchdog is not None else get_watchdog()

    def _container_state_for_bot(
        self,
        bot: Dict[str, Any],
        lifecycle: Optional[Dict[str, Any]],
        *,
        inspect_container: bool,
    ) -> Dict[str, Any]:
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

    def _project_bot_from_storage(self, bot: Dict[str, Any], *, inspect_container: bool = True) -> Dict[str, Any]:
        storage = self._storage_gateway()
        bot_id = str(bot.get("id") or "").strip()
        lifecycle = storage.get_latest_bot_run_lifecycle(bot_id)
        run_id = (
            str((lifecycle or {}).get("run_id") or "").strip()
            or storage.get_latest_bot_runtime_run_id(bot_id)
        )
        run = storage.get_bot_run(run_id) if run_id else None
        view_row = storage.get_latest_bot_run_view_state(bot_id=bot_id, run_id=run_id) if run_id else None
        container_state = self._container_state_for_bot(bot, lifecycle, inspect_container=inspect_container)
        return project_bot_state(
            bot,
            run=run,
            lifecycle=lifecycle,
            view_row=view_row,
            container_state=container_state,
            heartbeat_stale_ms=_BOT_RUNTIME_SETTINGS.status_heartbeat_stale_ms,
        )

    def _project_all_bots_from_storage(self) -> list[Dict[str, Any]]:
        return [self._project_bot_from_storage(bot) for bot in self._config.list_bots()]

    def start_bot(self, bot_id: str) -> Dict[str, object]:
        runner = self._resolve_runner()
        watchdog = self._watchdog_instance()
        orchestrator = BotStartupOrchestrator(
            config_service=self._config,
            storage=self._storage_gateway(),
            runner=runner,
            watchdog=watchdog,
        )
        try:
            ctx = orchestrator.start_bot(bot_id)
        except Exception:
            bot = self._config.get_bot(bot_id)
            projected = self._project_bot_from_storage(bot, inspect_container=False)
            self._broadcast("bot", {"bot": projected})
            raise

        logger.info(
            "bot_startup_contract_stamped | bot_id=%s | run_id=%s | container_id=%s | runner_id=%s",
            ctx.bot_id,
            ctx.run_id,
            ctx.container_id,
            watchdog.runner_id,
        )
        bot = self._config.get_bot(bot_id)
        projected = self._project_bot_from_storage(bot)
        self._broadcast("bot", {"bot": projected})
        return projected

    def stop_bot(self, bot_id: str) -> Dict[str, object]:
        runner = self._resolve_runner()
        runner.stop_bot(bot_id=bot_id)
        watchdog = self._watchdog_instance()
        watchdog.unregister_bot(bot_id)

        bot = self._config.get_bot(bot_id)
        payload = dict(bot)
        payload["status"] = BotLifecycleStatus.STOPPED.value
        payload["runner_id"] = None
        self._upsert_bot(payload)
        run_id = self._storage_gateway().get_latest_bot_runtime_run_id(bot_id)
        if run_id:
            checkpoint = lifecycle_checkpoint_payload(
                bot_id=bot_id,
                run_id=run_id,
                phase=BotLifecyclePhase.STOPPED.value,
                status=BotLifecycleStatus.STOPPED.value,
                owner=LifecycleOwner.BACKEND.value,
                message="Bot stop requested from backend control service.",
            )
            lifecycle_state = self._storage_gateway().record_bot_run_lifecycle_checkpoint(checkpoint)
            emit_lifecycle_event(
                {
                    **dict(lifecycle_state or {}),
                    "bot_id": bot_id,
                    "run_id": run_id,
                    "phase": BotLifecyclePhase.STOPPED.value,
                    "status": BotLifecycleStatus.STOPPED.value,
                    "owner": LifecycleOwner.BACKEND.value,
                    "message": "Bot stop requested from backend control service.",
                }
            )
            self._storage_gateway().update_bot_runtime_status(
                bot_id=bot_id,
                run_id=run_id,
                status=BotLifecycleStatus.STOPPED.value,
            )
        logger.info("bot_container_stopped | bot_id=%s | run_id=%s", bot_id, run_id)
        refreshed = self._config.get_bot(bot_id)
        projected = self._project_bot_from_storage(refreshed, inspect_container=True)
        self._broadcast("bot", {"bot": projected})
        return projected

    def bots_stream(self):
        return self._stream_manager.subscribe_all(self._project_all_bots_from_storage)

    def watchdog_status(self) -> Dict[str, Any]:
        watchdog = self._watchdog_instance()
        stale = watchdog.scan_stale_heartbeats()
        containers = watchdog.verify_container_ownership()
        status = watchdog.status()
        status["stale_marked_failed"] = stale
        status["container_marked_failed"] = containers
        return status
