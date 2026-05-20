"""Bot runtime control service: explicit backend-owned start/stop lifecycle orchestration."""

from __future__ import annotations

import logging
import hashlib
import json
import threading
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Callable, Dict, Optional, Protocol

from core.settings import get_settings

from .botlens_lifecycle_bridge import emit_lifecycle_event
from .bot_state_projection import project_bot_state
from .bot_stream import BotStreamManager
from .bot_watchdog import get_watchdog
from .config_service import BotConfigService
from .execution_behavior import execution_behavior_from_bot, normalize_execution_behavior
from .market_data_stream_policy import normalize_market_data_stream_policy
from .runner import BotRunner, DockerBotRunner
from .startup_lifecycle import (
    BotLifecyclePhase,
    BotLifecycleStatus,
    LifecycleOwner,
    build_failure_payload,
    is_active_run_state,
    is_terminal_run_state,
    lifecycle_checkpoint_payload,
)
from .startup_service import BotStartupOrchestrator
from ..storage import storage as storage_module
from ..storage.storage import upsert_bot

logger = logging.getLogger(__name__)
_BOT_RUNTIME_SETTINGS = get_settings().bot_runtime
_START_LOCKS: dict[str, threading.Lock] = {}
_START_LOCKS_GUARD = threading.Lock()


class BotControlStorage(Protocol):
    def upsert_bot(self, payload: Dict[str, Any]) -> None: ...
    def upsert_bot_run(self, payload: Dict[str, Any]) -> Dict[str, Any]: ...
    def get_bot_run(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    def list_bot_runs(self, *, bot_id: Optional[str] = None) -> list[Dict[str, Any]]: ...
    def get_latest_bot_runtime_run_id(self, bot_id: str) -> Optional[str]: ...
    def get_bot_run_lifecycle(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    def get_bot_run_lease(self, run_id: str) -> Optional[Dict[str, Any]]: ...
    def acquire_bot_run_lease(
        self,
        *,
        bot_id: str,
        run_id: str,
        runner_id: str,
        lease_token: str,
        ttl_seconds: float | int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]: ...
    def release_bot_run_lease(
        self,
        *,
        bot_id: str,
        run_id: str,
        runner_id: str | None = None,
        lease_token: str | None = None,
        status: str = "released",
        metadata: Mapping[str, Any] | None = None,
    ) -> Optional[Dict[str, Any]]: ...
    def get_latest_bot_run_lifecycle(self, bot_id: str) -> Optional[Dict[str, Any]]: ...
    def record_bot_run_lifecycle_checkpoint(self, payload: Dict[str, Any]) -> Dict[str, Any]: ...
    def update_bot_runtime_status(self, *, bot_id: str, run_id: str, status: str, telemetry_degraded: bool = False) -> None: ...


def _default_upsert_bot(payload: Dict[str, Any]) -> None:
    upsert_bot(dict(payload))


def _lock_for_bot(bot_id: str) -> threading.Lock:
    normalized = str(bot_id or "").strip()
    with _START_LOCKS_GUARD:
        lock = _START_LOCKS.get(normalized)
        if lock is None:
            lock = threading.Lock()
            _START_LOCKS[normalized] = lock
        return lock


def _json_stable(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _utc_iso() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat() + "Z"


def _start_config_projection(bot: Mapping[str, Any]) -> Dict[str, Any]:
    excluded = {
        "status",
        "last_run_at",
        "last_stats",
        "last_run_artifact",
        "runner_id",
        "heartbeat_at",
        "created_at",
        "updated_at",
    }
    return {str(key): value for key, value in dict(bot or {}).items() if str(key) not in excluded}


def _apply_start_overrides(bot: Mapping[str, Any], overrides: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    payload = dict(bot or {})
    for key, value in dict(overrides or {}).items():
        if value in (None, ""):
            continue
        if key == "execution_behavior":
            behavior = normalize_execution_behavior(value)
            payload["execution_behavior"] = behavior
            risk = dict(payload.get("risk") or {})
            risk["execution_behavior"] = behavior
            payload["risk"] = risk
        elif key == "run_type":
            payload["run_type"] = str(value).strip().lower()
        elif key == "duration_seconds":
            payload["duration_seconds"] = float(value)
        elif key == "market_data_stream_policy":
            payload["market_data_stream_policy"] = normalize_market_data_stream_policy(
                value if isinstance(value, Mapping) else {}
            )
        else:
            payload[key] = value
    if "execution_behavior" not in payload:
        payload["execution_behavior"] = execution_behavior_from_bot(payload)
    return payload


def _start_config_hash(bot: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_stable(_start_config_projection(bot)).encode("utf-8")).hexdigest()


def _run_start_request(run: Mapping[str, Any] | None, lifecycle: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    config = dict((run or {}).get("config_snapshot") or {}) if isinstance((run or {}).get("config_snapshot"), Mapping) else {}
    start_request = dict(config.get("start_request") or {}) if isinstance(config.get("start_request"), Mapping) else {}
    metadata = dict((lifecycle or {}).get("metadata") or {}) if isinstance((lifecycle or {}).get("metadata"), Mapping) else {}
    return {
        "request_id": str(start_request.get("request_id") or config.get("request_id") or metadata.get("request_id") or "").strip(),
        "config_hash": str(start_request.get("config_hash") or metadata.get("start_config_hash") or "").strip(),
    }


def _control_response(
    *,
    status: str,
    bot_id: str,
    request_id: str,
    message: str,
    bot: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    active_run_id: str | None = None,
    reason_code: str | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "status": status,
        "bot_id": str(bot_id),
        "run_id": run_id,
        "active_run_id": active_run_id or run_id,
        "request_id": request_id,
        "message": message,
    }
    if reason_code:
        payload["reason_code"] = reason_code
    if bot is not None:
        payload["bot"] = dict(bot)
    return payload


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
    def _telemetry_hub():
        from .telemetry_stream import telemetry_hub

        return telemetry_hub

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

            def list_bot_runs(self, *, bot_id: Optional[str] = None) -> list[Dict[str, Any]]:
                return storage_module.list_bot_runs(bot_id=str(bot_id)) if bot_id else storage_module.list_bot_runs()

            def get_latest_bot_runtime_run_id(self, bot_id: str) -> Optional[str]:
                return storage_module.get_latest_bot_runtime_run_id(str(bot_id))

            def get_bot_run_lifecycle(self, run_id: str) -> Optional[Dict[str, Any]]:
                return storage_module.get_bot_run_lifecycle(str(run_id))

            def get_bot_run_lease(self, run_id: str) -> Optional[Dict[str, Any]]:
                return storage_module.get_bot_run_lease(str(run_id))

            def acquire_bot_run_lease(
                self,
                *,
                bot_id: str,
                run_id: str,
                runner_id: str,
                lease_token: str,
                ttl_seconds: float | int | None = None,
                metadata: Mapping[str, Any] | None = None,
            ) -> Dict[str, Any]:
                return storage_module.acquire_bot_run_lease(
                    bot_id=bot_id,
                    run_id=run_id,
                    runner_id=runner_id,
                    lease_token=lease_token,
                    ttl_seconds=ttl_seconds,
                    metadata=metadata,
                )

            def release_bot_run_lease(
                self,
                *,
                bot_id: str,
                run_id: str,
                runner_id: str | None = None,
                lease_token: str | None = None,
                status: str = "released",
                metadata: Mapping[str, Any] | None = None,
            ) -> Optional[Dict[str, Any]]:
                return storage_module.release_bot_run_lease(
                    bot_id=bot_id,
                    run_id=run_id,
                    runner_id=runner_id,
                    lease_token=lease_token,
                    status=status,
                    metadata=metadata,
                )

            def get_latest_bot_run_lifecycle(self, bot_id: str) -> Optional[Dict[str, Any]]:
                return storage_module.get_latest_bot_run_lifecycle(str(bot_id))

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

    @staticmethod
    def _stop_runner(
        runner: BotRunner,
        *,
        bot_id: str,
        preserve_container: bool = False,
        run_id: str | None = None,
    ) -> None:
        try:
            runner.stop_bot(bot_id=bot_id, preserve_container=preserve_container, run_id=run_id)
        except TypeError as exc:
            if "run_id" not in str(exc):
                raise
            try:
                runner.stop_bot(bot_id=bot_id, preserve_container=preserve_container)
            except TypeError as preserve_exc:
                if "preserve_container" not in str(preserve_exc):
                    raise
                runner.stop_bot(bot_id=bot_id)

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
        lifecycle_metadata = lifecycle.get("metadata") if isinstance(lifecycle, dict) else {}
        persisted_status = str(bot.get("status") or "").strip().lower()
        should_inspect = bool(
            bot.get("runner_id")
            or bot.get("heartbeat_at")
            or is_active_run_state(status=lifecycle_status, phase=(lifecycle or {}).get("phase"))
            or is_active_run_state(status=persisted_status)
            or bool((lifecycle_metadata or {}).get("preserve_container"))
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
        run_snapshot = self._telemetry_hub().get_run_snapshot(run_id=run_id) if run_id else None
        container_state = self._container_state_for_bot(bot, lifecycle, inspect_container=inspect_container)
        return project_bot_state(
            bot,
            run=run,
            lifecycle=lifecycle,
            run_snapshot=run_snapshot,
            container_state=container_state,
            heartbeat_stale_ms=_BOT_RUNTIME_SETTINGS.status_heartbeat_stale_ms,
        )

    def _project_all_bots_from_storage(self) -> list[Dict[str, Any]]:
        return [self._project_bot_from_storage(bot) for bot in self._config.list_bots()]

    def _record_lifecycle(
        self,
        *,
        bot_id: str,
        run_id: str,
        phase: str,
        owner: str,
        message: str,
        metadata: Mapping[str, Any] | None = None,
        failure: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]:
        checkpoint = lifecycle_checkpoint_payload(
            bot_id=bot_id,
            run_id=run_id,
            phase=phase,
            owner=owner,
            message=message,
            metadata=dict(metadata or {}),
            failure=dict(failure or {}),
        )
        lifecycle_state = self._storage_gateway().record_bot_run_lifecycle_checkpoint(checkpoint)
        emit_lifecycle_event(
            {
                **dict(lifecycle_state or {}),
                "bot_id": bot_id,
                "run_id": run_id,
                "phase": phase,
                "status": checkpoint["status"],
                "owner": owner,
                "message": message,
                "metadata": dict(metadata or {}),
                "failure": dict(failure or {}),
            }
        )
        self._storage_gateway().update_bot_runtime_status(
            bot_id=bot_id,
            run_id=run_id,
            status=checkpoint["status"],
        )
        return lifecycle_state

    @staticmethod
    def _reconciled_failure_phase(lifecycle: Mapping[str, Any] | None, run: Mapping[str, Any] | None) -> str:
        phase = str((lifecycle or {}).get("phase") or "").strip().lower()
        status = str((run or {}).get("status") or (lifecycle or {}).get("status") or "").strip().lower()
        if phase in {"live", "degraded", "telemetry_degraded", "awaiting_first_snapshot"}:
            return BotLifecyclePhase.CRASHED.value
        if status in {"running", "degraded", "telemetry_degraded"}:
            return BotLifecyclePhase.CRASHED.value
        return BotLifecyclePhase.STARTUP_FAILED.value

    def _terminalize_active_run_from_container(
        self,
        *,
        bot_id: str,
        run: Mapping[str, Any],
        lifecycle: Mapping[str, Any] | None,
        container_state: Mapping[str, Any],
        reason_code: str,
        request_id: str,
    ) -> Dict[str, Any]:
        run_id = str(run.get("run_id") or (lifecycle or {}).get("run_id") or "").strip()
        if not run_id:
            raise RuntimeError(f"cannot reconcile active bot without run_id bot_id={bot_id}")
        container_status = str(container_state.get("status") or "").strip().lower()
        exit_code = container_state.get("exit_code")
        if container_status in {"exited", "dead"} and int(exit_code or 0) == 0:
            phase = BotLifecyclePhase.COMPLETED.value
            message = "Active run reconciled as completed after container exited successfully."
            failure: Dict[str, Any] = {}
        else:
            phase = self._reconciled_failure_phase(lifecycle, run)
            message = "Active run reconciled as terminal after runtime container was unavailable."
            failure = build_failure_payload(
                phase=phase,
                message=message,
                type="run_container_reconciliation",
                reason_code=reason_code,
                owner=LifecycleOwner.BACKEND.value,
                exit_code=int(exit_code) if exit_code not in (None, "") else None,
            )
        metadata = {
            "reason_code": reason_code,
            "request_id": request_id or None,
            "container_status": container_status or None,
            "container_exit_code": exit_code,
            "container_runtime_run_id": container_state.get("runtime_run_id"),
            "container_oom_killed": bool(container_state.get("oom_killed")),
            "terminal_actor": "lifecycle_reconciler",
            "terminal_reason_text": message,
        }
        return self._record_lifecycle(
            bot_id=bot_id,
            run_id=run_id,
            phase=phase,
            owner=LifecycleOwner.BACKEND.value,
            message=message,
            metadata=metadata,
            failure=failure,
        )

    def _active_runs_for_bot(self, bot_id: str) -> list[Dict[str, Any]]:
        storage = self._storage_gateway()
        if hasattr(storage, "list_bot_runs"):
            rows = [dict(row) for row in storage.list_bot_runs(bot_id=bot_id)]
        else:
            latest_run_id = str(storage.get_latest_bot_runtime_run_id(bot_id) or "").strip()
            latest = storage.get_bot_run(latest_run_id) if latest_run_id else None
            rows = [dict(latest)] if latest else []
        active: list[Dict[str, Any]] = []
        for row in rows:
            run_id = str(row.get("run_id") or "").strip()
            lifecycle = (
                storage.get_bot_run_lifecycle(run_id)
                if run_id and hasattr(storage, "get_bot_run_lifecycle")
                else storage.get_latest_bot_run_lifecycle(bot_id)
                if run_id and hasattr(storage, "get_latest_bot_run_lifecycle")
                else None
            )
            if is_active_run_state(status=row.get("status"), phase=(lifecycle or {}).get("phase")) or is_active_run_state(
                status=(lifecycle or {}).get("status"),
                phase=(lifecycle or {}).get("phase"),
            ):
                active.append({**row, "_lifecycle": lifecycle or {}})
        active.sort(
            key=lambda entry: (
                str(entry.get("started_at") or entry.get("updated_at") or entry.get("created_at") or ""),
                str(entry.get("run_id") or ""),
            ),
            reverse=True,
        )
        return active

    def _reconcile_active_runs_before_start(self, *, bot_id: str, runner: BotRunner | None, request_id: str) -> list[Dict[str, Any]]:
        active_runs = self._active_runs_for_bot(bot_id)
        if not active_runs:
            return []
        if runner is None:
            return active_runs
        try:
            container_state = DockerBotRunner.inspect_bot_container(bot_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("bot_active_run_reconcile_container_inspect_failed | bot_id=%s | error=%s", bot_id, exc)
            return active_runs
        reconciled: set[str] = set()
        container_running = bool(container_state.get("running"))
        container_status = str(container_state.get("status") or "").strip().lower()
        container_run_id = str(container_state.get("runtime_run_id") or "").strip()
        for run in active_runs:
            run_id = str(run.get("run_id") or "").strip()
            lifecycle = run.get("_lifecycle") if isinstance(run.get("_lifecycle"), Mapping) else {}
            if container_running:
                if container_run_id and container_run_id != run_id:
                    self._terminalize_active_run_from_container(
                        bot_id=bot_id,
                        run=run,
                        lifecycle=lifecycle,
                        container_state=container_state,
                        reason_code="active_run_container_claimed_by_other_run",
                        request_id=request_id,
                    )
                    reconciled.add(run_id)
                continue
            if container_status in {"missing", "exited", "dead"}:
                reason_code = (
                    "container_exited_zero"
                    if container_status in {"exited", "dead"} and int(container_state.get("exit_code") or 0) == 0
                    else "container_exited_nonzero"
                    if container_status in {"exited", "dead"}
                    else "container_missing"
                )
                self._terminalize_active_run_from_container(
                    bot_id=bot_id,
                    run=run,
                    lifecycle=lifecycle,
                    container_state=container_state,
                    reason_code=reason_code,
                    request_id=request_id,
                )
                reconciled.add(run_id)
        if not reconciled:
            return active_runs
        return [run for run in self._active_runs_for_bot(bot_id) if str(run.get("run_id") or "") not in reconciled]

    def _cleanup_terminal_container_before_start(self, *, bot_id: str, run_id: str, runner: BotRunner) -> None:
        if not run_id:
            return
        try:
            container_state = DockerBotRunner.inspect_bot_container(bot_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("bot_terminal_container_inspect_failed | bot_id=%s | run_id=%s | error=%s", bot_id, run_id, exc)
            return
        if not bool(container_state.get("running")):
            return
        container_run_id = str(container_state.get("runtime_run_id") or "").strip()
        if container_run_id != str(run_id):
            logger.warning(
                "bot_terminal_container_cleanup_skipped_run_mismatch | bot_id=%s | expected_run_id=%s | container_run_id=%s",
                bot_id,
                run_id,
                container_run_id or None,
            )
            return
        self._stop_runner(runner, bot_id=bot_id, run_id=run_id, preserve_container=False)

    def start_bot(
        self,
        bot_id: str,
        *,
        request_id: str | None = None,
        start_overrides: Mapping[str, Any] | None = None,
    ) -> Dict[str, object]:
        normalized_request_id = str(request_id or "").strip() or str(uuid.uuid4())
        lock = _lock_for_bot(bot_id)
        with lock:
            return self._start_bot_locked(
                bot_id,
                request_id=normalized_request_id,
                start_overrides=dict(start_overrides or {}),
            )

    def _start_bot_locked(
        self,
        bot_id: str,
        *,
        request_id: str,
        start_overrides: Mapping[str, Any] | None = None,
    ) -> Dict[str, object]:
        watchdog = self._watchdog_instance()
        storage = self._storage_gateway()
        stored_bot = self._config.get_bot(bot_id)
        bot = _apply_start_overrides(stored_bot, start_overrides)
        config_hash = _start_config_hash(bot)
        runner = self._resolve_runner()
        active_runs = self._reconcile_active_runs_before_start(bot_id=bot_id, runner=runner, request_id=request_id)
        if active_runs:
            active_run = active_runs[0]
            active_run_id = str(active_run.get("run_id") or "").strip()
            active_lifecycle = active_run.get("_lifecycle") if isinstance(active_run.get("_lifecycle"), Mapping) else {}
            start_request = _run_start_request(active_run, active_lifecycle)
            projected = self._project_bot_from_storage(stored_bot)
            if start_request.get("request_id") == request_id and start_request.get("config_hash") == config_hash:
                return _control_response(
                    status="already_started",
                    bot_id=bot_id,
                    run_id=active_run_id,
                    active_run_id=active_run_id,
                    request_id=request_id,
                    message="Start request is an idempotent retry for the active run.",
                    bot=projected,
                )
            return _control_response(
                status="conflict",
                bot_id=bot_id,
                run_id=None,
                active_run_id=active_run_id,
                request_id=request_id,
                message="Bot already has an active run.",
                reason_code="active_run_conflict",
                bot=projected,
            )

        latest_run_id = storage.get_latest_bot_runtime_run_id(bot_id)
        if latest_run_id and runner is not None:
            latest_lifecycle = (
                storage.get_bot_run_lifecycle(latest_run_id)
                if hasattr(storage, "get_bot_run_lifecycle")
                else storage.get_latest_bot_run_lifecycle(bot_id)
            )
            latest_run = storage.get_bot_run(latest_run_id) or {}
            if is_terminal_run_state(status=latest_run.get("status"), phase=(latest_lifecycle or {}).get("phase")):
                self._cleanup_terminal_container_before_start(bot_id=bot_id, run_id=latest_run_id, runner=runner)
        if runner is None:
            raise RuntimeError("docker runner resolution failed for bot start")
        orchestrator = BotStartupOrchestrator(
            config_service=self._config,
            storage=storage,
            runner=runner,
            watchdog=watchdog,
        )
        try:
            ctx = orchestrator.start_bot(
                bot_id,
                request_id=request_id,
                config_hash=config_hash,
                effective_bot=bot,
            )
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
        return _control_response(
            status="started",
            bot_id=bot_id,
            run_id=ctx.run_id,
            active_run_id=ctx.run_id,
            request_id=request_id,
            message="Bot start accepted and runtime container launched.",
            bot=projected,
        )

    def stop_bot(
        self,
        bot_id: str,
        *,
        preserve_container: bool = False,
        run_id: str | None = None,
        request_id: str | None = None,
    ) -> Dict[str, object]:
        normalized_request_id = str(request_id or "").strip() or str(uuid.uuid4())
        lock = _lock_for_bot(bot_id)
        with lock:
            return self._stop_bot_locked(
                bot_id,
                preserve_container=preserve_container,
                run_id=run_id,
                request_id=normalized_request_id,
            )

    def _stop_bot_locked(
        self,
        bot_id: str,
        *,
        preserve_container: bool,
        run_id: str | None,
        request_id: str,
    ) -> Dict[str, object]:
        runner = self._resolve_runner()
        storage = self._storage_gateway()
        target_run_id = str(run_id or "").strip()
        if not target_run_id:
            active_runs = self._active_runs_for_bot(bot_id)
            target_run_id = str((active_runs[0] if active_runs else {}).get("run_id") or "").strip()
        if not target_run_id:
            target_run_id = str(storage.get_latest_bot_runtime_run_id(bot_id) or "").strip()
        if not target_run_id:
            bot = self._config.get_bot(bot_id)
            projected = self._project_bot_from_storage(bot, inspect_container=False)
            return _control_response(
                status="already_terminal",
                bot_id=bot_id,
                run_id=None,
                active_run_id=None,
                request_id=request_id,
                message="No run exists for this bot.",
                reason_code="run_not_found",
                bot=projected,
            )

        run = storage.get_bot_run(target_run_id) or {}
        lifecycle = (
            storage.get_bot_run_lifecycle(target_run_id)
            if hasattr(storage, "get_bot_run_lifecycle")
            else storage.get_latest_bot_run_lifecycle(bot_id)
        ) or {}
        if str(run.get("bot_id") or lifecycle.get("bot_id") or bot_id) != str(bot_id):
            raise RuntimeError(f"run {target_run_id} does not belong to bot {bot_id}")

        if is_terminal_run_state(status=run.get("status"), phase=lifecycle.get("phase")) or is_terminal_run_state(
            status=lifecycle.get("status"),
            phase=lifecycle.get("phase"),
        ):
            bot = self._config.get_bot(bot_id)
            projected = self._project_bot_from_storage(bot)
            return _control_response(
                status="already_terminal",
                bot_id=bot_id,
                run_id=target_run_id,
                active_run_id=target_run_id,
                request_id=request_id,
                message="Cancel request was idempotent because the run is already terminal.",
                reason_code="already_terminal",
                bot=projected,
            )

        self._record_lifecycle(
            bot_id=bot_id,
            run_id=target_run_id,
            phase=BotLifecyclePhase.CANCEL_REQUESTED.value,
            owner=LifecycleOwner.BACKEND.value,
            message="Cancel requested from backend control service.",
            metadata={
                "request_id": request_id,
                "target_run_id": target_run_id,
                "preserve_container": bool(preserve_container),
            },
        )
        self._record_lifecycle(
            bot_id=bot_id,
            run_id=target_run_id,
            phase=BotLifecyclePhase.CANCELING.value,
            owner=LifecycleOwner.BACKEND.value,
            message="Stopping runtime container for cancel request.",
            metadata={
                "request_id": request_id,
                "target_run_id": target_run_id,
                "preserve_container": bool(preserve_container),
            },
        )
        self._stop_runner(runner, bot_id=bot_id, preserve_container=preserve_container, run_id=target_run_id)
        watchdog = self._watchdog_instance()
        watchdog.unregister_bot(bot_id)
        try:
            storage.release_bot_run_lease(
                bot_id=bot_id,
                run_id=target_run_id,
                runner_id=watchdog.runner_id,
                status="released",
                metadata={"reason": "platform_cancel", "request_id": request_id},
            )
        except Exception as exc:  # noqa: BLE001 - cancellation lifecycle is the primary control result.
            logger.warning(
                "bot_cancel_run_lease_release_failed | bot_id=%s | run_id=%s | runner_id=%s | error=%s",
                bot_id,
                target_run_id,
                watchdog.runner_id,
                exc,
            )

        bot = self._config.get_bot(bot_id)
        payload = dict(bot)
        payload["status"] = BotLifecycleStatus.CANCELED.value
        payload["runner_id"] = None
        self._upsert_bot(payload)
        reason_text = (
            "Bot cancel completed; container preserved for debugging."
            if preserve_container
            else "Bot cancel completed; runtime container stopped."
        )
        self._record_lifecycle(
            bot_id=bot_id,
            run_id=target_run_id,
            phase=BotLifecyclePhase.CANCELED.value,
            owner=LifecycleOwner.BACKEND.value,
            message=reason_text,
            metadata={
                "request_id": request_id,
                "target_run_id": target_run_id,
                "terminal_actor": "platform_cancel",
                "terminal_reason_text": reason_text,
                "preserve_container": bool(preserve_container),
            },
        )
        logger.info(
            "bot_container_cancelled | bot_id=%s | run_id=%s | preserve_container=%s | request_id=%s",
            bot_id,
            target_run_id,
            preserve_container,
            request_id,
        )
        refreshed = self._config.get_bot(bot_id)
        projected = self._project_bot_from_storage(refreshed, inspect_container=True)
        self._broadcast("bot", {"bot": projected})
        return _control_response(
            status="canceled",
            bot_id=bot_id,
            run_id=target_run_id,
            active_run_id=target_run_id,
            request_id=request_id,
            message=reason_text,
            bot=projected,
        )

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
