"""Backend-owned bot startup orchestration service."""

from __future__ import annotations

import logging
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Protocol

from core.settings import get_settings

from .botlens_lifecycle_bridge import emit_lifecycle_event
from .execution_behavior import execution_behavior_from_bot
from .startup_lifecycle import (
    BotLifecyclePhase,
    BotLifecycleStatus,
    BotStartupContext,
    LifecycleOwner,
    build_failure_payload,
    lifecycle_checkpoint_payload,
)

logger = logging.getLogger(__name__)
_BOT_RUNTIME_SETTINGS = get_settings().bot_runtime


def _execution_mode_from_bot(bot: Mapping[str, Any]) -> str:
    risk = bot.get("risk") if isinstance(bot.get("risk"), Mapping) else {}
    value = bot.get("execution_mode") or risk.get("execution_mode")
    normalized = str(value or "fast").strip().lower()
    return normalized if normalized in {"fast", "full"} else "fast"


def _duration_seconds_from_bot(bot: Mapping[str, Any]) -> float | None:
    value = bot.get("duration_seconds")
    if value in (None, ""):
        return None
    try:
        duration = float(value)
    except (TypeError, ValueError):
        raise ValueError("duration_seconds must be numeric") from None
    return duration if duration > 0 else None


def _bot_run_config_snapshot(bot: Mapping[str, Any]) -> Dict[str, Any]:
    """Return only run-effective bot config, not mutable operational state."""

    fields = (
        "id",
        "name",
        "strategy_id",
        "strategy_variant_id",
        "strategy_variant_name",
        "atm_template_id",
        "resolved_params",
        "risk_config",
        "risk",
        "wallet_config",
        "market_data_stream_policy",
        "mode",
        "execution_mode",
        "execution_behavior",
        "run_type",
        "playback_speed",
        "backtest_start",
        "backtest_end",
        "snapshot_interval_ms",
        "bot_env",
        "instrument_type",
        "duration_seconds",
    )
    snapshot: Dict[str, Any] = {}
    for field in fields:
        if field in bot:
            value = bot.get(field)
            if isinstance(value, Mapping):
                snapshot[field] = dict(value)
            elif isinstance(value, list):
                snapshot[field] = list(value)
            else:
                snapshot[field] = value
    return snapshot


class StartupStorage(Protocol):
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
    ) -> Dict[str, Any] | None: ...
    def upsert_bot(self, payload: Mapping[str, Any]) -> None: ...
    def upsert_bot_run(self, payload: Mapping[str, Any]) -> Dict[str, Any]: ...
    def record_bot_run_lifecycle_checkpoint(self, payload: Mapping[str, Any]) -> Dict[str, Any]: ...
    def update_bot_runtime_status(self, *, bot_id: str, run_id: str, status: str, telemetry_degraded: bool = False) -> None: ...


@dataclass
class BotStartupOrchestrator:
    config_service: Any
    storage: StartupStorage
    runner: Any
    watchdog: Any

    def start_bot(
        self,
        bot_id: str,
        *,
        request_id: str | None = None,
        config_hash: str | None = None,
        effective_bot: Mapping[str, Any] | None = None,
    ) -> BotStartupContext:
        persisted_bot = self._load_bot(bot_id)
        bot = dict(effective_bot or persisted_bot)
        normalized_request_id = str(request_id or "").strip()
        normalized_config_hash = str(config_hash or "").strip()
        ctx = BotStartupContext(
            bot_id=str(bot_id),
            bot_record=dict(bot),
            persisted_bot_record=dict(persisted_bot),
            run_id=str(uuid.uuid4()),
            strategy_id=str(bot.get("strategy_id") or "").strip(),
            strategy_snapshot=None,
            wallet_config={},
            runtime_readiness={},
            runtime_dependency_metadata={},
            request_id=normalized_request_id,
            config_hash=normalized_config_hash,
        )
        if normalized_request_id:
            ctx.lifecycle_metadata["request_id"] = normalized_request_id
        if normalized_config_hash:
            ctx.lifecycle_metadata["start_config_hash"] = normalized_config_hash
        # The backend owns run identity before the first lifecycle checkpoint so
        # lifecycle persistence can safely reference the active run via FK.
        self._ensure_run_record(ctx)
        self._acquire_run_lease(ctx)
        self._record_phase(
            ctx,
            BotLifecyclePhase.START_REQUESTED.value,
            message="Backend accepted bot start request.",
            metadata={"bot_status": str(bot.get("status") or "").strip().lower() or "idle"},
        )
        try:
            self._record_phase(
                ctx,
                BotLifecyclePhase.VALIDATING_CONFIGURATION.value,
                message="Validating bot configuration and startup prerequisites.",
            )
            artifacts = self.config_service.prepare_startup_artifacts(ctx.bot_record)
            ctx.strategy_id = str(artifacts.get("strategy_id") or ctx.strategy_id)
            ctx.strategy_snapshot = artifacts.get("strategy")
            ctx.wallet_config = dict(artifacts.get("wallet_config") or {})
            ctx.runtime_readiness = dict(artifacts.get("runtime_readiness") or {})
            symbols = list(ctx.runtime_readiness.get("symbols") or [])
            ctx.runtime_dependency_metadata = {
                "symbols": symbols,
                "symbol_count": len(symbols),
                "worker_count_planned": len(symbols),
                "profiles": list(ctx.runtime_readiness.get("profiles") or []),
            }
            ctx.bot_record["wallet_config"] = dict(ctx.wallet_config)

            strategy = ctx.strategy_snapshot
            strategy_payload = strategy.to_dict() if hasattr(strategy, "to_dict") else {}
            if isinstance(strategy_payload, dict):
                ctx.bot_record["resolved_params"] = dict(strategy_payload.get("resolved_params") or {})
                ctx.bot_record["atm_template_id"] = strategy_payload.get("atm_template_id")
                ctx.bot_record["strategy_variant_name"] = strategy_payload.get(
                    "variant_name",
                    ctx.bot_record.get("strategy_variant_name"),
                )
            self._record_phase(
                ctx,
                BotLifecyclePhase.RESOLVING_STRATEGY.value,
                message="Resolved backend startup strategy snapshot.",
                metadata={
                    "strategy_id": ctx.strategy_id,
                    "strategy_name": getattr(strategy, "name", None),
                    "timeframe": getattr(strategy, "timeframe", None),
                    "datasource": getattr(strategy, "datasource", None),
                    "exchange": getattr(strategy, "exchange", None),
                },
            )
            self._record_phase(
                ctx,
                BotLifecyclePhase.RESOLVING_RUNTIME_DEPENDENCIES.value,
                message="Resolved runtime dependencies and instrument readiness.",
                metadata=dict(ctx.runtime_dependency_metadata),
            )
            self._record_phase(
                ctx,
                BotLifecyclePhase.PREPARING_RUN.value,
                message="Persisting backend-owned run record and startup snapshot.",
                metadata={"run_id": ctx.run_id, "request_id": ctx.request_id or None, "start_config_hash": ctx.config_hash or None},
            )
            self._prepare_run_record(ctx)
            self._record_phase(
                ctx,
                BotLifecyclePhase.STAMPING_STARTING_STATE.value,
                message="Stamping backend-owned starting state before container launch.",
            )
            self._stamp_starting_state(ctx)
            self._record_phase(
                ctx,
                BotLifecyclePhase.LAUNCHING_CONTAINER.value,
                message="Launching runtime container with backend-owned run_id.",
            )
            ctx.bot_record["_runtime_request_id"] = ctx.request_id
            ctx.bot_record["_runtime_runner_id"] = self.watchdog.runner_id
            ctx.bot_record["_runtime_run_lease_token"] = ctx.run_lease_token
            ctx.container_id = str(self.runner.start_bot(bot=ctx.bot_record, run_id=ctx.run_id))
            self._record_phase(
                ctx,
                BotLifecyclePhase.CONTAINER_LAUNCHED.value,
                message="Runtime container launched successfully.",
                metadata={"container_id": ctx.container_id},
            )
            self.watchdog.register_bot(ctx.bot_id)
            self._record_phase(
                ctx,
                BotLifecyclePhase.AWAITING_CONTAINER_BOOT.value,
                message="Awaiting container bootstrap checkpoints.",
                metadata={"container_id": ctx.container_id, "runner_id": self.watchdog.runner_id},
            )
            return ctx
        except Exception as exc:  # noqa: BLE001
            if ctx.container_id:
                try:
                    self.runner.stop_bot(bot_id=ctx.bot_id)
                except Exception:  # noqa: BLE001
                    logger.exception("bot_startup_cleanup_stop_failed | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)
                try:
                    self.watchdog.unregister_bot(ctx.bot_id)
                except Exception:  # noqa: BLE001
                    logger.exception("bot_startup_cleanup_watchdog_failed | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)
            self._persist_startup_failure(ctx, exc, traceback_text=traceback.format_exc())
            self._release_run_lease(
                ctx,
                status="released",
                metadata={"reason": "startup_failed", "phase": ctx.current_phase},
            )
            raise

    def _load_bot(self, bot_id: str) -> Dict[str, Any]:
        bots = {str(bot["id"]): dict(bot) for bot in self.config_service.list_bots()}
        if bot_id not in bots:
            raise KeyError(f"Bot {bot_id} was not found")
        return bots[bot_id]

    def _ensure_run_record(self, ctx: BotStartupContext) -> None:
        self.storage.upsert_bot_run(
            {
                "run_id": ctx.run_id,
                "bot_id": ctx.bot_id,
                "bot_name": ctx.bot_record.get("name"),
                "strategy_id": ctx.strategy_id or None,
                "run_type": ctx.bot_record.get("run_type") or "backtest",
                "status": BotLifecycleStatus.STARTING.value,
                "started_at": ctx.started_at,
                "config_snapshot": {
                    "request_id": ctx.request_id or None,
                    "start_request": {
                        "request_id": ctx.request_id or None,
                        "config_hash": ctx.config_hash or None,
                    },
                },
            }
        )

    def _acquire_run_lease(self, ctx: BotStartupContext) -> None:
        from ..storage.storage import bot_run_lease_token_hash, new_bot_run_lease_token

        ctx.run_lease_token = new_bot_run_lease_token()
        lease = self.storage.acquire_bot_run_lease(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            runner_id=self.watchdog.runner_id,
            lease_token=ctx.run_lease_token,
            ttl_seconds=_BOT_RUNTIME_SETTINGS.run_lease_ttl_seconds,
            metadata={
                "owner": "backend_startup",
                "request_id": ctx.request_id or None,
                "start_config_hash": ctx.config_hash or None,
            },
        )
        ctx.lifecycle_metadata["run_lease"] = {
            "runner_id": self.watchdog.runner_id,
            "lease_token_hash": bot_run_lease_token_hash(ctx.run_lease_token),
            "status": lease.get("status"),
            "generation": lease.get("generation"),
            "expires_at": lease.get("expires_at"),
            "ttl_seconds": float(_BOT_RUNTIME_SETTINGS.run_lease_ttl_seconds),
        }

    def _release_run_lease(
        self,
        ctx: BotStartupContext,
        *,
        status: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if not ctx.run_lease_token:
            return
        try:
            self.storage.release_bot_run_lease(
                bot_id=ctx.bot_id,
                run_id=ctx.run_id,
                runner_id=self.watchdog.runner_id,
                lease_token=ctx.run_lease_token,
                status=status,
                metadata=metadata,
            )
        except Exception:  # noqa: BLE001 - startup failure is already being persisted.
            logger.exception("bot_startup_run_lease_release_failed | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)

    def _prepare_run_record(self, ctx: BotStartupContext) -> None:
        strategy = ctx.strategy_snapshot
        strategy_payload = strategy.to_dict() if hasattr(strategy, "to_dict") else {}
        run_strategy_snapshot = (
            dict(strategy_payload.get("run_strategy_snapshot") or {})
            if isinstance(strategy_payload, dict)
            else {}
        )
        effective_strategy_config = (
            dict(strategy_payload.get("effective_strategy_config") or {})
            if isinstance(strategy_payload, dict)
            else {}
        )
        execution_mode = _execution_mode_from_bot(ctx.bot_record)
        execution_behavior = execution_behavior_from_bot(ctx.bot_record)
        duration_seconds = _duration_seconds_from_bot(ctx.bot_record)
        bot_config_snapshot = _bot_run_config_snapshot(ctx.bot_record)
        start_request_overrides: Dict[str, Any] = {}
        if ctx.bot_record.get("run_type") is not None:
            start_request_overrides["run_type"] = ctx.bot_record.get("run_type")
        if execution_behavior:
            start_request_overrides["execution_behavior"] = execution_behavior
        if duration_seconds is not None:
            start_request_overrides["duration_seconds"] = duration_seconds
        if isinstance(ctx.bot_record.get("market_data_stream_policy"), Mapping):
            start_request_overrides["market_data_stream_policy"] = dict(ctx.bot_record["market_data_stream_policy"])
        self.storage.upsert_bot_run(
            {
                "run_id": ctx.run_id,
                "bot_id": ctx.bot_id,
                "bot_name": ctx.bot_record.get("name"),
                "strategy_id": ctx.strategy_id or None,
                "strategy_name": getattr(strategy, "name", None),
                "run_type": ctx.bot_record.get("run_type") or "backtest",
                "status": BotLifecycleStatus.STARTING.value,
                "timeframe": getattr(strategy, "timeframe", None),
                "datasource": getattr(strategy, "datasource", None),
                "exchange": getattr(strategy, "exchange", None),
                "symbols": list(ctx.runtime_dependency_metadata.get("symbols") or []),
                "backtest_start": ctx.bot_record.get("backtest_start"),
                "backtest_end": ctx.bot_record.get("backtest_end"),
                "started_at": ctx.started_at,
                "config_snapshot": {
                    "execution_mode": execution_mode,
                    "execution_behavior": execution_behavior,
                    "request_id": ctx.request_id or None,
                    "start_request": {
                        "request_id": ctx.request_id or None,
                        "config_hash": ctx.config_hash or None,
                        "overrides": start_request_overrides,
                    },
                    "bot": bot_config_snapshot,
                    "runtime_readiness": dict(ctx.runtime_readiness),
                    "run_strategy_snapshot": run_strategy_snapshot,
                    "effective_strategy_config": effective_strategy_config,
                },
            }
        )

    def _stamp_starting_state(self, ctx: BotStartupContext) -> None:
        payload = dict(ctx.persisted_bot_record or ctx.bot_record)
        payload["wallet_config"] = dict(ctx.wallet_config)
        payload["status"] = BotLifecycleStatus.STARTING.value
        payload["runner_id"] = self.watchdog.runner_id
        payload["last_run_at"] = ctx.started_at
        payload["last_run_artifact"] = {
            "startup": {
                "run_id": ctx.run_id,
                "request_id": ctx.request_id or None,
                "phase": ctx.current_phase,
                "message": "Backend stamped starting state.",
                "at": ctx.started_at,
            }
        }
        self.storage.upsert_bot(payload)
        self.storage.update_bot_runtime_status(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            status=BotLifecycleStatus.STARTING.value,
        )

    def _record_phase(
        self,
        ctx: BotStartupContext,
        phase: str,
        *,
        message: str,
        metadata: Mapping[str, Any] | None = None,
        failure: Mapping[str, Any] | None = None,
        owner: str = LifecycleOwner.BACKEND.value,
        status: str | None = None,
    ) -> Dict[str, Any]:
        ctx.current_phase = str(phase)
        merged_metadata = ctx.update_metadata(metadata)
        checkpoint = lifecycle_checkpoint_payload(
            bot_id=ctx.bot_id,
            run_id=ctx.run_id,
            phase=ctx.current_phase,
            owner=owner,
            message=message,
            metadata=merged_metadata,
            failure=failure,
            status=status,
        )
        lifecycle_state = self.storage.record_bot_run_lifecycle_checkpoint(checkpoint)
        emit_lifecycle_event(
            {
                **dict(lifecycle_state or {}),
                "bot_id": ctx.bot_id,
                "run_id": ctx.run_id,
                "phase": ctx.current_phase,
                "owner": owner,
                "message": message,
                "metadata": merged_metadata,
                "failure": dict(failure or lifecycle_state.get("failure") or {}),
                "status": str(lifecycle_state.get("status") or checkpoint["status"]).strip(),
            }
        )
        return lifecycle_state

    def _persist_startup_failure(self, ctx: BotStartupContext, exc: Exception, *, traceback_text: str | None = None) -> None:
        failure = build_failure_payload(
            phase=ctx.current_phase,
            message=str(exc),
            error_type=type(exc).__name__,
            type="startup_exception",
            reason_code="backend_startup_exception",
            owner=LifecycleOwner.BACKEND.value,
            exception_type=type(exc).__name__,
            traceback=traceback_text.strip() if traceback_text else None,
        )
        if ctx.request_id:
            failure["request_id"] = ctx.request_id
        try:
            self._record_phase(
                ctx,
                BotLifecyclePhase.STARTUP_FAILED.value,
                message=str(exc),
                failure=failure,
                status=BotLifecycleStatus.STARTUP_FAILED.value,
            )
        except Exception:  # noqa: BLE001
            logger.exception("bot_startup_failure_lifecycle_persist_failed | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)
        try:
            self.storage.update_bot_runtime_status(
                bot_id=ctx.bot_id,
                run_id=ctx.run_id,
                status=BotLifecycleStatus.STARTUP_FAILED.value,
            )
        except Exception:  # noqa: BLE001
            logger.exception("bot_startup_failure_status_persist_failed | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)
        payload = dict(ctx.persisted_bot_record or ctx.bot_record)
        payload["status"] = BotLifecycleStatus.STARTUP_FAILED.value
        payload["runner_id"] = None
        payload["last_run_at"] = ctx.started_at
        payload["last_run_artifact"] = {"error": failure}
        try:
            self.storage.upsert_bot(payload)
        except Exception:  # noqa: BLE001
            logger.exception("bot_startup_failure_bot_persist_failed | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)


__all__ = ["BotStartupOrchestrator"]
