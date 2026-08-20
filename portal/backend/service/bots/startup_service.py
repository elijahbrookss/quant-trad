"""Backend-owned bot startup orchestration service."""

from __future__ import annotations

import logging
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Protocol

from core.settings import get_settings
from engines.bot_runtime.strategy.models import Strategy
from engines.bot_runtime.core.execution_context import ResolvedExecutionContextBundle
from engines.bot_runtime.core.book_execution import ExecutionBookTapeBundle
from engines.bot_runtime.core.passive_execution import PassiveQueuePolicy

from ..provenance import RUNTIME_CONTRACT_VERSION, RUNTIME_STORAGE_SCHEMA_VERSION, source_revision
from ..market.backtest_dataset_service import validate_backtest_dataset
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


def _clean_hash(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


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
        "economic_claim_intent",
        "execution_assumptions",
        "resolved_execution_context_bundle",
        "execution_book_tape_bundle",
        "passive_queue_policy",
        "run_type",
        "playback_speed",
        "backtest_start",
        "backtest_end",
        "backtest_warmup_bars",
        "snapshot_interval_ms",
        "bot_env",
        "execution_semantics",
        "duration_seconds",
        "profile",
        "dataset_id",
        "dataset_binding",
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
    def upsert_bot_run(self, payload: Mapping[str, Any]) -> Dict[str, Any]: ...
    def record_bot_run_lifecycle_checkpoint(self, payload: Mapping[str, Any]) -> Dict[str, Any]: ...


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
            metadata={"bot_id": ctx.bot_id},
        )
        try:
            self._record_phase(
                ctx,
                BotLifecyclePhase.VALIDATING_CONFIGURATION.value,
                message="Validating bot configuration and startup prerequisites.",
            )
            artifacts = self.config_service.prepare_startup_artifacts(ctx.bot_record)
            ctx.strategy_id = str(artifacts.get("strategy_id") or ctx.strategy_id)
            strategy = artifacts.get("strategy")
            if not isinstance(strategy, Strategy):
                raise TypeError("startup artifacts must contain a typed Strategy")
            ctx.strategy_snapshot = strategy
            ctx.wallet_config = dict(artifacts.get("wallet_config") or {})
            ctx.runtime_readiness = dict(artifacts.get("runtime_readiness") or {})
            raw_context_bundle = artifacts.get("resolved_execution_context_bundle")
            if raw_context_bundle is not None:
                if not isinstance(raw_context_bundle, Mapping):
                    raise TypeError("startup artifacts resolved_execution_context_bundle must be a mapping")
                context_bundle = ResolvedExecutionContextBundle.from_dict(raw_context_bundle)
                ctx.bot_record["resolved_execution_context_bundle"] = context_bundle.to_dict()
            else:
                context_bundle = None
            raw_book_bundle = artifacts.get("execution_book_tape_bundle")
            if raw_book_bundle is not None:
                if not isinstance(raw_book_bundle, Mapping):
                    raise TypeError("startup artifacts execution_book_tape_bundle must be a mapping")
                book_bundle = ExecutionBookTapeBundle.from_dict(raw_book_bundle)
                ctx.bot_record["execution_book_tape_bundle"] = book_bundle.to_dict()
            else:
                book_bundle = None
            raw_queue_policy = artifacts.get("passive_queue_policy")
            if raw_queue_policy is not None:
                if not isinstance(raw_queue_policy, Mapping):
                    raise TypeError("startup artifacts passive_queue_policy must be a mapping")
                queue_policy = PassiveQueuePolicy.from_dict(raw_queue_policy)
                ctx.bot_record["passive_queue_policy"] = queue_policy.to_dict()
            else:
                queue_policy = None
            symbols = list(ctx.runtime_readiness.get("symbols") or [])
            ctx.runtime_dependency_metadata = {
                "symbols": symbols,
                "symbol_count": len(symbols),
                "worker_count_planned": len(symbols),
                "profiles": list(ctx.runtime_readiness.get("profiles") or []),
                "resolved_execution_context_bundle_hash": (
                    context_bundle.bundle_hash if context_bundle is not None else None
                ),
                "resolved_execution_context_count": (
                    len(context_bundle.contexts) if context_bundle is not None else 0
                ),
                "execution_book_tape_bundle_hash": (
                    book_bundle.bundle_hash if book_bundle is not None else None
                ),
                "execution_book_tape_count": (
                    len(book_bundle.tapes) if book_bundle is not None else 0
                ),
                "passive_queue_policy_hash": (
                    queue_policy.policy_hash if queue_policy is not None else None
                ),
            }
            ctx.bot_record["wallet_config"] = dict(ctx.wallet_config)

            ctx.bot_record["resolved_params"] = dict(strategy.resolved_params)
            ctx.bot_record["atm_template_id"] = strategy.atm_template_id
            ctx.bot_record["strategy_variant_name"] = (
                strategy.variant_name
                or ctx.bot_record.get("strategy_variant_name")
            )
            if str(ctx.bot_record.get("run_type") or "backtest").strip().lower() == "backtest":
                dataset_id = str(ctx.bot_record.get("dataset_id") or "").strip()
                binding = validate_backtest_dataset(
                    dataset_id=dataset_id,
                    bot=ctx.bot_record,
                    strategy=strategy,
                )
                ctx.dataset_binding = dict(binding)
                ctx.bot_record["dataset_binding"] = dict(binding)
                ctx.runtime_dependency_metadata["dataset"] = {
                    "dataset_id": binding["dataset_id"],
                    "dataset_hash": binding["dataset_hash"],
                    "contract_version": binding["dataset_contract_version"],
                    "quality": dict(binding["quality"]),
                }
                ctx.lifecycle_metadata["dataset"] = dict(ctx.runtime_dependency_metadata["dataset"])
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
            try:
                self.watchdog.register_bot(ctx.bot_id, run_id=ctx.run_id)
            except TypeError as exc:
                if "run_id" not in str(exc):
                    raise
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
                    self.runner.stop_bot(bot_id=ctx.bot_id, run_id=ctx.run_id)
                except Exception:  # noqa: BLE001
                    logger.exception("bot_startup_cleanup_stop_failed | bot_id=%s | run_id=%s", ctx.bot_id, ctx.run_id)
                try:
                    try:
                        self.watchdog.unregister_bot(ctx.bot_id, run_id=ctx.run_id)
                    except TypeError as unregister_exc:
                        if "run_id" not in str(unregister_exc):
                            raise
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
        return dict(self.config_service.get_bot(bot_id))

    def _ensure_run_record(self, ctx: BotStartupContext) -> None:
        self.storage.upsert_bot_run(
            {
                "run_id": ctx.run_id,
                "bot_id": ctx.bot_id,
                "bot_name": ctx.bot_record.get("name"),
                "strategy_id": ctx.strategy_id or None,
                "run_type": ctx.bot_record.get("run_type") or "backtest",
                "config_snapshot": {
                    "request_id": ctx.request_id or None,
                    "start_request": {
                        "request_id": ctx.request_id or None,
                        "config_hash": ctx.config_hash or None,
                    },
                },
                "runtime_contract_version": RUNTIME_CONTRACT_VERSION,
                "runtime_source_revision": source_revision(),
                "runtime_image": _BOT_RUNTIME_SETTINGS.image,
                "storage_schema_version": RUNTIME_STORAGE_SCHEMA_VERSION,
            }
        )

    def _acquire_run_lease(self, ctx: BotStartupContext) -> None:
        from ..storage.repos.run_leases import (
            bot_run_lease_token_hash,
            new_bot_run_lease_token,
        )

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
        if not isinstance(strategy, Strategy):
            raise TypeError("startup context requires a typed Strategy")
        run_strategy_snapshot = dict(strategy.run_strategy_snapshot)
        effective_strategy_config = dict(strategy.effective_strategy_config)
        execution_mode = _execution_mode_from_bot(ctx.bot_record)
        execution_behavior = execution_behavior_from_bot(ctx.bot_record)
        duration_seconds = _duration_seconds_from_bot(ctx.bot_record)
        bot_config_snapshot = _bot_run_config_snapshot(ctx.bot_record)
        start_request_overrides: Dict[str, Any] = {}
        if ctx.bot_record.get("run_type") is not None:
            start_request_overrides["run_type"] = ctx.bot_record.get("run_type")
        if ctx.bot_record.get("dataset_id") is not None:
            start_request_overrides["dataset_id"] = ctx.bot_record.get("dataset_id")
        if bool(ctx.bot_record.get("profile")):
            start_request_overrides["profile"] = True
        if execution_behavior:
            start_request_overrides["execution_behavior"] = execution_behavior
        start_request_overrides["economic_claim_intent"] = ctx.bot_record.get("economic_claim_intent")
        if isinstance(ctx.bot_record.get("execution_assumptions"), Mapping):
            start_request_overrides["execution_assumptions"] = dict(ctx.bot_record["execution_assumptions"])
        if isinstance(ctx.bot_record.get("execution_book_tape_bundle"), Mapping):
            start_request_overrides["execution_book_tape_bundle"] = dict(
                ctx.bot_record["execution_book_tape_bundle"]
            )
        if isinstance(ctx.bot_record.get("passive_queue_policy"), Mapping):
            start_request_overrides["passive_queue_policy"] = dict(
                ctx.bot_record["passive_queue_policy"]
            )
        if duration_seconds is not None:
            start_request_overrides["duration_seconds"] = duration_seconds
        if isinstance(ctx.bot_record.get("market_data_stream_policy"), Mapping):
            start_request_overrides["market_data_stream_policy"] = dict(ctx.bot_record["market_data_stream_policy"])
        strategy_hash = (
            _clean_hash(ctx.dataset_binding.get("strategy_hash"))
            or _clean_hash(run_strategy_snapshot.get("strategy_hash"))
            or _clean_hash(effective_strategy_config.get("strategy_hash"))
        )
        if strategy_hash:
            run_strategy_snapshot["strategy_hash"] = strategy_hash
        self.storage.upsert_bot_run(
            {
                "run_id": ctx.run_id,
                "bot_id": ctx.bot_id,
                "bot_name": ctx.bot_record.get("name"),
                "strategy_id": ctx.strategy_id or None,
                "strategy_name": getattr(strategy, "name", None),
                "run_type": ctx.bot_record.get("run_type") or "backtest",
                "timeframe": getattr(strategy, "timeframe", None),
                "datasource": getattr(strategy, "datasource", None),
                "exchange": getattr(strategy, "exchange", None),
                "symbols": list(ctx.runtime_dependency_metadata.get("symbols") or []),
                "backtest_start": ctx.bot_record.get("backtest_start"),
                "backtest_end": ctx.bot_record.get("backtest_end"),
                "config_snapshot": {
                    "execution_mode": execution_mode,
                    "execution_behavior": execution_behavior,
                    "economic_claim_intent": ctx.bot_record.get("economic_claim_intent"),
                    "execution_assumptions": dict(ctx.bot_record.get("execution_assumptions") or {}),
                    "resolved_execution_context_bundle": dict(
                        ctx.bot_record.get("resolved_execution_context_bundle") or {}
                    ),
                    "execution_book_tape_bundle": dict(
                        ctx.bot_record.get("execution_book_tape_bundle") or {}
                    ),
                    "passive_queue_policy": dict(
                        ctx.bot_record.get("passive_queue_policy") or {}
                    ),
                    "dataset_binding": dict(ctx.dataset_binding),
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
                "strategy_hash": strategy_hash,
                "runtime_contract_version": RUNTIME_CONTRACT_VERSION,
                "runtime_source_revision": source_revision(),
                "runtime_image": _BOT_RUNTIME_SETTINGS.image,
                "storage_schema_version": RUNTIME_STORAGE_SCHEMA_VERSION,
            }
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

__all__ = ["BotStartupOrchestrator"]
