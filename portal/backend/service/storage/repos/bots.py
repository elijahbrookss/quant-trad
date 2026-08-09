"""Storage repository module."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional

from ._shared import (
    BotRecord,
    SQLAlchemyError,
    _json_safe,
    _parse_optional_timestamp,
    _utcnow,
    db,
    logger,
    select,
)
from ....service.bots.startup_lifecycle import build_failure_payload

_BOT_LIST_COLUMNS = (
    BotRecord.id,
    BotRecord.name,
    BotRecord.strategy_id,
    BotRecord.strategy_variant_id,
    BotRecord.strategy_variant_name,
    BotRecord.atm_template_id,
    BotRecord.resolved_params,
    BotRecord.risk_config,
    BotRecord.mode,
    BotRecord.run_type,
    BotRecord.playback_speed,
    BotRecord.backtest_start,
    BotRecord.backtest_end,
    BotRecord.risk,
    BotRecord.wallet_config,
    BotRecord.market_data_stream_policy,
    BotRecord.snapshot_interval_ms,
    BotRecord.bot_env,
    BotRecord.created_at,
    BotRecord.updated_at,
)

_REMOVED_RUNTIME_FIELDS = frozenset(
    {
        "status",
        "last_run_at",
        "last_stats",
        "last_run_artifact",
        "runner_id",
        "heartbeat_at",
    }
)


def _watchdog_reason_code(reason: str) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized.startswith("startup_container_ambiguous:"):
        return "startup_container_ambiguous"
    if normalized.startswith("container_not_running:"):
        return "container_not_running"
    if normalized.startswith("stale_run_lease:"):
        return "stale_run_lease"
    if normalized.startswith("server_restart:"):
        return "server_restart"
    return "watchdog_orphaned"


def _watchdog_recoverable(reason: str) -> bool:
    normalized = str(reason or "").strip().lower()
    return normalized.startswith("stale_run_lease:") or normalized.startswith("startup_container_ambiguous:")


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _watchdog_diagnostics_metadata(diagnostics: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not diagnostics:
        return {}
    payload = _json_safe(dict(diagnostics))
    if not isinstance(payload, dict) or not payload:
        return {}
    return {"watchdog_diagnostics": payload}


def _watchdog_terminal_metadata(
    bot_id: str,
    reason: str,
    diagnostics: Optional[Mapping[str, Any]] = None,
    *,
    run_id: str | None = None,
) -> Dict[str, Any]:
    normalized = str(reason or "").strip().lower()
    metadata: Dict[str, Any] = {
        "terminal_actor": "watchdog_stop",
        "terminal_reason_text": str(reason or "").strip() or "watchdog_orphaned",
    }
    if normalized.startswith("container_not_running:"):
        try:
            from ....service.bots.runner import DockerBotRunner

            container = DockerBotRunner.inspect_bot_container(bot_id, run_id=run_id)
        except Exception:
            container = {}
        container_status = str(container.get("status") or "").strip().lower()
        metadata["container_status"] = container_status or None
        metadata["container_exit_code"] = container.get("exit_code")
        metadata["container_oom_killed"] = bool(container.get("oom_killed"))
        metadata["container_error"] = container.get("error")
        if bool(container.get("oom_killed")):
            metadata["terminal_actor"] = "oom_kill"
        elif container_status in {"exited", "dead"}:
            metadata["terminal_actor"] = "process_exit"
        elif container_status == "missing":
            metadata["terminal_actor"] = "unknown"
    metadata.update(_watchdog_diagnostics_metadata(diagnostics))
    return metadata


def _iso_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() + "Z" if hasattr(value, "isoformat") else str(value)


def _bot_mapping_to_dict(row: Mapping[str, Any]) -> Dict[str, Any]:
    risk_payload = dict(row.get("risk") or {})
    execution_mode = str(risk_payload.get("execution_mode") or "fast").strip().lower()
    if execution_mode not in {"fast", "full"}:
        execution_mode = "fast"
    execution_behavior = str(risk_payload.get("execution_behavior") or "simulated").strip().lower().replace("_", "-")
    if execution_behavior not in {"simulated", "observe-only"}:
        execution_behavior = "simulated"
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "strategy_id": row.get("strategy_id"),
        "strategy_variant_id": row.get("strategy_variant_id"),
        "strategy_variant_name": row.get("strategy_variant_name"),
        "atm_template_id": row.get("atm_template_id"),
        "resolved_params": dict(row.get("resolved_params") or {}),
        "risk_config": dict(row.get("risk_config") or {}),
        "mode": row.get("mode"),
        "execution_mode": execution_mode,
        "execution_behavior": execution_behavior,
        "run_type": row.get("run_type"),
        "playback_speed": float(row.get("playback_speed") if row.get("playback_speed") is not None else 0.0),
        "backtest_start": _iso_timestamp(row.get("backtest_start")),
        "backtest_end": _iso_timestamp(row.get("backtest_end")),
        "risk": risk_payload,
        "wallet_config": dict(row.get("wallet_config") or {}),
        "market_data_stream_policy": dict(row.get("market_data_stream_policy") or {}),
        "snapshot_interval_ms": int(row.get("snapshot_interval_ms") or 0),
        "bot_env": dict(row.get("bot_env") or {}),
        "created_at": _iso_timestamp(row.get("created_at")),
        "updated_at": _iso_timestamp(row.get("updated_at")),
    }


def load_bots() -> List[Dict[str, Any]]:
    """Return all persisted bot configurations."""

    if not db.available:
        return []
    with db.session() as session:
        rows = session.execute(select(*_BOT_LIST_COLUMNS)).mappings().all()
        if not rows:
            return []
        payload: List[Dict[str, Any]] = []
        for row in rows:
            record = _bot_mapping_to_dict(row)
            strategy_id = row.get("strategy_id")
            record["strategy_ids"] = [strategy_id] if strategy_id else []
            payload.append(record)
        return payload




def upsert_bot(payload: Dict[str, Any]) -> None:
    """Persist a bot configuration row."""

    bot_id = payload["id"]
    runtime_fields = sorted(_REMOVED_RUNTIME_FIELDS.intersection(payload))
    if runtime_fields:
        raise ValueError(
            "portal_bots is definition-only; runtime fields belong to run/lifecycle/lease/report tables: "
            + ", ".join(runtime_fields)
        )
    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(BotRecord, bot_id)
            now = _utcnow()
            if record is None:
                record = BotRecord(id=bot_id, name=payload.get("name") or bot_id)
                session.add(record)
            record.name = payload.get("name") or record.name
            strategy_ids: Optional[Iterable[str]] = payload.get("strategy_ids")
            first_strategy = None
            if strategy_ids:
                for strategy_id in strategy_ids:
                    if strategy_id:
                        candidate = str(strategy_id).strip()
                        if candidate:
                            first_strategy = candidate
                            break
            if not first_strategy:
                fallback = payload.get("strategy_id")
                if fallback:
                    candidate = str(fallback).strip()
                    if candidate:
                        first_strategy = candidate
            record.strategy_id = first_strategy
            if "strategy_variant_id" in payload:
                variant_id = payload.get("strategy_variant_id")
                record.strategy_variant_id = str(variant_id).strip() if variant_id else None
            if "strategy_variant_name" in payload:
                variant_name = payload.get("strategy_variant_name")
                record.strategy_variant_name = str(variant_name).strip() if variant_name else None
            if "atm_template_id" in payload:
                atm_template_id = payload.get("atm_template_id")
                record.atm_template_id = str(atm_template_id).strip() if atm_template_id else None
            if "resolved_params" in payload:
                record.resolved_params = dict(_json_safe(payload.get("resolved_params") or {}))
            if "risk_config" in payload:
                record.risk_config = dict(_json_safe(payload.get("risk_config") or {}))
            # datasource/exchange/timeframe are no longer stored on bots; derive from strategy at runtime
            record.mode = payload.get("mode") or record.mode
            record.run_type = payload.get("run_type") or record.run_type
            record.playback_speed = 0.0
            if "risk" in payload:
                record.risk = dict(payload.get("risk") or {})
            if "execution_mode" in payload:
                risk_payload = dict(record.risk or {})
                risk_payload["execution_mode"] = payload.get("execution_mode")
                record.risk = risk_payload
            if "execution_behavior" in payload:
                risk_payload = dict(record.risk or {})
                risk_payload["execution_behavior"] = payload.get("execution_behavior")
                record.risk = risk_payload
            if "wallet_config" in payload:
                record.wallet_config = dict(payload.get("wallet_config") or {})
            if "market_data_stream_policy" in payload:
                record.market_data_stream_policy = dict(_json_safe(payload.get("market_data_stream_policy") or {}))
            if "snapshot_interval_ms" in payload:
                record.snapshot_interval_ms = int(payload.get("snapshot_interval_ms") or 0)
            if "bot_env" in payload:
                record.bot_env = dict(payload.get("bot_env") or {})
            record.backtest_start = _parse_optional_timestamp(payload.get("backtest_start")) or record.backtest_start
            record.backtest_end = _parse_optional_timestamp(payload.get("backtest_end")) or record.backtest_end
            record.updated_at = now
            if record.created_at is None:
                record.created_at = now
    except SQLAlchemyError as exc:
        logger.warning("bot_persist_failed | id=%s | error=%s", bot_id, exc)


def mark_bot_crashed(
    bot_id: str,
    reason: str = "orphaned",
    diagnostics: Optional[Mapping[str, Any]] = None,
    *,
    run_id: str | None = None,
) -> bool:
    """Record a watchdog crash/degradation checkpoint for one exact run.

    Returns True if the bot was updated, False otherwise.
    """

    if not db.available:
        return False
    latest_run_id = ""
    try:
        from ....service.bots.startup_lifecycle import BotLifecyclePhase, BotLifecycleStatus, LifecycleOwner
        from .lifecycle import get_bot_run_lifecycle, get_latest_bot_run_lifecycle, record_bot_run_lifecycle_checkpoint
        from .run_leases import get_bot_run_lease

        with db.session() as session:
            record = session.get(BotRecord, bot_id)
            if record is None:
                return False
        requested_run_id = str(run_id or "").strip()
        latest_lifecycle = (
            get_bot_run_lifecycle(requested_run_id)
            if requested_run_id
            else get_latest_bot_run_lifecycle(bot_id)
        )
        latest_run_id = str(_row_value(latest_lifecycle, "run_id") or "").strip()
        lifecycle_bot_id = str(_row_value(latest_lifecycle, "bot_id") or "").strip()
        if requested_run_id and latest_run_id != requested_run_id:
            logger.error(
                "bot_mark_crashed_run_context_missing | id=%s | run_id=%s | reason=%s",
                bot_id,
                requested_run_id,
                reason,
            )
            return False
        if lifecycle_bot_id and lifecycle_bot_id != str(bot_id):
            logger.error(
                "bot_mark_crashed_run_owner_mismatch | id=%s | run_id=%s | lifecycle_bot_id=%s",
                bot_id,
                latest_run_id or requested_run_id,
                lifecycle_bot_id,
            )
            return False
        latest_phase = str(_row_value(latest_lifecycle, "phase") or "").strip().lower()
        latest_status = str(_row_value(latest_lifecycle, "status") or "").strip().lower()
        lease = get_bot_run_lease(latest_run_id) if latest_run_id else None
        diagnostics_runner = (
            diagnostics.get("previous_runner")
            if isinstance(diagnostics, Mapping)
            else None
        )
        previous_runner = str(diagnostics_runner or (lease or {}).get("runner_id") or "").strip() or None
        if latest_phase in {
            BotLifecyclePhase.COMPLETED.value,
            BotLifecyclePhase.STOPPED.value,
            BotLifecyclePhase.STARTUP_FAILED.value,
            BotLifecyclePhase.CRASHED.value,
        } or latest_status in {
            BotLifecycleStatus.COMPLETED.value,
            BotLifecycleStatus.STOPPED.value,
            BotLifecycleStatus.STARTUP_FAILED.value,
            BotLifecycleStatus.CRASHED.value,
        }:
            logger.info(
                "bot_mark_crashed_skipped_terminal | id=%s | reason=%s | phase=%s | status=%s",
                bot_id,
                reason,
                latest_phase or None,
                latest_status or None,
            )
            return False
        if not latest_run_id:
            logger.error(
                "bot_mark_crashed_missing_run_context | id=%s | reason=%s | previous_runner=%s",
                bot_id,
                reason,
                previous_runner,
            )
            return False
        recoverable_watchdog_condition = _watchdog_recoverable(reason)
        if recoverable_watchdog_condition:
            logger.warning(
                "bot_watchdog_recoverable_condition_recorded | id=%s | reason=%s | previous_runner=%s",
                bot_id,
                reason,
                previous_runner,
            )
        else:
            logger.info(
                "bot_marked_crashed | id=%s | reason=%s | previous_runner=%s",
                bot_id,
                reason,
                previous_runner,
            )
        if latest_run_id:
            recoverable_watchdog_condition = _watchdog_recoverable(reason)
            terminal_metadata = (
                {
                    "watchdog_condition": _watchdog_reason_code(reason),
                    "watchdog_classification": "recoverable",
                    "recoverable": True,
                    "watchdog_reason_text": str(reason or "").strip() or "stale_run_lease",
                }
                if recoverable_watchdog_condition
                else _watchdog_terminal_metadata(
                    bot_id,
                    reason,
                    diagnostics,
                    run_id=latest_run_id,
                )
            )
            if recoverable_watchdog_condition:
                terminal_metadata.update(_watchdog_diagnostics_metadata(diagnostics))
            phase = (
                BotLifecyclePhase.DEGRADED.value
                if recoverable_watchdog_condition
                else BotLifecyclePhase.CRASHED.value
            )
            status = (
                BotLifecycleStatus.DEGRADED.value
                if recoverable_watchdog_condition
                else BotLifecycleStatus.CRASHED.value
            )
            reason_code = _watchdog_reason_code(reason)
            message = (
                f"Recoverable watchdog condition observed: {reason}"
                if recoverable_watchdog_condition
                else f"Bot marked crashed by watchdog: {reason}"
            )
            failure_type = f"watchdog_{reason_code}" if recoverable_watchdog_condition else "watchdog_crash"
            record_bot_run_lifecycle_checkpoint(
                {
                    "bot_id": bot_id,
                    "run_id": latest_run_id,
                    "phase": phase,
                    "status": status,
                    "owner": LifecycleOwner.WATCHDOG.value,
                    "message": message,
                    "metadata": terminal_metadata,
                    "failure": build_failure_payload(
                        phase=phase,
                        message=message,
                        type=failure_type,
                        reason_code=reason_code,
                        owner=LifecycleOwner.WATCHDOG.value,
                    )
                    | {"recoverable": recoverable_watchdog_condition}
                    | {"reason": reason}
                    | terminal_metadata,
                }
            )
        return True
    except SQLAlchemyError as exc:
        logger.warning("bot_mark_crashed_failed | id=%s | error=%s", bot_id, exc)
        return False

def get_bot(bot_id: str) -> Optional[Dict[str, Any]]:
    """Return a persisted bot configuration."""

    if not db.available:
        return None
    if not bot_id:
        return None
    with db.session() as session:
        row = (
            session.execute(select(*_BOT_LIST_COLUMNS).where(BotRecord.id == str(bot_id)))
            .mappings()
            .first()
        )
        if not row:
            return None
        record = _bot_mapping_to_dict(row)
        strategy_id = row.get("strategy_id")
        record["strategy_ids"] = [strategy_id] if strategy_id else []
        return record




def delete_bot(bot_id: str) -> None:
    """Remove a bot configuration permanently."""

    if not db.available:
        return
    try:
        with db.session() as session:
            record = session.get(BotRecord, bot_id)
            if record:
                session.delete(record)
    except SQLAlchemyError as exc:
        logger.warning("bot_delete_failed | id=%s | error=%s", bot_id, exc)
