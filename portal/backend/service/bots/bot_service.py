"""Facade for bot services (config + runtime control)."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Mapping

from core.settings import get_settings

from ..market import candle_service
from .bot_state_projection import project_bot_state
from .runner import DockerBotRunner
from .runtime_composition import get_runtime_composition
from .startup_lifecycle import is_active_run_state, is_terminal_run_state

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


def _telemetry_hub():
    from .telemetry_stream import telemetry_hub

    return telemetry_hub


def ensure_watchdog_stream_bridge() -> None:
    _ensure_watchdog_callback()


def _broadcast_bot_stream(event: str, payload: Dict[str, Any]) -> None:
    _composition().stream_manager.broadcast(event, payload)


def _load_projection_inputs(
    bot: Mapping[str, Any],
) -> tuple[
    Optional[Mapping[str, Any]],
    Optional[Mapping[str, Any]],
    Any,
]:
    bot_id = str(bot.get("id") or "").strip()
    lifecycle = _composition().storage.get_latest_bot_run_lifecycle(bot_id) if bot_id else None
    run_id = (
        str((lifecycle or {}).get("run_id") or "").strip()
        or _composition().storage.get_latest_bot_runtime_run_id(bot_id)
        if bot_id
        else None
    )
    run = _composition().storage.get_bot_run(run_id) if run_id else None
    if run and run_id:
        try:
            report_status = _composition().storage.get_report_materialization_status(run_id)
            run = {**dict(run), "report_materialization": report_status, "report_status": report_status.get("status")}
        except Exception as exc:  # noqa: BLE001 - bot cards must still render if report status is unavailable.
            logger.warning("bot_report_materialization_status_unavailable | bot_id=%s | run_id=%s | error=%s", bot_id, run_id, exc)
    run_snapshot = _telemetry_hub().get_run_snapshot(run_id=run_id) if run_id else None
    return run, lifecycle, run_snapshot


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
    lifecycle_metadata = lifecycle.get("metadata") if isinstance(lifecycle, Mapping) else {}
    lifecycle_status = str((lifecycle or {}).get("status") or "").strip().lower()
    persisted_status = str(bot.get("status") or "").strip().lower()
    should_inspect = bool(
        bot.get("runner_id")
        or bot.get("heartbeat_at")
        or lifecycle_status in {"starting", "running", "degraded", "telemetry_degraded"}
        or persisted_status in {"starting", "running", "degraded", "telemetry_degraded"}
        or bool((lifecycle_metadata or {}).get("preserve_container"))
    )
    if not should_inspect:
        return default_state
    try:
        return DockerBotRunner.inspect_bot_container(bot_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bot_container_inspect_failed | bot_id=%s | error=%s", bot_id, exc)
        return {**default_state, "status": "unknown", "error": str(exc)}


def _project_bot(bot: Mapping[str, Any], *, inspect_container: bool = True) -> Dict[str, Any]:
    run, lifecycle, run_snapshot = _load_projection_inputs(bot)
    container_state = _container_state_for_bot(bot, lifecycle, inspect_container=inspect_container)
    return project_bot_state(
        bot,
        run=run,
        lifecycle=lifecycle,
        run_snapshot=run_snapshot,
        container_state=container_state,
        heartbeat_stale_ms=_BOT_RUNTIME_SETTINGS.status_heartbeat_stale_ms,
    )


def list_bots() -> List[Dict[str, object]]:
    return [_project_bot(bot) for bot in _composition().config_service.list_bots()]


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _metric_subset(summary: Mapping[str, Any]) -> Dict[str, Any]:
    keys = (
        "net_pnl",
        "gross_pnl",
        "total_return_pct",
        "return_pct",
        "max_drawdown",
        "max_drawdown_pct",
        "profit_factor",
        "expectancy",
        "win_rate",
        "trades",
        "closed_trades",
        "total_trades",
        "fees",
        "exposure_pct",
        "time_in_market_pct",
        "execution_behavior",
        "market_event_counts",
        "duration_seconds",
        "orders_submitted",
        "fills_recorded",
        "wallet_mutations",
    )
    return {key: summary.get(key) for key in keys if summary.get(key) is not None}


def _run_strategy_snapshot(run: Mapping[str, Any]) -> Mapping[str, Any]:
    config = _as_mapping(run.get("config_snapshot"))
    snapshot = _as_mapping(config.get("run_strategy_snapshot"))
    if snapshot:
        return snapshot
    strategy = _as_mapping(config.get("strategy"))
    if strategy:
        return strategy
    return {}


def _latest_run_for_bot(bot_id: str, lifecycle: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
    run_id = (
        _clean_text(_as_mapping(lifecycle).get("run_id"))
        or _clean_text(_composition().storage.get_latest_bot_runtime_run_id(bot_id))
    )
    return _composition().storage.get_bot_run(run_id) if run_id else {}


def _report_status(run_id: str | None) -> Mapping[str, Any]:
    if not run_id:
        return {}
    try:
        return _composition().storage.get_report_materialization_status(run_id)
    except Exception as exc:  # noqa: BLE001 - run context should remain inspectable when report status is unavailable.
        logger.warning("bot_run_context_report_status_unavailable | run_id=%s | error=%s", run_id, exc)
        return {"status": "unavailable", "error": str(exc)}


def _controls_for(status: Any, phase: Any) -> Dict[str, Any]:
    active = is_active_run_state(status=status, phase=phase)
    terminal = is_terminal_run_state(status=status, phase=phase)
    normalized_status = str(status or "").strip().lower()
    if terminal and normalized_status in {"failed", "startup_failed", "crashed", "degraded_terminal"}:
        start_label = "Restart"
    elif terminal:
        start_label = "Rerun"
    else:
        start_label = "Start"
    return {
        "can_start": not active,
        "can_stop": active,
        "start_label": start_label,
    }


def _bot_run_context(bot: Mapping[str, Any]) -> Dict[str, Any]:
    bot_id = str(bot.get("id") or "").strip()
    lifecycle = _composition().storage.get_latest_bot_run_lifecycle(bot_id) if bot_id else {}
    lifecycle_map = _as_mapping(lifecycle)
    latest_run = _latest_run_for_bot(bot_id, lifecycle_map) if bot_id else {}
    latest_run_map = _as_mapping(latest_run)
    latest_run_id = _clean_text(latest_run_map.get("run_id")) or _clean_text(lifecycle_map.get("run_id"))
    status = _clean_text(lifecycle_map.get("status")) or _clean_text(latest_run_map.get("status")) or _clean_text(bot.get("status")) or "unknown"
    phase = _clean_text(lifecycle_map.get("phase"))
    active_run_id = latest_run_id if is_active_run_state(status=status, phase=phase) else None
    strategy_snapshot = _run_strategy_snapshot(latest_run_map)
    latest_config = _as_mapping(latest_run_map.get("config_snapshot"))
    latest_bot_snapshot = _as_mapping(latest_config.get("bot"))
    latest_bot_risk = _as_mapping(latest_bot_snapshot.get("risk"))
    report_status = _report_status(latest_run_id)
    summary = _as_mapping(latest_run_map.get("summary"))
    controls = _controls_for(status, phase)

    return {
        "schema_version": "bot_run_context.v1",
        "bot_id": bot_id,
        "name": bot.get("name"),
        "status": status,
        "phase": phase,
        "controls": controls,
        "can_start": controls["can_start"],
        "can_stop": controls["can_stop"],
        "strategy": {
            "strategy_id": bot.get("strategy_id"),
            "strategy_variant_id": bot.get("strategy_variant_id"),
            "strategy_variant_name": bot.get("strategy_variant_name"),
            "effective_strategy_config_hash": strategy_snapshot.get("effective_strategy_config_hash")
            or strategy_snapshot.get("strategy_hash"),
            "effective_params": strategy_snapshot.get("effective_params"),
            "variant_overrides": strategy_snapshot.get("variant_overrides"),
            "param_source_map": strategy_snapshot.get("param_source_map"),
        },
        "execution": {
            "run_type": latest_bot_snapshot.get("run_type") or bot.get("run_type"),
            "mode": latest_bot_snapshot.get("mode") or bot.get("mode"),
            "execution_mode": latest_config.get("execution_mode") or latest_bot_snapshot.get("execution_mode") or bot.get("execution_mode"),
            "execution_behavior": latest_config.get("execution_behavior")
            or latest_bot_snapshot.get("execution_behavior")
            or latest_bot_risk.get("execution_behavior")
            or bot.get("execution_behavior"),
            "datasource": latest_run_map.get("datasource") or bot.get("datasource"),
            "exchange": latest_run_map.get("exchange") or bot.get("exchange"),
            "timeframe": latest_run_map.get("timeframe"),
            "symbols": list(latest_run_map.get("symbols") or []),
            "backtest_start": bot.get("backtest_start"),
            "backtest_end": bot.get("backtest_end"),
            "snapshot_interval_ms": bot.get("snapshot_interval_ms"),
            "atm_template_id": bot.get("atm_template_id"),
            "risk_config": bot.get("risk_config") or {},
            "wallet_config": bot.get("wallet_config") or {},
            "instrument_type": bot.get("instrument_type"),
        },
        "active_run": {
            "run_id": active_run_id,
            "status": status if active_run_id else None,
            "phase": phase if active_run_id else None,
            "checkpoint_at": lifecycle_map.get("checkpoint_at") if active_run_id else None,
        },
        "latest_run": {
            "run_id": latest_run_id,
            "status": latest_run_map.get("status") or status,
            "started_at": latest_run_map.get("started_at"),
            "ended_at": latest_run_map.get("ended_at"),
            "summary": _metric_subset(summary),
            "report_status": report_status.get("status"),
        },
    }


def list_bot_run_contexts() -> Dict[str, Any]:
    items = [_bot_run_context(bot) for bot in _composition().config_service.list_bots()]
    return {
        "schema_version": "bot_run_context_list.v1",
        "items": items,
        "total": len(items),
    }


def get_bot_run_context(bot_id: str) -> Dict[str, Any]:
    return _bot_run_context(_composition().config_service.get_bot(bot_id))


def get_bot_run_status(bot_id: str, run_id: str) -> Dict[str, Any]:
    bot = _composition().config_service.get_bot(bot_id)
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required")
    run = _as_mapping(_composition().storage.get_bot_run(normalized_run_id))
    if run and str(run.get("bot_id") or "") != str(bot.get("id") or ""):
        raise KeyError(f"run {normalized_run_id!r} does not belong to bot {bot_id!r}")
    lifecycle = _as_mapping(_composition().storage.get_bot_run_lifecycle(normalized_run_id))
    if lifecycle and str(lifecycle.get("bot_id") or bot_id) != str(bot.get("id") or ""):
        raise KeyError(f"run {normalized_run_id!r} does not belong to bot {bot_id!r}")
    if not run and not lifecycle:
        raise KeyError(normalized_run_id)
    status = _clean_text(lifecycle.get("status")) or _clean_text(run.get("status")) or "unknown"
    phase = _clean_text(lifecycle.get("phase"))
    report_status = _report_status(normalized_run_id)
    terminal = is_terminal_run_state(status=status, phase=phase)
    return {
        "schema_version": "bot_run_status.v1",
        "bot_id": bot_id,
        "run_id": normalized_run_id,
        "status": status,
        "phase": phase,
        "terminal": terminal,
        "completed": status == "completed" or phase == "completed",
        "active": is_active_run_state(status=status, phase=phase),
        "checkpoint_at": lifecycle.get("checkpoint_at"),
        "updated_at": lifecycle.get("updated_at") or run.get("updated_at"),
        "started_at": run.get("started_at"),
        "ended_at": run.get("ended_at"),
        "summary": _metric_subset(_as_mapping(run.get("summary"))),
        "report": {
            "status": report_status.get("status"),
            "can_view": report_status.get("can_view"),
            "can_build": report_status.get("can_build"),
            "can_retry": report_status.get("can_retry"),
            "artifact_path": report_status.get("artifact_path"),
        },
    }


def start_bot_run_context(
    bot_id: str,
    *,
    request_id: str | None = None,
    start_overrides: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    start_payload = start_bot(bot_id, request_id=request_id, start_overrides=start_overrides)
    run_id = _clean_text(_as_mapping(start_payload).get("run_id")) or _clean_text(_as_mapping(start_payload).get("active_run_id"))
    context = get_bot_run_context(bot_id)
    payload = {
        "schema_version": "bot_run_start.v1",
        "request_id": _as_mapping(start_payload).get("request_id") or request_id,
        "bot_id": bot_id,
        "run_id": run_id or _as_mapping(context.get("active_run")).get("run_id"),
        "status": _as_mapping(start_payload).get("status") or context.get("status"),
        "phase": _as_mapping(start_payload).get("phase") or context.get("phase"),
        "context": context,
    }
    return payload


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


def preflight_bot_data(bot_id: str, *, start: str, end: str) -> Dict[str, Any]:
    """Return compact pre-run candle coverage for the bot's strategy instruments."""

    bot = _composition().config_service.get_bot(bot_id)
    windowed_bot = {**dict(bot), "backtest_start": start, "backtest_end": end}
    artifacts = _composition().config_service.prepare_startup_artifacts(windowed_bot)
    readiness = artifacts.get("runtime_readiness") if isinstance(artifacts.get("runtime_readiness"), Mapping) else {}
    timeframe = str(readiness.get("timeframe") or "").strip()
    checks: list[Dict[str, Any]] = []
    for profile in readiness.get("profiles") or []:
        if not isinstance(profile, Mapping):
            continue
        instrument_id = str(profile.get("instrument_id") or "").strip()
        if not instrument_id:
            checks.append(
                {
                    "schema_version": "candle_coverage_preflight.v1",
                    "symbol": profile.get("symbol"),
                    "timeframe": timeframe,
                    "status": "error",
                    "severity": "error",
                    "message": "Strategy instrument link is missing instrument_id.",
                }
            )
            continue
        checks.append(candle_service.preflight_candle_coverage_by_instrument(instrument_id, start, end, timeframe))
    has_error = any(str(item.get("severity") or item.get("status") or "").lower() == "error" for item in checks)
    has_warning = any(str(item.get("severity") or item.get("status") or "").lower() == "warning" for item in checks)
    return {
        "schema_version": "bot_data_preflight.v1",
        "bot_id": bot_id,
        "strategy": {
            "strategy_id": bot.get("strategy_id"),
            "strategy_variant_id": bot.get("strategy_variant_id"),
            "strategy_variant_name": bot.get("strategy_variant_name"),
        },
        "execution": {
            "provider": readiness.get("datasource"),
            "exchange": readiness.get("exchange"),
            "timeframe": timeframe,
            "requested_start": start,
            "requested_end": end,
        },
        "status": "error" if has_error else "warning" if has_warning else "ok",
        "checks": checks,
    }


def delete_bot_record(bot_id: str) -> None:
    _composition().config_service.delete_bot_record(bot_id)
    logger.info("[BotService] bot deleted", extra={"bot_id": bot_id})
    _broadcast_bot_stream("bot_deleted", {"bot_id": bot_id})


def start_bot(
    bot_id: str,
    *,
    request_id: str | None = None,
    start_overrides: Mapping[str, Any] | None = None,
) -> Dict[str, object]:
    _ensure_watchdog_callback()
    return _composition().runtime_control_service.start_bot(
        bot_id,
        request_id=request_id,
        start_overrides=start_overrides,
    )


def stop_bot(
    bot_id: str,
    *,
    preserve_container: bool = False,
    run_id: str | None = None,
    request_id: str | None = None,
) -> Dict[str, object]:
    return _composition().runtime_control_service.stop_bot(
        bot_id,
        preserve_container=preserve_container,
        run_id=run_id,
        request_id=request_id,
    )


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
        summary_state = _telemetry_hub().get_run_snapshot(run_id=run_id)
        runtime_payload = summary_state.health.to_dict() if summary_state is not None else {}
        summary = dict(run.get("summary") or {})
        if not summary:
            symbol_index = summary_state.symbol_catalog.entries if summary_state is not None else {}
            total_trades = 0
            for item in symbol_index.values():
                if not isinstance(item, Mapping):
                    continue
                stats = item.get("stats") if isinstance(item.get("stats"), Mapping) else {}
                total_trades += int(stats.get("total_trades") or 0)
            if total_trades > 0:
                summary = {"total_trades": total_trades}
        projected_runs.append(
            {
                **dict(run),
                "is_active": run_id == active_run_id,
                "runtime_status": str(runtime_payload.get("status") or run.get("status") or ""),
                "botlens_available": summary_state is not None,
                "botlens_reason": None if summary_state is not None else "snapshot_unavailable",
                "last_snapshot_at": runtime_payload.get("last_event_at"),
                "known_at": runtime_payload.get("last_event_at"),
                "seq": int(summary_state.seq or 0) if summary_state is not None else None,
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
    telemetry_unavailable_bots = 0

    for bot in _composition().config_service.list_bots():
        status = str(bot.get("status") or "").strip().lower()
        if status not in active_statuses:
            continue
        running_bots += 1
        runtime_payload: Mapping[str, Any] = {}
        bot_id = str(bot.get("id") or "")
        lifecycle = _composition().storage.get_latest_bot_run_lifecycle(bot_id)
        run_id = (
            str((lifecycle or {}).get("run_id") or "").strip()
            or _composition().storage.get_latest_bot_runtime_run_id(bot_id)
        )
        summary_state = _telemetry_hub().get_run_snapshot(run_id=run_id) if run_id else None
        if summary_state is None:
            telemetry_unavailable_bots += 1
            continue
        if summary_state is not None:
            runtime_payload = summary_state.health.to_dict()
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
        "telemetry_unavailable_bots": telemetry_unavailable_bots,
        "estimate_incomplete": telemetry_unavailable_bots > 0,
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
    "preflight_bot_data",
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
