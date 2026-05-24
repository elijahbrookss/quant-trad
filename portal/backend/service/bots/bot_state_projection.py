"""Pure authoritative bot lifecycle projection for API and SSE consumers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .botlens_state import RunProjectionSnapshot
from .runner import DockerBotRunner
from .startup_lifecycle import (
    ACTIVE_PHASES,
    ACTIVE_RUN_STATUSES,
    BACKEND_OWNED_PHASES,
    TERMINAL_PHASES,
    TERMINAL_RUN_STATUSES,
    is_active_run_state,
)

_ACTIVE_STATUSES = set(ACTIVE_RUN_STATUSES) | {"paused"}
_TERMINAL_STATUSES = set(TERMINAL_RUN_STATUSES) | {"idle", "error", "cancelled"}
_TELEMETRY_REASON_NO_ACTIVE_RUN = "no_active_run"
_TELEMETRY_REASON_SNAPSHOT_UNAVAILABLE = "snapshot_unavailable"


def _normalize_status(value: Any, default: str = "idle") -> str:
    text = str(value or "").strip().lower()
    return text or default


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _normalize_execution_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"fast", "full"} else "fast"


def _sequence(value: Any) -> list[Any]:
    return list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) else []


def _finite_number(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric == numeric and numeric not in {float("inf"), float("-inf")} else None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _heartbeat_projection(
    bot: Mapping[str, Any],
    *,
    claimed_active: bool,
    heartbeat_stale_ms: int,
) -> Dict[str, Any]:
    heartbeat_at = _parse_timestamp(bot.get("heartbeat_at"))
    runner_id = str(bot.get("runner_id") or "").strip() or None
    age_ms: Optional[int] = None
    if heartbeat_at is not None:
        age_ms = max(0, int((datetime.now(timezone.utc) - heartbeat_at).total_seconds() * 1000.0))
    if heartbeat_at is None:
        state = "missing" if claimed_active or runner_id else "inactive"
    elif age_ms is not None and age_ms > heartbeat_stale_ms:
        state = "stale"
    else:
        state = "fresh"
    return {
        "runner_id": runner_id,
        "at": bot.get("heartbeat_at"),
        "age_ms": age_ms,
        "state": state,
        "fresh": state == "fresh",
        "stale": state == "stale",
    }


def _default_container_state(bot_id: str) -> Dict[str, Any]:
    return {
        "name": DockerBotRunner.container_name_for(bot_id),
        "status": "missing",
        "running": False,
        "id": None,
        "started_at": None,
        "finished_at": None,
        "exit_code": None,
        "error": None,
    }


def _resolve_status(
    *,
    persisted_status: str,
    lifecycle_status: str,
    engine_status: str,
    container_status: str,
    container_running: bool,
    heartbeat_state: str,
) -> str:
    if lifecycle_status in _TERMINAL_STATUSES and container_running:
        if engine_status in _ACTIVE_STATUSES:
            return engine_status
        if persisted_status in _ACTIVE_STATUSES:
            return persisted_status
        return "degraded"
    if lifecycle_status in _ACTIVE_STATUSES | _TERMINAL_STATUSES:
        if lifecycle_status in _ACTIVE_STATUSES and container_status in {"exited", "dead"}:
            return "crashed"
        return lifecycle_status
    if container_running and engine_status:
        return engine_status
    if container_status in {"exited", "dead"}:
        return "crashed"
    if heartbeat_state == "stale" and persisted_status in _ACTIVE_STATUSES:
        return "crashed"
    if persisted_status in _ACTIVE_STATUSES | _TERMINAL_STATUSES:
        return persisted_status
    return "idle"


def _default_phase_for_status(status: str, *, container_running: bool, has_run_snapshot: bool) -> str:
    if status == "starting":
        return "container_booting" if container_running else "launching_container"
    if status in {"running", "paused"}:
        return "live" if has_run_snapshot else "awaiting_first_snapshot"
    if status in {"degraded", "telemetry_degraded"}:
        return status
    if status in {"startup_failed", "failed", "crashed", "stopped", "completed", "canceled", "cancelled", "degraded_terminal"}:
        return status
    return "idle"


def _resolve_reason(
    *,
    status: str,
    phase: str,
    container_status: str,
    heartbeat_state: str,
    selected_run_id: Optional[str],
) -> str:
    if phase == "launching_container":
        return "container_start_pending"
    if phase in {"container_booting", "loading_bot_config", "claiming_run"}:
        return "runtime_booting"
    if phase in {"awaiting_first_snapshot", "waiting_for_series_bootstrap"}:
        return "awaiting_first_snapshot"
    if phase == "live":
        return "live_runtime"
    if phase in {"degraded", "telemetry_degraded"}:
        return "runtime_degraded"
    if phase == "startup_failed":
        return "startup_failed"
    if phase == "completed":
        return "run_completed"
    if phase == "stopped":
        return "run_stopped"
    if phase in {"canceled", "cancelled"}:
        return "run_canceled"
    if phase == "degraded_terminal":
        return "run_degraded_terminal"
    if phase == "crashed":
        if container_status in {"exited", "dead"}:
            return "container_exited"
        if container_status == "missing" and selected_run_id:
            return "container_missing"
        if heartbeat_state == "stale":
            return "runner_stale"
        return "runtime_crashed"
    return status or "idle"


def _crash_summary(
    *,
    status: str,
    phase: str,
    reason: str,
    failure: Dict[str, Any],
) -> Optional[str]:
    if status not in {"crashed", "startup_failed"}:
        return None
    error_type = failure.get("error_type") or ""
    failed_at = failure.get("phase") or phase
    if status == "startup_failed":
        label = f"Startup failed at {failed_at}"
        return f"{label}: {error_type}" if error_type else label
    if reason == "container_exited":
        return f"Container exited during {failed_at}"
    if reason == "container_missing":
        return f"Container disappeared during {failed_at}"
    if reason == "runner_stale":
        return "Runner became unresponsive"
    return f"Runtime crashed during {failed_at}"


def _resolve_controls(*, status: str, phase: str, container_running: bool, has_run: bool) -> Dict[str, Any]:
    active = status in _ACTIVE_STATUSES
    can_start = not active
    start_label = "Start"
    if status == "completed":
        start_label = "Rerun"
    elif status in {"crashed", "error", "failed", "startup_failed", "stopped", "canceled", "cancelled", "degraded", "telemetry_degraded", "degraded_terminal"}:
        start_label = "Restart"
    elif phase in ACTIVE_PHASES:
        start_label = "Starting"
    return {
        "can_start": bool(can_start),
        "can_stop": bool(container_running or active or phase in ACTIVE_PHASES),
        "can_open_lens": bool(has_run),
        "can_delete": not active and phase not in ACTIVE_PHASES,
        "start_label": start_label,
    }


def _telemetry_reason(*, selected_run_id: Optional[str], has_run_snapshot: bool) -> Optional[str]:
    if has_run_snapshot:
        return None
    if selected_run_id:
        return _TELEMETRY_REASON_SNAPSHOT_UNAVAILABLE
    return _TELEMETRY_REASON_NO_ACTIVE_RUN


def _active_run_id_for_projection(*, selected_run_id: Optional[str], status: str, phase: str) -> Optional[str]:
    if not selected_run_id:
        return None
    return selected_run_id if is_active_run_state(status=status, phase=phase) else None


def _aggregate_symbol_catalog_stats(summary_state: RunProjectionSnapshot | None) -> Dict[str, Any]:
    entries = getattr(getattr(summary_state, "symbol_catalog", None), "entries", {}) or {}
    if not isinstance(entries, Mapping):
        return {}

    numeric_sums = {
        "wins": 0.0,
        "losses": 0.0,
        "gross_pnl": 0.0,
        "net_pnl": 0.0,
        "fees_paid": 0.0,
        "total_fees": 0.0,
        "total_trades": 0.0,
        "completed_trades": 0.0,
    }
    seen: set[str] = set()
    quote_currency: str | None = None
    for entry in entries.values():
        if not isinstance(entry, Mapping):
            continue
        stats = entry.get("stats") if isinstance(entry.get("stats"), Mapping) else {}
        if not stats:
            continue
        seen.add(str(entry.get("symbol_key") or entry.get("symbol") or len(seen)))
        for key in numeric_sums:
            value = _finite_number(stats.get(key))
            if value is not None:
                numeric_sums[key] += value
        if quote_currency is None:
            candidate = str(stats.get("quote_currency") or "").strip().upper()
            quote_currency = candidate or None

    if not seen:
        return {}

    stats: Dict[str, Any] = {"stats_source": "botlens_symbol_catalog"}
    integer_keys = {"wins", "losses", "total_trades", "completed_trades"}
    for key, value in numeric_sums.items():
        if value == 0:
            stats[key] = 0 if key in integer_keys else 0.0
        else:
            stats[key] = int(value) if key in integer_keys else value
    denominators = stats.get("wins", 0) + stats.get("losses", 0)
    if denominators:
        stats["win_rate"] = float(stats.get("wins", 0)) / float(denominators)
    if quote_currency:
        stats["quote_currency"] = quote_currency
    return stats


def _wallet_start_balance(config: Mapping[str, Any], quote_currency: str | None) -> tuple[float | None, str | None]:
    balances = _mapping(_mapping(config.get("wallet_config")).get("balances"))
    if not balances:
        return None, quote_currency
    normalized_quote = str(quote_currency or "").strip().upper() or None
    if normalized_quote and normalized_quote in balances:
        value = _finite_number(balances.get(normalized_quote))
        if value is not None:
            return value, normalized_quote
    for key, raw_value in balances.items():
        value = _finite_number(raw_value)
        if value is not None:
            return value, str(key or "").strip().upper() or normalized_quote
    return None, normalized_quote


def _latest_symbol_activity_at(summary_state: RunProjectionSnapshot | None) -> str | None:
    entries = getattr(getattr(summary_state, "symbol_catalog", None), "entries", {}) or {}
    timestamps = [
        str(entry.get("last_activity_at") or entry.get("last_event_at") or "").strip()
        for entry in entries.values()
        if isinstance(entry, Mapping)
    ]
    timestamps = [value for value in timestamps if value]
    return max(timestamps) if timestamps else None


def _attach_lightweight_equity_trace(
    stats: Dict[str, Any],
    *,
    bot_payload: Mapping[str, Any],
    selected_run: Mapping[str, Any],
    selected_run_config: Mapping[str, Any],
    summary_state: RunProjectionSnapshot | None,
) -> Dict[str, Any]:
    if not stats or isinstance(stats.get("equity_curve"), list):
        return stats
    net_pnl = _finite_number(stats.get("net_pnl"))
    if net_pnl is None:
        return stats
    selected_run_bot = _mapping(selected_run_config.get("bot"))
    wallet_source = (
        selected_run_bot
        if _mapping(selected_run_bot.get("wallet_config"))
        else selected_run_config
        if _mapping(selected_run_config.get("wallet_config"))
        else bot_payload
    )
    start_balance, quote_currency = _wallet_start_balance(
        wallet_source,
        str(stats.get("quote_currency") or "").strip().upper() or None,
    )
    if start_balance is None:
        return stats
    started_at = (
        bot_payload.get("backtest_start")
        or selected_run_bot.get("backtest_start")
        or selected_run.get("started_at")
        or bot_payload.get("last_run_at")
    )
    latest_at = _latest_symbol_activity_at(summary_state) or selected_run.get("ended_at") or selected_run.get("updated_at")
    if not started_at or not latest_at:
        return stats
    equity_end = start_balance + net_pnl
    next_stats = dict(stats)
    next_stats["equity_start"] = start_balance
    next_stats["equity_end"] = equity_end
    if quote_currency and not next_stats.get("quote_currency"):
        next_stats["quote_currency"] = quote_currency
    next_stats["equity_curve"] = [
        {"time": started_at, "value": start_balance},
        {"time": latest_at, "value": equity_end},
    ]
    next_stats["equity_curve_source"] = "realized_pnl_summary"
    return next_stats


def project_bot_state(
    bot: Mapping[str, Any],
    *,
    run: Mapping[str, Any] | None = None,
    lifecycle: Mapping[str, Any] | None = None,
    run_snapshot: RunProjectionSnapshot | None = None,
    view_row: Mapping[str, Any] | None = None,
    container_state: Mapping[str, Any] | None = None,
    heartbeat_stale_ms: int = 30000,
) -> Dict[str, Any]:
    payload = dict(bot or {})
    bot_id = str(payload.get("id") or "").strip()
    if not bot_id:
        raise ValueError("bot id is required for projection")

    selected_run = _mapping(run)
    lifecycle_row = _mapping(lifecycle)
    selected_run_id = (
        str(lifecycle_row.get("run_id") or "").strip()
        or str(selected_run.get("run_id") or "").strip()
        or None
    )
    del view_row
    summary_state = run_snapshot
    has_run_snapshot = summary_state is not None
    health_payload = summary_state.health.to_dict() if has_run_snapshot else {}
    snapshot_runtime = {
        "status": health_payload.get("status") if has_run_snapshot else None,
        "worker_count": int(health_payload.get("worker_count") or 0) if has_run_snapshot else None,
        "active_workers": int(health_payload.get("active_workers") or 0) if has_run_snapshot else None,
    }
    warnings = (
        [dict(entry) for entry in _sequence(health_payload.get("warnings")) if isinstance(entry, Mapping)]
        if has_run_snapshot
        else None
    )
    engine_status = _normalize_status(snapshot_runtime.get("status"), default="") if has_run_snapshot else ""
    persisted_status = _normalize_status(payload.get("status"))
    lifecycle_status = _normalize_status(lifecycle_row.get("status"), default="")
    claimed_active = bool(selected_run_id) and (
        persisted_status in _ACTIVE_STATUSES
        or lifecycle_status in _ACTIVE_STATUSES
        or bool(payload.get("runner_id"))
    )
    heartbeat = _heartbeat_projection(payload, claimed_active=claimed_active, heartbeat_stale_ms=heartbeat_stale_ms)
    container = dict(container_state or _default_container_state(bot_id))
    status = _resolve_status(
        persisted_status=persisted_status,
        lifecycle_status=lifecycle_status,
        engine_status=engine_status,
        container_status=str(container.get("status") or ""),
        container_running=bool(container.get("running")),
        heartbeat_state=str(heartbeat.get("state") or ""),
    )
    lifecycle_phase = str(lifecycle_row.get("phase") or "").strip()
    default_phase = _default_phase_for_status(
        status,
        container_running=bool(container.get("running")),
        has_run_snapshot=has_run_snapshot,
    )
    phase = default_phase if lifecycle_phase in TERMINAL_PHASES and status in _ACTIVE_STATUSES else (lifecycle_phase or default_phase)
    reason = _resolve_reason(
        status=status,
        phase=phase,
        container_status=str(container.get("status") or ""),
        heartbeat_state=str(heartbeat.get("state") or ""),
        selected_run_id=selected_run_id,
    )

    selected_run_config = _mapping(selected_run.get("config_snapshot"))
    selected_run_bot = _mapping(selected_run_config.get("bot"))
    selected_run_risk = _mapping(selected_run_config.get("risk_settings")) or _mapping(selected_run_bot.get("risk"))
    payload_risk = _mapping(payload.get("risk"))
    runtime_stats = _mapping(snapshot_runtime.get("stats"))
    if not runtime_stats:
        runtime_stats = _aggregate_symbol_catalog_stats(summary_state)
    if not runtime_stats:
        runtime_stats = _mapping(selected_run.get("summary")) or _mapping(payload.get("last_stats"))
    runtime_stats = _attach_lightweight_equity_trace(
        runtime_stats,
        bot_payload=payload,
        selected_run=selected_run,
        selected_run_config=selected_run_config,
        summary_state=summary_state,
    )
    execution_mode = _normalize_execution_mode(
        payload.get("execution_mode")
        or payload_risk.get("execution_mode")
        or selected_run.get("execution_mode")
        or selected_run_config.get("execution_mode")
        or selected_run_bot.get("execution_mode")
        or selected_run_risk.get("execution_mode")
    )
    telemetry_reason = _telemetry_reason(selected_run_id=selected_run_id, has_run_snapshot=has_run_snapshot)
    telemetry = {
        "run_id": selected_run_id,
        "execution_mode": execution_mode,
        "intrabar_execution": execution_mode == "full",
        "seq": int(summary_state.seq or 0) if has_run_snapshot else None,
        "available": has_run_snapshot,
        "reason": telemetry_reason,
        "known_at": health_payload.get("last_event_at") if has_run_snapshot else None,
        "last_snapshot_at": health_payload.get("last_event_at") if has_run_snapshot else None,
        "warning_count": int(health_payload.get("warning_count") or 0) if has_run_snapshot else None,
        "series_count": len(getattr(getattr(summary_state, "symbol_catalog", None), "entries", {}) or {}) if has_run_snapshot else None,
        "trade_count": len(getattr(getattr(summary_state, "open_trades", None), "entries", {}) or {}) if has_run_snapshot else None,
        "engine_status": (engine_status or None) if has_run_snapshot else None,
        "worker_count": snapshot_runtime.get("worker_count"),
        "active_workers": snapshot_runtime.get("active_workers"),
    }
    runtime = {
        **snapshot_runtime,
        "status": status,
        "phase": phase,
        "engine_status": (engine_status or None) if has_run_snapshot else None,
        "execution_mode": execution_mode,
        "intrabar_execution": execution_mode == "full",
        "run_id": selected_run_id,
        "seq": telemetry["seq"],
        "known_at": telemetry["known_at"],
        "last_snapshot_at": telemetry["last_snapshot_at"],
        "warnings": warnings,
        "stats": runtime_stats,
        "equity_curve": runtime_stats.get("equity_curve") if isinstance(runtime_stats.get("equity_curve"), list) else None,
        "started_at": snapshot_runtime.get("started_at") or selected_run.get("started_at") or payload.get("last_run_at"),
        "ended_at": snapshot_runtime.get("ended_at") or selected_run.get("ended_at"),
    }
    lifecycle_payload = {
        "status": status,
        "phase": phase,
        "reason": reason,
        "crash_summary": _crash_summary(
            status=status,
            phase=phase,
            reason=reason,
            failure=_mapping(lifecycle_row.get("failure")),
        ),
        "owner": lifecycle_row.get("owner"),
        "message": lifecycle_row.get("message"),
        "metadata": _mapping(lifecycle_row.get("metadata")),
        "failure": _mapping(lifecycle_row.get("failure")),
        "checkpoint_at": lifecycle_row.get("checkpoint_at"),
        "updated_at": lifecycle_row.get("updated_at"),
        "backend_owned": phase in BACKEND_OWNED_PHASES,
        "terminal": phase in TERMINAL_PHASES,
        "container": container,
        "heartbeat": heartbeat,
        "telemetry": telemetry,
        "live": bool(container.get("running")) and status in {"running", "degraded", "telemetry_degraded"},
    }
    controls = _resolve_controls(
        status=status,
        phase=phase,
        container_running=bool(container.get("running")),
        has_run=bool(selected_run_id),
    )
    active_run_id = _active_run_id_for_projection(selected_run_id=selected_run_id, status=status, phase=phase)

    payload["status"] = status
    payload["runtime"] = runtime
    payload["lifecycle"] = lifecycle_payload
    payload["controls"] = controls
    payload["active_run_id"] = active_run_id
    payload["latest_run_id"] = selected_run_id
    payload["run"] = dict(selected_run) if selected_run else None
    if selected_run:
        report_materialization = _mapping(selected_run.get("report_materialization"))
        if report_materialization:
            payload["report_materialization"] = report_materialization
            payload["report_status"] = report_materialization.get("status")
    return payload


def project_bot_states(
    bots: Sequence[Mapping[str, Any]],
    *,
    runs_by_bot_id: Mapping[str, Mapping[str, Any]] | None = None,
    lifecycle_by_bot_id: Mapping[str, Mapping[str, Any]] | None = None,
    view_rows_by_bot_id: Mapping[str, Mapping[str, Any]] | None = None,
    container_states_by_bot_id: Mapping[str, Mapping[str, Any]] | None = None,
    heartbeat_stale_ms: int = 30000,
) -> list[Dict[str, Any]]:
    projected: list[Dict[str, Any]] = []
    run_lookup = {str(key): value for key, value in dict(runs_by_bot_id or {}).items()}
    lifecycle_lookup = {str(key): value for key, value in dict(lifecycle_by_bot_id or {}).items()}
    view_lookup = {str(key): value for key, value in dict(view_rows_by_bot_id or {}).items()}
    container_lookup = {str(key): value for key, value in dict(container_states_by_bot_id or {}).items()}
    for bot in bots or []:
        if not isinstance(bot, Mapping):
            continue
        bot_id = str(bot.get("id") or "").strip()
        projected.append(
            project_bot_state(
                bot,
                run=run_lookup.get(bot_id),
                lifecycle=lifecycle_lookup.get(bot_id),
                view_row=view_lookup.get(bot_id),
                container_state=container_lookup.get(bot_id),
                heartbeat_stale_ms=heartbeat_stale_ms,
            )
        )
    return projected


__all__ = ["project_bot_state", "project_bot_states"]
