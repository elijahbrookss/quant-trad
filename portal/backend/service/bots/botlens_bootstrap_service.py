from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Dict

from ..observability import BackendObserver
from . import bot_service
from .botlens_candle_continuity import continuity_summary_from_candles, emit_candle_continuity_summary
from .botlens_state import empty_run_projection_snapshot, select_default_symbol_key
from .botlens_transport import run_bootstrap_contract
from ..storage.storage import get_bot_run

_ACTIVE_STATUSES = {"starting", "running", "paused", "degraded", "telemetry_degraded"}
logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_bootstrap_service", event_logger=logger)
_STARTUP_WAIT_PHASES = frozenset(
    {
        "start_requested",
        "validating_configuration",
        "resolving_strategy",
        "resolving_runtime_dependencies",
        "preparing_run",
        "stamping_starting_state",
        "launching_container",
        "container_launched",
        "awaiting_container_boot",
        "container_booting",
        "loading_bot_config",
        "claiming_run",
        "loading_strategy_snapshot",
        "preparing_wallet",
        "planning_series_workers",
        "spawning_series_workers",
        "waiting_for_series_bootstrap",
        "warming_up_runtime",
        "runtime_subscribing",
        "awaiting_first_snapshot",
    }
)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _telemetry_hub():
    from .telemetry_stream import telemetry_hub

    return telemetry_hub


def _run_meta(
    *,
    run_id: str,
    projected_bot: Mapping[str, Any],
    health_state: Mapping[str, Any],
) -> Dict[str, Any]:
    row = _mapping(get_bot_run(run_id))
    summary_health = _mapping(health_state)
    return {
        "run_id": str(run_id),
        "bot_id": str(row.get("bot_id") or projected_bot.get("id") or "").strip() or None,
        "status": str(summary_health.get("status") or row.get("status") or projected_bot.get("status") or "").strip() or None,
        "started_at": row.get("started_at"),
        "ended_at": row.get("ended_at"),
        "strategy_id": row.get("strategy_id"),
        "strategy_name": row.get("strategy_name"),
        "run_type": row.get("run_type"),
        "datasource": row.get("datasource"),
        "exchange": row.get("exchange"),
        "symbols": list(row.get("symbols") or []) if isinstance(row.get("symbols"), list) else [],
        "summary": dict(row.get("summary") or {}) if isinstance(row.get("summary"), Mapping) else {},
    }


def _startup_bootstrap_state(
    *,
    lifecycle: Mapping[str, Any],
) -> tuple[str, str]:
    lifecycle_payload = _mapping(lifecycle)
    phase = str(lifecycle_payload.get("phase") or "").strip().lower()
    metadata = _mapping(lifecycle_payload.get("metadata"))
    series_progress = _mapping(metadata.get("series_progress"))
    total_series = max(int(series_progress.get("total_series") or 0), 0)
    bootstrapped_count = len(series_progress.get("bootstrapped_series") or [])
    warming_count = len(series_progress.get("warming_series") or [])
    live_count = len(series_progress.get("live_series") or [])

    if phase == "waiting_for_series_bootstrap":
        return (
            phase,
            f"Waiting for series bootstrap ({bootstrapped_count}/{total_series} series bootstrapped).",
        )
    if phase == "warming_up_runtime":
        return (
            phase,
            f"Workers are warming runtime state ({warming_count}/{total_series} series warming).",
        )
    if phase == "runtime_subscribing":
        return (
            phase,
            f"Runtime warm-up finished; subscribing workers to live facts ({bootstrapped_count}/{total_series} series bootstrapped).",
        )
    if phase == "awaiting_first_snapshot":
        return (
            phase,
            "Bootstrap completed; waiting for first live runtime facts "
            f"({live_count}/{total_series} series live).",
        )
    if phase in _STARTUP_WAIT_PHASES:
        return (
            phase,
            str(lifecycle_payload.get("message") or "").strip() or "Active runtime is still starting.",
        )
    return (
        "waiting_for_symbols",
        "Active runtime is starting. BotLens has not received the first symbol bootstrap yet.",
    )


async def get_active_botlens_run_bootstrap(*, bot_id: str) -> Dict[str, Any]:
    projected_bot = _mapping(bot_service.get_bot(str(bot_id)))
    active_run_id = str(projected_bot.get("active_run_id") or "").strip() or None
    lifecycle = _mapping(projected_bot.get("lifecycle"))
    bot_status = str(projected_bot.get("status") or "").strip().lower()

    if not active_run_id:
        return run_bootstrap_contract(
            bot_id=str(bot_id),
            run_id=None,
            run_meta=None,
            lifecycle=lifecycle,
            health={},
            symbol_catalog={},
            open_trades={},
            selected_symbol_key=None,
            state="inactive",
            run_live=False,
            transport_eligible=False,
            message="No active runtime is attached to this bot.",
            bootstrap_seq=0,
            base_seq=0,
            stream_session_id=None,
        )

    run_state = await _telemetry_hub().ensure_run_snapshot(run_id=active_run_id, bot_id=str(bot_id))
    cursor = await _telemetry_hub().current_cursor(run_id=active_run_id, bot_id=str(bot_id))
    symbol_index = run_state.symbol_catalog.entries
    open_trades_index = run_state.open_trades.entries

    if not symbol_index:
        bootstrap_state, bootstrap_message = _startup_bootstrap_state(
            lifecycle=run_state.lifecycle.to_dict() or lifecycle,
        )
        return run_bootstrap_contract(
            bot_id=str(bot_id),
            run_id=active_run_id,
            run_meta=_run_meta(run_id=active_run_id, projected_bot=projected_bot, health_state=run_state.health.to_dict()),
            lifecycle=run_state.lifecycle.to_dict() or lifecycle,
            health=run_state.health.to_dict(),
            symbol_catalog={},
            open_trades=open_trades_index,
            selected_symbol_key=None,
            state=bootstrap_state,
            run_live=bool(run_state.readiness.run_live),
            transport_eligible=bot_status in _ACTIVE_STATUSES,
            message=bootstrap_message,
            bootstrap_seq=int(run_state.seq or 0),
            base_seq=int(cursor.get("base_seq") or 0),
            stream_session_id=str(cursor.get("stream_session_id") or "").strip() or None,
        )

    selected_symbol_key = select_default_symbol_key(
        symbol_catalog=symbol_index,
        open_trades=open_trades_index,
    )
    if not selected_symbol_key:
        raise ValueError(f"BotLens could not resolve a default symbol for run_id={active_run_id}")
    selected_symbol_state = await _telemetry_hub().ensure_symbol_snapshot(
        run_id=active_run_id,
        bot_id=str(bot_id),
        symbol_key=selected_symbol_key,
    )
    if selected_symbol_state.readiness.snapshot_ready:
        continuity_summary = continuity_summary_from_candles(
            selected_symbol_state.candles.candles,
            timeframe=selected_symbol_state.identity.timeframe,
            series_key=selected_symbol_state.symbol_key,
        )
        emit_candle_continuity_summary(
            _OBSERVER,
            stage="botlens_run_bootstrap_snapshot",
            summary=continuity_summary,
            bot_id=bot_id,
            run_id=active_run_id,
            instrument_id=selected_symbol_state.identity.instrument_id,
            series_key=selected_symbol_state.symbol_key,
            symbol=selected_symbol_state.identity.symbol,
            timeframe=selected_symbol_state.identity.timeframe,
            message_kind="ephemeral",
            boundary_name="run_bootstrap_selected_symbol",
            extra={
                "contract": "botlens_run_bootstrap",
                "scope": "selected_symbol",
                "snapshot_seq": int(selected_symbol_state.seq or 0),
            },
        )

    return run_bootstrap_contract(
        bot_id=str(bot_id),
        run_id=active_run_id,
        run_meta=_run_meta(run_id=active_run_id, projected_bot=projected_bot, health_state=run_state.health.to_dict()),
        lifecycle=run_state.lifecycle.to_dict() or lifecycle,
        health=run_state.health.to_dict(),
        symbol_catalog=symbol_index,
        open_trades=open_trades_index,
        selected_symbol_key=selected_symbol_key,
        state="ready",
        run_live=bool(run_state.readiness.run_live),
        transport_eligible=bot_status in _ACTIVE_STATUSES,
        message="BotLens run bootstrap ready.",
        bootstrap_seq=int(run_state.seq or 0),
        base_seq=int(cursor.get("base_seq") or 0),
        stream_session_id=str(cursor.get("stream_session_id") or "").strip() or None,
        selected_symbol_state=selected_symbol_state,
    )


def resolve_active_botlens_stream(*, bot_id: str) -> Dict[str, Any]:
    projected_bot = _mapping(bot_service.get_bot(str(bot_id)))
    run_id = str(projected_bot.get("active_run_id") or "").strip()
    if not run_id:
        raise ValueError("BotLens live session is unavailable")
    return {
        "run_id": run_id,
        "run_bootstrap": {
            "scope": {
                "bot_id": str(bot_id),
                "run_id": run_id,
            }
        },
    }


__all__ = ["get_active_botlens_run_bootstrap", "resolve_active_botlens_stream"]
