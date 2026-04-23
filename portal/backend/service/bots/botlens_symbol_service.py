from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Dict

from ..observability import BackendObserver
from .botlens_candle_continuity import continuity_summary_from_candles, emit_candle_continuity_summary
from .botlens_contract import normalize_series_key
from .botlens_event_replay import rebuild_run_projection_snapshot, rebuild_symbol_projection_snapshot
from .botlens_state import (
    RunProjectionSnapshot,
    SymbolCandlesState,
    SymbolProjectionSnapshot,
    empty_run_projection_snapshot,
    empty_symbol_projection_snapshot,
    merge_candles,
)
from .botlens_transport import (
    selected_symbol_snapshot_contract,
    symbol_catalog_response_contract,
    symbol_detail_response_contract,
)

logger = logging.getLogger(__name__)
_OBSERVER = BackendObserver(component="botlens_symbol_service", event_logger=logger)


def get_bot_run(run_id: str):
    from ..storage.storage import get_bot_run as _get_bot_run

    return _get_bot_run(run_id)


def _telemetry_hub():
    from .telemetry_stream import telemetry_hub

    return telemetry_hub


def _run_bot_id(*, run_id: str) -> str:
    run_row = get_bot_run(str(run_id)) or {}
    bot_id = str(run_row.get("bot_id") or "").strip()
    if not bot_id:
        raise ValueError(f"bot_id missing for run_id={run_id}")
    return bot_id


def _historical_run_snapshot(*, run_id: str, max_seq: int | None = None) -> tuple[str, Any]:
    bot_id = _run_bot_id(run_id=run_id)
    return bot_id, rebuild_run_projection_snapshot(bot_id=bot_id, run_id=str(run_id), max_seq=max_seq)


def _resolved_run_state(*, bot_id: str, run_id: str, run_state: RunProjectionSnapshot | None) -> RunProjectionSnapshot:
    return run_state or empty_run_projection_snapshot(bot_id=bot_id, run_id=str(run_id))


def _trim_symbol_snapshot(
    *,
    symbol_state: SymbolProjectionSnapshot,
    limit: int,
) -> SymbolProjectionSnapshot:
    return replace(
        symbol_state,
        candles=SymbolCandlesState(
            candles=merge_candles(symbol_state.candles.candles, limit=max(1, int(limit or 320)))
        ),
    )


def _symbol_projection_error(symbol_state: SymbolProjectionSnapshot | None) -> str | None:
    for entry in tuple(getattr(getattr(symbol_state, "diagnostics", None), "diagnostics", ()) or ()):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("type") or "").strip().lower() == "projection_error":
            return str(entry.get("message") or "Symbol projection is unavailable because ledger rebuild failed.").strip()
    return None


async def load_symbol_detail_state(
    *,
    run_id: str,
    symbol_key: str,
    limit: int = 320,
    max_seq: int | None = None,
) -> Dict[str, Any]:
    # Explicit detail/debug reads may still reconstruct bounded state from the ledger.
    # This is not the normal selected-symbol switching path.
    normalized_symbol_key = normalize_series_key(symbol_key)
    if not normalized_symbol_key:
        raise ValueError("canonical symbol_key is required")
    bounded_max_seq = int(max_seq) if max_seq is not None else None
    live_snapshot = None
    if bounded_max_seq is None or bounded_max_seq <= 0:
        live_snapshot = _telemetry_hub().get_symbol_snapshot(run_id=str(run_id), symbol_key=normalized_symbol_key)
        if live_snapshot is None:
            bot_id = _run_bot_id(run_id=run_id)
            live_snapshot = await _telemetry_hub().ensure_symbol_snapshot(
                run_id=str(run_id),
                bot_id=bot_id,
                symbol_key=normalized_symbol_key,
            )
    if live_snapshot is not None:
        return _trim_symbol_snapshot(symbol_state=live_snapshot, limit=limit)

    bot_id, run_state = _historical_run_snapshot(run_id=run_id)
    detail_state = rebuild_symbol_projection_snapshot(
        bot_id=bot_id,
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
        max_seq=bounded_max_seq,
    )
    if detail_state is None:
        if run_state is None or normalized_symbol_key not in run_state.symbol_catalog.entries:
            raise ValueError(f"symbol {normalized_symbol_key!r} was not found for run_id={run_id}")
        return empty_symbol_projection_snapshot(normalized_symbol_key)

    return _trim_symbol_snapshot(symbol_state=detail_state, limit=limit)


async def get_symbol_detail(*, run_id: str, symbol_key: str, limit: int = 320) -> Dict[str, Any]:
    detail_state = await load_symbol_detail_state(run_id=run_id, symbol_key=symbol_key, limit=limit)
    bot_id = _run_bot_id(run_id=run_id)
    run_state = _telemetry_hub().get_run_snapshot(run_id=str(run_id))
    if run_state is None:
        run_state = await _telemetry_hub().ensure_run_snapshot(run_id=str(run_id), bot_id=bot_id)
    resolved_run_state = _resolved_run_state(bot_id=bot_id, run_id=str(run_id), run_state=run_state)
    return symbol_detail_response_contract(
        run_id=str(run_id),
        symbol_state=detail_state,
        run_health=resolved_run_state.health.to_dict(),
    )


async def get_selected_symbol_snapshot(*, run_id: str, symbol_key: str, limit: int = 320) -> Dict[str, Any]:
    bot_id = _run_bot_id(run_id=run_id)
    normalized_symbol_key = normalize_series_key(symbol_key)
    if not normalized_symbol_key:
        raise ValueError("canonical symbol_key is required")

    # Normal selected-symbol switching is a read of canonical projector state.
    # If a projector is missing, we lazily ensure it once through projector ownership.
    run_state = _telemetry_hub().get_run_snapshot(run_id=str(run_id))
    if run_state is None:
        run_state = await _telemetry_hub().ensure_run_snapshot(run_id=str(run_id), bot_id=bot_id)
    resolved_run_state = _resolved_run_state(bot_id=bot_id, run_id=str(run_id), run_state=run_state)
    symbol_catalog_entry = dict(resolved_run_state.symbol_catalog.entries.get(normalized_symbol_key) or {})
    if not symbol_catalog_entry:
        raise ValueError(f"symbol {normalized_symbol_key!r} was not found for run_id={run_id}")

    symbol_state = _telemetry_hub().get_symbol_snapshot(run_id=str(run_id), symbol_key=normalized_symbol_key)
    if symbol_state is None:
        symbol_state = await _telemetry_hub().ensure_symbol_snapshot(
            run_id=str(run_id),
            bot_id=bot_id,
            symbol_key=normalized_symbol_key,
        )

    cursor = await _telemetry_hub().current_cursor(run_id=str(run_id), bot_id=bot_id)
    projection_error = _symbol_projection_error(symbol_state)
    if symbol_state is None or int(symbol_state.seq or 0) <= 0 or projection_error:
        return selected_symbol_snapshot_contract(
            bot_id=bot_id,
            run_id=str(run_id),
            symbol_key=normalized_symbol_key,
            symbol_state=None,
            symbol_catalog_entry=symbol_catalog_entry,
            run_health=resolved_run_state.health.to_dict(),
            run_bootstrap_seq=int(resolved_run_state.seq or 0),
            base_seq=int(cursor.get("base_seq") or 0),
            stream_session_id=str(cursor.get("stream_session_id") or "").strip() or None,
            run_live=bool(resolved_run_state.readiness.run_live),
            transport_eligible=bool(resolved_run_state.readiness.run_live),
            state="unavailable",
            unavailable_reason="projection_error" if projection_error else "symbol_snapshot_unavailable",
            message=projection_error or "BotLens selected-symbol snapshot is unavailable because projector state has not been built yet.",
        )

    projected_symbol_state = _trim_symbol_snapshot(symbol_state=symbol_state, limit=limit)
    continuity_summary = continuity_summary_from_candles(
        projected_symbol_state.candles.candles,
        timeframe=projected_symbol_state.identity.timeframe,
        series_key=projected_symbol_state.symbol_key,
    )
    emit_candle_continuity_summary(
        _OBSERVER,
        stage="botlens_selected_symbol_snapshot",
        summary=continuity_summary,
        bot_id=bot_id,
        run_id=str(run_id),
        instrument_id=projected_symbol_state.identity.instrument_id,
        series_key=projected_symbol_state.symbol_key,
        symbol=projected_symbol_state.identity.symbol,
        timeframe=projected_symbol_state.identity.timeframe,
        message_kind="ephemeral",
        boundary_name="selected_symbol_snapshot",
        extra={
            "contract": "botlens_selected_symbol_snapshot",
            "scope": "selected_symbol",
            "snapshot_seq": int(projected_symbol_state.seq or 0),
        },
    )
    return selected_symbol_snapshot_contract(
        bot_id=bot_id,
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
        symbol_state=projected_symbol_state,
        symbol_catalog_entry=symbol_catalog_entry,
        run_health=resolved_run_state.health.to_dict(),
        run_bootstrap_seq=int(resolved_run_state.seq or 0),
        base_seq=int(cursor.get("base_seq") or 0),
        stream_session_id=str(cursor.get("stream_session_id") or "").strip() or None,
        run_live=bool(resolved_run_state.readiness.run_live),
        transport_eligible=bool(resolved_run_state.readiness.run_live),
        message="BotLens selected-symbol snapshot ready.",
    )


async def get_selected_symbol_bootstrap(*, run_id: str, symbol_key: str, limit: int = 320) -> Dict[str, Any]:
    return await get_selected_symbol_snapshot(run_id=run_id, symbol_key=symbol_key, limit=limit)


async def get_selected_symbol_visual(*, run_id: str, symbol_key: str, limit: int = 320) -> Dict[str, Any]:
    return await get_selected_symbol_snapshot(run_id=run_id, symbol_key=symbol_key, limit=limit)


async def list_run_symbols(*, run_id: str) -> Dict[str, Any]:
    bot_id = _run_bot_id(run_id=run_id)
    summary = _telemetry_hub().get_run_snapshot(run_id=str(run_id))
    if summary is None:
        summary = await _telemetry_hub().ensure_run_snapshot(run_id=str(run_id), bot_id=bot_id)
    return symbol_catalog_response_contract(
        run_id=str(run_id),
        symbol_catalog=(summary.symbol_catalog.entries if summary is not None else {}),
    )


__all__ = [
    "get_selected_symbol_bootstrap",
    "get_selected_symbol_snapshot",
    "get_selected_symbol_visual",
    "get_symbol_detail",
    "list_run_symbols",
    "load_symbol_detail_state",
]
