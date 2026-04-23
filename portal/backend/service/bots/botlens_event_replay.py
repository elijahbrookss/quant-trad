from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

from .botlens_contract import RUN_SCOPE_KEY, normalize_series_key
from .botlens_domain_events import BOTLENS_DOMAIN_PREFIX, deserialize_botlens_domain_event
from .botlens_state import (
    ProjectionBatch,
    RunProjectionSnapshot,
    SymbolProjectionSnapshot,
    apply_run_batch,
    apply_symbol_batch,
    empty_run_projection_snapshot,
    empty_symbol_projection_snapshot,
)

_PAGE_SIZE = 5000
_LIVE_SERIES_EVENT_NAMES = (
    "SERIES_METADATA_REPORTED",
    "CANDLE_OBSERVED",
    "OVERLAY_STATE_CHANGED",
    "SERIES_STATS_REPORTED",
    "SIGNAL_EMITTED",
    "DECISION_EMITTED",
    "TRADE_OPENED",
    "TRADE_UPDATED",
    "TRADE_CLOSED",
    "DIAGNOSTIC_RECORDED",
)
_RUN_LIVE_OR_TERMINAL_EVENT_NAMES = (
    "RUN_READY",
    "RUN_COMPLETED",
    "RUN_FAILED",
    "RUN_STOPPED",
    "RUN_CANCELLED",
)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_bot_runtime_events(**kwargs):
    from ..storage.storage import list_bot_runtime_events

    return list_bot_runtime_events(**kwargs)


def load_domain_projection_batches(
    *,
    bot_id: str,
    run_id: str,
    series_key: Optional[str] = None,
    max_seq: int | None = None,
) -> Tuple[ProjectionBatch, ...]:
    after_seq = 0
    after_row_id = 0
    current_seq: int | None = None
    current_rows: List[Dict[str, Any]] = []
    current_events: List[Any] = []
    batches: List[ProjectionBatch] = []
    normalized_symbol_key = normalize_series_key(series_key) if series_key and str(series_key) != RUN_SCOPE_KEY else None
    bounded_max_seq = int(max_seq) if max_seq is not None else None

    if bounded_max_seq is not None and bounded_max_seq <= 0:
        return ()

    while True:
        rows = _list_bot_runtime_events(
            bot_id=str(bot_id),
            run_id=str(run_id),
            after_seq=after_seq,
            after_row_id=after_row_id,
            limit=_PAGE_SIZE,
            event_type_prefixes=[BOTLENS_DOMAIN_PREFIX],
            series_key=normalized_symbol_key,
        )
        if not rows:
            break

        stop = False
        for row in rows:
            row_payload = _mapping(row.get("payload"))
            event = deserialize_botlens_domain_event(row_payload)
            row_seq = int(row.get("seq") or 0)
            if row_seq <= 0:
                continue
            if bounded_max_seq is not None and row_seq > bounded_max_seq:
                stop = True
                break
            if current_seq is None:
                current_seq = row_seq
            if current_seq != row_seq and current_rows:
                last_row = current_rows[-1]
                batches.append(
                    ProjectionBatch(
                        batch_kind="ledger_rebuild",
                        run_id=str(run_id),
                        bot_id=str(bot_id),
                        seq=int(current_seq),
                        event_time=last_row.get("event_time") or last_row.get("known_at"),
                        known_at=last_row.get("known_at") or last_row.get("event_time"),
                        symbol_key=normalized_symbol_key,
                        bridge_session_id=None,
                        events=tuple(current_events),
                    )
                )
                current_rows = []
                current_events = []
                current_seq = row_seq
            current_rows.append(dict(row))
            current_events.append(event)
            after_seq = row_seq
            after_row_id = int(row.get("id") or 0)

        if stop:
            break
        if len(rows) < _PAGE_SIZE:
            break

    if current_rows and current_events and current_seq is not None:
        last_row = current_rows[-1]
        batches.append(
            ProjectionBatch(
                batch_kind="ledger_rebuild",
                run_id=str(run_id),
                bot_id=str(bot_id),
                seq=int(current_seq),
                event_time=last_row.get("event_time") or last_row.get("known_at"),
                known_at=last_row.get("known_at") or last_row.get("event_time"),
                symbol_key=normalized_symbol_key,
                bridge_session_id=None,
                events=tuple(current_events),
            )
        )

    return tuple(batches)


def load_live_series_projection_batches_after(
    *,
    bot_id: str,
    run_id: str,
    after_seq: int = 0,
    after_row_id: int = 0,
    limit: int = 1000,
) -> tuple[Tuple[ProjectionBatch, ...], tuple[int, int]]:
    rows = _list_bot_runtime_events(
        bot_id=str(bot_id),
        run_id=str(run_id),
        after_seq=max(int(after_seq or 0), 0),
        after_row_id=max(int(after_row_id or 0), 0),
        limit=max(1, min(int(limit or 1000), _PAGE_SIZE)),
        event_type_prefixes=[BOTLENS_DOMAIN_PREFIX],
        event_names=_LIVE_SERIES_EVENT_NAMES,
    )
    if not rows:
        return (), (max(int(after_seq or 0), 0), max(int(after_row_id or 0), 0))

    batches: List[ProjectionBatch] = []
    current_key: tuple[int, str] | None = None
    current_rows: List[Dict[str, Any]] = []
    current_events: List[Any] = []
    cursor = (max(int(after_seq or 0), 0), max(int(after_row_id or 0), 0))

    def flush() -> None:
        nonlocal current_key, current_rows, current_events
        if current_key is None or not current_rows or not current_events:
            current_key = None
            current_rows = []
            current_events = []
            return
        seq, symbol_key = current_key
        last_row = current_rows[-1]
        batches.append(
            ProjectionBatch(
                batch_kind="ledger_tail",
                run_id=str(run_id),
                bot_id=str(bot_id),
                seq=int(seq),
                event_time=last_row.get("event_time") or last_row.get("known_at"),
                known_at=last_row.get("known_at") or last_row.get("event_time"),
                symbol_key=symbol_key,
                bridge_session_id=None,
                events=tuple(current_events),
            )
        )
        current_key = None
        current_rows = []
        current_events = []

    for row in rows:
        row_seq = int(row.get("seq") or 0)
        row_id = int(row.get("id") or 0)
        cursor = (row_seq, row_id)
        if row_seq <= 0:
            continue
        row_payload = _mapping(row.get("payload"))
        event = deserialize_botlens_domain_event(row_payload)
        context = event.context.to_dict() if hasattr(event.context, "to_dict") else {}
        symbol_key = normalize_series_key(row.get("series_key") or context.get("series_key"))
        if not symbol_key:
            continue
        next_key = (row_seq, symbol_key)
        if current_key is not None and current_key != next_key:
            flush()
        current_key = next_key
        current_rows.append(dict(row))
        current_events.append(event)

    flush()
    return tuple(batches), cursor


def load_run_live_or_terminal_cursor(
    *,
    bot_id: str,
    run_id: str,
) -> tuple[int, int, str] | None:
    rows = _list_bot_runtime_events(
        bot_id=str(bot_id),
        run_id=str(run_id),
        limit=1,
        event_type_prefixes=[BOTLENS_DOMAIN_PREFIX],
        event_names=_RUN_LIVE_OR_TERMINAL_EVENT_NAMES,
    )
    if not rows:
        return None
    row = dict(rows[0])
    event_name = str(row.get("event_name") or "").strip().upper()
    state = "live" if event_name == "RUN_READY" else "terminal"
    return int(row.get("seq") or 0), int(row.get("id") or 0), state


def rebuild_run_projection_snapshot(
    *,
    bot_id: str,
    run_id: str,
    max_seq: int | None = None,
) -> RunProjectionSnapshot | None:
    batches = load_domain_projection_batches(
        bot_id=str(bot_id),
        run_id=str(run_id),
        series_key=None,
        max_seq=max_seq,
    )
    if not batches:
        return None
    state = empty_run_projection_snapshot(bot_id=str(bot_id), run_id=str(run_id))
    for batch in batches:
        state, _ = apply_run_batch(state, batch=batch)
    return state


def rebuild_symbol_projection_snapshot(
    *,
    bot_id: str,
    run_id: str,
    symbol_key: str,
    max_seq: int | None = None,
) -> SymbolProjectionSnapshot | None:
    normalized_symbol_key = normalize_series_key(symbol_key)
    if not normalized_symbol_key:
        raise ValueError("canonical symbol_key is required")
    batches = load_domain_projection_batches(
        bot_id=str(bot_id),
        run_id=str(run_id),
        series_key=normalized_symbol_key,
        max_seq=max_seq,
    )
    if not batches:
        return None
    state = empty_symbol_projection_snapshot(normalized_symbol_key)
    for batch in batches:
        state, _ = apply_symbol_batch(state, batch=batch)
    return state


__all__ = [
    "load_domain_projection_batches",
    "load_live_series_projection_batches_after",
    "load_run_live_or_terminal_cursor",
    "rebuild_run_projection_snapshot",
    "rebuild_symbol_projection_snapshot",
]
