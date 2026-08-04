from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Dict, List, Optional, Tuple

from .botlens_contract import RUN_SCOPE_KEY, normalize_series_key
from .botlens_domain_events import (
    BOTLENS_DOMAIN_PREFIX,
    BotLensDomainEventName,
    deserialize_botlens_domain_event,
)
from .botlens_state import (
    ProjectionBatch,
    RunProjectionSnapshot,
    SymbolOverlaysState,
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
RUN_PROJECTION_EVENT_NAMES = (
    "RUN_PHASE_REPORTED",
    "RUN_STARTED",
    "RUN_READY",
    "RUN_DEGRADED",
    "RUN_COMPLETED",
    "RUN_FAILED",
    "RUN_STOPPED",
    "RUN_CANCELLED",
    "HEALTH_STATUS_REPORTED",
    "FAULT_RECORDED",
    "SERIES_METADATA_REPORTED",
    "TRADE_OPENED",
    "TRADE_UPDATED",
    "TRADE_CLOSED",
)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _event_order(row: Mapping[str, Any]) -> int:
    return int(row.get("run_seq") or row.get("seq") or 0)


def _ordered_event_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda row: (_event_order(row), int(row.get("id") or 0)))


def _overlay_commit_seq(batch: ProjectionBatch) -> int:
    for event in batch.events:
        if event.event_name == BotLensDomainEventName.OVERLAY_STATE_CHANGED:
            context = event.context.to_dict() if hasattr(event.context, "to_dict") else {}
            delta = _mapping(context.get("overlay_delta"))
            return int(delta.get("overlay_commit_seq") or 0)
    return 0


def _list_bot_runtime_events(**kwargs):
    from ..storage.repos.runtime_events import list_bot_runtime_events

    return list_bot_runtime_events(**kwargs)


def _list_overlay_replay_headers(**kwargs):
    from ..storage.repos.runtime_events import list_botlens_overlay_replay_headers

    return list_botlens_overlay_replay_headers(**kwargs)


def _list_bot_runtime_events_by_event_ids(event_ids: tuple[str, ...]):
    from ..storage.repos.runtime_events import list_bot_runtime_events_by_event_ids

    return list_bot_runtime_events_by_event_ids(event_ids)


def _list_symbol_projection_seed_rows(**kwargs):
    from ..storage.repos.runtime_events import list_botlens_symbol_projection_seed_rows

    return list_botlens_symbol_projection_seed_rows(**kwargs)


def _projection_batches_from_rows(
    rows: List[Dict[str, Any]],
    *,
    bot_id: str,
    run_id: str,
    symbol_key: str,
) -> Tuple[ProjectionBatch, ...]:
    batches: List[ProjectionBatch] = []
    for row in _ordered_event_rows([dict(row) for row in rows]):
        event = deserialize_botlens_domain_event(_mapping(row.get("payload")))
        batches.append(
            ProjectionBatch(
                batch_kind="ledger_rebuild",
                run_id=str(run_id),
                bot_id=str(bot_id),
                seq=_event_order(row),
                event_time=row.get("event_time") or row.get("known_at"),
                known_at=row.get("known_at") or row.get("event_time"),
                symbol_key=symbol_key,
                bridge_session_id=None,
                events=(event,),
            )
        )
    return tuple(batches)


def _plan_overlay_replay(headers: List[Dict[str, Any]]) -> tuple[tuple[str, ...], Dict[str, Any] | None]:
    expected_overlay_seq = 0
    replay_start = 0
    gap: Dict[str, Any] | None = None
    accepted_indices: List[int] = []
    for index, header in enumerate(headers):
        overlay_seq = int(header.get("overlay_commit_seq") or 0)
        base_overlay_seq = int(header.get("base_overlay_commit_seq") or 0)
        if str(header.get("checkpoint_kind") or "").strip().lower() == "full_state":
            if overlay_seq < expected_overlay_seq:
                continue
            expected_overlay_seq = overlay_seq
            replay_start = len(accepted_indices)
            accepted_indices.append(index)
            gap = None
            continue
        if overlay_seq <= expected_overlay_seq:
            continue
        if gap is not None:
            continue
        if base_overlay_seq != expected_overlay_seq:
            gap = {
                "status": "invalid",
                "invalid_reason": "overlay_clock_gap",
                "invalid_detail": (
                    "Overlay evidence is incomplete: expected base commit "
                    f"{expected_overlay_seq} but received {base_overlay_seq}."
                ),
                "invalidated_at_run_seq": int(header.get("run_seq") or 0),
                "gap_expected_overlay_commit_seq": expected_overlay_seq,
                "gap_observed_base_overlay_commit_seq": base_overlay_seq,
                "gap_observed_overlay_commit_seq": overlay_seq,
            }
            continue
        expected_overlay_seq = overlay_seq
        accepted_indices.append(index)

    if gap is not None:
        return (), gap
    selected_headers = [headers[index] for index in accepted_indices[replay_start:]]
    return tuple(str(header.get("event_id") or "") for header in selected_headers if header.get("event_id")), None


def load_domain_projection_batches(
    *,
    bot_id: str,
    run_id: str,
    series_key: Optional[str] = None,
    max_seq: int | None = None,
    event_names: tuple[str, ...] | None = None,
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
            event_names=event_names,
            series_key=normalized_symbol_key,
        )
        if not rows:
            break

        stop = False
        for row in _ordered_event_rows([dict(row) for row in rows]):
            row_payload = _mapping(row.get("payload"))
            event = deserialize_botlens_domain_event(row_payload)
            row_seq = _event_order(row)
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

    for row in _ordered_event_rows([dict(row) for row in rows]):
        row_seq = _event_order(row)
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
    return _event_order(row), int(row.get("id") or 0), state


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
        event_names=RUN_PROJECTION_EVENT_NAMES,
    )
    if not batches:
        return None
    state = empty_run_projection_snapshot(bot_id=str(bot_id), run_id=str(run_id))
    for batch in batches:
        try:
            state, _ = apply_run_batch(state, batch=batch)
        except Exception as exc:
            event_names = sorted({str(event.event_name) for event in batch.events})
            event_ids = [str(event.event_id) for event in batch.events[:3]]
            raise ValueError(
                "run projection replay failed "
                f"run_id={run_id} seq={batch.seq} event_ids={event_ids} "
                f"event_names={event_names}: {exc}"
            ) from exc
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
    non_overlay_batches = _projection_batches_from_rows(
        _list_symbol_projection_seed_rows(
            bot_id=str(bot_id),
            run_id=str(run_id),
            series_key=normalized_symbol_key,
            max_seq=max_seq,
        ),
        bot_id=str(bot_id),
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
    )
    overlay_headers = _list_overlay_replay_headers(
        bot_id=str(bot_id),
        run_id=str(run_id),
        series_key=normalized_symbol_key,
        max_seq=max_seq,
    )
    overlay_event_ids, overlay_gap = _plan_overlay_replay(overlay_headers)
    overlay_batches = _projection_batches_from_rows(
        _list_bot_runtime_events_by_event_ids(overlay_event_ids),
        bot_id=str(bot_id),
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
    )
    if not non_overlay_batches and not overlay_batches and overlay_gap is None:
        return None
    state = empty_symbol_projection_snapshot(normalized_symbol_key)
    replay_batches = (
        *non_overlay_batches,
        *sorted(
            overlay_batches,
            key=lambda batch: (_overlay_commit_seq(batch), int(batch.seq)),
        ),
    )
    for batch in replay_batches:
        try:
            state, _ = apply_symbol_batch(state, batch=batch)
        except Exception as exc:
            event_names = sorted({str(event.event_name) for event in batch.events})
            event_ids = [str(event.event_id) for event in batch.events[:3]]
            raise ValueError(
                "symbol projection replay failed "
                f"run_id={run_id} symbol_key={normalized_symbol_key} "
                f"seq={batch.seq} event_ids={event_ids} event_names={event_names}: {exc}"
            ) from exc
    if overlay_gap is not None:
        state = replace(
            state,
            overlays=SymbolOverlaysState(
                overlays=(),
                overlay_commit_seq=int(overlay_gap.get("gap_expected_overlay_commit_seq") or 0),
                overlay_commit_seq_status="overlay_scoped",
                validity_status="invalid",
                invalid_reason=str(overlay_gap.get("invalid_reason") or "overlay_clock_gap"),
                invalid_detail=str(overlay_gap.get("invalid_detail") or "Overlay evidence is incomplete."),
                invalidated_at_run_seq=int(overlay_gap.get("invalidated_at_run_seq") or 0) or None,
                gap_expected_overlay_commit_seq=int(overlay_gap.get("gap_expected_overlay_commit_seq") or 0),
                gap_observed_base_overlay_commit_seq=int(overlay_gap.get("gap_observed_base_overlay_commit_seq") or 0),
                gap_observed_overlay_commit_seq=int(overlay_gap.get("gap_observed_overlay_commit_seq") or 0),
            ),
        )
    return state


__all__ = [
    "load_domain_projection_batches",
    "load_live_series_projection_batches_after",
    "load_run_live_or_terminal_cursor",
    "RUN_PROJECTION_EVENT_NAMES",
    "rebuild_run_projection_snapshot",
    "rebuild_symbol_projection_snapshot",
]
