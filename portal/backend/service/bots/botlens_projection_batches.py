from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict

from .botlens_contract import BRIDGE_BOOTSTRAP_KIND, BRIDGE_FACTS_KIND, normalize_bridge_session_id, normalize_series_key
from .botlens_domain_events import BotLensDomainEvent, BotLensDomainEventName, botlens_domain_event_type, serialize_botlens_domain_event
from .botlens_state import ProjectionBatch

_CANONICAL_FACT_EVENT_NAMES = frozenset(
    {
        BotLensDomainEventName.RUN_PHASE_REPORTED,
        BotLensDomainEventName.RUN_STARTED,
        BotLensDomainEventName.RUN_READY,
        BotLensDomainEventName.RUN_DEGRADED,
        BotLensDomainEventName.RUN_COMPLETED,
        BotLensDomainEventName.RUN_FAILED,
        BotLensDomainEventName.RUN_STOPPED,
        BotLensDomainEventName.RUN_CANCELLED,
        BotLensDomainEventName.CANDLE_OBSERVED,
        BotLensDomainEventName.SIGNAL_EMITTED,
        BotLensDomainEventName.DECISION_EMITTED,
        BotLensDomainEventName.TRADE_OPENED,
        BotLensDomainEventName.TRADE_UPDATED,
        BotLensDomainEventName.TRADE_CLOSED,
    }
)


def is_canonical_fact_event(event: BotLensDomainEvent) -> bool:
    return event.event_name in _CANONICAL_FACT_EVENT_NAMES


def split_fact_events(
    events: Sequence[BotLensDomainEvent],
) -> tuple[tuple[BotLensDomainEvent, ...], tuple[BotLensDomainEvent, ...]]:
    canonical = tuple(event for event in events if is_canonical_fact_event(event))
    derived = tuple(event for event in events if not is_canonical_fact_event(event))
    return canonical, derived


def projection_batch_from_payload(
    *,
    batch_kind: str,
    run_id: str,
    bot_id: str,
    symbol_key: str | None,
    payload: Mapping[str, Any],
    events: Sequence[BotLensDomainEvent],
    seq: int | None = None,
) -> ProjectionBatch:
    known_at = payload.get("known_at") or payload.get("checkpoint_at") or payload.get("event_time") or payload.get("updated_at")
    event_time = payload.get("event_time") or payload.get("checkpoint_at") or payload.get("updated_at") or known_at
    resolved_seq = int(seq) if seq is not None else int(payload.get("run_seq") or payload.get("seq") or 0)
    return ProjectionBatch(
        batch_kind=batch_kind,
        run_id=str(run_id),
        bot_id=str(bot_id),
        symbol_key=normalize_series_key(symbol_key) or None,
        bridge_session_id=normalize_bridge_session_id(payload) if batch_kind in {BRIDGE_FACTS_KIND, BRIDGE_BOOTSTRAP_KIND} else None,
        seq=resolved_seq,
        event_time=event_time,
        known_at=known_at,
        events=tuple(events),
    )


def runtime_event_rows_from_batch(
    *,
    batch: ProjectionBatch,
    events: Sequence[BotLensDomainEvent] | None = None,
) -> list[Dict[str, Any]]:
    batch_events = tuple(events) if events is not None else tuple(batch.events)
    if not batch_events:
        return []
    event_time = batch.event_time
    known_at = batch.known_at
    return [
        {
            "event_id": event.event_id,
            "bot_id": batch.bot_id,
            "run_id": batch.run_id,
            "seq": int(batch.seq),
            "event_type": botlens_domain_event_type(event.event_name),
            "critical": event.event_name.value in {"FAULT_RECORDED"},
            "schema_version": event.schema_version,
            "event_time": event.serialize().get("event_ts") or event_time,
            "known_at": event.serialize().get("event_ts") or known_at,
            "payload": serialize_botlens_domain_event(event),
        }
        for event in batch_events
    ]


__all__ = [
    "is_canonical_fact_event",
    "projection_batch_from_payload",
    "runtime_event_rows_from_batch",
    "split_fact_events",
]
