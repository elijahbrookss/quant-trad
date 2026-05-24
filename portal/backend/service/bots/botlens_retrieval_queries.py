from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .botlens_contract import normalize_series_key
from .botlens_domain_events import (
    BOTLENS_DOMAIN_PREFIX,
    deserialize_botlens_domain_event,
    serialize_botlens_domain_event,
)


def _to_mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def list_bot_runtime_events(**kwargs):
    from ..storage.storage import list_bot_runtime_events as _list_bot_runtime_events

    return _list_bot_runtime_events(**kwargs)


@dataclass(frozen=True)
class DomainTruthEvent:
    row_id: int
    seq: int
    bot_id: str
    run_id: str
    event_id: str
    event_name: str
    event_type: str
    event_ts: Any
    created_at: Any
    known_at: Any
    root_event_id: Optional[str]
    parent_event_id: Optional[str]
    correlation_id: Optional[str]
    series_key: Optional[str]
    context: Dict[str, Any]


def _canonical_domain_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload_root = _to_mapping(row.get("payload"))
    return serialize_botlens_domain_event(deserialize_botlens_domain_event(payload_root))


def _truth_event_from_row(row: Mapping[str, Any]) -> DomainTruthEvent:
    payload_root = _canonical_domain_payload(row)
    context = _to_mapping(payload_root.get("context"))
    return DomainTruthEvent(
        row_id=_to_int(row.get("id"), 0),
        seq=_to_int(row.get("seq"), 0),
        bot_id=str(row.get("bot_id") or ""),
        run_id=str(row.get("run_id") or ""),
        event_id=str(payload_root.get("event_id") or row.get("event_id") or ""),
        event_name=str(row.get("event_name") or "").strip().upper(),
        event_type=str(row.get("event_type") or ""),
        event_ts=payload_root.get("event_ts") or row.get("event_time") or row.get("known_at") or row.get("created_at"),
        created_at=row.get("created_at"),
        known_at=row.get("known_at"),
        root_event_id=_optional_text(row.get("root_id")),
        parent_event_id=_optional_text(payload_root.get("parent_id")),
        correlation_id=_optional_text(row.get("correlation_id")),
        series_key=normalize_series_key(row.get("series_key")) or None,
        context=context,
    )


def list_run_domain_truth_page(
    *,
    bot_id: str,
    run_id: str,
    after_seq: int = 0,
    after_row_id: int = 0,
    limit: int = 500,
    event_names: Optional[Sequence[str]] = None,
    series_key: Optional[str] = None,
    root_event_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    signal_id: Optional[str] = None,
    decision_id: Optional[str] = None,
    trade_id: Optional[str] = None,
    bar_time_gte: Any = None,
    bar_time_lt: Any = None,
) -> List[DomainTruthEvent]:
    rows = list_bot_runtime_events(
        bot_id=str(bot_id),
        run_id=str(run_id),
        after_seq=max(0, int(after_seq or 0)),
        after_row_id=max(0, int(after_row_id or 0)),
        limit=max(1, int(limit or 500)),
        event_type_prefixes=[BOTLENS_DOMAIN_PREFIX],
        event_names=[str(value).strip().upper() for value in (event_names or []) if str(value).strip()],
        series_key=normalize_series_key(series_key) or None,
        root_id=_optional_text(root_event_id),
        correlation_id=_optional_text(correlation_id),
        signal_id=_optional_text(signal_id),
        decision_id=_optional_text(decision_id),
        trade_id=_optional_text(trade_id),
        bar_time_gte=bar_time_gte,
        bar_time_lt=bar_time_lt,
    )
    return [_truth_event_from_row(row) for row in rows if isinstance(row, Mapping)]


def list_all_run_domain_truth(
    *,
    bot_id: str,
    run_id: str,
    event_names: Optional[Sequence[str]] = None,
    series_key: Optional[str] = None,
    root_event_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    signal_id: Optional[str] = None,
    decision_id: Optional[str] = None,
    trade_id: Optional[str] = None,
    bar_time_gte: Any = None,
    bar_time_lt: Any = None,
    page_size: int = 5000,
) -> List[DomainTruthEvent]:
    rows: List[DomainTruthEvent] = []
    after_seq = 0
    after_row_id = 0
    while True:
        batch = list_run_domain_truth_page(
            bot_id=bot_id,
            run_id=run_id,
            after_seq=after_seq,
            after_row_id=after_row_id,
            limit=page_size,
            event_names=event_names,
            series_key=series_key,
            root_event_id=root_event_id,
            correlation_id=correlation_id,
            signal_id=signal_id,
            decision_id=decision_id,
            trade_id=trade_id,
            bar_time_gte=bar_time_gte,
            bar_time_lt=bar_time_lt,
        )
        if not batch:
            break
        rows.extend(batch)
        next_after_seq = int(batch[-1].seq)
        next_after_row_id = int(batch[-1].row_id)
        if next_after_seq < after_seq or (next_after_seq == after_seq and next_after_row_id <= after_row_id):
            raise RuntimeError(
                "BotLens domain truth traversal did not advance pagination cursor "
                f"(after_seq={after_seq}, after_row_id={after_row_id}, "
                f"next_after_seq={next_after_seq}, next_after_row_id={next_after_row_id})"
            )
        after_seq = next_after_seq
        after_row_id = next_after_row_id
    return rows


def iter_all_run_domain_truth(
    *,
    bot_id: str,
    run_id: str,
    event_names: Optional[Sequence[str]] = None,
    series_key: Optional[str] = None,
    root_event_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    signal_id: Optional[str] = None,
    decision_id: Optional[str] = None,
    trade_id: Optional[str] = None,
    bar_time_gte: Any = None,
    bar_time_lt: Any = None,
    page_size: int = 5000,
) -> Iterable[DomainTruthEvent]:
    after_seq = 0
    after_row_id = 0
    while True:
        batch = list_run_domain_truth_page(
            bot_id=bot_id,
            run_id=run_id,
            after_seq=after_seq,
            after_row_id=after_row_id,
            limit=page_size,
            event_names=event_names,
            series_key=series_key,
            root_event_id=root_event_id,
            correlation_id=correlation_id,
            signal_id=signal_id,
            decision_id=decision_id,
            trade_id=trade_id,
            bar_time_gte=bar_time_gte,
            bar_time_lt=bar_time_lt,
        )
        if not batch:
            return
        for event in batch:
            yield event
        next_after_seq = int(batch[-1].seq)
        next_after_row_id = int(batch[-1].row_id)
        if next_after_seq < after_seq or (next_after_seq == after_seq and next_after_row_id <= after_row_id):
            raise RuntimeError(
                "BotLens domain truth traversal did not advance pagination cursor "
                f"(after_seq={after_seq}, after_row_id={after_row_id}, "
                f"next_after_seq={next_after_seq}, next_after_row_id={next_after_row_id})"
            )
        after_seq = next_after_seq
        after_row_id = next_after_row_id


__all__ = [
    "DomainTruthEvent",
    "iter_all_run_domain_truth",
    "list_all_run_domain_truth",
    "list_run_domain_truth_page",
]
