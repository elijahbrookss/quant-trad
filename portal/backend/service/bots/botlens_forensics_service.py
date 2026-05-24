from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional

from .botlens_contract import normalize_series_key
from .botlens_forensics_contracts import forensic_event_page_contract, signal_forensic_contract
from .botlens_retrieval_queries import DomainTruthEvent, list_all_run_domain_truth, list_run_domain_truth_page


def _optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _matches_event_filters(
    event: DomainTruthEvent,
    *,
    root_event_id: Optional[str],
    parent_event_id: Optional[str],
    correlation_id: Optional[str],
) -> bool:
    if root_event_id and event.root_event_id != root_event_id:
        return False
    if parent_event_id and event.parent_event_id != parent_event_id:
        return False
    if correlation_id and event.correlation_id != correlation_id:
        return False
    return True


def _truth_document(event: DomainTruthEvent) -> Dict[str, Any]:
    return {
        "document_id": event.event_id or f"row:{event.row_id}",
        "cursor": {
            "after_seq": int(event.seq),
            "after_row_id": int(event.row_id),
        },
        "truth": {
            "row_id": int(event.row_id),
            "seq": int(event.seq),
            "event_id": event.event_id,
            "event_name": event.event_name,
            "event_type": event.event_type,
            "event_ts": event.event_ts,
            "created_at": event.created_at,
            "known_at": event.known_at,
            "bot_id": event.bot_id,
            "run_id": event.run_id,
            "root_event_id": event.root_event_id,
            "parent_event_id": event.parent_event_id,
            "correlation_id": event.correlation_id,
            "series_key": event.series_key,
            "context": dict(event.context),
        },
    }


def list_run_forensic_events(
    *,
    bot_id: str,
    run_id: str,
    after_seq: int = 0,
    after_row_id: int = 0,
    limit: int = 200,
    event_names: Optional[Sequence[str]] = None,
    series_key: Optional[str] = None,
    root_event_id: Optional[str] = None,
    parent_event_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_limit = max(1, min(int(limit or 200), 1000))
    normalized_series_key = normalize_series_key(series_key) or None
    normalized_root_event_id = _optional_text(root_event_id)
    normalized_parent_event_id = _optional_text(parent_event_id)
    normalized_correlation_id = _optional_text(correlation_id)
    normalized_event_names = [str(value).strip().upper() for value in (event_names or []) if str(value).strip()]

    matched: List[DomainTruthEvent] = []
    scan_after_seq = max(0, int(after_seq or 0))
    scan_after_row_id = max(0, int(after_row_id or 0))
    while len(matched) <= normalized_limit:
        batch = list_run_domain_truth_page(
            bot_id=str(bot_id),
            run_id=str(run_id),
            after_seq=scan_after_seq,
            after_row_id=scan_after_row_id,
            limit=min(max(normalized_limit * 2, 200), 1000),
            event_names=normalized_event_names or None,
            series_key=normalized_series_key,
            root_event_id=normalized_root_event_id,
            correlation_id=normalized_correlation_id,
        )
        if not batch:
            break
        for event in batch:
            if _matches_event_filters(
                event,
                root_event_id=normalized_root_event_id,
                parent_event_id=normalized_parent_event_id,
                correlation_id=normalized_correlation_id,
            ):
                matched.append(event)
                if len(matched) > normalized_limit:
                    break
        scan_after_seq = int(batch[-1].seq)
        scan_after_row_id = int(batch[-1].row_id)

    page_rows = matched[:normalized_limit]
    last_row = page_rows[-1] if page_rows else None
    next_after_seq = int(last_row.seq) if last_row is not None else max(0, int(after_seq or 0))
    next_after_row_id = int(last_row.row_id) if last_row is not None else max(0, int(after_row_id or 0))
    return forensic_event_page_contract(
        bot_id=str(bot_id),
        run_id=str(run_id),
        after_seq=max(0, int(after_seq or 0)),
        after_row_id=max(0, int(after_row_id or 0)),
        limit=normalized_limit,
        filters={
            "event_names": normalized_event_names,
            "series_key": normalized_series_key,
            "root_event_id": normalized_root_event_id,
            "parent_event_id": normalized_parent_event_id,
            "correlation_id": normalized_correlation_id,
        },
        documents=[_truth_document(row) for row in page_rows],
        next_after_seq=next_after_seq,
        next_after_row_id=next_after_row_id,
        has_more=len(matched) > normalized_limit,
    )


def _signal_summary(event: DomainTruthEvent) -> Dict[str, Any]:
    payload = event.context
    return {
        "signal_id": _optional_text(payload.get("signal_id")),
        "decision_id": _optional_text(payload.get("decision_id")),
        "strategy_id": _optional_text(payload.get("strategy_id")),
        "symbol_key": event.series_key,
        "symbol": _optional_text(payload.get("symbol")),
        "timeframe": _optional_text(payload.get("timeframe")),
        "bar_time": payload.get("bar_time"),
        "bar_epoch": payload.get("bar_epoch"),
        "signal_type": _optional_text(payload.get("signal_type")),
        "direction": _optional_text(payload.get("direction")),
        "signal_price": payload.get("signal_price"),
        "intent": _optional_text(payload.get("intent")),
        "rule_id": _optional_text(payload.get("rule_id")),
        "event_key": _optional_text(payload.get("event_key")),
    }


def _related_signal_event(
    event: DomainTruthEvent,
    *,
    signal_event_id: str,
    signal_correlation_id: Optional[str],
    signal_refs: set[str],
) -> bool:
    if event.event_id == signal_event_id or event.root_event_id == signal_event_id or event.parent_event_id == signal_event_id:
        return True
    if not signal_correlation_id or event.correlation_id != signal_correlation_id:
        return False
    row_refs = {
        ref
        for ref in (
            _optional_text(event.context.get("signal_id")),
            _optional_text(event.context.get("decision_id")),
            _optional_text(event.context.get("trade_id")),
        )
        if ref
    }
    return bool(signal_refs & row_refs)


def get_run_signal_forensics(*, bot_id: str, run_id: str, signal_id: str) -> Dict[str, Any]:
    target_signal_id = str(signal_id or "").strip()
    if not target_signal_id:
        raise ValueError("signal_id is required")
    signal_candidates = list_all_run_domain_truth(
        bot_id=str(bot_id),
        run_id=str(run_id),
        event_names=["SIGNAL_EMITTED"],
        signal_id=target_signal_id,
        page_size=200,
    )
    signal_event = next(
        (
            event
            for event in signal_candidates
            if _optional_text(event.context.get("signal_id")) == target_signal_id
        ),
        None,
    )
    if signal_event is None:
        raise KeyError("BotLens signal not found")

    signal_refs = {
        ref
        for ref in (
            _optional_text(signal_event.context.get("signal_id")),
            _optional_text(signal_event.context.get("decision_id")),
        )
        if ref
    }
    candidate_rows: Dict[tuple[int, int], DomainTruthEvent] = {
        (int(signal_event.seq), int(signal_event.row_id)): signal_event,
    }
    if signal_event.root_event_id:
        for event in list_all_run_domain_truth(
            bot_id=str(bot_id),
            run_id=str(run_id),
            root_event_id=signal_event.root_event_id,
        ):
            candidate_rows[(int(event.seq), int(event.row_id))] = event
    if signal_event.correlation_id:
        for event in list_all_run_domain_truth(
            bot_id=str(bot_id),
            run_id=str(run_id),
            correlation_id=signal_event.correlation_id,
        ):
            candidate_rows[(int(event.seq), int(event.row_id))] = event
    related = [
        event
        for event in candidate_rows.values()
        if _related_signal_event(
            event,
            signal_event_id=signal_event.event_id,
            signal_correlation_id=signal_event.correlation_id,
            signal_refs=signal_refs,
        )
    ]
    related.sort(key=lambda item: (int(item.seq), int(item.row_id)))
    return signal_forensic_contract(
        bot_id=str(bot_id),
        run_id=str(run_id),
        signal_id=target_signal_id,
        signal=_signal_summary(signal_event),
        root_event_id=signal_event.root_event_id,
        correlation_id=signal_event.correlation_id,
        documents=[_truth_document(event) for event in related],
    )


__all__ = ["get_run_signal_forensics", "list_run_forensic_events"]
