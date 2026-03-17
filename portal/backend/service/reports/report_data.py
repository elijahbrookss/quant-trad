"""Thin data-access layer for report-related storage queries."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from engines.bot_runtime.runtime.event_types import RUNTIME_PREFIX

from ..storage import storage


def list_runs(
    *,
    run_type: str,
    status: str,
    bot_id: Optional[str] = None,
    timeframe: Optional[str] = None,
    started_after: Optional[str] = None,
    started_before: Optional[str] = None,
) -> List[Dict[str, Any]]:
    return storage.list_bot_runs(
        run_type=run_type,
        status=status,
        bot_id=bot_id,
        timeframe=timeframe,
        started_after=started_after,
        started_before=started_before,
    )


def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    return storage.get_bot_run(run_id)


def _runtime_decision_entry_from_event(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    event_name = str(payload.get("event_name") or "").strip().upper()
    if not event_name or event_name in {"WALLET_INITIALIZED", "WALLET_DEPOSITED"}:
        return None
    event_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    event_subtype = event_payload.get("event_subtype")
    if event_name == "SIGNAL_EMITTED":
        event_subtype = "strategy_signal"
    elif event_name == "DECISION_ACCEPTED":
        event_subtype = "signal_accepted"
    elif event_name == "DECISION_REJECTED":
        event_subtype = "signal_rejected"
    elif event_name == "ENTRY_FILLED":
        event_subtype = "entry"
    elif event_name == "EXIT_FILLED":
        event_subtype = str(event_payload.get("exit_kind") or "close").lower()
    elif event_name == "RUNTIME_ERROR":
        event_subtype = "runtime_error"
    return {
        "event_id": payload.get("event_id") or row.get("event_id"),
        "event_ts": payload.get("event_ts") or row.get("event_time"),
        "event_type": str(payload.get("category") or "").strip().lower() or "runtime",
        "event_subtype": event_subtype,
        "reason_code": payload.get("reason_code"),
        "parent_event_id": payload.get("parent_id"),
        "trade_id": event_payload.get("trade_id"),
        "strategy_id": payload.get("strategy_id"),
        "symbol": payload.get("symbol"),
        "timeframe": payload.get("timeframe"),
        "side": event_payload.get("direction") or event_payload.get("side"),
        "qty": event_payload.get("qty"),
        "price": event_payload.get("price"),
        "event_impact_pnl": event_payload.get("event_impact_pnl"),
        "trade_net_pnl": event_payload.get("trade_net_pnl"),
        "reason_detail": event_payload.get("message"),
        "context": event_payload.get("context"),
        "created_at": row.get("created_at"),
        "instrument_id": event_payload.get("instrument_id"),
        "strategy_name": event_payload.get("strategy_name"),
        "evidence_refs": event_payload.get("evidence_refs") or [],
        "alternatives_rejected": event_payload.get("alternatives_rejected") or [],
    }


def list_run_events(
    run_id: str,
    *,
    event_types: Optional[Sequence[str]] = None,
    event_type_prefixes: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    run = get_run(run_id)
    if not run:
        return []
    bot_id = str(run.get("bot_id") or "").strip()
    if not bot_id:
        return []
    after_seq = 0
    rows: List[Dict[str, Any]] = []
    while True:
        batch = storage.list_bot_runtime_events(
            bot_id=bot_id,
            run_id=run_id,
            after_seq=after_seq,
            limit=5000,
            event_types=event_types,
            event_type_prefixes=event_type_prefixes,
        )
        if not batch:
            break
        rows.extend(batch)
        after_seq = int(batch[-1].get("seq") or after_seq)
        if len(batch) < 5000:
            break
    return rows


def list_decision_ledger(run_id: str) -> List[Dict[str, Any]]:
    rows = list_run_events(run_id, event_type_prefixes=[RUNTIME_PREFIX])
    ledger: List[Dict[str, Any]] = []
    for row in rows:
        projected = _runtime_decision_entry_from_event(row)
        if projected is not None:
            ledger.append(projected)
    return ledger


def list_trades_for_run(run_id: str) -> List[Dict[str, Any]]:
    return storage.list_bot_trades_for_run(run_id)


def list_trade_events_for_trades(trade_ids: Sequence[str]) -> List[Dict[str, Any]]:
    return storage.list_bot_trade_events_for_trades(trade_ids)


def find_instrument(
    datasource: Optional[str],
    exchange: Optional[str],
    symbol: str,
) -> Optional[Dict[str, Any]]:
    return storage.find_instrument(datasource, exchange, symbol)


__all__ = [
    "find_instrument",
    "get_run",
    "list_decision_ledger",
    "list_run_events",
    "list_runs",
    "list_trade_events_for_trades",
    "list_trades_for_run",
]
