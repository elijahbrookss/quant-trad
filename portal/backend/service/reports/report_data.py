"""Thin data-access layer for report-related storage queries."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Sequence

from engines.bot_runtime.core.runtime_events import decision_trace_entry_from_runtime_event, runtime_event_from_dict
from engines.bot_runtime.runtime.event_types import RUNTIME_PREFIX

from ..storage import storage


BOTLENS_DOMAIN_PREFIX = "botlens_domain."
BOTLENS_DECISION_EVENT_TYPE = f"{BOTLENS_DOMAIN_PREFIX}decision_emitted"
BOTLENS_TRADE_OPENED_EVENT_TYPE = f"{BOTLENS_DOMAIN_PREFIX}trade_opened"
BOTLENS_TRADE_CLOSED_EVENT_TYPE = f"{BOTLENS_DOMAIN_PREFIX}trade_closed"


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
    event = runtime_event_from_dict(payload)
    decision_entry = decision_trace_entry_from_runtime_event(event)
    if decision_entry is None:
        return None
    event_payload = dict(event.context.to_dict())
    return {
        **decision_entry,
        "created_at": row.get("created_at"),
        "instrument_id": event_payload.get("instrument_id"),
        "strategy_name": event_payload.get("strategy_name"),
        "evidence_refs": event_payload.get("evidence_refs") or [],
        "alternatives_rejected": event_payload.get("alternatives_rejected") or [],
    }


def _payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload = row.get("payload")
    return dict(payload) if isinstance(payload, Mapping) else {}


def _context(row: Mapping[str, Any]) -> Dict[str, Any]:
    context = _payload(row).get("context")
    return dict(context) if isinstance(context, Mapping) else {}


def _event_name(row: Mapping[str, Any]) -> str:
    payload = _payload(row)
    return str(payload.get("event_name") or row.get("event_name") or "").strip().upper()


def _event_id(row: Mapping[str, Any]) -> Optional[Any]:
    payload = _payload(row)
    return payload.get("event_id") or row.get("event_id")


def _event_ts(row: Mapping[str, Any], context: Mapping[str, Any]) -> Optional[Any]:
    payload = _payload(row)
    return payload.get("event_ts") or context.get("event_time") or context.get("bar_time") or row.get("event_time")


def _parent_event_id(row: Mapping[str, Any]) -> Optional[Any]:
    payload = _payload(row)
    return payload.get("parent_id") or row.get("parent_id")


def _botlens_decision_entry_from_event(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if _event_name(row) != "DECISION_EMITTED":
        return None
    context = _context(row)
    decision_state = str(context.get("decision_state") or "").strip().lower()
    if decision_state not in {"accepted", "rejected"}:
        return None
    reason_code = row.get("reason_code") or context.get("reason_code")
    return {
        "event_id": _event_id(row),
        "event_ts": _event_ts(row, context),
        "event_type": "decision",
        "event_subtype": "signal_accepted" if decision_state == "accepted" else "signal_rejected",
        "decision_state": decision_state,
        "reason_code": reason_code,
        "parent_event_id": _parent_event_id(row),
        "signal_id": context.get("signal_id") or row.get("signal_id"),
        "source_type": context.get("source_type"),
        "source_id": context.get("source_id"),
        "trade_id": context.get("trade_id") or row.get("trade_id"),
        "strategy_id": context.get("strategy_id"),
        "strategy_hash": context.get("strategy_hash"),
        "symbol": context.get("symbol") or row.get("symbol"),
        "timeframe": context.get("timeframe") or row.get("timeframe"),
        "side": context.get("direction") or context.get("side"),
        "decision_id": context.get("decision_id") or row.get("decision_id"),
        "rule_id": context.get("rule_id"),
        "intent": context.get("intent"),
        "event_key": context.get("event_key"),
        "qty": context.get("qty"),
        "price": context.get("signal_price") or context.get("price"),
        "event_impact_pnl": context.get("event_impact_pnl"),
        "trade_net_pnl": context.get("trade_net_pnl"),
        "reason_detail": context.get("message"),
        "rejection_stage": context.get("rejection_stage"),
        "context": context,
        "created_at": row.get("created_at"),
        "instrument_id": context.get("instrument_id") or row.get("instrument_id"),
        "strategy_name": context.get("strategy_name"),
        "evidence_refs": context.get("evidence_refs") or [],
        "alternatives_rejected": context.get("alternatives_rejected") or [],
    }


def _stored_decision_ledger(run: Mapping[str, Any]) -> List[Dict[str, Any]]:
    ledger = run.get("decision_ledger")
    if not isinstance(ledger, list):
        return []
    return [dict(entry) for entry in ledger if isinstance(entry, Mapping)]


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
    run = get_run(run_id)
    if not run:
        return []

    rows = list_run_events(run_id, event_types=[BOTLENS_DECISION_EVENT_TYPE])
    ledger: List[Dict[str, Any]] = []
    for row in rows:
        projected = _botlens_decision_entry_from_event(row)
        if projected is not None:
            ledger.append(projected)
    if ledger:
        return ledger

    stored_ledger = _stored_decision_ledger(run)
    if stored_ledger:
        return stored_ledger

    rows = list_run_events(run_id, event_type_prefixes=[RUNTIME_PREFIX])
    ledger: List[Dict[str, Any]] = []
    for row in rows:
        projected = _runtime_decision_entry_from_event(row)
        if projected is not None:
            ledger.append(projected)
    return ledger


def summarize_decision_ledger(ledger: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
    accepted = 0
    rejected = 0
    for entry in ledger:
        state = str(entry.get("decision_state") or "").strip().lower()
        subtype = str(entry.get("event_subtype") or "").strip().lower()
        if state == "accepted" or "accepted" in subtype:
            accepted += 1
        elif state == "rejected" or "rejected" in subtype:
            rejected += 1
    return {
        "total": accepted + rejected,
        "accepted": accepted,
        "rejected": rejected,
    }


def _trade_id(row: Mapping[str, Any]) -> str:
    context = _context(row)
    return str(context.get("trade_id") or row.get("trade_id") or "").strip()


def get_result_readiness(run_id: str, *, require_artifacts: bool = False) -> Dict[str, Any]:
    run = get_run(run_id)
    if not run:
        return {
            "run_id": run_id,
            "safe_to_compare": False,
            "completed": False,
            "reason": "run_not_found",
        }

    completed = str(run.get("status") or "").strip().lower() == "completed"
    decision_rows = list_run_events(run_id, event_types=[BOTLENS_DECISION_EVENT_TYPE])
    ledger = list_decision_ledger(run_id)
    opened_rows = list_run_events(run_id, event_types=[BOTLENS_TRADE_OPENED_EVENT_TYPE])
    closed_rows = list_run_events(run_id, event_types=[BOTLENS_TRADE_CLOSED_EVENT_TYPE])

    accepted_trade_ids = {
        str(entry.get("trade_id") or "").strip()
        for entry in ledger
        if str(entry.get("trade_id") or "").strip()
        and (
            str(entry.get("decision_state") or "").strip().lower() == "accepted"
            or "accepted" in str(entry.get("event_subtype") or "").strip().lower()
        )
    }
    opened_trade_ids = {_trade_id(row) for row in opened_rows if _trade_id(row)}
    closed_trade_ids = {_trade_id(row) for row in closed_rows if _trade_id(row)}
    missing_open = sorted(accepted_trade_ids - opened_trade_ids)
    missing_close = sorted(accepted_trade_ids - closed_trade_ids)
    decision_ledger_ready = not decision_rows or len(ledger) >= len(decision_rows)
    artifacts_ready = not require_artifacts
    safe_to_compare = (
        completed
        and decision_ledger_ready
        and not missing_open
        and not missing_close
        and artifacts_ready
    )
    reason = "ready" if safe_to_compare else "not_ready"
    if not completed:
        reason = "run_not_completed"
    elif missing_open or missing_close:
        reason = "trade_lifecycle_incomplete"
    elif not decision_ledger_ready:
        reason = "decision_ledger_unavailable"
    elif not artifacts_ready:
        reason = "artifacts_unavailable"

    return {
        "run_id": run_id,
        "safe_to_compare": safe_to_compare,
        "completed": completed,
        "decision_ledger_ready": decision_ledger_ready,
        "artifacts_ready": artifacts_ready,
        "accepted_trade_count": len(accepted_trade_ids),
        "missing_trade_opened": missing_open,
        "missing_trade_closed": missing_close,
        "reason": reason,
    }


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
    "get_result_readiness",
    "list_decision_ledger",
    "list_run_events",
    "list_runs",
    "list_trade_events_for_trades",
    "list_trades_for_run",
    "summarize_decision_ledger",
]
