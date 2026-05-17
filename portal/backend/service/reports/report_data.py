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


def get_report_materialization_status(run_id: str) -> Dict[str, Any]:
    return storage.get_report_materialization_status(run_id)


def get_materialized_run_report(run_id: str) -> Optional[Dict[str, Any]]:
    return storage.get_materialized_run_report(run_id)


def claim_report_materialization_build(
    run_id: str,
    *,
    cache_key: Optional[str] = None,
    force: bool = False,
) -> tuple[Dict[str, Any], bool, bool]:
    return storage.claim_report_materialization_build(run_id, cache_key=cache_key, force=force)


def store_materialized_run_report(
    run_id: str,
    payload: Mapping[str, Any],
    *,
    cache_key: Optional[str] = None,
    duration_ms: Optional[float] = None,
) -> Dict[str, Any]:
    return storage.store_materialized_run_report(run_id, payload, cache_key=cache_key, duration_ms=duration_ms)


def mark_report_materialization_failed(
    run_id: str,
    *,
    error: str,
    cache_key: Optional[str] = None,
    duration_ms: Optional[float] = None,
) -> Dict[str, Any]:
    return storage.mark_report_materialization_failed(
        run_id,
        error=error,
        cache_key=cache_key,
        duration_ms=duration_ms,
    )


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
        "seq": row.get("seq"),
        "run_seq": row.get("run_seq"),
        "run_seq_status": context.get("run_seq_status"),
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
        "bar_time": row.get("bar_time") or context.get("bar_time"),
        "known_at": row.get("known_at") or context.get("known_at") or context.get("bar_time") or _event_ts(row, context),
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
    after_row_id = 0
    rows: List[Dict[str, Any]] = []
    while True:
        batch = storage.list_bot_runtime_events(
            bot_id=bot_id,
            run_id=run_id,
            after_seq=after_seq,
            after_row_id=after_row_id,
            limit=5000,
            event_types=event_types,
            event_type_prefixes=event_type_prefixes,
        )
        if not batch:
            break
        rows.extend(batch)
        after_seq = int(batch[-1].get("run_seq") or batch[-1].get("seq") or after_seq)
        after_row_id = int(batch[-1].get("id") or after_row_id)
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


def list_observability_events(run_id: str, *, limit: int = 2000) -> List[Dict[str, Any]]:
    """Return operational observability rows for report diagnostics.

    Observability rows are diagnostic-only. Callers must not treat them as
    canonical report identity unless a later contract explicitly promotes them.
    """

    list_events = getattr(storage, "list_observability_events", None)
    if not callable(list_events):
        return []
    try:
        return [dict(row) for row in list_events(run_id=run_id, limit=limit)]
    except TypeError:
        rows = list_events(limit=limit)
        return [dict(row) for row in rows if str(row.get("run_id") or "") == str(run_id)]


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


def _stored_trade_is_open(row: Mapping[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    exit_time = row.get("exit_time") or row.get("closed_at")
    if exit_time not in (None, ""):
        return False
    return status not in {"closed", "completed", "complete"}


def _summary_ready(summary: Any) -> bool:
    return isinstance(summary, Mapping) and bool(summary)


def _decision_summary_ready(summary: Any) -> bool:
    if not isinstance(summary, Mapping):
        return False
    return all(key in summary for key in ("total", "accepted", "rejected"))


def get_result_readiness(
    run_id: str,
    *,
    decision_summary: Mapping[str, Any] | None = None,
    financial_summary: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    run = get_run(run_id)
    if not run:
        return {
            "run_id": run_id,
            "results_ready": False,
            "safe_to_compare": False,
            "completed": False,
            "reason": "run_not_found",
            "conditions": {
                "run_completed": False,
                "dataset_ready": False,
                "report_available": False,
                "report_read_model_available": False,
                "export_ready": False,
                "decision_summary_ready": False,
                "financial_summary_ready": False,
                "accepted_trade_lifecycle_complete": False,
                "no_terminal_open_trades": False,
                "comparable_metrics_available": False,
            },
            "dataset_ready": False,
            "dataset_status": "blocked",
            "results_status": "blocked",
            "comparison_status": "blocked",
            "export_status": "unavailable",
            "caveats": ["run_not_found"],
        }

    completed = str(run.get("status") or "").strip().lower() == "completed"
    decision_rows = list_run_events(run_id, event_types=[BOTLENS_DECISION_EVENT_TYPE])
    ledger = list_decision_ledger(run_id)
    opened_rows = list_run_events(run_id, event_types=[BOTLENS_TRADE_OPENED_EVENT_TYPE])
    closed_rows = list_run_events(run_id, event_types=[BOTLENS_TRADE_CLOSED_EVENT_TYPE])
    stored_trades = storage.list_bot_trades_for_run(run_id)

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
    terminal_open_trade_ids = sorted(
        str(row.get("trade_id") or row.get("id") or "").strip()
        for row in stored_trades
        if _stored_trade_is_open(row) and str(row.get("trade_id") or row.get("id") or "").strip()
    )
    decision_ledger_ready = not decision_rows or len(ledger) >= len(decision_rows)
    resolved_decision_summary = (
        dict(decision_summary)
        if isinstance(decision_summary, Mapping)
        else summarize_decision_ledger(ledger)
    )
    resolved_financial_summary = (
        dict(financial_summary)
        if isinstance(financial_summary, Mapping)
        else dict(run.get("summary") or {}) if isinstance(run.get("summary"), Mapping) else {}
    )
    decision_summary_ready = decision_ledger_ready and _decision_summary_ready(resolved_decision_summary)
    financial_summary_ready = _summary_ready(resolved_financial_summary)
    report_available = completed and financial_summary_ready
    accepted_lifecycle_complete = not missing_open and not missing_close
    no_terminal_open_trades = not terminal_open_trade_ids
    report_read_model_available = bool(run)
    comparable_metrics_available = financial_summary_ready and all(
        key in resolved_financial_summary
        for key in ("net_pnl", "total_trades")
    )
    dataset_ready = (
        completed
        and report_read_model_available
        and decision_summary_ready
        and financial_summary_ready
    )
    results_ready = dataset_ready and accepted_lifecycle_complete
    safe_to_compare = (
        results_ready
        and no_terminal_open_trades
        and comparable_metrics_available
    )
    reason = "ready" if safe_to_compare else "not_ready"
    if not completed:
        reason = "run_not_completed"
    elif not decision_summary_ready:
        reason = "decision_summary_unavailable"
    elif not financial_summary_ready:
        reason = "financial_summary_unavailable"
    elif missing_open or missing_close:
        reason = "trade_lifecycle_incomplete"
    elif terminal_open_trade_ids:
        reason = "terminal_open_trades"
    elif not comparable_metrics_available:
        reason = "comparable_metrics_unavailable"

    conditions = {
        "run_completed": completed,
        "dataset_ready": dataset_ready,
        "report_available": report_available,
        "report_read_model_available": report_read_model_available,
        "export_ready": dataset_ready,
        "decision_summary_ready": decision_summary_ready,
        "financial_summary_ready": financial_summary_ready,
        "accepted_trade_lifecycle_complete": accepted_lifecycle_complete,
        "no_terminal_open_trades": no_terminal_open_trades,
        "comparable_metrics_available": comparable_metrics_available,
    }
    caveats: List[str] = []
    if terminal_open_trade_ids:
        caveats.append("terminal_open_trades")
    if missing_open or missing_close:
        caveats.append("accepted_trade_lifecycle_incomplete")
    return {
        "run_id": run_id,
        "dataset_ready": dataset_ready,
        "dataset_status": "ready" if dataset_ready else "partial" if completed else "blocked",
        "results_ready": results_ready,
        "results_status": "ready" if results_ready else "partial" if dataset_ready else "blocked",
        "safe_to_compare": safe_to_compare,
        "comparison_status": "ready" if safe_to_compare else "blocked",
        "completed": completed,
        "decision_ledger_ready": decision_ledger_ready,
        "decision_summary_ready": decision_summary_ready,
        "financial_summary_ready": financial_summary_ready,
        "export_ready": dataset_ready,
        "export_status": "available" if dataset_ready else "partial" if completed else "unavailable",
        "report_available": report_available,
        "accepted_trade_count": len(accepted_trade_ids),
        "missing_trade_opened": missing_open,
        "missing_trade_closed": missing_close,
        "terminal_open_trades": terminal_open_trade_ids,
        "conditions": conditions,
        "reason": reason,
        "caveats": caveats,
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
    "get_materialized_run_report",
    "get_report_materialization_status",
    "claim_report_materialization_build",
    "list_decision_ledger",
    "list_observability_events",
    "list_run_events",
    "list_runs",
    "list_trade_events_for_trades",
    "list_trades_for_run",
    "mark_report_materialization_failed",
    "store_materialized_run_report",
    "summarize_decision_ledger",
]
