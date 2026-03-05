"""Read-only ledger projections for BotLens event auditing."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Sequence

from ..storage.storage import list_bot_runtime_events

_EVENT_NAME_TO_CATEGORY: Dict[str, str] = {
    "SIGNAL_EMITTED": "SIGNAL",
    "DECISION_ACCEPTED": "DECISION",
    "DECISION_REJECTED": "DECISION",
    "ENTRY_FILLED": "EXECUTION",
    "EXIT_FILLED": "OUTCOME",
    "WALLET_INITIALIZED": "WALLET",
    "WALLET_DEPOSITED": "WALLET",
    "RUNTIME_ERROR": "RUNTIME",
    "SYMBOL_DEGRADED": "RUNTIME",
    "SYMBOL_RECOVERED": "RUNTIME",
}

_EVENT_NAME_TO_SUBTYPE: Dict[str, str] = {
    "SIGNAL_EMITTED": "strategy_signal",
    "DECISION_ACCEPTED": "signal_accepted",
    "DECISION_REJECTED": "signal_rejected",
    "ENTRY_FILLED": "entry",
    "EXIT_FILLED": "close",
    "WALLET_INITIALIZED": "wallet_initialized",
    "WALLET_DEPOSITED": "wallet_deposited",
    "RUNTIME_ERROR": "runtime_error",
    "SYMBOL_DEGRADED": "symbol_degraded",
    "SYMBOL_RECOVERED": "symbol_recovered",
}


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _to_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _project_runtime_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload_root = _to_mapping(row.get("payload"))
    event_payload = _to_mapping(payload_root.get("payload"))
    event_name = str(payload_root.get("event_name") or row.get("event_type") or "").strip().upper()
    category = str(payload_root.get("category") or _EVENT_NAME_TO_CATEGORY.get(event_name, "RUNTIME")).strip().upper()

    event_subtype = str(event_payload.get("event_subtype") or "").strip().lower()
    if not event_subtype:
        if event_name == "EXIT_FILLED":
            event_subtype = str(event_payload.get("exit_kind") or "").strip().lower() or "close"
        else:
            event_subtype = _EVENT_NAME_TO_SUBTYPE.get(event_name, event_name.lower())

    event_ts = (
        payload_root.get("event_ts")
        or row.get("event_time")
        or row.get("known_at")
        or row.get("created_at")
    )
    side = event_payload.get("direction")
    if side is None:
        side = event_payload.get("side")
    price = event_payload.get("price")
    if price is None:
        price = event_payload.get("signal_price")
    wallet_delta = _to_mapping(event_payload.get("wallet_delta"))
    fee_paid = event_payload.get("fee_paid")
    if fee_paid is None:
        fee_paid = wallet_delta.get("fee_paid")
    reason_code = payload_root.get("reason_code")
    if reason_code is None:
        reason_code = event_payload.get("reason_code")
    reason_detail = event_payload.get("message")
    if reason_detail is None:
        reason_detail = event_payload.get("reason_detail")

    context_payload = event_payload.get("context")
    context = dict(context_payload) if isinstance(context_payload, Mapping) else None

    return {
        "event_id": payload_root.get("event_id") or row.get("event_id"),
        "seq": _to_int(row.get("seq"), 0),
        "bot_id": row.get("bot_id"),
        "run_id": row.get("run_id"),
        "critical": bool(row.get("critical", False)),
        "schema_version": _to_int(payload_root.get("schema_version") or row.get("schema_version"), 1),
        "event_name": event_name,
        "event_type": category.lower(),
        "category": category,
        "event_subtype": event_subtype,
        "event_ts": event_ts,
        "created_at": row.get("created_at"),
        "known_at": row.get("known_at"),
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "root_event_id": payload_root.get("root_id"),
        "parent_event_id": payload_root.get("parent_id"),
        "correlation_id": payload_root.get("correlation_id"),
        "strategy_id": payload_root.get("strategy_id"),
        "symbol": payload_root.get("symbol"),
        "timeframe": payload_root.get("timeframe"),
        "trade_id": event_payload.get("trade_id"),
        "side": side,
        "qty": event_payload.get("qty"),
        "price": price,
        "fee_paid": fee_paid,
        "event_impact_pnl": event_payload.get("event_impact_pnl"),
        "trade_net_pnl": event_payload.get("trade_net_pnl"),
        "context": context,
        "payload": event_payload,
    }


def list_run_ledger_events(
    *,
    bot_id: str,
    run_id: str,
    after_seq: int = 0,
    limit: int = 500,
    event_names: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    max_limit = max(1, min(int(limit or 500), 5000))
    filter_names = [str(item).strip().upper() for item in (event_names or []) if str(item).strip()]
    rows = list_bot_runtime_events(
        bot_id=str(bot_id),
        run_id=str(run_id),
        after_seq=max(0, int(after_seq or 0)),
        limit=max_limit,
        event_types=filter_names or None,
    )
    events = [_project_runtime_row(row) for row in rows if isinstance(row, Mapping)]
    next_after_seq = max((int(item.get("seq") or 0) for item in events), default=max(0, int(after_seq or 0)))
    return {
        "bot_id": str(bot_id),
        "run_id": str(run_id),
        "after_seq": max(0, int(after_seq or 0)),
        "next_after_seq": int(next_after_seq),
        "count": int(len(events)),
        "events": events,
    }


__all__ = ["list_run_ledger_events"]
