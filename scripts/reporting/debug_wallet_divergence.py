#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from engines.bot_runtime.core.wallet import (  # noqa: E402
    canonical_wallet_ledger_events,
    first_wallet_ledger_state_issue,
    project_wallet_from_events,
)
from portal.backend.service.reports import report_data  # noqa: E402
from portal.backend.service.reports.contract import get_run_research_dataset  # noqa: E402


WALLET_EVENT_NAMES = {
    "WALLET_INITIALIZED",
    "WALLET_DEPOSITED",
    "ENTRY_FILLED",
    "EXIT_FILLED",
    "MARGIN_RESERVED",
    "MARGIN_REJECTED",
    "MARGIN_RELEASED",
    "FEE_APPLIED",
    "REALIZED_PNL_APPLIED",
    "POSITION_OPENED",
    "POSITION_CLOSED",
    "EQUITY_UPDATED",
}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(row.get("payload"))


def _context(row: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_payload(row).get("context"))


def _event_name(row: Mapping[str, Any]) -> str:
    payload = _payload(row)
    return str(payload.get("event_name") or row.get("event_name") or row.get("event_type") or "").strip().upper()


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decision_key(row: Mapping[str, Any]) -> str:
    return str(row.get("decision_id") or row.get("event_id") or "").strip()


def _decision_status(row: Mapping[str, Any]) -> str:
    value = str(row.get("decision_state") or row.get("status") or row.get("verdict") or "").strip().lower()
    if value in {"accepted", "rejected"}:
        return value
    return "accepted" if row.get("accepted") else "rejected"


def _decision_context(row: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(row.get("context") or row.get("decision_context"))


def _wallet_events_before(run_id: str, decision: Mapping[str, Any]) -> list[dict[str, Any]]:
    decision_context = _decision_context(decision)
    wallet_eval_seq = _optional_int(decision.get("wallet_eval_seq") or decision_context.get("wallet_eval_seq"))
    if wallet_eval_seq is None:
        raise ValueError(
            "wallet_eval_seq is required to inspect wallet state before decision "
            f"| run_id={run_id} | decision_id={_decision_key(decision)}"
        )
    events = []
    for row in report_data.list_run_events(run_id):
        name = _event_name(row)
        if name not in WALLET_EVENT_NAMES:
            continue
        context = _context(row)
        wallet_commit_seq = _optional_int(context.get("wallet_commit_seq"))
        if wallet_commit_seq is None:
            raise ValueError(
                "wallet_commit_seq is required on wallet ledger fact "
                f"| run_id={run_id} | event_id={row.get('event_id') or _payload(row).get('event_id')}"
            )
        if wallet_commit_seq > wallet_eval_seq:
            continue
        payload = _payload(row)
        events.append(
            {
                "event_id": payload.get("event_id") or row.get("event_id"),
                "event_name": payload.get("event_name") or row.get("event_name"),
                "event_ts": payload.get("event_ts") or row.get("event_time"),
                "context": context,
            }
        )
    return [dict(event) for event in canonical_wallet_ledger_events(events)]


def _wallet_state(events: list[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not events:
        return None
    state = project_wallet_from_events(events)
    return {
        "balances": dict(getattr(state, "balances", {}) or {}),
        "locked_margin": dict(getattr(state, "locked_margin", {}) or {}),
        "free_collateral": dict(getattr(state, "free_collateral", {}) or {}),
        "margin_positions": dict(getattr(state, "margin_positions", {}) or {}),
    }


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _initialization_state(events: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    for event in events:
        if str(event.get("event_name") or "").strip().upper() != "WALLET_INITIALIZED":
            continue
        context = _mapping(event.get("context"))
        wallet_after = _mapping(context.get("wallet_after"))
        return {
            "event_id": event.get("event_id"),
            "balances": _mapping(context.get("balances")) or _mapping(wallet_after.get("balances")),
            "balance_after": context.get("balance_after"),
            "currency": context.get("currency"),
            "known_at": context.get("known_at") or event.get("event_ts"),
        }
    return None


def _first_missing_wallet_event(events: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    for event in events:
        name = str(event.get("event_name") or "").strip().upper()
        context = _mapping(event.get("context"))
        wallet_after = _mapping(context.get("wallet_after"))
        if name == "WALLET_INITIALIZED":
            if not (_mapping(context.get("balances")) or _mapping(wallet_after.get("balances")) or context.get("balance_after") is not None):
                return {"event_id": event.get("event_id"), "event_name": name, "reason": "missing_initial_balance"}
        if name == "MARGIN_REJECTED":
            if (
                not context.get("decision_id")
                or not context.get("signal_id")
                or _float(context.get("margin_available")) is None
                or _float(context.get("margin_required")) in (None, 0.0)
                or _float(context.get("selected_quantity")) is None
                or not _mapping(context.get("wallet_before"))
                or not _mapping(context.get("wallet_after"))
                or not context.get("source_refs")
            ):
                return {
                    "event_id": event.get("event_id"),
                    "event_name": name,
                    "decision_id": context.get("decision_id"),
                    "reason": "incomplete_margin_rejection_evidence",
                }
    return None


def _trade_for_decision(run_id: str, decision_id: str) -> dict[str, Any] | None:
    dataset = get_run_research_dataset(run_id)
    for trade in dataset.get("trades") or []:
        if isinstance(trade, Mapping) and str(trade.get("decision_id") or "") == decision_id:
            return dict(trade)
    return None


def _first_changed_decision(left: list[Mapping[str, Any]], right: list[Mapping[str, Any]]) -> dict[str, Any] | None:
    left_by_id = {_decision_key(row): row for row in left if _decision_key(row)}
    right_by_id = {_decision_key(row): row for row in right if _decision_key(row)}
    candidates = []
    for decision_id in set(left_by_id) & set(right_by_id):
        left_row = left_by_id[decision_id]
        right_row = right_by_id[decision_id]
        left_context = _decision_context(left_row)
        right_context = _decision_context(right_row)
        left_reason = left_row.get("reason_code") or left_context.get("reason_code")
        right_reason = right_row.get("reason_code") or right_context.get("reason_code")
        if _decision_status(left_row) == _decision_status(right_row) and left_reason == right_reason:
            continue
        candidates.append(
            {
                "decision_id": decision_id,
                "sort_time": str(left_row.get("known_at") or right_row.get("known_at") or left_row.get("bar_time") or right_row.get("bar_time") or ""),
                "left": left_row,
                "right": right_row,
            }
        )
    if not candidates:
        return None
    return sorted(candidates, key=lambda row: (row["sort_time"], row["decision_id"]))[0]


def _trade_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("entry_time") or row.get("bar_time") or ""),
        str(row.get("symbol") or ""),
        str(row.get("decision_id") or ""),
        str(row.get("trade_id") or ""),
    )


def _first_sizing_divergence(left_run: str, right_run: str) -> dict[str, Any] | None:
    left_trades = [
        dict(row)
        for row in (get_run_research_dataset(left_run).get("trades") or [])
        if isinstance(row, Mapping)
    ]
    right_trades = [
        dict(row)
        for row in (get_run_research_dataset(right_run).get("trades") or [])
        if isinstance(row, Mapping)
    ]
    left_ordered = sorted(left_trades, key=_trade_sort_key)
    right_ordered = sorted(right_trades, key=_trade_sort_key)
    for index, (left, right) in enumerate(zip(left_ordered, right_ordered)):
        if (
            str(left.get("decision_id") or "") != str(right.get("decision_id") or "")
            or _float(left.get("quantity") or left.get("contracts")) != _float(right.get("quantity") or right.get("contracts"))
            or _float(left.get("entry_price")) != _float(right.get("entry_price"))
        ):
            return {
                "index": index,
                "left": {
                    "decision_id": left.get("decision_id"),
                    "symbol": left.get("symbol"),
                    "entry_time": left.get("entry_time"),
                    "quantity": left.get("quantity") or left.get("contracts"),
                    "entry_price": left.get("entry_price"),
                    "trade_id": left.get("trade_id"),
                },
                "right": {
                    "decision_id": right.get("decision_id"),
                    "symbol": right.get("symbol"),
                    "entry_time": right.get("entry_time"),
                    "quantity": right.get("quantity") or right.get("contracts"),
                    "entry_price": right.get("entry_price"),
                    "trade_id": right.get("trade_id"),
                },
            }
    if len(left_ordered) != len(right_ordered):
        return {
            "index": min(len(left_ordered), len(right_ordered)),
            "left_count": len(left_ordered),
            "right_count": len(right_ordered),
            "reason": "trade_count_changed",
        }
    return None


def _explain(left: Mapping[str, Any], right: Mapping[str, Any]) -> str:
    left_context = _decision_context(left)
    right_context = _decision_context(right)
    left_reason = left.get("reason_code") or left_context.get("reason_code")
    right_reason = right.get("reason_code") or right_context.get("reason_code")
    if str(left_reason) != str(right_reason):
        return f"verdict reason changed from {left_reason or _decision_status(left)} to {right_reason or _decision_status(right)}"
    return f"decision state changed from {_decision_status(left)} to {_decision_status(right)}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Find the first wallet-backed decision divergence between two runs.")
    parser.add_argument("--run-a", required=True)
    parser.add_argument("--run-b", required=True)
    parser.add_argument("--prior-events", type=int, default=12)
    args = parser.parse_args()

    left_decisions = report_data.list_decision_ledger(str(args.run_a))
    right_decisions = report_data.list_decision_ledger(str(args.run_b))
    changed = _first_changed_decision(left_decisions, right_decisions)
    if changed is None:
        output = {"run_a": args.run_a, "run_b": args.run_b, "first_differing_decision": None, "status": "match"}
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0

    decision_id = str(changed["decision_id"])
    try:
        left_events = _wallet_events_before(str(args.run_a), changed["left"])
        right_events = _wallet_events_before(str(args.run_b), changed["right"])
    except Exception as exc:  # noqa: BLE001 - operator diagnostic should return structured failure
        output = {
            "run_a": str(args.run_a),
            "run_b": str(args.run_b),
            "first_differing_decision": {
                "decision_id": decision_id,
                "bar_time": changed["left"].get("bar_time") or changed["right"].get("bar_time"),
                "symbol": changed["left"].get("symbol") or changed["right"].get("symbol"),
            },
            "error": str(exc),
            "status": "fail",
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 1
    prior_limit = max(1, int(args.prior_events or 12))
    output = {
        "run_a": str(args.run_a),
        "run_b": str(args.run_b),
        "initialization_state": {
            str(args.run_a): _initialization_state(left_events),
            str(args.run_b): _initialization_state(right_events),
        },
        "first_missing_wallet_event": {
            str(args.run_a): _first_missing_wallet_event(left_events),
            str(args.run_b): _first_missing_wallet_event(right_events),
        },
        "first_wallet_ledger_state_issue": {
            str(args.run_a): first_wallet_ledger_state_issue(left_events),
            str(args.run_b): first_wallet_ledger_state_issue(right_events),
        },
        "first_sizing_divergence": _first_sizing_divergence(str(args.run_a), str(args.run_b)),
        "first_differing_decision": {
            "decision_id": decision_id,
            "bar_time": changed["left"].get("bar_time") or changed["right"].get("bar_time"),
            "symbol": changed["left"].get("symbol") or changed["right"].get("symbol"),
            "run_a_status": _decision_status(changed["left"]),
            "run_a_reason": changed["left"].get("reason_code") or _decision_context(changed["left"]).get("reason_code"),
            "run_b_status": _decision_status(changed["right"]),
            "run_b_reason": changed["right"].get("reason_code") or _decision_context(changed["right"]).get("reason_code"),
        },
        "wallet_state_before_decision": {
            str(args.run_a): _wallet_state(left_events),
            str(args.run_b): _wallet_state(right_events),
        },
        "prior_wallet_events": {
            str(args.run_a): left_events[-prior_limit:],
            str(args.run_b): right_events[-prior_limit:],
        },
        "trade_state": {
            str(args.run_a): _trade_for_decision(str(args.run_a), decision_id),
            str(args.run_b): _trade_for_decision(str(args.run_b), decision_id),
        },
        "explanation": _explain(changed["left"], changed["right"]),
        "status": "diverged",
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
