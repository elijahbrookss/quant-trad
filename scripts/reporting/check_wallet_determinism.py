#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from engines.bot_runtime.core.wallet import project_wallet_from_events  # noqa: E402
from portal.backend.service.reports import report_data  # noqa: E402


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(row.get("payload"))


def _context(row: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_payload(row).get("context"))


def _event_name(row: Mapping[str, Any]) -> str:
    payload = _payload(row)
    return str(payload.get("event_name") or row.get("event_name") or row.get("event_type") or "").strip()


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ordering_key(row: Mapping[str, Any]) -> tuple[int, int, int, int, str]:
    context = _context(row)
    wallet_commit_seq = _optional_int(context.get("wallet_commit_seq"))
    run_seq = _optional_int(row.get("run_seq")) or _optional_int(context.get("run_seq")) or 0
    event_order = _optional_int(context.get("wallet_event_order")) or 0
    missing_wallet_clock = 1 if wallet_commit_seq is None else 0
    event_id = str(row.get("event_id") or _payload(row).get("event_id") or "")
    return missing_wallet_clock, wallet_commit_seq if wallet_commit_seq is not None else 0, event_order, run_seq, event_id


def _wallet_projection_events(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    names = {
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
    rows = []
    for row in sorted(events, key=_ordering_key):
        if _event_name(row).upper() not in names:
            continue
        payload = _payload(row)
        rows.append(
            {
                "event_id": payload.get("event_id") or row.get("event_id"),
                "event_name": payload.get("event_name") or row.get("event_name"),
                "context": _context(row),
            }
        )
    return rows


def _wallet_state_payload(state: Any) -> dict[str, Any]:
    return {
        "balances": dict(getattr(state, "balances", {}) or {}),
        "locked_margin": dict(getattr(state, "locked_margin", {}) or {}),
        "free_collateral": dict(getattr(state, "free_collateral", {}) or {}),
        "margin_positions": dict(getattr(state, "margin_positions", {}) or {}),
    }


def _decision_key(decision: Mapping[str, Any]) -> str:
    return str(decision.get("decision_id") or decision.get("event_id") or "")


def _decision_context(decision: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(decision.get("context") or decision.get("decision_context"))


def _decision_status(decision: Mapping[str, Any]) -> str:
    value = str(decision.get("decision_state") or decision.get("status") or decision.get("verdict") or "").strip().lower()
    if value in {"accepted", "rejected"}:
        return value
    return "accepted" if decision.get("accepted") else "rejected"


def _wallet_trace_missing(decision: Mapping[str, Any]) -> bool:
    context = _decision_context(decision)
    reason = str(decision.get("reason_code") or context.get("reason_code") or "").strip().upper()
    needs_trace = _decision_status(decision) == "accepted" or reason.startswith("WALLET_") or "MARGIN" in reason
    if not needs_trace:
        return False
    return not bool(_mapping(context.get("wallet_snapshot")) or _mapping(context.get("wallet_before")))


def _run_summary(run_id: str) -> dict[str, Any]:
    events = report_data.list_run_events(run_id)
    decisions = report_data.list_decision_ledger(run_id)
    event_counts = Counter(_event_name(row) or str(row.get("event_type") or "unknown") for row in events)
    decision_counts = Counter(_decision_status(row) for row in decisions)
    margin_rejections = [
        {
            "decision_id": _decision_key(row),
            "reason_code": row.get("reason_code") or _decision_context(row).get("reason_code"),
            "symbol": row.get("symbol") or _decision_context(row).get("symbol"),
            "bar_time": row.get("bar_time") or _decision_context(row).get("bar_time"),
        }
        for row in decisions
        if "MARGIN" in str(row.get("reason_code") or _decision_context(row).get("reason_code") or "").upper()
    ]
    missing_trace = [
        {
            "decision_id": _decision_key(row),
            "status": _decision_status(row),
            "reason_code": row.get("reason_code") or _decision_context(row).get("reason_code"),
            "symbol": row.get("symbol") or _decision_context(row).get("symbol"),
            "bar_time": row.get("bar_time") or _decision_context(row).get("bar_time"),
        }
        for row in decisions
        if _wallet_trace_missing(row)
    ]
    projection_events = _wallet_projection_events(events)
    projection_error = None
    wallet_state = None
    try:
        wallet_state = _wallet_state_payload(project_wallet_from_events(projection_events))
    except Exception as exc:  # noqa: BLE001 - diagnostic helper reports the failure.
        projection_error = str(exc)
    return {
        "run_id": run_id,
        "event_count": len(events),
        "event_counts": dict(sorted(event_counts.items())),
        "decision_count": len(decisions),
        "decision_counts": dict(sorted(decision_counts.items())),
        "margin_rejections": margin_rejections,
        "missing_wallet_trace_count": len(missing_trace),
        "missing_wallet_trace": missing_trace[:50],
        "wallet_projection_event_count": len(projection_events),
        "wallet_projection_error": projection_error,
        "wallet_projection": wallet_state,
    }


def _changed_decisions(left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    left_by_id = {_decision_key(row): row for row in left if _decision_key(row)}
    right_by_id = {_decision_key(row): row for row in right if _decision_key(row)}
    changes = []
    for decision_id in sorted(set(left_by_id) & set(right_by_id)):
        left_row = left_by_id[decision_id]
        right_row = right_by_id[decision_id]
        left_status = _decision_status(left_row)
        right_status = _decision_status(right_row)
        left_reason = left_row.get("reason_code") or _decision_context(left_row).get("reason_code")
        right_reason = right_row.get("reason_code") or _decision_context(right_row).get("reason_code")
        if left_status == right_status and left_reason == right_reason:
            continue
        left_context = _decision_context(left_row)
        right_context = _decision_context(right_row)
        changes.append(
            {
                "decision_id": decision_id,
                "symbol": right_row.get("symbol") or right_context.get("symbol") or left_row.get("symbol") or left_context.get("symbol"),
                "bar_time": right_row.get("bar_time") or right_context.get("bar_time") or left_row.get("bar_time") or left_context.get("bar_time"),
                "left_status": left_status,
                "left_reason_code": left_reason,
                "right_status": right_status,
                "right_reason_code": right_reason,
                "left_wallet_trace_available": not _wallet_trace_missing(left_row),
                "right_wallet_trace_available": not _wallet_trace_missing(right_row),
                "left_wallet_snapshot": _mapping(left_context.get("wallet_snapshot") or left_context.get("wallet_before")),
                "right_wallet_snapshot": _mapping(right_context.get("wallet_snapshot") or right_context.get("wallet_before")),
                "left_margin_requirement": _mapping(left_context.get("margin_requirement") or left_context.get("required_delta")),
                "right_margin_requirement": _mapping(right_context.get("margin_requirement") or right_context.get("required_delta")),
            }
        )
    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description="Check wallet trace and replay health for one or two runs.")
    parser.add_argument("--run-id", required=True, help="Run ID to inspect.")
    parser.add_argument("--compare-run-id", help="Optional prior run ID to compare against.")
    args = parser.parse_args()

    current = _run_summary(str(args.run_id))
    output: dict[str, Any] = {"current": current}
    exit_code = 0
    if current["missing_wallet_trace_count"] or current["wallet_projection_error"]:
        exit_code = 1

    if args.compare_run_id:
        prior = _run_summary(str(args.compare_run_id))
        current_decisions = report_data.list_decision_ledger(str(args.run_id))
        prior_decisions = report_data.list_decision_ledger(str(args.compare_run_id))
        changes = _changed_decisions(prior_decisions, current_decisions)
        output["compare"] = {
            "prior": prior,
            "changed_decision_count": len(changes),
            "changed_decisions": changes,
        }
        if changes and any(not row["left_wallet_trace_available"] or not row["right_wallet_trace_available"] for row in changes):
            exit_code = 1

    print(json.dumps(output, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
