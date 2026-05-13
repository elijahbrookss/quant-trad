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
    payload = _payload(row)
    return _mapping(payload.get("context"))


def _event_name(row: Mapping[str, Any]) -> str:
    payload = _payload(row)
    return str(payload.get("event_name") or row.get("event_name") or row.get("event_type") or "").strip().upper()


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
    return (
        missing_wallet_clock,
        wallet_commit_seq if wallet_commit_seq is not None else 0,
        event_order,
        run_seq,
        str(row.get("event_id") or _payload(row).get("event_id") or ""),
    )


def _wallet_events(run_id: str) -> list[dict[str, Any]]:
    rows = report_data.list_run_events(run_id)
    events = []
    for row in sorted(rows, key=_ordering_key):
        if _event_name(row) not in WALLET_EVENT_NAMES:
            continue
        payload = _payload(row)
        events.append(
            {
                "event_id": payload.get("event_id") or row.get("event_id"),
                "event_name": payload.get("event_name") or row.get("event_name"),
                "event_ts": payload.get("event_ts") or row.get("event_time"),
                "context": _context(row),
            }
        )
    return [dict(event) for event in canonical_wallet_ledger_events(events)]


def _state_payload(state: Any) -> dict[str, Any]:
    return {
        "balances": dict(getattr(state, "balances", {}) or {}),
        "locked_margin": dict(getattr(state, "locked_margin", {}) or {}),
        "free_collateral": dict(getattr(state, "free_collateral", {}) or {}),
        "margin_positions": dict(getattr(state, "margin_positions", {}) or {}),
    }


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


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_missing_wallet_event(events: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    for event in events:
        name = str(event.get("event_name") or "").strip().upper()
        context = _mapping(event.get("context"))
        wallet_after = _mapping(context.get("wallet_after"))
        if name == "WALLET_INITIALIZED":
            if not (_mapping(context.get("balances")) or _mapping(wallet_after.get("balances")) or context.get("balance_after") is not None):
                return {
                    "event_id": event.get("event_id"),
                    "event_name": name,
                    "reason": "missing_initial_balance",
                }
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
                    "context": context,
                }
    return None


def _first_persisted_state_divergence(events: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    issue = first_wallet_ledger_state_issue(events)
    if issue:
        return issue
    replay_prefix: list[Mapping[str, Any]] = []
    for event in events:
        replay_prefix.append(event)
        context = _mapping(event.get("context"))
        currency = str(context.get("currency") or "USD").upper()
        expected = _float(context.get("balance_after"))
        if expected is None:
            continue
        state = project_wallet_from_events(replay_prefix)
        observed = _float(dict(getattr(state, "balances", {}) or {}).get(currency))
        if observed is None or abs(observed - expected) > 0.01:
            return {
                "event_id": event.get("event_id"),
                "event_name": event.get("event_name"),
                "currency": currency,
                "expected_balance_after": expected,
                "observed_balance_after": observed,
                "context": context,
            }
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay canonical wallet ledger events for a run.")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    try:
        events = _wallet_events(str(args.run_id))
    except Exception as exc:  # noqa: BLE001 - operator diagnostic should return structured failure
        output = {
            "run_id": str(args.run_id),
            "wallet_event_count": 0,
            "projection_error": str(exc),
            "status": "fail",
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 1
    projection_error = None
    projection = None
    first_divergence = None
    first_missing_wallet_event = _first_missing_wallet_event(events)
    try:
        state = project_wallet_from_events(events)
        projection = _state_payload(state)
        first_divergence = _first_persisted_state_divergence(events)
    except Exception as exc:  # noqa: BLE001
        projection_error = str(exc)

    dataset = get_run_research_dataset(str(args.run_id))
    summary = _mapping(dataset.get("summary"))
    final_equity = _float(summary.get("equity_end") or summary.get("final_equity"))
    final_usd = None
    final_matches_report = None
    if projection:
        final_usd = _float(_mapping(projection.get("balances")).get("USD"))
        if final_usd is not None and final_equity is not None:
            final_matches_report = abs(final_usd - final_equity) <= 0.01

    output = {
        "run_id": str(args.run_id),
        "wallet_event_count": len(events),
        "initialization_state": _initialization_state(events),
        "projection_error": projection_error,
        "projection": projection,
        "final_report_equity": final_equity,
        "final_wallet_usd": final_usd,
        "final_balance_matches_report": final_matches_report,
        "first_missing_wallet_event": first_missing_wallet_event,
        "first_persisted_state_divergence": first_divergence,
        "status": (
            "pass"
            if events
            and not projection_error
            and first_divergence is None
            and first_missing_wallet_event is None
            and final_matches_report is not False
            else "fail"
        ),
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if output["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
