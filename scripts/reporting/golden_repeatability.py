#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

os.environ.setdefault("QT_LOGGING_LOKI_URL", "")
os.environ.setdefault("QT_LOGGING_DEBUG", "false")
os.environ.setdefault("QT_LOGGING_LEVEL", "WARNING")
os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from portal.backend.service.reports import report_data  # noqa: E402
from portal.backend.service.reports.contract import get_run_research_dataset  # noqa: E402
from portal.backend.service.reports.run_research_dataset import _runtime_ordering_health  # noqa: E402


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _norm_num(value: Any) -> Any:
    return round(value, 10) if isinstance(value, float) else value


def _decision_status(row: Mapping[str, Any]) -> str:
    value = str(row.get("verdict") or row.get("status") or row.get("decision_state") or "").strip().lower()
    if value:
        return value
    return "accepted" if row.get("accepted") else "rejected"


def _decision_context(row: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(row.get("decision_context") or row.get("context"))


def _wallet_trace_missing(row: Mapping[str, Any]) -> bool:
    context = _decision_context(row)
    reason = str(row.get("reason_code") or context.get("reason_code") or row.get("reason") or "").strip().upper()
    needs_trace = bool(row.get("accepted")) or _decision_status(row) == "accepted" or reason.startswith("WALLET_") or "MARGIN" in reason
    if not needs_trace:
        return False
    return not bool(_mapping(context.get("wallet_snapshot")) or _mapping(context.get("wallet_before")))


def _decision_signature(dataset: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in dataset.get("decisions") or []:
        if not isinstance(row, Mapping):
            continue
        rows.append(
            {
                "decision_id": row.get("decision_id"),
                "symbol": row.get("symbol"),
                "bar_time": row.get("bar_time"),
                "action": row.get("action"),
                "status": _decision_status(row),
                "accepted": bool(row.get("accepted")),
                "reason_code": row.get("reason_code"),
            }
        )
    return sorted(rows, key=lambda row: (str(row.get("decision_id") or ""), str(row.get("bar_time") or ""), str(row.get("symbol") or "")))


def _trade_signature(dataset: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in dataset.get("trades") or []:
        if not isinstance(row, Mapping):
            continue
        rows.append(
            {
                "symbol": row.get("symbol"),
                "timeframe": row.get("timeframe"),
                "direction": row.get("direction") or row.get("side"),
                "entry_time": row.get("entry_time"),
                "entry_price": _norm_num(row.get("entry_price")),
                "exit_time": row.get("exit_time"),
                "exit_price": _norm_num(row.get("exit_price")),
                "close_reason": row.get("close_reason") or row.get("exit_reason"),
                "status": row.get("status"),
                "quantity": _norm_num(row.get("quantity")),
                "gross_pnl": _norm_num(row.get("gross_pnl")),
                "fees": _norm_num(row.get("fees") if row.get("fees") is not None else row.get("fees_paid")),
                "net_pnl": _norm_num(row.get("net_pnl")),
                "decision_id": row.get("decision_id"),
            }
        )
    return sorted(rows, key=lambda row: (str(row.get("entry_time") or ""), str(row.get("symbol") or ""), str(row.get("direction") or ""), str(row.get("decision_id") or "")))


def _summary_signature(dataset: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(dataset.get("summary"))
    return {key: _norm_num(summary.get(key)) for key in sorted(summary)}


def _report_summary(dataset: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(dataset.get("summary"))
    keys = (
        "total_decisions",
        "accepted_decisions",
        "rejected_decisions",
        "trades",
        "closed_trades",
        "open_trades",
        "gross_pnl",
        "fees",
        "net_pnl",
        "equity_end",
    )
    return {key: summary.get(key) for key in keys}


def _diagnostics_signature(dataset: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(_mapping(dataset.get("diagnostics")).get("summary"))
    return {
        "blocking_codes": sorted(summary.get("blocking_codes") or []),
        "degraded_codes": sorted(summary.get("degraded_codes") or []),
        "by_code": dict(sorted(_mapping(summary.get("by_code")).items())),
        "readiness_impact": dict(sorted(_mapping(summary.get("readiness_impact")).items())),
    }


def _material(dataset: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _mapping(dataset.get("metadata"))
    readiness = _mapping(dataset.get("readiness"))
    semantic_fingerprint = (
        metadata.get("report_semantic_fingerprint")
        or readiness.get("semantic_fingerprint")
        or metadata.get("report_material_fingerprint")
        or readiness.get("material_fingerprint")
    )
    operational_fingerprint = (
        metadata.get("report_operational_fingerprint")
        or readiness.get("operational_fingerprint")
    )
    return {
        "strategy_hash": metadata.get("strategy_hash"),
        "material_config_hash": metadata.get("material_config_hash"),
        "data_snapshot_hash": metadata.get("data_snapshot_hash"),
        "report_material_fingerprint": metadata.get("report_material_fingerprint"),
        "report_semantic_fingerprint": semantic_fingerprint,
        "report_operational_fingerprint": operational_fingerprint,
        "golden_candidate_status": readiness.get("golden_candidate_status"),
        "golden_blocking_reasons": readiness.get("golden_blocking_reasons") or [],
        "repeatability_status": readiness.get("repeatability_status"),
        "comparison_status": readiness.get("comparison_status"),
    }


def _wallet_summary(dataset: Mapping[str, Any]) -> dict[str, Any]:
    decisions = [row for row in dataset.get("decisions") or [] if isinstance(row, Mapping)]
    missing = [
        {
            "decision_id": row.get("decision_id"),
            "symbol": row.get("symbol"),
            "bar_time": row.get("bar_time"),
            "status": _decision_status(row),
            "reason_code": row.get("reason_code"),
        }
        for row in decisions
        if _wallet_trace_missing(row)
    ]
    return {
        "decision_count": len(decisions),
        "missing_wallet_trace_count": len(missing),
        "missing_wallet_trace_first": missing[:3],
    }


def _coerce_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _coerce_epoch(value: Any) -> int | None:
    numeric = _coerce_int(value)
    if numeric is not None:
        return numeric
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _wallet_market_time_check_enabled(dataset: Mapping[str, Any]) -> bool:
    metadata = _mapping(dataset.get("metadata"))
    if str(metadata.get("run_type") or "").strip().lower() != "backtest":
        return False
    symbols = metadata.get("symbols")
    if isinstance(symbols, Sequence) and not isinstance(symbols, (str, bytes, bytearray)):
        return len({str(symbol) for symbol in symbols if str(symbol).strip()}) > 1
    decision_symbols = {
        str(row.get("symbol") or "").strip()
        for row in dataset.get("decisions") or []
        if isinstance(row, Mapping) and str(row.get("symbol") or "").strip()
    }
    return len(decision_symbols) > 1


def _is_wallet_affecting_decision(row: Mapping[str, Any]) -> bool:
    context = _decision_context(row)
    action = str(row.get("action") or context.get("intent") or context.get("action") or "").strip().lower()
    reason = str(row.get("reason_code") or context.get("reason_code") or row.get("reason") or "").strip().upper()
    status = _decision_status(row)
    if bool(row.get("accepted")) or status == "accepted":
        return True
    if action.startswith("enter_") or action in {"buy", "sell", "enter"}:
        return True
    if reason.startswith("WALLET_") or "MARGIN" in reason:
        return True
    return False


def _decision_market_order_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    context = _decision_context(row)
    run_seq = _coerce_int(row.get("run_seq") or context.get("run_seq"))
    bar_epoch = _coerce_epoch(
        context.get("bar_epoch")
        or row.get("bar_time")
        or context.get("bar_time")
        or row.get("known_at")
    )
    if run_seq is None or bar_epoch is None:
        return None
    return {
        "run_seq": run_seq,
        "seq": _coerce_int(row.get("seq")),
        "bar_epoch": bar_epoch,
        "bar_time": row.get("bar_time") or context.get("bar_time") or row.get("known_at"),
        "symbol": row.get("symbol") or context.get("symbol"),
        "decision_id": row.get("decision_id") or context.get("decision_id"),
    }


def _wallet_market_time_overtake_summary(dataset: Mapping[str, Any]) -> dict[str, Any]:
    if not _wallet_market_time_check_enabled(dataset):
        return {
            "checked": False,
            "reason": "not_multi_symbol_backtest",
            "decision_count": 0,
            "first_overtake": None,
        }
    sequence = []
    for row in dataset.get("decisions") or []:
        if not isinstance(row, Mapping) or not _is_wallet_affecting_decision(row):
            continue
        order_row = _decision_market_order_row(row)
        if order_row is not None:
            sequence.append(order_row)
    sequence.sort(
        key=lambda row: (
            row["run_seq"],
            row["seq"] if row.get("seq") is not None else 0,
            str(row.get("decision_id") or ""),
        )
    )
    latest_market_time: dict[str, Any] | None = None
    for row in sequence:
        if latest_market_time is not None and int(row["bar_epoch"]) < int(latest_market_time["bar_epoch"]):
            return {
                "checked": True,
                "decision_count": len(sequence),
                "first_overtake": {
                    "prior_run_seq": latest_market_time.get("run_seq"),
                    "prior_bar_time": latest_market_time.get("bar_time"),
                    "prior_symbol": latest_market_time.get("symbol"),
                    "prior_decision_id": latest_market_time.get("decision_id"),
                    "current_run_seq": row.get("run_seq"),
                    "current_bar_time": row.get("bar_time"),
                    "current_symbol": row.get("symbol"),
                    "current_decision_id": row.get("decision_id"),
                },
            }
        if latest_market_time is None or int(row["bar_epoch"]) > int(latest_market_time["bar_epoch"]):
            latest_market_time = row
    return {"checked": True, "decision_count": len(sequence), "first_overtake": None}


def _runtime_ordering_summary(run_id: str) -> dict[str, Any]:
    events = report_data.list_run_events(run_id)
    health = _runtime_ordering_health(events)
    payload_run_seq = 0
    payload_run_seq_status = 0
    for row in events:
        payload = _mapping(row.get("payload"))
        context = _mapping(payload.get("context"))
        if str(context.get("run_seq") or "").strip():
            payload_run_seq += 1
        if str(context.get("run_seq_status") or "").strip():
            payload_run_seq_status += 1
    return {
        **health,
        "event_count": len(events),
        "payload_run_seq_count": payload_run_seq,
        "payload_run_seq_status_count": payload_run_seq_status,
    }


def _dict_diff(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    diff = {}
    for key in sorted(set(left) | set(right)):
        if left.get(key) != right.get(key):
            diff[key] = {"left": left.get(key), "right": right.get(key)}
    return diff


def _first_list_diff(left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]], section: str) -> dict[str, Any] | None:
    for index in range(max(len(left), len(right))):
        left_row = left[index] if index < len(left) else None
        right_row = right[index] if index < len(right) else None
        if left_row != right_row:
            return {"section": section, "index": index, "left": left_row, "right": right_row}
    return None


def _decision_compare(left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    left_by_id = {str(row.get("decision_id")): row for row in left if row.get("decision_id")}
    right_by_id = {str(row.get("decision_id")): row for row in right if row.get("decision_id")}
    missing_ids = sorted(set(left_by_id) - set(right_by_id))
    extra_ids = sorted(set(right_by_id) - set(left_by_id))
    verdict_changes = []
    legacy_verdict_changes = []
    for decision_id in sorted(set(left_by_id) & set(right_by_id)):
        left_row = left_by_id[decision_id]
        right_row = right_by_id[decision_id]
        left_verdict = (left_row.get("status"), left_row.get("accepted"), left_row.get("reason_code"))
        right_verdict = (right_row.get("status"), right_row.get("accepted"), right_row.get("reason_code"))
        if left_verdict != right_verdict:
            legacy_verdict_changes.append({"decision_id": decision_id, "left": left_row, "right": right_row})
            verdict_changes.append(_verdict_change_row(decision_id, left_row, right_row))
    return {
        "left_count": len(left),
        "right_count": len(right),
        "missing_ids_count": len(missing_ids),
        "extra_ids_count": len(extra_ids),
        "missing_decision_count": len(missing_ids),
        "extra_decision_count": len(extra_ids),
        "verdict_change_count": len(verdict_changes),
        "missing_decision_ids": missing_ids,
        "extra_decision_ids": extra_ids,
        "verdict_changes": verdict_changes,
        "first_missing_id": missing_ids[0] if missing_ids else None,
        "first_extra_id": extra_ids[0] if extra_ids else None,
        "first_verdict_change": legacy_verdict_changes[0] if legacy_verdict_changes else None,
    }


def _verdict_change_row(decision_id: str, left_row: Mapping[str, Any], right_row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "decision_id": decision_id,
        "symbol": left_row.get("symbol") or right_row.get("symbol"),
        "timeframe": left_row.get("timeframe") or right_row.get("timeframe"),
        "bar_time": left_row.get("bar_time") or right_row.get("bar_time"),
        "left_verdict": left_row.get("status"),
        "right_verdict": right_row.get("status"),
        "left_reason": left_row.get("reason_code"),
        "right_reason": right_row.get("reason_code"),
        "left_action": left_row.get("action"),
        "right_action": right_row.get("action"),
        "left_accepted": left_row.get("accepted"),
        "right_accepted": right_row.get("accepted"),
    }


def _first_divergence(
    *,
    material_diff: Mapping[str, Any],
    decision_first: Mapping[str, Any] | None,
    trade_first: Mapping[str, Any] | None,
    summary_diff: Mapping[str, Any],
    diagnostics_diff: Mapping[str, Any],
) -> dict[str, Any] | None:
    for field in ("material_config_hash", "data_snapshot_hash", "strategy_hash", "report_semantic_fingerprint", "golden_candidate_status"):
        if field in material_diff:
            return {"section": "material", "field": field, **material_diff[field]}
    if decision_first:
        return dict(decision_first)
    if trade_first:
        return dict(trade_first)
    if summary_diff:
        field = next(iter(summary_diff))
        return {"section": "summary_metrics", "field": field, **summary_diff[field]}
    if diagnostics_diff:
        field = next(iter(diagnostics_diff))
        return {"section": "diagnostics", "field": field, **diagnostics_diff[field]}
    return None


def _first_golden_candidate(
    *,
    datasets: Sequence[Mapping[str, Any]],
    verdict: str,
    run_ids: Sequence[str],
    check_prior: bool,
) -> tuple[bool | None, str]:
    if verdict != "PASS":
        return False, "not_a_golden_candidate_pair"
    if not check_prior:
        return None, "not_checked"
    left = datasets[0]
    metadata = _mapping(left.get("metadata"))
    target = _material(left)
    bot_id = str(metadata.get("bot_id") or "").strip()
    if not bot_id:
        return None, "bot_id_unavailable"
    prior_runs = report_data.list_runs(
        run_type=str(metadata.get("run_type") or "backtest"),
        status="completed",
        bot_id=bot_id,
        timeframe=str(metadata.get("timeframe") or "") or None,
        started_before=str(metadata.get("started_at") or "") or None,
    )
    for run in sorted(prior_runs, key=lambda row: str(row.get("started_at") or "")):
        run_id = str(run.get("run_id") or "")
        if run_id in run_ids:
            continue
        try:
            prior = get_run_research_dataset(run_id)
        except Exception:  # noqa: BLE001 - prior scan is best-effort audit context.
            continue
        prior_material = _material(prior)
        if (
            prior_material.get("golden_candidate_status") == "certified"
            and prior_material.get("material_config_hash") == target.get("material_config_hash")
            and prior_material.get("data_snapshot_hash") == target.get("data_snapshot_hash")
            and prior_material.get("strategy_hash") == target.get("strategy_hash")
        ):
            return False, f"prior_certified_candidate={run_id}"
    return True, "no_prior_certified_candidate_for_same_material"


def compare_runs(left_run_id: str, right_run_id: str, *, out_dir: Path, check_prior: bool) -> dict[str, Any]:
    run_ids = [left_run_id, right_run_id]
    out_dir.mkdir(parents=True, exist_ok=True)
    datasets = [get_run_research_dataset(run_id) for run_id in run_ids]
    for run_id, dataset in zip(run_ids, datasets):
        (out_dir / f"{run_id}.run_research_dataset.json").write_text(
            json.dumps(dataset, indent=2, sort_keys=True, default=str)
        )

    left, right = datasets
    left_decisions = _decision_signature(left)
    right_decisions = _decision_signature(right)
    left_trades = _trade_signature(left)
    right_trades = _trade_signature(right)
    left_material = _material(left)
    right_material = _material(right)
    left_summary = _summary_signature(left)
    right_summary = _summary_signature(right)
    left_diagnostics = _diagnostics_signature(left)
    right_diagnostics = _diagnostics_signature(right)
    material_diff = _dict_diff(left_material, right_material)
    summary_diff = _dict_diff(left_summary, right_summary)
    diagnostics_diff = _dict_diff(left_diagnostics, right_diagnostics)
    decision_first = _first_list_diff(left_decisions, right_decisions, "decisions")
    trade_first = _first_list_diff(left_trades, right_trades, "trade_lifecycle")
    decision_compare = _decision_compare(left_decisions, right_decisions)
    wallet = {run_ids[0]: _wallet_summary(left), run_ids[1]: _wallet_summary(right)}
    wallet_market_time = {
        run_ids[0]: _wallet_market_time_overtake_summary(left),
        run_ids[1]: _wallet_market_time_overtake_summary(right),
    }
    runtime_ordering = {run_id: _runtime_ordering_summary(run_id) for run_id in run_ids}

    blocking_codes = set(left_diagnostics["blocking_codes"]) | set(right_diagnostics["blocking_codes"])
    blocking_reasons = set(left_material["golden_blocking_reasons"]) | set(right_material["golden_blocking_reasons"])
    fail_reasons = []
    if any(_mapping(dataset.get("metadata")).get("status") != "completed" for dataset in datasets):
        fail_reasons.append("run_not_completed")
    if any(entry["missing_wallet_trace_count"] for entry in wallet.values()):
        fail_reasons.append("wallet_trace_missing")
    if any(entry.get("first_overtake") for entry in wallet_market_time.values()):
        fail_reasons.append("wallet_market_time_overtake")
    if any("ordering" in code or "projection" in code or "lifecycle" in code for code in blocking_codes | blocking_reasons):
        fail_reasons.append("lifecycle_projection_or_ordering_blocker")
    if left_material.get("report_semantic_fingerprint") != right_material.get("report_semantic_fingerprint"):
        fail_reasons.append("semantic_fingerprint_mismatch")
    if (
        decision_compare["missing_ids_count"]
        or decision_compare["extra_ids_count"]
        or decision_compare["verdict_change_count"]
    ):
        fail_reasons.append("decision_verdict_or_id_mismatch")
    if left_material.get("golden_candidate_status") != "certified" or right_material.get("golden_candidate_status") != "certified":
        fail_reasons.append("golden_candidate_blocked")

    verdict = "PASS" if not fail_reasons else "FAIL"
    first_candidate, first_candidate_reason = _first_golden_candidate(
        datasets=datasets,
        verdict=verdict,
        run_ids=run_ids,
        check_prior=check_prior,
    )
    result = {
        "run_ids": run_ids,
        "verdict": verdict,
        "fail_reasons": sorted(set(fail_reasons)),
        "material": {run_ids[0]: left_material, run_ids[1]: right_material},
        "material_diff": material_diff,
        "operational_diff": {
            key: value
            for key, value in material_diff.items()
            if key in {"report_operational_fingerprint"}
        },
        "decision_compare": decision_compare,
        "wallet_trace": wallet,
        "wallet_market_time_ordering": wallet_market_time,
        "trade_lifecycle_compare": {
            "left_count": len(left_trades),
            "right_count": len(right_trades),
            "equal": left_trades == right_trades,
            "first_diff": trade_first,
        },
        "summary_metrics": {run_ids[0]: _report_summary(left), run_ids[1]: _report_summary(right)},
        "summary_diff": summary_diff,
        "diagnostics": {run_ids[0]: left_diagnostics, run_ids[1]: right_diagnostics},
        "diagnostics_diff": diagnostics_diff,
        "runtime_ordering": runtime_ordering,
        "first_divergence": _first_divergence(
            material_diff=material_diff,
            decision_first=decision_first,
            trade_first=trade_first,
            summary_diff=summary_diff,
            diagnostics_diff=diagnostics_diff,
        ),
        "first_golden_candidate": first_candidate,
        "first_golden_candidate_reason": first_candidate_reason,
        "artifacts": {
            run_ids[0]: str(out_dir / f"{run_ids[0]}.run_research_dataset.json"),
            run_ids[1]: str(out_dir / f"{run_ids[1]}.run_research_dataset.json"),
            "comparison": str(out_dir / "comparison_summary.json"),
        },
    }
    (out_dir / "comparison_summary.json").write_text(json.dumps(result, indent=2, sort_keys=True, default=str))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare two completed runs as a golden repeatability candidate.")
    parser.add_argument("--left-run-id", required=True, help="Baseline run ID.")
    parser.add_argument("--right-run-id", required=True, help="Comparison run ID.")
    parser.add_argument("--out-dir", default="logs/reports/golden-repeatability", help="Directory for dataset/comparison artifacts.")
    parser.add_argument("--check-prior", action="store_true", help="Scan prior completed runs to decide whether this is the first certified candidate.")
    parser.add_argument("--no-fail", action="store_true", help="Always exit 0 after writing the comparison payload.")
    args = parser.parse_args()

    result = compare_runs(
        str(args.left_run_id),
        str(args.right_run_id),
        out_dir=Path(args.out_dir).expanduser(),
        check_prior=bool(args.check_prior),
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if args.no_fail or result.get("verdict") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
