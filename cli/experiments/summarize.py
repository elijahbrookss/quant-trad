from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cli.audit import safe_path_part

from .contracts import json_safe
from .state_store import ExperimentStateStore, find_experiment_dir


SUMMARY_SCHEMA = "experiment_summary.v1"

METRIC_KEYS = (
    "trades",
    "total_trades",
    "closed_trades",
    "accepted_decisions",
    "rejected_decisions",
    "net_pnl",
    "gross_pnl",
    "return_pct",
    "win_rate",
    "profit_factor",
    "max_drawdown",
    "max_drawdown_pct",
    "expectancy",
    "fees",
    "exposure_pct",
    "average_holding_seconds",
    "sharpe",
    "sortino",
    "calmar",
)

DELTA_KEYS = (
    "net_pnl",
    "total_return_pct",
    "trade_count",
    "win_rate",
    "profit_factor",
    "max_drawdown",
    "max_drawdown_pct",
    "expectancy",
    "fees",
    "exposure_pct",
    "time_in_market_pct",
)


def _read_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _read_optional_json(path: str | Path | None) -> dict[str, Any] | None:
    if not path:
        return None
    resolved = Path(path).expanduser()
    if not resolved.exists():
        return None
    return _read_json(resolved)


def _variant_index(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item.get("id")): dict(item) for item in plan.get("variants") or [] if item.get("id") is not None}


def _comparison_for_ref(plan: dict[str, Any], ref: dict[str, Any]) -> dict[str, Any]:
    baseline = str(ref.get("baseline_variant_id") or "")
    candidate = str(ref.get("candidate_variant_id") or "")
    for comparison in plan.get("comparisons") or []:
        if str(comparison.get("baseline_variant_id") or "") == baseline and str(comparison.get("candidate_variant_id") or "") == candidate:
            return dict(comparison)
    return {
        "id": f"{safe_path_part(baseline)}_vs_{safe_path_part(candidate)}",
        "baseline_variant_id": baseline,
        "candidate_variant_id": candidate,
    }


def _section_index(summary: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not summary:
        return {}
    sections: dict[str, dict[str, Any]] = {}
    for item in summary.get("sections") or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        sections[name] = {
            "status": item.get("status"),
            "available": item.get("available"),
            "row_count": item.get("row_count"),
        }
    return sections


def _compact_metrics(summary: dict[str, Any] | None, record: dict[str, Any]) -> dict[str, Any]:
    raw: dict[str, Any] = {}
    terminal_status = record.get("terminal_status") if isinstance(record.get("terminal_status"), dict) else {}
    terminal_summary = terminal_status.get("summary") if isinstance(terminal_status.get("summary"), dict) else {}
    raw.update(terminal_summary)
    if summary and isinstance(summary.get("metrics"), dict):
        raw.update(summary["metrics"])

    metrics = {key: raw[key] for key in METRIC_KEYS if key in raw}
    if "trades" not in metrics:
        for alias in ("total_trades", "closed_trades"):
            if alias in raw:
                metrics["trades"] = raw[alias]
                break
    return metrics


def _run_execution(record: dict[str, Any]) -> dict[str, Any]:
    start = record.get("start") if isinstance(record.get("start"), dict) else {}
    context = start.get("context") if isinstance(start.get("context"), dict) else {}
    execution = context.get("execution") if isinstance(context.get("execution"), dict) else {}
    keys = (
        "datasource",
        "exchange",
        "execution_mode",
        "execution_behavior",
        "execution_semantics",
        "run_type",
        "timeframe",
        "symbols",
        "backtest_start",
        "backtest_end",
    )
    return {key: execution.get(key) for key in keys if key in execution}


def _run_summary(record: dict[str, Any], plan_variant: dict[str, Any], summary: dict[str, Any] | None) -> dict[str, Any]:
    variant = dict(plan_variant)
    if isinstance(record.get("variant"), dict):
        variant.update(record["variant"])
    execution = _run_execution(record)
    readiness = summary.get("readiness") if summary and isinstance(summary.get("readiness"), dict) else {}
    dataset_identity = (
        summary.get("dataset_identity")
        if summary and isinstance(summary.get("dataset_identity"), dict)
        else {}
    )
    symbols = summary.get("symbols") if summary and isinstance(summary.get("symbols"), list) else execution.get("symbols")
    timeframe = summary.get("timeframe") if summary and summary.get("timeframe") else execution.get("timeframe")
    research_ref = record.get("research_summary") if isinstance(record.get("research_summary"), dict) else {}
    export_ref = record.get("export") if isinstance(record.get("export"), dict) else {}
    return {
        "window_id": record.get("window_id"),
        "variant_id": record.get("variant_id"),
        "label": variant.get("label"),
        "role": variant.get("role"),
        "comparison_group": variant.get("comparison_group"),
        "execution_semantics": variant.get("execution_semantics") or execution.get("execution_semantics"),
        "bot_id": record.get("bot_id"),
        "run_id": record.get("run_id"),
        "status": record.get("status"),
        "run_type": summary.get("run_type") if summary else execution.get("run_type"),
        "symbols": symbols or [],
        "timeframe": timeframe,
        "execution": execution,
        "window": record.get("window"),
        "metrics": _compact_metrics(summary, record),
        "dataset_identity": dict(dataset_identity),
        "readiness": {
            "comparison_status": readiness.get("comparison_status"),
            "safe_to_compare": readiness.get("safe_to_compare"),
            "dataset_status": readiness.get("dataset_status"),
            "results_status": readiness.get("results_status"),
            "golden_candidate_status": readiness.get("golden_candidate_status"),
            "repeatability_status": readiness.get("repeatability_status"),
            "data_quality_status": readiness.get("data_quality_status"),
            "execution_quality_status": readiness.get(
                "execution_quality_status"
            ),
            "blocking_reasons": readiness.get("blocking_reasons") or [],
            "golden_blocking_reasons": readiness.get(
                "golden_blocking_reasons"
            )
            or [],
            "caveats": readiness.get("caveats") or [],
            "degraded_sections": readiness.get("degraded_sections") or [],
            "unavailable_sections": readiness.get("unavailable_sections") or [],
        },
        "sections": _section_index(summary),
        "artifacts": {
            "run_record": str(record.get("_path")) if record.get("_path") else None,
            "research_summary": research_ref.get("path"),
            "report_export": export_ref.get("path"),
        },
    }


def _delta_payload(raw_delta: Any) -> dict[str, Any]:
    if isinstance(raw_delta, dict):
        return {
            "left": raw_delta.get("left"),
            "right": raw_delta.get("right"),
            "delta": raw_delta.get("delta"),
            "valid": raw_delta.get("valid"),
            "unit": raw_delta.get("unit"),
            "invalid_reason": raw_delta.get("invalid_reason"),
        }
    return {"delta": raw_delta}


def _comparison_summary(ref: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    comparison = _comparison_for_ref(plan, ref)
    summary = _read_optional_json(ref.get("summary_path"))
    performance_delta = summary.get("performance_delta") if summary and isinstance(summary.get("performance_delta"), dict) else {}
    behavior_delta = summary.get("behavior_delta") if summary and isinstance(summary.get("behavior_delta"), dict) else {}
    deltas = {key: _delta_payload(performance_delta[key]) for key in DELTA_KEYS if key in performance_delta}
    return {
        "comparison_id": comparison.get("id"),
        "window_id": ref.get("window_id"),
        "baseline_variant_id": ref.get("baseline_variant_id"),
        "candidate_variant_id": ref.get("candidate_variant_id"),
        "baseline_run_id": ref.get("baseline_run_id"),
        "candidate_run_id": ref.get("candidate_run_id"),
        "status": ref.get("status"),
        "summary_path": ref.get("summary_path"),
        "comparison_status": summary.get("comparison_status") if summary else None,
        "comparison_verdict": summary.get("comparison_verdict") if summary else None,
        "can_compare": summary.get("can_compare") if summary else None,
        "blocked_reason": summary.get("blocked_reason") if summary else None,
        "first_divergence": summary.get("first_divergence") if summary else None,
        "performance_delta": deltas,
        "behavior_delta": {
            "decision_count_delta": behavior_delta.get("decision_count_delta"),
            "accepted_delta": behavior_delta.get("accepted_delta"),
            "rejected_delta": behavior_delta.get("rejected_delta"),
            "entry_count_delta": behavior_delta.get("entry_count_delta"),
            "exit_count_delta": behavior_delta.get("exit_count_delta"),
            "golden_artifact_status": behavior_delta.get("golden_artifact_status"),
        },
    }


def _compact_data_preflight(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    checks: list[dict[str, Any]] = []
    for item in payload.get("checks") or []:
        if not isinstance(item, dict):
            continue
        continuity = item.get("continuity") if isinstance(item.get("continuity"), dict) else {}
        checks.append(
            {
                "window_id": item.get("window_id"),
                "variant_id": item.get("variant_id"),
                "bot_id": item.get("bot_id"),
                "instrument_id": item.get("instrument_id"),
                "provider": item.get("provider"),
                "exchange": item.get("exchange"),
                "symbol": item.get("symbol"),
                "timeframe": item.get("timeframe"),
                "status": item.get("status"),
                "severity": item.get("severity"),
                "row_count": item.get("row_count"),
                "requested_start": item.get("requested_start"),
                "requested_end": item.get("requested_end"),
                "available_start": item.get("available_start"),
                "available_end": item.get("available_end"),
                "missing_range_count": len(item.get("missing_ranges") or []),
                "continuity": {
                    "status": continuity.get("final_status"),
                    "continuity_ratio": continuity.get("continuity_ratio"),
                    "candle_count": continuity.get("candle_count"),
                    "detected_gap_count": continuity.get("detected_gap_count"),
                    "defect_gap_count": continuity.get("defect_gap_count"),
                    "missing_candle_estimate": continuity.get("missing_candle_estimate"),
                    "max_gap_seconds": continuity.get("max_gap_seconds") or continuity.get("max_gap"),
                    "gap_count_by_type": continuity.get("gap_count_by_type") or {},
                },
            }
        )
    return {
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "summary": payload.get("summary") or {},
        "checks": checks,
        "route_errors": payload.get("route_errors") or [],
    }


def _load_run_records(store: ExperimentStateStore) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not store.runs_dir.exists():
        return records
    for path in sorted(store.runs_dir.glob("*.json")):
        record = _read_json(path)
        record["_path"] = str(path)
        records.append(record)
    return records


def _status_counts(items: list[dict[str, Any]], key: str = "status") -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        status = str(item.get(key) or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def summarize_experiment(root: str | Path, ref: str) -> dict[str, Any]:
    path = find_experiment_dir(root, ref)
    store = ExperimentStateStore(root, path=path)
    plan = store.load_plan()
    state = store.load_state()
    variants = _variant_index(plan)

    runs: list[dict[str, Any]] = []
    for record in _load_run_records(store):
        variant_id = str(record.get("variant_id") or "")
        research_ref = record.get("research_summary") if isinstance(record.get("research_summary"), dict) else {}
        research_summary = _read_optional_json(research_ref.get("path"))
        runs.append(_run_summary(record, variants.get(variant_id, {}), research_summary))

    comparisons = [_comparison_summary(dict(ref_item), plan) for ref_item in state.get("comparison_refs") or [] if isinstance(ref_item, dict)]
    pass_gate_result = _read_optional_json(state.get("pass_gate_result_ref") or store.artifacts_dir / "summaries" / "pass_gate_result.json")
    data_preflight = _compact_data_preflight(_read_optional_json(state.get("data_preflight_ref") or store.artifacts_dir / "summaries" / "data_preflight.json"))
    semantics = sorted({str(run.get("execution_semantics")) for run in runs if run.get("execution_semantics")})
    comparison_groups = sorted({str(run.get("comparison_group")) for run in runs if run.get("comparison_group")})
    completed_runs = [run for run in runs if str(run.get("status") or "").lower() == "completed"]
    return {
        "schema_version": SUMMARY_SCHEMA,
        "experiment_id": state.get("experiment_id") or store.experiment_id,
        "name": plan.get("name"),
        "hypothesis": plan.get("hypothesis"),
        "status": state.get("status"),
        "plan_hash": state.get("plan_hash") or plan.get("plan_hash"),
        "experiment_dir": str(store.path),
        "counts": {
            "windows": len(plan.get("windows") or []),
            "variants": len(plan.get("variants") or []),
            "runs": len(runs),
            "completed_runs": len(completed_runs),
            "comparisons": len(comparisons),
        },
        "run_status_counts": _status_counts(runs),
        "comparison_status_counts": _status_counts(comparisons, key="comparison_status"),
        "instrument_semantics": {
            "execution_semantics": semantics,
            "mixed_execution_semantics": len(semantics) > 1,
            "contains_proxy_derivative": "proxy_derivative" in semantics,
            "comparison_groups": comparison_groups,
        },
        "windows": plan.get("windows") or [],
        "runs": runs,
        "comparisons": comparisons,
        "pass_gates": {
            "status": pass_gate_result.get("status") if pass_gate_result else None,
            "gates": pass_gate_result.get("gates") if pass_gate_result else [],
            "ref": state.get("pass_gate_result_ref"),
        },
        "data_preflight": data_preflight,
        "paths": {
            "plan": str(store.plan_path),
            "state": str(store.state_path),
            "events": str(store.events_path),
        },
    }


def write_experiment_summary(path: str | Path, payload: dict[str, Any]) -> Path:
    resolved = Path(path).expanduser()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return resolved
