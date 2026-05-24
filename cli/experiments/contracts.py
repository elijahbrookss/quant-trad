from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any


PLAN_SCHEMA = "experiment_plan.v1"
STATE_SCHEMA = "experiment_suite_state.v1"
STEP_SCHEMA = "experiment_step_state.v1"
EVENT_SCHEMA = "experiment_event.v1"
PASS_GATE_RESULT_SCHEMA = "pass_gate_result.v1"
COMPARISON_REF_SCHEMA = "comparison_result_ref.v1"

TERMINAL_RUN_STATUSES = {
    "completed",
    "failed",
    "crashed",
    "canceled",
    "cancelled",
    "startup_failed",
    "degraded_terminal",
    "stopped",
}

TERMINAL_EXPERIMENT_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "PARTIALLY_COMPLETED"}


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def stable_json(value: Any) -> str:
    return json.dumps(json_safe(value), sort_keys=True, separators=(",", ":"), default=str)


def sha256_payload(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


def _require_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return dict(value)


def _require_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    return list(value)


def _clean_id(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label} is required")
    return text


def _unique_ids(items: Sequence[Mapping[str, Any]], label: str) -> None:
    seen: set[str] = set()
    for item in items:
        item_id = _clean_id(item.get("id"), f"{label}.id")
        if item_id in seen:
            raise ValueError(f"duplicate {label} id: {item_id}")
        seen.add(item_id)


def _normalize_windows(raw: Any) -> list[dict[str, Any]]:
    windows = []
    for item in _require_list(raw, "windows"):
        window = _require_mapping(item, "windows[]")
        windows.append(
            {
                "id": _clean_id(window.get("id"), "windows[].id"),
                "start": _clean_id(window.get("start"), "windows[].start"),
                "end": _clean_id(window.get("end"), "windows[].end"),
                **{key: value for key, value in window.items() if key not in {"id", "start", "end"}},
            }
        )
    if not windows:
        raise ValueError("at least one window is required")
    _unique_ids(windows, "window")
    return windows


def _normalize_variants(raw: Any) -> list[dict[str, Any]]:
    variants = []
    for item in _require_list(raw, "variants"):
        variant = _require_mapping(item, "variants[]")
        variants.append(
            {
                "id": _clean_id(variant.get("id"), "variants[].id"),
                "bot_id": _clean_id(variant.get("bot_id"), "variants[].bot_id"),
                **{key: value for key, value in variant.items() if key not in {"id", "bot_id"}},
            }
        )
    if not variants:
        raise ValueError("at least one variant is required")
    _unique_ids(variants, "variant")
    return variants


def _normalize_comparisons(raw: Any, *, variants: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if raw is None:
        return []
    variant_ids = {str(item.get("id")) for item in variants}
    comparisons = []
    for index, item in enumerate(_require_list(raw, "comparisons")):
        comparison = _require_mapping(item, "comparisons[]")
        baseline_id = _clean_id(comparison.get("baseline_variant_id"), "comparisons[].baseline_variant_id")
        candidate_id = _clean_id(comparison.get("candidate_variant_id"), "comparisons[].candidate_variant_id")
        if baseline_id not in variant_ids:
            raise ValueError(f"comparison baseline variant does not exist: {baseline_id}")
        if candidate_id not in variant_ids:
            raise ValueError(f"comparison candidate variant does not exist: {candidate_id}")
        comparisons.append(
            {
                "id": str(comparison.get("id") or f"{baseline_id}_vs_{candidate_id}_{index + 1}"),
                "baseline_variant_id": baseline_id,
                "candidate_variant_id": candidate_id,
                "compare_per_window": bool(comparison.get("compare_per_window", True)),
                "aggregate_summary": bool(comparison.get("aggregate_summary", True)),
                **{
                    key: value
                    for key, value in comparison.items()
                    if key
                    not in {
                        "id",
                        "baseline_variant_id",
                        "candidate_variant_id",
                        "compare_per_window",
                        "aggregate_summary",
                    }
                },
            }
        )
    _unique_ids(comparisons, "comparison")
    return comparisons


def _comparison_ids(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    return [
        (str(item.get("baseline_variant_id")), str(item.get("candidate_variant_id")))
        for item in plan.get("comparisons") or []
    ]


def _gate_spec(gate_id: str, gate_type: str, **fields: Any) -> dict[str, Any]:
    return {"id": gate_id, "type": gate_type, **fields}


def _normalize_pass_gates(raw: Any) -> dict[str, Any]:
    payload = dict(raw or {})
    explicit = payload.get("gates")
    gates: list[dict[str, Any]] = []
    if explicit is not None:
        for item in _require_list(explicit, "pass_gates.gates"):
            gate = _require_mapping(item, "pass_gates.gates[]")
            gates.append({"id": _clean_id(gate.get("id"), "pass_gates.gates[].id"), **gate})
    shorthand = {key: value for key, value in payload.items() if key != "gates"}
    if "max_drawdown_pct" in shorthand:
        gates.append(
            _gate_spec(
                "max_drawdown_pct",
                "candidate_metric_threshold",
                metric="max_drawdown_pct",
                operator="<=",
                threshold=shorthand["max_drawdown_pct"],
                scope="per_window",
            )
        )
    if "min_trade_count_per_window" in shorthand:
        gates.append(
            _gate_spec(
                "min_trade_count_per_window",
                "candidate_metric_threshold",
                metric="trade_count",
                operator=">=",
                threshold=shorthand["min_trade_count_per_window"],
                scope="per_window",
            )
        )
    if "min_trade_count_ratio_vs_baseline" in shorthand:
        gates.append(
            _gate_spec(
                "min_trade_count_ratio_vs_baseline",
                "baseline_candidate_ratio",
                baseline_metric="trade_count",
                candidate_metric="trade_count",
                operator=">=",
                threshold=shorthand["min_trade_count_ratio_vs_baseline"],
                scope="per_window",
            )
        )
    if "min_windows_with_pf_gt_1" in shorthand:
        gates.append(
            _gate_spec(
                "min_windows_with_pf_gt_1",
                "candidate_window_count",
                metric="profit_factor",
                operator=">",
                threshold=1.0,
                count_operator=">=",
                count_threshold=shorthand["min_windows_with_pf_gt_1"],
            )
        )
    if "max_low_sample_symbol_net_contribution_pct" in shorthand:
        gates.append(
            _gate_spec(
                "max_low_sample_symbol_net_contribution_pct",
                "low_sample_symbol_contribution",
                operator="<=",
                threshold=shorthand["max_low_sample_symbol_net_contribution_pct"],
                min_symbol_trades=payload.get("min_symbol_trade_count", payload.get("min_trade_count_per_window")),
            )
        )
    _unique_ids(gates, "pass gate")
    return {
        "schema_version": "pass_gate_spec_set.v1",
        "gates": gates,
    }


def normalize_plan(raw: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(_require_mapping(raw, "experiment plan"))
    schema_version = str(payload.get("schema_version") or PLAN_SCHEMA)
    if schema_version != PLAN_SCHEMA:
        raise ValueError(f"unsupported experiment plan schema_version: {schema_version}")
    windows = _normalize_windows(payload.get("windows"))
    variants = _normalize_variants(payload.get("variants"))
    comparisons = _normalize_comparisons(payload.get("comparisons"), variants=variants)
    run_policy = {
        "mode": "sequential",
        "stop_on_first_failure": False,
        "poll_interval_seconds": 30.0,
        "run_timeout_seconds": 3600.0,
        "update_bot_window": True,
        **dict(payload.get("run_policy") or {}),
    }
    if run_policy.get("mode") != "sequential":
        raise ValueError("only run_policy.mode=sequential is supported")
    export_policy = {
        "enabled": True,
        "include_json": True,
        "include_csv": True,
        "include_candles": False,
        **dict(payload.get("export_policy") or {}),
    }
    materialization_policy = {
        "build": True,
        "require_ready": True,
        "force_rebuild": False,
        **dict(payload.get("materialization_policy") or {}),
    }
    comparison_policy = {
        "include_golden": True,
        "require_golden": False,
        **dict(payload.get("comparison_policy") or {}),
    }
    notification_policy = {
        "schema_version": "notification_policy.v1",
        "enabled": False,
        "sinks": ["file"],
        "on_states": ["COMPLETED", "FAILED", "PARTIALLY_COMPLETED"],
        **dict(payload.get("notification_policy") or {}),
    }
    normalized = {
        "schema_version": PLAN_SCHEMA,
        "name": _clean_id(payload.get("name"), "name"),
        "hypothesis": str(payload.get("hypothesis") or "").strip(),
        "run_policy": run_policy,
        "windows": windows,
        "variants": variants,
        "comparisons": comparisons,
        "export_policy": export_policy,
        "materialization_policy": materialization_policy,
        "comparison_policy": comparison_policy,
        "pass_gates": _normalize_pass_gates(payload.get("pass_gates") or {}),
        "notification_policy": notification_policy,
        "metadata": dict(payload.get("metadata") or {}),
    }
    normalized["plan_hash"] = sha256_payload({key: value for key, value in normalized.items() if key != "plan_hash"})
    _ = _comparison_ids(normalized)
    return normalized


def build_step_plan(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    for window in plan.get("windows") or []:
        window_id = str(window.get("id"))
        for variant in plan.get("variants") or []:
            variant_id = str(variant.get("id"))
            base = {
                "schema_version": STEP_SCHEMA,
                "window_id": window_id,
                "variant_id": variant_id,
                "bot_id": variant.get("bot_id"),
                "status": "PENDING",
                "artifact_refs": [],
                "started_at": None,
                "completed_at": None,
                "error": None,
            }
            steps.append({"step_id": f"run__{window_id}__{variant_id}", "type": "RUN_BOT", **base})
            if dict(plan.get("export_policy") or {}).get("enabled", True):
                steps.append({"step_id": f"export__{window_id}__{variant_id}", "type": "EXPORT_REPORT", **base})
            if dict(plan.get("materialization_policy") or {}).get("build", True):
                steps.append({"step_id": f"materialize__{window_id}__{variant_id}", "type": "MATERIALIZE_REPORT", **base})
            steps.append({"step_id": f"summary__{window_id}__{variant_id}", "type": "FETCH_SUMMARY", **base})
    for comparison in plan.get("comparisons") or []:
        if not comparison.get("compare_per_window", True):
            continue
        comparison_id = str(comparison.get("id"))
        for window in plan.get("windows") or []:
            window_id = str(window.get("id"))
            steps.append(
                {
                    "schema_version": STEP_SCHEMA,
                    "step_id": f"compare__{window_id}__{comparison_id}",
                    "type": "COMPARE_REPORTS",
                    "window_id": window_id,
                    "variant_id": None,
                    "bot_id": None,
                    "comparison_id": comparison_id,
                    "status": "PENDING",
                    "artifact_refs": [],
                    "started_at": None,
                    "completed_at": None,
                    "error": None,
                }
            )
    steps.append(
        {
            "schema_version": STEP_SCHEMA,
            "step_id": "evaluate_gates",
            "type": "EVALUATE_GATES",
            "window_id": None,
            "variant_id": None,
            "bot_id": None,
            "status": "PENDING",
            "artifact_refs": [],
            "started_at": None,
            "completed_at": None,
            "error": None,
        }
    )
    steps.append(
        {
            "schema_version": STEP_SCHEMA,
            "step_id": "notify",
            "type": "NOTIFY",
            "window_id": None,
            "variant_id": None,
            "bot_id": None,
            "status": "PENDING",
            "artifact_refs": [],
            "started_at": None,
            "completed_at": None,
            "error": None,
        }
    )
    return steps

