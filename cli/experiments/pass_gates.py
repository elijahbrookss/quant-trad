from __future__ import annotations

import operator as py_operator
from collections.abc import Mapping
from typing import Any

from .contracts import PASS_GATE_RESULT_SCHEMA


_OPS = {
    "<": py_operator.lt,
    "<=": py_operator.le,
    ">": py_operator.gt,
    ">=": py_operator.ge,
    "==": py_operator.eq,
    "equals": py_operator.eq,
    "!=": py_operator.ne,
}

_METRIC_ALIASES = {
    "trade_count": ("trade_count", "total_trades", "trades", "closed_trades"),
    "total_trades": ("total_trades", "trade_count", "trades", "closed_trades"),
    "closed_trades": ("closed_trades", "trade_count", "total_trades", "trades"),
}


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric_value(value: Any) -> Any:
    if isinstance(value, Mapping) and "value" in value:
        return value.get("value")
    return value


def _metric(summary: Mapping[str, Any] | None, name: str) -> Any:
    if not summary:
        return None
    names = _METRIC_ALIASES.get(name, (name,))
    metrics = summary.get("metrics")
    if isinstance(metrics, Mapping):
        for metric_name in names:
            if metric_name in metrics:
                return _metric_value(metrics.get(metric_name))
    for metric_name in names:
        if metric_name in summary:
            return _metric_value(summary.get(metric_name))
    for container_name in ("summary", "portfolio_metrics", "performance", "stats"):
        container = summary.get(container_name)
        if isinstance(container, Mapping):
            for metric_name in names:
                if metric_name in container:
                    return _metric_value(container.get(metric_name))
    return None


def _compare(left: Any, op: str, right: Any) -> bool | None:
    func = _OPS.get(str(op))
    if func is None:
        return None
    left_num = _number(left)
    right_num = _number(right)
    if left_num is None or right_num is None:
        return None
    return bool(func(left_num, right_num))


def _gate_result(gate: Mapping[str, Any], status: str, *, observed: Any = None, reason: str | None = None, details: Any = None) -> dict[str, Any]:
    return {
        "gate_id": gate.get("id"),
        "gate_type": gate.get("type"),
        "status": status,
        "observed": observed,
        "threshold": gate.get("threshold", gate.get("count_threshold")),
        "operator": gate.get("operator", gate.get("count_operator")),
        "reason": reason,
        "details": details,
    }


def _variant_summaries(
    *,
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
    comparison: Mapping[str, Any],
    window_id: str,
) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
    baseline_id = str(comparison.get("baseline_variant_id"))
    candidate_id = str(comparison.get("candidate_variant_id"))
    return summaries.get((window_id, baseline_id)), summaries.get((window_id, candidate_id))


def _evaluate_candidate_metric_threshold(
    gate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    metric_name = str(gate.get("metric") or "")
    for comparison in plan.get("comparisons") or []:
        candidate_id = str(comparison.get("candidate_variant_id"))
        for window in plan.get("windows") or []:
            window_id = str(window.get("id"))
            summary = summaries.get((window_id, candidate_id))
            observed = _metric(summary, metric_name)
            matched = _compare(observed, str(gate.get("operator") or ""), gate.get("threshold"))
            details = {"window_id": window_id, "candidate_variant_id": candidate_id, "metric": metric_name}
            if matched is None:
                results.append(_gate_result(gate, "UNSUPPORTED", observed=observed, reason="required_metric_missing_or_non_numeric", details=details))
            else:
                results.append(_gate_result(gate, "PASSED" if matched else "FAILED", observed=observed, details=details))
    return results


def _evaluate_ratio(
    gate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    for comparison in plan.get("comparisons") or []:
        for window in plan.get("windows") or []:
            window_id = str(window.get("id"))
            baseline, candidate = _variant_summaries(summaries=summaries, comparison=comparison, window_id=window_id)
            baseline_value = _number(_metric(baseline, str(gate.get("baseline_metric") or "")))
            candidate_value = _number(_metric(candidate, str(gate.get("candidate_metric") or "")))
            details = {
                "window_id": window_id,
                "baseline_variant_id": comparison.get("baseline_variant_id"),
                "candidate_variant_id": comparison.get("candidate_variant_id"),
                "baseline_value": baseline_value,
                "candidate_value": candidate_value,
            }
            if baseline_value in (None, 0.0) or candidate_value is None:
                results.append(_gate_result(gate, "UNSUPPORTED", observed=None, reason="ratio_inputs_missing_or_zero", details=details))
                continue
            ratio = candidate_value / baseline_value
            matched = _compare(ratio, str(gate.get("operator") or ""), gate.get("threshold"))
            results.append(_gate_result(gate, "PASSED" if matched else "FAILED", observed=ratio, details=details))
    return results


def _evaluate_window_count(
    gate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    metric_name = str(gate.get("metric") or "")
    for comparison in plan.get("comparisons") or []:
        candidate_id = str(comparison.get("candidate_variant_id"))
        matched_windows: list[str] = []
        unsupported_windows: list[str] = []
        for window in plan.get("windows") or []:
            window_id = str(window.get("id"))
            observed = _metric(summaries.get((window_id, candidate_id)), metric_name)
            matched = _compare(observed, str(gate.get("operator") or ""), gate.get("threshold"))
            if matched is None:
                unsupported_windows.append(window_id)
            elif matched:
                matched_windows.append(window_id)
        details = {
            "candidate_variant_id": candidate_id,
            "matched_windows": matched_windows,
            "unsupported_windows": unsupported_windows,
            "metric": metric_name,
        }
        if unsupported_windows:
            results.append(_gate_result(gate, "UNSUPPORTED", observed=len(matched_windows), reason="one_or_more_windows_missing_metric", details=details))
            continue
        count_match = _compare(len(matched_windows), str(gate.get("count_operator") or ""), gate.get("count_threshold"))
        results.append(_gate_result(gate, "PASSED" if count_match else "FAILED", observed=len(matched_windows), details=details))
    return results


def _symbol_rows(summary: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    if not summary:
        return []
    for key in ("symbols", "symbol_contribution", "per_symbol", "by_symbol"):
        value = summary.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
        if isinstance(value, Mapping):
            return [dict({"symbol": symbol}, **payload) for symbol, payload in value.items() if isinstance(payload, Mapping)]
    return []


def _evaluate_low_sample_symbol_contribution(
    gate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    min_trades = _number(gate.get("min_symbol_trades"))
    if min_trades is None:
        return [_gate_result(gate, "UNSUPPORTED", reason="min_symbol_trades_missing")]
    for comparison in plan.get("comparisons") or []:
        candidate_id = str(comparison.get("candidate_variant_id"))
        for window in plan.get("windows") or []:
            window_id = str(window.get("id"))
            rows = _symbol_rows(summaries.get((window_id, candidate_id)))
            details = {"window_id": window_id, "candidate_variant_id": candidate_id}
            if not rows:
                results.append(_gate_result(gate, "UNSUPPORTED", reason="symbol_contribution_fields_missing", details=details))
                continue
            total_abs = 0.0
            low_sample_abs = 0.0
            for row in rows:
                trade_count = _number(row.get("trade_count") or row.get("trades"))
                net_pnl = _number(row.get("net_pnl") or row.get("pnl"))
                if trade_count is None or net_pnl is None:
                    continue
                total_abs += abs(net_pnl)
                if trade_count < min_trades:
                    low_sample_abs += abs(net_pnl)
            if total_abs <= 0:
                results.append(_gate_result(gate, "UNSUPPORTED", reason="symbol_net_pnl_total_missing_or_zero", details=details))
                continue
            observed = (low_sample_abs / total_abs) * 100.0
            matched = _compare(observed, str(gate.get("operator") or ""), gate.get("threshold"))
            results.append(_gate_result(gate, "PASSED" if matched else "FAILED", observed=observed, details=details))
    return results


def evaluate_pass_gates(
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
    comparison_refs: list[Mapping[str, Any]],
) -> dict[str, Any]:
    gate_results: list[dict[str, Any]] = []
    for gate in dict(plan.get("pass_gates") or {}).get("gates") or []:
        gate_type = str(gate.get("type") or "")
        if gate_type == "candidate_metric_threshold":
            gate_results.extend(_evaluate_candidate_metric_threshold(gate, plan=plan, summaries=summaries))
        elif gate_type == "baseline_candidate_ratio":
            gate_results.extend(_evaluate_ratio(gate, plan=plan, summaries=summaries))
        elif gate_type == "candidate_window_count":
            gate_results.extend(_evaluate_window_count(gate, plan=plan, summaries=summaries))
        elif gate_type == "low_sample_symbol_contribution":
            gate_results.extend(_evaluate_low_sample_symbol_contribution(gate, plan=plan, summaries=summaries))
        else:
            gate_results.append(_gate_result(gate, "UNSUPPORTED", reason=f"unsupported_gate_type:{gate_type}"))
    failed = [item for item in gate_results if item.get("status") in {"FAILED", "UNSUPPORTED"}]
    return {
        "schema_version": PASS_GATE_RESULT_SCHEMA,
        "status": "FAILED" if failed else "PASSED",
        "gates": gate_results,
        "comparison_refs": list(comparison_refs),
    }
