"""Registered experiment comparison gates owned by the research service."""

from __future__ import annotations

import operator
from collections.abc import Mapping
from typing import Any


PASS_GATE_RESULT_SCHEMA = "pass_gate_result.v2"
_OPERATORS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "equals": operator.eq,
    "!=": operator.ne,
}
_METRIC_ALIASES = {
    "trade_count": ("trade_count", "total_trades", "trades", "closed_trades"),
    "total_trades": ("total_trades", "trade_count", "trades", "closed_trades"),
    "closed_trades": ("closed_trades", "trade_count", "total_trades", "trades"),
}
_GATE_EVALUATOR_VERSION = "research.pass_gate_evaluator.v2"


def _number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric(summary: Mapping[str, Any] | None, name: str) -> Any:
    if not summary:
        return None
    names = _METRIC_ALIASES.get(name, (name,))
    for container in (
        summary.get("metrics"),
        summary,
        summary.get("summary"),
        summary.get("portfolio_metrics"),
        summary.get("performance"),
        summary.get("stats"),
    ):
        if not isinstance(container, Mapping):
            continue
        for metric_name in names:
            if metric_name not in container:
                continue
            value = container.get(metric_name)
            return value.get("value") if isinstance(value, Mapping) else value
    return None


def _compare(left: Any, operation: str, right: Any) -> bool | None:
    evaluator = _OPERATORS.get(str(operation or ""))
    left_number = _number(left)
    right_number = _number(right)
    if evaluator is None or left_number is None or right_number is None:
        return None
    return bool(evaluator(left_number, right_number))


def _result(
    gate: Mapping[str, Any],
    status: str,
    *,
    observed: Any = None,
    reason: str | None = None,
    details: Any = None,
) -> dict[str, Any]:
    return {
        "gate_id": gate.get("id"),
        "gate_type": gate.get("type"),
        "evaluator_version": _GATE_EVALUATOR_VERSION,
        "status": status,
        "observed": observed,
        "threshold": gate.get("threshold", gate.get("count_threshold")),
        "operator": gate.get("operator", gate.get("count_operator")),
        "reason": reason,
        "details": details,
    }


def _candidate_metric(
    gate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    metric_name = str(gate.get("metric") or "")
    for comparison in plan.get("comparisons") or []:
        candidate_id = str(comparison.get("candidate_variant_id") or "")
        for window in plan.get("windows") or []:
            window_id = str(window.get("id") or "")
            observed = _metric(summaries.get((window_id, candidate_id)), metric_name)
            matched = _compare(observed, gate.get("operator"), gate.get("threshold"))
            details = {
                "window_id": window_id,
                "candidate_variant_id": candidate_id,
                "metric": metric_name,
            }
            rows.append(
                _result(
                    gate,
                    "UNSUPPORTED"
                    if matched is None
                    else "PASSED"
                    if matched
                    else "FAILED",
                    observed=observed,
                    reason=(
                        "required_metric_missing_or_non_numeric"
                        if matched is None
                        else None
                    ),
                    details=details,
                )
            )
    return rows


def _ratio(
    gate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for comparison in plan.get("comparisons") or []:
        baseline_id = str(comparison.get("baseline_variant_id") or "")
        candidate_id = str(comparison.get("candidate_variant_id") or "")
        for window in plan.get("windows") or []:
            window_id = str(window.get("id") or "")
            baseline = _number(
                _metric(
                    summaries.get((window_id, baseline_id)),
                    str(gate.get("baseline_metric") or ""),
                )
            )
            candidate = _number(
                _metric(
                    summaries.get((window_id, candidate_id)),
                    str(gate.get("candidate_metric") or ""),
                )
            )
            details = {
                "window_id": window_id,
                "baseline_variant_id": baseline_id,
                "candidate_variant_id": candidate_id,
                "baseline_value": baseline,
                "candidate_value": candidate,
            }
            if baseline in (None, 0.0) or candidate is None:
                rows.append(
                    _result(
                        gate,
                        "UNSUPPORTED",
                        reason="ratio_inputs_missing_or_zero",
                        details=details,
                    )
                )
                continue
            observed = candidate / baseline
            matched = _compare(observed, gate.get("operator"), gate.get("threshold"))
            rows.append(
                _result(
                    gate,
                    "UNSUPPORTED"
                    if matched is None
                    else "PASSED"
                    if matched
                    else "FAILED",
                    observed=observed,
                    reason="unsupported_operator" if matched is None else None,
                    details=details,
                )
            )
    return rows


def _window_count(
    gate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    metric_name = str(gate.get("metric") or "")
    for comparison in plan.get("comparisons") or []:
        candidate_id = str(comparison.get("candidate_variant_id") or "")
        matched_windows: list[str] = []
        unsupported_windows: list[str] = []
        for window in plan.get("windows") or []:
            window_id = str(window.get("id") or "")
            observed = _metric(summaries.get((window_id, candidate_id)), metric_name)
            matched = _compare(observed, gate.get("operator"), gate.get("threshold"))
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
            rows.append(
                _result(
                    gate,
                    "UNSUPPORTED",
                    observed=len(matched_windows),
                    reason="one_or_more_windows_missing_metric",
                    details=details,
                )
            )
            continue
        matched = _compare(
            len(matched_windows),
            gate.get("count_operator"),
            gate.get("count_threshold"),
        )
        rows.append(
            _result(
                gate,
                "UNSUPPORTED"
                if matched is None
                else "PASSED"
                if matched
                else "FAILED",
                observed=len(matched_windows),
                reason="unsupported_count_operator" if matched is None else None,
                details=details,
            )
        )
    return rows


def _symbol_rows(summary: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    if not summary:
        return []
    for key in ("symbols", "symbol_contribution", "per_symbol", "by_symbol"):
        value = summary.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, Mapping)]
        if isinstance(value, Mapping):
            return [
                {"symbol": symbol, **dict(payload)}
                for symbol, payload in value.items()
                if isinstance(payload, Mapping)
            ]
    return []


def _low_sample_contribution(
    gate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    minimum = _number(gate.get("min_symbol_trades"))
    if minimum is None:
        return [_result(gate, "UNSUPPORTED", reason="min_symbol_trades_missing")]
    results: list[dict[str, Any]] = []
    for comparison in plan.get("comparisons") or []:
        candidate_id = str(comparison.get("candidate_variant_id") or "")
        for window in plan.get("windows") or []:
            window_id = str(window.get("id") or "")
            rows = _symbol_rows(summaries.get((window_id, candidate_id)))
            details = {
                "window_id": window_id,
                "candidate_variant_id": candidate_id,
            }
            if not rows:
                results.append(
                    _result(
                        gate,
                        "UNSUPPORTED",
                        reason="symbol_contribution_fields_missing",
                        details=details,
                    )
                )
                continue
            total = 0.0
            low_sample = 0.0
            for row in rows:
                trades = _number(row.get("trade_count") or row.get("trades"))
                pnl = _number(row.get("net_pnl") or row.get("pnl"))
                if trades is None or pnl is None:
                    continue
                total += abs(pnl)
                if trades < minimum:
                    low_sample += abs(pnl)
            if total <= 0:
                results.append(
                    _result(
                        gate,
                        "UNSUPPORTED",
                        reason="symbol_net_pnl_total_missing_or_zero",
                        details=details,
                    )
                )
                continue
            observed = low_sample / total * 100.0
            matched = _compare(observed, gate.get("operator"), gate.get("threshold"))
            results.append(
                _result(
                    gate,
                    "UNSUPPORTED"
                    if matched is None
                    else "PASSED"
                    if matched
                    else "FAILED",
                    observed=observed,
                    reason="unsupported_operator" if matched is None else None,
                    details=details,
                )
            )
    return results


def evaluate_pass_gates(
    *,
    plan: Mapping[str, Any],
    summaries: Mapping[tuple[str, str], Mapping[str, Any]],
    comparison_refs: list[Mapping[str, Any]],
) -> dict[str, Any]:
    """Evaluate the bounded registered gate set over canonical report summaries."""

    gate_results: list[dict[str, Any]] = []
    configured = list(dict(plan.get("pass_gates") or {}).get("gates") or [])
    if not configured and str(plan.get("intent") or "").lower() in {
        "selection",
        "promotion",
    }:
        gate_results.append(
            {
                "gate_id": "mandatory_non_empty_gate_set",
                "gate_type": "protocol_guard",
                "evaluator_version": _GATE_EVALUATOR_VERSION,
                "status": "FAILED",
                "observed": 0,
                "threshold": 1,
                "operator": ">=",
                "reason": "selection_or_promotion_requires_non_empty_pass_gates",
                "details": None,
            }
        )
    evaluators = {
        "candidate_metric_threshold": _candidate_metric,
        "baseline_candidate_ratio": _ratio,
        "candidate_window_count": _window_count,
        "low_sample_symbol_contribution": _low_sample_contribution,
    }
    for gate in configured:
        evaluator = evaluators.get(str(gate.get("type") or ""))
        if evaluator is None:
            gate_results.append(
                _result(
                    gate,
                    "UNSUPPORTED",
                    reason=f"unsupported_gate_type:{gate.get('type') or ''}",
                )
            )
        else:
            gate_results.extend(
                evaluator(gate, plan=plan, summaries=summaries)
            )
    failed = [
        row for row in gate_results if row.get("status") in {"FAILED", "UNSUPPORTED"}
    ]
    return {
        "schema_version": PASS_GATE_RESULT_SCHEMA,
        "evaluator_version": _GATE_EVALUATOR_VERSION,
        "status": "FAILED" if failed else "PASSED",
        "gates": gate_results,
        "comparison_refs": [dict(row) for row in comparison_refs],
        "promotion_authority": False,
        "execution_authority": False,
    }


def evaluate_pass_gate_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    plan = payload.get("plan")
    raw_summaries = payload.get("summaries")
    if not isinstance(plan, Mapping) or not isinstance(raw_summaries, list):
        raise ValueError("pass_gate_request_invalid: plan and summaries are required")
    summaries: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in raw_summaries:
        if not isinstance(row, Mapping) or not isinstance(row.get("summary"), Mapping):
            raise ValueError("pass_gate_request_invalid: summary rows must be objects")
        key = (str(row.get("window_id") or ""), str(row.get("variant_id") or ""))
        if not all(key) or key in summaries:
            raise ValueError("pass_gate_request_invalid: summary identity is empty or duplicated")
        summaries[key] = dict(row["summary"])
    return evaluate_pass_gates(
        plan=dict(plan),
        summaries=summaries,
        comparison_refs=[
            dict(row)
            for row in payload.get("comparison_refs") or []
            if isinstance(row, Mapping)
        ],
    )


__all__ = [
    "PASS_GATE_RESULT_SCHEMA",
    "evaluate_pass_gate_request",
    "evaluate_pass_gates",
]
