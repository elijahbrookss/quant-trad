"""Materialized RunReportDTO v2 comparison service."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional

from .materialization import (
    RunReportMaterializationNotTerminal,
    materialized_run_report,
    report_materialization_status,
)
from .golden_evidence import read_golden_comparison_evidence
from .schemas import (
    BehaviorDeltaDTO,
    CoordinatorWaitDeltaDTO,
    FirstDivergenceDTO,
    GoldenEvidenceDTO,
    MetricDeltaDTO,
    OperationalDriftDTO,
    PerformanceDeltaDTO,
    RunComparisonDTO,
    RunReportDTO,
    SymbolDeltaDTO,
    TrustComparisonDTO,
    WalletComparisonDTO,
)


def compare_materialized_run_reports(
    left_run_id: str,
    right_run_id: str,
    *,
    include_golden: bool = True,
    require_golden: bool = False,
) -> RunComparisonDTO:
    """Compare two ready materialized RunReportDTO v2 artifacts without building them."""

    left_status = _status_payload(left_run_id)
    right_status = _status_payload(right_run_id)
    blocked_reason = _blocked_reason(left_status, right_status)
    if blocked_reason:
        return _blocked_comparison(left_run_id, right_run_id, blocked_reason, left_status, right_status)

    left_payload = materialized_run_report(left_run_id)
    if left_payload is None:
        return _blocked_comparison(left_run_id, right_run_id, "left_report_not_ready", left_status, right_status)
    right_payload = materialized_run_report(right_run_id)
    if right_payload is None:
        return _blocked_comparison(left_run_id, right_run_id, "right_report_not_ready", left_status, right_status)

    left = RunReportDTO.model_validate(left_payload)
    right = RunReportDTO.model_validate(right_payload)
    golden_evidence = (
        _report_golden_evidence(read_golden_comparison_evidence(left_run_id, right_run_id), left, right)
        if include_golden
        else GoldenEvidenceDTO(status="not_requested")
    )
    if require_golden and not golden_evidence.available:
        return _blocked_comparison(
            left_run_id,
            right_run_id,
            "golden_evidence_not_available",
            left_status,
            right_status,
            golden_evidence=golden_evidence,
        )
    return _ready_comparison(left, right, left_status, right_status, golden_evidence=golden_evidence)


def summarize_run_report_comparison(comparison: RunComparisonDTO) -> Dict[str, Any]:
    """Return the compact comparison shape used by CLI/agent workflows."""

    return {
        "schema_version": "run_report_comparison_summary.v1",
        "left_run_id": comparison.left_run_id,
        "right_run_id": comparison.right_run_id,
        "comparison_status": comparison.comparison_status,
        "comparison_verdict": comparison.comparison_verdict,
        "can_compare": comparison.can_compare,
        "blocked_reason": comparison.blocked_reason,
        "trust": _model_subset(
            comparison.trust_comparison,
            (
                "semantic_fingerprint_match",
                "operational_fingerprint_match",
                "data_snapshot_hash_match",
                "config_hash_match",
                "strategy_hash_match",
                "first_blocker_reason",
            ),
        ),
        "performance_delta": _model_subset(
            comparison.performance_delta,
            (
                "net_pnl",
                "total_return_pct",
                "max_drawdown",
                "max_drawdown_pct",
                "profit_factor",
                "expectancy",
                "win_rate",
                "trade_count",
                "fees",
                "exposure_pct",
                "time_in_market_pct",
            ),
        ),
        "behavior_delta": _model_subset(
            comparison.behavior_delta,
            (
                "decision_count_delta",
                "accepted_delta",
                "rejected_delta",
                "rejection_reason_deltas",
                "action_distribution_deltas",
                "entry_count_delta",
                "exit_count_delta",
                "average_holding_duration_delta",
                "trade_lifecycle_equal",
                "verdict_changes",
                "golden_artifact_status",
            ),
        ),
        "wallet": _model_subset(
            comparison.wallet_comparison,
            (
                "wallet_trace_complete_left",
                "wallet_trace_complete_right",
                "missing_wallet_trace_count_left",
                "missing_wallet_trace_count_right",
                "wallet_projection_status_left",
                "wallet_projection_status_right",
                "wallet_projection_equal",
                "final_wallet_value_delta",
                "margin_warnings_delta",
            ),
        ),
        "symbols": [
            _model_subset(
                row,
                (
                    "symbol",
                    "left_trade_count",
                    "right_trade_count",
                    "trade_count_delta",
                    "net_pnl_delta",
                    "fees_delta",
                    "win_rate_delta",
                    "contribution_delta",
                    "decision_delta",
                    "accepted_delta",
                    "rejected_delta",
                    "missing_on_left",
                    "missing_on_right",
                ),
            )
            for row in comparison.symbol_deltas
        ],
        "first_divergence": _model_subset(
            comparison.first_divergence,
            (
                "present",
                "divergence_type",
                "symbol",
                "timeframe",
                "bar_time",
                "decision_id",
                "trade_id",
                "field_path",
                "explanation",
                "source",
            ),
        ),
    }


def _model_subset(value: Any, keys: Iterable[str]) -> Dict[str, Any]:
    if hasattr(value, "model_dump"):
        payload = value.model_dump(mode="json")
    elif isinstance(value, Mapping):
        payload = dict(value)
    else:
        payload = {}
    return {key: payload.get(key) for key in keys if payload.get(key) is not None}


def _status_payload(run_id: str) -> Dict[str, Any]:
    try:
        return report_materialization_status(run_id, require_terminal=True)
    except RunReportMaterializationNotTerminal as exc:
        return {
            "contract_version": "run_report_v2",
            "schema_version": "run_report_materialization_status.v1",
            "run_id": run_id,
            "report_status": {
                "status": "run_not_terminal",
                "contract_version": "run_report_v2",
                "can_view": False,
                "can_build": False,
                "can_retry": False,
            },
            "run_status": exc.status,
        }


def _status_name(payload: Mapping[str, Any]) -> str:
    return str((payload.get("report_status") or {}).get("status") or "not_started")


def _blocked_reason(left_status: Mapping[str, Any], right_status: Mapping[str, Any]) -> Optional[str]:
    left = _status_name(left_status)
    right = _status_name(right_status)
    if left == "run_not_terminal" or right == "run_not_terminal":
        return "run_not_terminal"
    if left == "building":
        return "left_report_building"
    if right == "building":
        return "right_report_building"
    if left == "failed":
        return "left_report_failed"
    if right == "failed":
        return "right_report_failed"
    if left != "ready" or (left_status.get("report_status") or {}).get("can_view") is not True:
        return "left_report_not_ready"
    if right != "ready" or (right_status.get("report_status") or {}).get("can_view") is not True:
        return "right_report_not_ready"
    return None


def _blocked_comparison(
    left_run_id: str,
    right_run_id: str,
    reason: str,
    left_status: Optional[Mapping[str, Any]] = None,
    right_status: Optional[Mapping[str, Any]] = None,
    golden_evidence: Optional[GoldenEvidenceDTO] = None,
) -> RunComparisonDTO:
    return RunComparisonDTO(
        left_run_id=left_run_id,
        right_run_id=right_run_id,
        comparison_status="blocked",
        comparison_verdict="blocked",
        can_compare=False,
        blocked_reason=reason,
        first_divergence=FirstDivergenceDTO(
            present=False,
            divergence_type="not_available",
            explanation=f"Comparison blocked: {reason}.",
            source="report_materialization_status",
        ),
        golden_evidence=golden_evidence or GoldenEvidenceDTO(),
        raw_refs={
            "source": "portal_report_materializations_v1",
            "left_report_status": left_status or {},
            "right_report_status": right_status or {},
            "golden_evidence_status": (golden_evidence.status if golden_evidence else "not_available"),
            "cold_build_triggered": False,
        },
    )


def _ready_comparison(
    left: RunReportDTO,
    right: RunReportDTO,
    left_status: Mapping[str, Any],
    right_status: Mapping[str, Any],
    golden_evidence: GoldenEvidenceDTO,
) -> RunComparisonDTO:
    trust = _trust_comparison(left, right)
    semantic_match = _golden_or_report_bool(golden_evidence.semantic_fingerprint_match, trust.semantic_fingerprint_match) is True
    operational_match = _golden_or_report_bool(golden_evidence.operational_fingerprint_match, trust.operational_fingerprint_match) is True
    data_match = _golden_or_report_bool(golden_evidence.data_snapshot_hash_match, trust.data_snapshot_hash_match) is not False

    if _golden_semantic_drift(golden_evidence):
        verdict = "semantic_drift"
    elif semantic_match and operational_match:
        verdict = "semantic_match"
    elif semantic_match:
        verdict = "semantic_match_operational_drift"
    else:
        verdict = "semantic_drift"

    status = "ready" if data_match else "ready_with_caveats"
    return RunComparisonDTO(
        left_run_id=left.run_id,
        right_run_id=right.run_id,
        comparison_status=status,
        comparison_verdict=verdict,
        can_compare=True,
        blocked_reason=None,
        trust_comparison=trust,
        performance_delta=_performance_delta(left.performance, right.performance),
        behavior_delta=_behavior_delta(left.behavior, right.behavior, golden_evidence),
        wallet_comparison=_wallet_comparison(left, right, golden_evidence),
        symbol_deltas=_symbol_deltas(left.symbol_breakdown, right.symbol_breakdown),
        coordinator_wait_delta=_coordinator_wait_delta(left.coordinator_waits, right.coordinator_waits),
        operational_drift=_operational_drift(left, right, semantic_match, operational_match),
        first_divergence=golden_evidence.first_divergence if golden_evidence.status != "not_requested" else _first_divergence(left, right, semantic_match),
        golden_evidence=golden_evidence,
        raw_refs={
            "source": "portal_report_materializations_v1",
            "left_artifact_id": (left_status.get("report_status") or {}).get("artifact_id"),
            "right_artifact_id": (right_status.get("report_status") or {}).get("artifact_id"),
            "left_contract_version": left.contract_version,
            "right_contract_version": right.contract_version,
            "golden_artifact_path": golden_evidence.artifact_path,
            "golden_evidence_status": golden_evidence.status,
            "golden_service_integration": "artifact_reader",
            "cold_build_triggered": False,
        },
    )


def _report_golden_evidence(evidence: GoldenEvidenceDTO, left: RunReportDTO, right: RunReportDTO) -> GoldenEvidenceDTO:
    """Fill order checks available on materialized reports without changing golden verdicts."""

    return evidence.model_copy(
        update={
            "entry_decision_order_timeout_left": evidence.entry_decision_order_timeout_left
            if evidence.entry_decision_order_timeout_left is not None
            else left.trust.entry_decision_order_timeout_count,
            "entry_decision_order_timeout_right": evidence.entry_decision_order_timeout_right
            if evidence.entry_decision_order_timeout_right is not None
            else right.trust.entry_decision_order_timeout_count,
        }
    )


def _golden_or_report_bool(golden_value: Optional[bool], report_value: Optional[bool]) -> Optional[bool]:
    return golden_value if golden_value is not None else report_value


def _golden_semantic_drift(evidence: GoldenEvidenceDTO) -> bool:
    if not evidence.available:
        return False
    if evidence.semantic_fingerprint_match is False:
        return True
    return bool(
        evidence.missing_decision_count
        or evidence.extra_decision_count
        or evidence.verdict_change_count
        or evidence.trade_lifecycle_equal is False
    )


def _trust_comparison(left: RunReportDTO, right: RunReportDTO) -> TrustComparisonDTO:
    left_trust = left.trust
    right_trust = right.trust
    return TrustComparisonDTO(
        lifecycle_status_left=left_trust.lifecycle_status,
        lifecycle_status_right=right_trust.lifecycle_status,
        readiness_status_left=left_trust.readiness_status,
        readiness_status_right=right_trust.readiness_status,
        golden_status_left=left_trust.golden_status or left_trust.golden_candidate_status,
        golden_status_right=right_trust.golden_status or right_trust.golden_candidate_status,
        semantic_fingerprint_match=_same_if_present(left_trust.semantic_fingerprint, right_trust.semantic_fingerprint),
        operational_fingerprint_match=_same_if_present(left_trust.operational_fingerprint, right_trust.operational_fingerprint),
        data_snapshot_hash_match=_same_if_present(left_trust.data_snapshot_hash, right_trust.data_snapshot_hash),
        config_hash_match=_same_if_present(left_trust.material_config_hash or left_trust.config_hash, right_trust.material_config_hash or right_trust.config_hash),
        strategy_hash_match=_same_if_present(left_trust.strategy_hash, right_trust.strategy_hash),
        runtime_ordering_status_left=left_trust.runtime_ordering_status,
        runtime_ordering_status_right=right_trust.runtime_ordering_status,
        wallet_trace_complete_left=left_trust.wallet_trace_complete,
        wallet_trace_complete_right=right_trust.wallet_trace_complete,
        candle_continuity_left=left_trust.candle_continuity_status,
        candle_continuity_right=right_trust.candle_continuity_status,
        observer_safety_left=left_trust.observer_invariance_status,
        observer_safety_right=right_trust.observer_invariance_status,
        first_blocker_reason=left_trust.first_failure_reason or right_trust.first_failure_reason,
    )


def _same_if_present(left: Any, right: Any) -> Optional[bool]:
    if left in (None, "") or right in (None, ""):
        return None
    return left == right


def _performance_delta(left: Any, right: Any) -> PerformanceDeltaDTO:
    fields = {
        "net_pnl": _metric_delta(left.net_pnl, right.net_pnl),
        "total_return_pct": _metric_delta(left.total_return_pct, right.total_return_pct),
        "max_drawdown": _metric_delta(left.max_drawdown, right.max_drawdown),
        "max_drawdown_pct": _metric_delta(left.max_drawdown_pct, right.max_drawdown_pct),
        "sharpe": _metric_delta(left.sharpe, right.sharpe),
        "sortino": _metric_delta(left.sortino, right.sortino),
        "calmar": _metric_delta(left.calmar, right.calmar),
        "profit_factor": _metric_delta(left.profit_factor, right.profit_factor),
        "expectancy": _metric_delta(left.expectancy, right.expectancy),
        "win_rate": _metric_delta(left.win_rate, right.win_rate),
        "trade_count": _metric_delta(left.trade_count, right.trade_count),
        "fees": _metric_delta(left.fees, right.fees),
        "exposure_pct": _metric_delta(left.exposure_pct, right.exposure_pct),
        "time_in_market_pct": _metric_delta(left.time_in_market_pct, right.time_in_market_pct),
    }
    return PerformanceDeltaDTO(**fields)


def _metric_delta(left: Any, right: Any) -> MetricDeltaDTO:
    left_value = getattr(left, "value", None)
    right_value = getattr(right, "value", None)
    unit = getattr(left, "unit", None) or getattr(right, "unit", None)
    left_valid = getattr(left, "valid", False) is True
    right_valid = getattr(right, "valid", False) is True
    if not left_valid or not right_valid:
        return MetricDeltaDTO(
            left=left_value,
            right=right_value,
            valid=False,
            unit=unit,
            method=getattr(left, "method", None) or getattr(right, "method", None),
            source=getattr(left, "source", None) or getattr(right, "source", None),
            invalid_reason=_invalid_metric_reason(left, right),
            caveats=list(getattr(left, "caveats", []) or []) + list(getattr(right, "caveats", []) or []),
        )
    left_number = _number(left_value)
    right_number = _number(right_value)
    if left_number is None or right_number is None:
        return MetricDeltaDTO(
            left=left_value,
            right=right_value,
            valid=False,
            unit=unit,
            invalid_reason="non_numeric_metric_value",
        )
    return MetricDeltaDTO(
        left=left_number,
        right=right_number,
        delta=right_number - left_number,
        valid=True,
        unit=unit,
        method=getattr(left, "method", None) or getattr(right, "method", None),
        source=getattr(left, "source", None) or getattr(right, "source", None),
        caveats=list(getattr(left, "caveats", []) or []) + list(getattr(right, "caveats", []) or []),
    )


def _invalid_metric_reason(left: Any, right: Any) -> str:
    left_reason = getattr(left, "invalid_reason", None)
    right_reason = getattr(right, "invalid_reason", None)
    if left_reason and right_reason and left_reason != right_reason:
        return f"left:{left_reason}; right:{right_reason}"
    if left_reason:
        return f"left:{left_reason}"
    if right_reason:
        return f"right:{right_reason}"
    return "metric_not_comparable"


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _int_delta(left: Optional[int], right: Optional[int]) -> Optional[int]:
    if left is None or right is None:
        return None
    return int(right) - int(left)


def _dict_delta(left: Mapping[str, int], right: Mapping[str, int]) -> Dict[str, int]:
    keys = set(left) | set(right)
    return {key: int(right.get(key, 0)) - int(left.get(key, 0)) for key in sorted(keys)}


def _behavior_delta(left: Any, right: Any, golden_evidence: Optional[GoldenEvidenceDTO] = None) -> BehaviorDeltaDTO:
    if golden_evidence and golden_evidence.available:
        return BehaviorDeltaDTO(
            decision_count_delta=_int_delta(golden_evidence.decision_count_left, golden_evidence.decision_count_right),
            accepted_delta=_int_delta(left.accepted_decisions, right.accepted_decisions),
            rejected_delta=_int_delta(left.rejected_decisions, right.rejected_decisions),
            rejection_reason_deltas=_dict_delta(left.rejection_reasons or {}, right.rejection_reasons or {}),
            action_distribution_deltas=_dict_delta(left.action_distribution or {}, right.action_distribution or {}),
            entry_count_delta=_int_delta(left.entry_count, right.entry_count),
            exit_count_delta=_int_delta(left.exit_count, right.exit_count),
            average_holding_duration_delta=_metric_delta(left.average_holding_period, right.average_holding_period),
            trade_lifecycle_equal=golden_evidence.trade_lifecycle_equal,
            trade_lifecycle_source="golden",
            missing_decision_ids=golden_evidence.missing_decision_ids,
            extra_decision_ids=golden_evidence.extra_decision_ids,
            verdict_changes=golden_evidence.verdict_change_count,
            golden_artifact_status="available",
        )
    return BehaviorDeltaDTO(
        decision_count_delta=_int_delta(left.total_decisions, right.total_decisions),
        accepted_delta=_int_delta(left.accepted_decisions, right.accepted_decisions),
        rejected_delta=_int_delta(left.rejected_decisions, right.rejected_decisions),
        rejection_reason_deltas=_dict_delta(left.rejection_reasons or {}, right.rejection_reasons or {}),
        action_distribution_deltas=_dict_delta(left.action_distribution or {}, right.action_distribution or {}),
        entry_count_delta=_int_delta(left.entry_count, right.entry_count),
        exit_count_delta=_int_delta(left.exit_count, right.exit_count),
        average_holding_duration_delta=_metric_delta(left.average_holding_period, right.average_holding_period),
        trade_lifecycle_equal=None,
        trade_lifecycle_source="golden_artifact_not_available" if golden_evidence else "golden_artifact_not_integrated",
        verdict_changes=None,
        golden_artifact_status=golden_evidence.status if golden_evidence else "not_integrated",
    )


def _wallet_comparison(left: RunReportDTO, right: RunReportDTO, golden_evidence: Optional[GoldenEvidenceDTO] = None) -> WalletComparisonDTO:
    left_wallet = left.wallet
    right_wallet = right.wallet
    left_status = left_wallet.wallet_projection_status
    right_status = right_wallet.wallet_projection_status
    return WalletComparisonDTO(
        wallet_trace_complete_left=left_wallet.wallet_trace_complete,
        wallet_trace_complete_right=right_wallet.wallet_trace_complete,
        missing_wallet_trace_count_left=(
            golden_evidence.wallet_trace_missing_left
            if golden_evidence and golden_evidence.wallet_trace_missing_left is not None
            else left_wallet.missing_wallet_trace_count
        ),
        missing_wallet_trace_count_right=(
            golden_evidence.wallet_trace_missing_right
            if golden_evidence and golden_evidence.wallet_trace_missing_right is not None
            else right_wallet.missing_wallet_trace_count
        ),
        wallet_projection_status_left=left_status,
        wallet_projection_status_right=right_status,
        wallet_projection_equal=_same_if_present(left_status, right_status),
        final_wallet_value_delta=_metric_delta(left_wallet.final_wallet_value, right_wallet.final_wallet_value),
        margin_warnings_delta=len(right_wallet.margin_warnings or []) - len(left_wallet.margin_warnings or []),
        reservation_leak_status_left=left_wallet.reservation_leaks or {},
        reservation_leak_status_right=right_wallet.reservation_leaks or {},
    )


def _symbol_deltas(left_items: Iterable[Any], right_items: Iterable[Any]) -> list[SymbolDeltaDTO]:
    left_by_symbol = {item.symbol: item for item in left_items}
    right_by_symbol = {item.symbol: item for item in right_items}
    rows: list[SymbolDeltaDTO] = []
    for symbol in sorted(set(left_by_symbol) | set(right_by_symbol)):
        left = left_by_symbol.get(symbol)
        right = right_by_symbol.get(symbol)
        rows.append(
            SymbolDeltaDTO(
                symbol=symbol,
                left_trade_count=left.trade_count if left else None,
                right_trade_count=right.trade_count if right else None,
                trade_count_delta=_int_delta(left.trade_count if left else None, right.trade_count if right else None),
                net_pnl_delta=_metric_delta(left.net_pnl if left else None, right.net_pnl if right else None),
                fees_delta=_metric_delta(left.fees if left else None, right.fees if right else None),
                win_rate_delta=_metric_delta(left.win_rate if left else None, right.win_rate if right else None),
                contribution_delta=_metric_delta(left.contribution_pct if left else None, right.contribution_pct if right else None),
                decision_delta=_int_delta(left.decision_count if left else None, right.decision_count if right else None),
                accepted_delta=_int_delta(left.accepted_decisions if left else None, right.accepted_decisions if right else None),
                rejected_delta=_int_delta(left.rejected_decisions if left else None, right.rejected_decisions if right else None),
                rejection_reason_deltas=_dict_delta(left.rejection_reasons if left else {}, right.rejection_reasons if right else {}),
                missing_on_left=left is None,
                missing_on_right=right is None,
            )
        )
    return rows


def _coordinator_wait_delta(left: Any, right: Any) -> CoordinatorWaitDeltaDTO:
    return CoordinatorWaitDeltaDTO(
        total_wait_delta=_float_delta(left.total_wait_ms, right.total_wait_ms),
        max_wait_delta=_float_delta(left.max_wait_ms, right.max_wait_ms),
        wait_count_delta=_int_delta(left.wait_count, right.wait_count),
        top_wait_symbols_left=_top_wait_symbols(left.top_waits),
        top_wait_symbols_right=_top_wait_symbols(right.top_waits),
        candidate_blocker_comparison={"left_status": left.status, "right_status": right.status},
        caveats=list(left.caveats or []) + list(right.caveats or []),
    )


def _float_delta(left: Optional[float], right: Optional[float]) -> Optional[float]:
    left_number = _number(left)
    right_number = _number(right)
    if left_number is None or right_number is None:
        return None
    return right_number - left_number


def _top_wait_symbols(items: Iterable[Any]) -> list[str]:
    symbols: list[str] = []
    for item in items or []:
        symbol = getattr(item, "candidate_symbol", None)
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _operational_drift(
    left: RunReportDTO,
    right: RunReportDTO,
    semantic_match: bool,
    operational_match: bool,
) -> OperationalDriftDTO:
    left_diag = left.operational_diagnostics
    right_diag = right.operational_diagnostics
    differences: list[str] = []
    if left_diag.db_slow_write_warning_count != right_diag.db_slow_write_warning_count:
        differences.append("db_slow_write_warning_count")
    if len(left_diag.telemetry_warnings or []) != len(right_diag.telemetry_warnings or []):
        differences.append("telemetry_warning_count")
    if len(left_diag.step_trace_warnings or []) != len(right_diag.step_trace_warnings or []):
        differences.append("step_trace_warning_count")
    if left_diag.diagnostics_degraded_status != right_diag.diagnostics_degraded_status:
        differences.append("diagnostics_degraded_status")

    if operational_match:
        summary = "operational_match"
    elif semantic_match:
        summary = "operational_drift_only"
    else:
        summary = "operational_and_semantic_drift"

    statement = None
    if semantic_match and not operational_match:
        statement = "Operational drift is diagnostic-only here because semantic fingerprints match."

    return OperationalDriftDTO(
        operational_fingerprint_match=operational_match,
        operational_drift_summary=summary,
        telemetry_caveats=_diagnostic_caveats(left_diag.telemetry_warnings, right_diag.telemetry_warnings),
        db_caveats=_count_caveat("db_slow_write_warning_count", left_diag.db_slow_write_warning_count, right_diag.db_slow_write_warning_count),
        step_trace_caveats=_diagnostic_caveats(left_diag.step_trace_warnings, right_diag.step_trace_warnings),
        diagnostic_only_differences=differences,
        statement=statement,
    )


def _diagnostic_caveats(left: Iterable[Mapping[str, Any]], right: Iterable[Mapping[str, Any]]) -> list[str]:
    caveats: list[str] = []
    if len(list(left or [])) != len(list(right or [])):
        caveats.append("warning_count_differs")
    return caveats


def _count_caveat(label: str, left: Optional[int], right: Optional[int]) -> list[str]:
    if left != right:
        return [f"{label}: left={left}, right={right}"]
    return []


def _first_divergence(left: RunReportDTO, right: RunReportDTO, semantic_match: bool) -> FirstDivergenceDTO:
    if semantic_match:
        return FirstDivergenceDTO(
            present=False,
            divergence_type="none",
            explanation="No semantic divergence detected by materialized report fingerprints.",
            source="report_comparison",
        )
    return FirstDivergenceDTO(
        present=True,
        divergence_type="semantic_fingerprint_mismatch",
        field_path="trust.semantic_fingerprint",
        left_value=left.trust.semantic_fingerprint,
        right_value=right.trust.semantic_fingerprint,
        explanation="Semantic fingerprints differ; exact first decision-level divergence requires golden artifact service integration.",
        source="report_comparison",
    )
