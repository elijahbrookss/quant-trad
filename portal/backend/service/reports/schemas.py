"""Typed reporting API schemas for the canonical reporting data product."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ReportDiagnosticModel(BaseModel):
    severity: Literal["info", "warning", "critical"]
    source: str
    code: str
    message: str
    affected_identity: Dict[str, Any] = Field(default_factory=dict)
    timestamp: Optional[str] = None
    known_at: Optional[str] = None
    readiness_impact: str = "none"
    suggested_next_step: Optional[str] = None


class ReportDiagnosticsResponse(BaseModel):
    schema_version: str
    run_id: str
    items: List[ReportDiagnosticModel] = Field(default_factory=list)
    summary: Dict[str, Any] = Field(default_factory=dict)


class ReportReadinessResponse(BaseModel):
    schema_version: str = "report_readiness.v1"
    run_id: str
    dataset_ready: bool
    results_ready: bool
    safe_to_compare: bool
    reason: str
    conditions: Dict[str, bool] = Field(default_factory=dict)
    export_status: str
    dataset_status: str
    results_status: str = "blocked"
    comparison_status: str = "blocked"
    data_quality_status: str = "unknown"
    execution_quality_status: str = "unknown"
    blocking_reasons: List[str] = Field(default_factory=list)
    degraded_sections: List[str] = Field(default_factory=list)
    unavailable_sections: List[str] = Field(default_factory=list)
    golden_candidate_status: str = "unknown"
    golden_blocking_reasons: List[str] = Field(default_factory=list)
    repeatability_status: str = "unknown"
    material_fingerprint: Optional[str] = None
    semantic_fingerprint: Optional[str] = None
    operational_fingerprint: Optional[str] = None
    caveats: List[str] = Field(default_factory=list)
    diagnostics: ReportDiagnosticsResponse


class RunReportSummaryResponse(BaseModel):
    schema_version: str = "run_report_summary.v1"
    run_id: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    readiness: Dict[str, Any] = Field(default_factory=dict)
    summary: Dict[str, Any] = Field(default_factory=dict)
    portfolio_metrics: Dict[str, Any] = Field(default_factory=dict)
    sections: Dict[str, Any] = Field(default_factory=dict)


class MetricValueDTO(BaseModel):
    value: Any = None
    valid: bool = False
    unit: Optional[str] = None
    method: Optional[str] = None
    source: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    sample_count: Optional[int] = None
    minimum_sample_count: Optional[int] = None
    invalid_reason: Optional[str] = None
    caveats: List[str] = Field(default_factory=list)


class ResearchTrustDTO(BaseModel):
    lifecycle_status: Optional[str] = None
    terminal_reason: Optional[str] = None
    golden_status: str = "not_available"
    golden_candidate_status: str = "unknown"
    research_status: str = "unknown"
    readiness_status: str = "unknown"
    readiness_blockers: List[str] = Field(default_factory=list)
    caveats: List[str] = Field(default_factory=list)
    config_hash: Optional[str] = None
    material_config_hash: Optional[str] = None
    data_snapshot_hash: Optional[str] = None
    strategy_hash: Optional[str] = None
    semantic_fingerprint: Optional[str] = None
    operational_fingerprint: Optional[str] = None
    runtime_ordering_status: str = "unknown"
    run_seq_gap_count: Optional[int] = None
    run_seq_duplicate_count: Optional[int] = None
    wallet_trace_complete: Optional[bool] = None
    wallet_market_time_overtake_count: Optional[int] = None
    entry_decision_order_timeout_count: Optional[int] = None
    candle_continuity_status: str = "unknown"
    canonical_continuity_evidence_status: str = "unknown"
    observer_invariance_status: str = "unknown"
    first_failure_reason: Optional[str] = None


class PerformanceMetricsDTO(BaseModel):
    net_pnl: MetricValueDTO = Field(default_factory=MetricValueDTO)
    gross_pnl: MetricValueDTO = Field(default_factory=MetricValueDTO)
    realized_pnl: MetricValueDTO = Field(default_factory=MetricValueDTO)
    unrealized_pnl: MetricValueDTO = Field(default_factory=MetricValueDTO)
    total_return_pct: MetricValueDTO = Field(default_factory=MetricValueDTO)
    annualized_return_pct: MetricValueDTO = Field(default_factory=MetricValueDTO)
    max_drawdown: MetricValueDTO = Field(default_factory=MetricValueDTO)
    max_drawdown_pct: MetricValueDTO = Field(default_factory=MetricValueDTO)
    drawdown_duration: MetricValueDTO = Field(default_factory=MetricValueDTO)
    sharpe: MetricValueDTO = Field(default_factory=MetricValueDTO)
    sortino: MetricValueDTO = Field(default_factory=MetricValueDTO)
    calmar: MetricValueDTO = Field(default_factory=MetricValueDTO)
    profit_factor: MetricValueDTO = Field(default_factory=MetricValueDTO)
    expectancy: MetricValueDTO = Field(default_factory=MetricValueDTO)
    win_rate: MetricValueDTO = Field(default_factory=MetricValueDTO)
    loss_rate: MetricValueDTO = Field(default_factory=MetricValueDTO)
    average_win: MetricValueDTO = Field(default_factory=MetricValueDTO)
    average_loss: MetricValueDTO = Field(default_factory=MetricValueDTO)
    average_win_loss_ratio: MetricValueDTO = Field(default_factory=MetricValueDTO)
    largest_win: MetricValueDTO = Field(default_factory=MetricValueDTO)
    largest_loss: MetricValueDTO = Field(default_factory=MetricValueDTO)
    trade_count: MetricValueDTO = Field(default_factory=MetricValueDTO)
    winning_trades: MetricValueDTO = Field(default_factory=MetricValueDTO)
    losing_trades: MetricValueDTO = Field(default_factory=MetricValueDTO)
    breakeven_trades: MetricValueDTO = Field(default_factory=MetricValueDTO)
    fees: MetricValueDTO = Field(default_factory=MetricValueDTO)
    slippage: MetricValueDTO = Field(default_factory=MetricValueDTO)
    exposure_pct: MetricValueDTO = Field(default_factory=MetricValueDTO)
    time_in_market_pct: MetricValueDTO = Field(default_factory=MetricValueDTO)
    average_trade_duration: MetricValueDTO = Field(default_factory=MetricValueDTO)
    margin_usage: MetricValueDTO = Field(default_factory=MetricValueDTO)


class SymbolBreakdownDTO(BaseModel):
    symbol: str
    trade_count: int = 0
    decision_count: Optional[int] = None
    accepted_decisions: Optional[int] = None
    rejected_decisions: Optional[int] = None
    rejection_count: Optional[int] = None
    rejection_reasons: Dict[str, int] = Field(default_factory=dict)
    net_pnl: MetricValueDTO = Field(default_factory=MetricValueDTO)
    gross_pnl: MetricValueDTO = Field(default_factory=MetricValueDTO)
    fees: MetricValueDTO = Field(default_factory=MetricValueDTO)
    win_rate: MetricValueDTO = Field(default_factory=MetricValueDTO)
    average_win: MetricValueDTO = Field(default_factory=MetricValueDTO)
    average_loss: MetricValueDTO = Field(default_factory=MetricValueDTO)
    contribution_pct: MetricValueDTO = Field(default_factory=MetricValueDTO)
    caveats: List[str] = Field(default_factory=list)


class DecisionBehaviorDTO(BaseModel):
    total_signals: int = 0
    total_decisions: int = 0
    accepted_decisions: int = 0
    rejected_decisions: int = 0
    rejection_reasons: Dict[str, int] = Field(default_factory=dict)
    action_distribution: Dict[str, int] = Field(default_factory=dict)
    entry_count: Optional[int] = None
    exit_count: Optional[int] = None
    average_holding_period: MetricValueDTO = Field(default_factory=MetricValueDTO)
    median_holding_period: MetricValueDTO = Field(default_factory=MetricValueDTO)
    longest_trade_duration: MetricValueDTO = Field(default_factory=MetricValueDTO)
    shortest_trade_duration: MetricValueDTO = Field(default_factory=MetricValueDTO)
    margin_rejection_count: Optional[int] = None
    position_policy_rejection_count: Optional[int] = None


class WalletPerformanceDTO(BaseModel):
    wallet_trace_complete: Optional[bool] = None
    missing_wallet_trace_count: Optional[int] = None
    wallet_projection_status: str = "unknown"
    final_wallet_value: MetricValueDTO = Field(default_factory=MetricValueDTO)
    final_cash_collateral: MetricValueDTO = Field(default_factory=MetricValueDTO)
    margin_warnings: List[Dict[str, Any]] = Field(default_factory=list)
    reservation_leaks: Dict[str, Any] = Field(default_factory=dict)
    caveats: List[str] = Field(default_factory=list)


class CoordinatorWaitTopWaitDTO(BaseModel):
    candidate_id: Optional[str] = None
    decision_id: Optional[str] = None
    candidate_symbol: Optional[str] = None
    candidate_timeframe: Optional[str] = None
    candidate_bar_time: Optional[str] = None
    wait_elapsed_ms: Optional[float] = None
    wait_poll_count: Optional[int] = None
    final_action: Optional[str] = None
    release_reason: Optional[str] = None
    blocker_symbols: List[str] = Field(default_factory=list)
    first_blocker_watermarks: List[Dict[str, Any]] = Field(default_factory=list)
    release_watermarks: List[Dict[str, Any]] = Field(default_factory=list)
    worker_id: Optional[str] = None
    caveats: List[str] = Field(default_factory=list)


class CoordinatorWaitSummaryDTO(BaseModel):
    status: str = "not_available"
    total_wait_ms: Optional[float] = None
    wait_count: Optional[int] = None
    max_wait_ms: Optional[float] = None
    release_count: Optional[int] = None
    fail_count: Optional[int] = None
    top_waits: List[CoordinatorWaitTopWaitDTO] = Field(default_factory=list)
    caveats: List[str] = Field(default_factory=list)


class OperationalDiagnosticsDTO(BaseModel):
    operational_fingerprint: Optional[str] = None
    operational_drift_status: str = "not_computed"
    telemetry_warnings: List[Dict[str, Any]] = Field(default_factory=list)
    db_slow_write_warning_count: Optional[int] = None
    step_trace_warnings: List[Dict[str, Any]] = Field(default_factory=list)
    botlens_diagnostic_caveats: List[str] = Field(default_factory=list)
    diagnostics_degraded_status: str = "unknown"
    caveats: List[str] = Field(default_factory=list)


class RunReportDTO(BaseModel):
    contract_version: str = "run_report_v2"
    schema_version: str = "run_report.v2"
    run_id: str
    identity: Dict[str, Any] = Field(default_factory=dict)
    trust: ResearchTrustDTO = Field(default_factory=ResearchTrustDTO)
    performance: PerformanceMetricsDTO = Field(default_factory=PerformanceMetricsDTO)
    behavior: DecisionBehaviorDTO = Field(default_factory=DecisionBehaviorDTO)
    wallet: WalletPerformanceDTO = Field(default_factory=WalletPerformanceDTO)
    symbol_breakdown: List[SymbolBreakdownDTO] = Field(default_factory=list)
    coordinator_waits: CoordinatorWaitSummaryDTO = Field(default_factory=CoordinatorWaitSummaryDTO)
    operational_diagnostics: OperationalDiagnosticsDTO = Field(default_factory=OperationalDiagnosticsDTO)
    sections: Dict[str, Any] = Field(default_factory=dict)
    raw_refs: Dict[str, Any] = Field(default_factory=dict)


class ReportMaterializationStatusDTO(BaseModel):
    status: str = "not_started"
    contract_version: str = "run_report_v2"
    artifact_id: Optional[str] = None
    artifact_path: Optional[str] = None
    built_at: Optional[str] = None
    started_at: Optional[str] = None
    duration_ms: Optional[float] = None
    error: Optional[str] = None
    stale_reason: Optional[str] = None
    cache_key: Optional[str] = None
    can_view: bool = False
    can_build: bool = False
    can_retry: bool = False


class RunReportMaterializationResponse(BaseModel):
    contract_version: str = "run_report_v2"
    schema_version: str = "run_report_materialization_status.v1"
    run_id: str
    report_status: ReportMaterializationStatusDTO


class MetricDeltaDTO(BaseModel):
    left: Any = None
    right: Any = None
    delta: Any = None
    valid: bool = False
    unit: Optional[str] = None
    method: Optional[str] = None
    source: Optional[str] = None
    invalid_reason: Optional[str] = None
    caveats: List[str] = Field(default_factory=list)


class TrustComparisonDTO(BaseModel):
    lifecycle_status_left: Optional[str] = None
    lifecycle_status_right: Optional[str] = None
    readiness_status_left: Optional[str] = None
    readiness_status_right: Optional[str] = None
    golden_status_left: Optional[str] = None
    golden_status_right: Optional[str] = None
    semantic_fingerprint_match: Optional[bool] = None
    operational_fingerprint_match: Optional[bool] = None
    data_snapshot_hash_match: Optional[bool] = None
    config_hash_match: Optional[bool] = None
    strategy_hash_match: Optional[bool] = None
    runtime_ordering_status_left: Optional[str] = None
    runtime_ordering_status_right: Optional[str] = None
    wallet_trace_complete_left: Optional[bool] = None
    wallet_trace_complete_right: Optional[bool] = None
    candle_continuity_left: Optional[str] = None
    candle_continuity_right: Optional[str] = None
    observer_safety_left: Optional[str] = None
    observer_safety_right: Optional[str] = None
    first_blocker_reason: Optional[str] = None


class PerformanceDeltaDTO(BaseModel):
    net_pnl: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    total_return_pct: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    max_drawdown: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    max_drawdown_pct: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    sharpe: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    sortino: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    calmar: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    profit_factor: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    expectancy: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    win_rate: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    trade_count: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    fees: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    exposure_pct: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    time_in_market_pct: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)


class BehaviorDeltaDTO(BaseModel):
    decision_count_delta: Optional[int] = None
    accepted_delta: Optional[int] = None
    rejected_delta: Optional[int] = None
    rejection_reason_deltas: Dict[str, int] = Field(default_factory=dict)
    action_distribution_deltas: Dict[str, int] = Field(default_factory=dict)
    entry_count_delta: Optional[int] = None
    exit_count_delta: Optional[int] = None
    average_holding_duration_delta: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    trade_lifecycle_equal: Optional[bool] = None
    trade_lifecycle_source: str = "not_available"
    missing_decision_ids: List[str] = Field(default_factory=list)
    extra_decision_ids: List[str] = Field(default_factory=list)
    verdict_changes: Optional[int] = None
    golden_artifact_status: str = "not_integrated"


class SymbolDeltaDTO(BaseModel):
    symbol: str
    left_trade_count: Optional[int] = None
    right_trade_count: Optional[int] = None
    trade_count_delta: Optional[int] = None
    net_pnl_delta: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    fees_delta: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    win_rate_delta: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    contribution_delta: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    decision_delta: Optional[int] = None
    accepted_delta: Optional[int] = None
    rejected_delta: Optional[int] = None
    rejection_reason_deltas: Dict[str, int] = Field(default_factory=dict)
    missing_on_left: bool = False
    missing_on_right: bool = False


class WalletComparisonDTO(BaseModel):
    wallet_trace_complete_left: Optional[bool] = None
    wallet_trace_complete_right: Optional[bool] = None
    missing_wallet_trace_count_left: Optional[int] = None
    missing_wallet_trace_count_right: Optional[int] = None
    wallet_projection_status_left: Optional[str] = None
    wallet_projection_status_right: Optional[str] = None
    wallet_projection_equal: Optional[bool] = None
    final_wallet_value_delta: MetricDeltaDTO = Field(default_factory=MetricDeltaDTO)
    margin_warnings_delta: Optional[int] = None
    reservation_leak_status_left: Dict[str, Any] = Field(default_factory=dict)
    reservation_leak_status_right: Dict[str, Any] = Field(default_factory=dict)


class CoordinatorWaitDeltaDTO(BaseModel):
    total_wait_delta: Optional[float] = None
    max_wait_delta: Optional[float] = None
    wait_count_delta: Optional[int] = None
    top_wait_symbols_left: List[str] = Field(default_factory=list)
    top_wait_symbols_right: List[str] = Field(default_factory=list)
    candidate_blocker_comparison: Dict[str, Any] = Field(default_factory=dict)
    caveats: List[str] = Field(default_factory=list)


class OperationalDriftDTO(BaseModel):
    operational_fingerprint_match: Optional[bool] = None
    operational_drift_summary: str = "not_available"
    telemetry_caveats: List[str] = Field(default_factory=list)
    db_caveats: List[str] = Field(default_factory=list)
    step_trace_caveats: List[str] = Field(default_factory=list)
    diagnostic_only_differences: List[str] = Field(default_factory=list)
    statement: Optional[str] = None


class FirstDivergenceDTO(BaseModel):
    present: bool = False
    divergence_type: str = "not_computed"
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    bar_time: Optional[str] = None
    decision_id: Optional[str] = None
    trade_id: Optional[str] = None
    field_path: Optional[str] = None
    left_value: Any = None
    right_value: Any = None
    explanation: Optional[str] = None
    source: str = "not_computed"


class GoldenEvidenceDTO(BaseModel):
    available: bool = False
    status: str = "not_available"
    artifact_path: Optional[str] = None
    generated_at: Optional[str] = None
    verdict: Optional[str] = None
    fail_reasons: List[str] = Field(default_factory=list)
    semantic_fingerprint_match: Optional[bool] = None
    operational_fingerprint_match: Optional[bool] = None
    data_snapshot_hash_match: Optional[bool] = None
    material_config_hash_match: Optional[bool] = None
    strategy_hash_match: Optional[bool] = None
    decision_count_left: Optional[int] = None
    decision_count_right: Optional[int] = None
    missing_decision_count: Optional[int] = None
    extra_decision_count: Optional[int] = None
    missing_decision_ids: List[str] = Field(default_factory=list)
    extra_decision_ids: List[str] = Field(default_factory=list)
    decision_diff_full_lists_available: bool = False
    verdict_change_count: Optional[int] = None
    verdict_changes: List[Dict[str, Any]] = Field(default_factory=list)
    verdict_changes_full_available: bool = False
    trade_lifecycle_equal: Optional[bool] = None
    trade_count_left: Optional[int] = None
    trade_count_right: Optional[int] = None
    wallet_trace_missing_left: Optional[int] = None
    wallet_trace_missing_right: Optional[int] = None
    wallet_market_time_overtake_left: Optional[int] = None
    wallet_market_time_overtake_right: Optional[int] = None
    entry_decision_order_timeout_left: Optional[int] = None
    entry_decision_order_timeout_right: Optional[int] = None
    runtime_ordering_left: Dict[str, Any] = Field(default_factory=dict)
    runtime_ordering_right: Dict[str, Any] = Field(default_factory=dict)
    first_divergence: FirstDivergenceDTO = Field(
        default_factory=lambda: FirstDivergenceDTO(
            present=False,
            divergence_type="not_available",
            explanation="Golden evidence not available.",
            source="golden",
        )
    )
    raw: Dict[str, Any] = Field(default_factory=dict)


class RunComparisonDTO(BaseModel):
    contract_version: str = "run_report_comparison_v1"
    left_run_id: str
    right_run_id: str
    comparison_status: str
    comparison_verdict: str
    can_compare: bool
    blocked_reason: Optional[str] = None
    trust_comparison: TrustComparisonDTO = Field(default_factory=TrustComparisonDTO)
    performance_delta: PerformanceDeltaDTO = Field(default_factory=PerformanceDeltaDTO)
    behavior_delta: BehaviorDeltaDTO = Field(default_factory=BehaviorDeltaDTO)
    wallet_comparison: WalletComparisonDTO = Field(default_factory=WalletComparisonDTO)
    symbol_deltas: List[SymbolDeltaDTO] = Field(default_factory=list)
    coordinator_wait_delta: CoordinatorWaitDeltaDTO = Field(default_factory=CoordinatorWaitDeltaDTO)
    operational_drift: OperationalDriftDTO = Field(default_factory=OperationalDriftDTO)
    first_divergence: FirstDivergenceDTO = Field(default_factory=FirstDivergenceDTO)
    golden_evidence: GoldenEvidenceDTO = Field(default_factory=GoldenEvidenceDTO)
    raw_refs: Dict[str, Any] = Field(default_factory=dict)


class ReportSectionsResponse(BaseModel):
    schema_version: str
    items: List[Dict[str, Any]] = Field(default_factory=list)


class DatasetPageResponse(BaseModel):
    schema_version: str
    run_id: str
    section: str
    limit: int
    offset: int
    total: int
    items: List[Dict[str, Any]] = Field(default_factory=list)


class MetricExplanationResponse(BaseModel):
    schema_version: str = "metric_explanation.v1"
    run_id: str
    metric_name: str
    value: Any = None
    unit: Optional[str] = None
    formula: Dict[str, Any] = Field(default_factory=dict)
    source_sections: List[str] = Field(default_factory=list)
    source_refs: List[Dict[str, Any]] = Field(default_factory=list)
    availability: str = "available"
    caveats: List[str] = Field(default_factory=list)


class ExportManifestResponse(BaseModel):
    schema_version: str = "export_manifest.v1"
    export_manifest_version: str = "export_manifest.v1"
    run_id: str
    dataset_schema_version: str
    status: str
    filename: str
    files: List[Dict[str, Any]] = Field(default_factory=list)
    unavailable_sections: List[Dict[str, Any]] = Field(default_factory=list)
    diagnostics: Dict[str, Any] = Field(default_factory=dict)


class RunResearchDatasetResponse(BaseModel):
    schema_version: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    readiness: Dict[str, Any] = Field(default_factory=dict)
    summary: Dict[str, Any] = Field(default_factory=dict)
    sections: Dict[str, Any] = Field(default_factory=dict)
    timeseries: Dict[str, Any] = Field(default_factory=dict)
    diagnostics: ReportDiagnosticsResponse
    decisions: List[Dict[str, Any]] = Field(default_factory=list)
    signals: List[Dict[str, Any]] = Field(default_factory=list)
    trades: List[Dict[str, Any]] = Field(default_factory=list)
    context: Dict[str, Any] = Field(default_factory=dict)
    candle_catalog: Dict[str, Any] = Field(default_factory=dict)
    fee_accounting: Dict[str, Any] = Field(default_factory=dict)
    wallet_accounting: Dict[str, Any] = Field(default_factory=dict)
    execution: Dict[str, Any] = Field(default_factory=dict)
    candle_gaps: Dict[str, Any] = Field(default_factory=dict)
    portfolio_metrics: Dict[str, Any] = Field(default_factory=dict)
    performance: Dict[str, Any] = Field(default_factory=dict)
    operational_health: Dict[str, Any] = Field(default_factory=dict)
    strategy_insights: Dict[str, Any] = Field(default_factory=dict)
    narrative_summary: str


class ReportListResponse(BaseModel):
    schema_version: str = "report_list.v1"
    items: List[Dict[str, Any]] = Field(default_factory=list)
    total: int
    limit: int
    offset: int


class ReportCompareRequest(BaseModel):
    run_ids: List[str] = Field(min_length=2)


class RunComparisonResultResponse(BaseModel):
    schema_version: str = "run_comparison_result.v1"
    status: Literal["ready", "ready_with_caveats", "blocked"]
    run_ids: List[str]
    baseline_run_id: Optional[str] = None
    dataset_schema_version: Optional[str] = None
    readiness: Dict[str, Any] = Field(default_factory=dict)
    compatibility: Dict[str, Any] = Field(default_factory=dict)
    blocked_reasons: List[Dict[str, Any]] = Field(default_factory=list)
    reports: List[Dict[str, Any]] = Field(default_factory=list)
    comparisons: List[Dict[str, Any]] = Field(default_factory=list)


class ReportExportRequest(BaseModel):
    include_json: bool = True
    include_csv: bool = True
    include_candles: bool = False


class CandleCatalogResponse(BaseModel):
    schema_version: str = "candle_catalog.v1"
    run_id: str
    items: List[Dict[str, Any]] = Field(default_factory=list)
    caveats: List[str] = Field(default_factory=list)


class CandleDatasetResponse(BaseModel):
    schema_version: str = "report_candles.v1"
    run_id: str
    section: str = "candles"
    limit: int
    offset: int = 0
    total: int
    items: List[Dict[str, Any]] = Field(default_factory=list)
    window: Dict[str, Any] = Field(default_factory=dict)
    caveats: List[str] = Field(default_factory=list)
