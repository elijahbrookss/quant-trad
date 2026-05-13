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
