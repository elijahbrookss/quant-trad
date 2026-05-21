"""FastAPI router for canonical report listing, retrieval, comparison, and export."""

from __future__ import annotations

import io
import logging
from typing import Any, Callable, Dict, Optional, TypeVar

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from ..service.reports.contract import (
    compare_run_datasets as _compare_run_datasets,
    get_candle_catalog as _get_candle_catalog,
    get_candle_dataset as _get_candle_dataset,
    get_context_dataset as _get_context_dataset,
    get_decision_dataset as _get_decision_dataset,
    get_decision_candle_window as _get_decision_candle_window,
    get_metric_explanation as _get_metric_explanation,
    get_operational_health as _get_operational_health,
    get_report_diagnostics as _get_report_diagnostics,
    get_report_metrics as _get_report_metrics,
    get_report_readiness as _get_report_readiness,
    get_report_sections as _get_report_sections,
    get_run_report_summary as _get_run_report_summary,
    get_run_research_summary as _get_run_research_summary,
    get_run_research_dataset as _get_run_research_dataset,
    get_signal_dataset as _get_signal_dataset,
    get_signal_candle_window as _get_signal_candle_window,
    get_timeseries_dataset as _get_timeseries_dataset,
    get_trade_candle_window as _get_trade_candle_window,
    get_trade_dataset as _get_trade_dataset,
    list_report_summaries as _list_report_summaries,
)
from ..service.reports.comparison import (
    compare_materialized_run_reports as _compare_materialized_run_reports,
    summarize_run_report_comparison as _summarize_run_report_comparison,
)
from ..service.reports.export_bundle import build_export_archive, build_export_manifest
from ..service.reports.materialization import (
    RunReportMaterializationNotTerminal,
    ensure_report_materialization as _ensure_report_materialization,
    materialized_run_report as _materialized_run_report,
    report_materialization_status as _report_materialization_status,
)
from ..service.reports.schemas import (
    CandleCatalogResponse,
    CandleDatasetResponse,
    DatasetPageResponse,
    ExportManifestResponse,
    MetricExplanationResponse,
    ReportCompareRequest,
    ReportDiagnosticsResponse,
    ReportExportRequest,
    ReportListResponse,
    RunComparisonDTO,
    RunReportMaterializationResponse,
    ReportReadinessResponse,
    ReportSectionsResponse,
    RunComparisonResultResponse,
    RunReportDTO,
    RunReportSummaryResponse,
    RunResearchDatasetResponse,
)
from utils.log_context import build_log_context, with_log_context


router = APIRouter()
logger = logging.getLogger(__name__)
T = TypeVar("T")


async def _run_report_task(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run synchronous report work outside the API event loop."""

    return await run_in_threadpool(func, *args, **kwargs)


@router.get("/", response_model=ReportListResponse)
async def list_reports(
    type: str = Query("backtest", alias="type"),
    status: str = Query("completed"),
    limit: int = Query(50, ge=0, le=500),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, alias="search"),
    bot_id: Optional[str] = Query(None, alias="botId"),
    instrument: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    started_after: Optional[str] = Query(None, alias="start"),
    started_before: Optional[str] = Query(None, alias="end"),
) -> Dict[str, Any]:
    """Return report list entries for completed runs."""

    context = build_log_context(
        run_type=type,
        status=status,
        limit=limit,
        offset=offset,
        bot_id=bot_id,
        instrument=instrument,
        timeframe=timeframe,
        search=search,
        start=started_after,
        end=started_before,
    )
    logger.info(with_log_context("report_list_request", context))
    try:
        payload = await _run_report_task(
            _list_report_summaries,
            run_type=type,
            status=status,
            limit=limit,
            offset=offset,
            search=search,
            bot_id=bot_id,
            instrument=instrument,
            timeframe=timeframe,
            started_after=started_after,
            started_before=started_before,
        )
        logger.info(with_log_context("report_list_success", context))
        return payload
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_list_failed", context), exc_info=exc)
        raise HTTPException(500, "Report list query failed") from exc


@router.post("/compare", response_model=RunComparisonResultResponse)
async def compare_reports(payload: ReportCompareRequest) -> RunComparisonResultResponse:
    """Return a gated comparison result for multiple runs."""

    context = build_log_context(run_ids=payload.run_ids, runs=len(payload.run_ids))
    logger.info(with_log_context("report_compare_request", context))
    try:
        result = await _run_report_task(_compare_run_datasets, payload.run_ids)
        logger.info(with_log_context("report_compare_success", context))
        return RunComparisonResultResponse.model_validate(result)
    except ValueError as exc:
        logger.warning(with_log_context("report_compare_invalid", context))
        raise HTTPException(400, str(exc)) from exc
    except KeyError as exc:
        logger.warning(with_log_context("report_compare_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_compare_failed", context), exc_info=exc)
        raise HTTPException(500, "Report comparison failed") from exc


@router.get("/compare", response_model=RunComparisonDTO)
async def compare_materialized_reports(
    left_run_id: str = Query(..., alias="left_run_id"),
    right_run_id: str = Query(..., alias="right_run_id"),
    include_golden: bool = Query(True, description="Read existing golden comparison evidence when available."),
    require_golden: bool = Query(False, description="Block comparison when existing golden evidence is unavailable."),
) -> RunComparisonDTO:
    """Compare two ready materialized RunReportDTO v2 artifacts without building reports."""

    context = build_log_context(left_run_id=left_run_id, right_run_id=right_run_id, include_golden=include_golden, require_golden=require_golden)
    logger.info(with_log_context("run_report_compare_request", context))
    try:
        result = await _run_report_task(
            _compare_materialized_run_reports,
            left_run_id,
            right_run_id,
            include_golden=include_golden,
            require_golden=require_golden,
        )
        logger.info(
            with_log_context(
                "run_report_compare_success",
                context
                | {
                    "comparison_status": result.comparison_status,
                    "comparison_verdict": result.comparison_verdict,
                    "blocked_reason": result.blocked_reason,
                },
            )
        )
        return result
    except KeyError as exc:
        logger.warning(with_log_context("run_report_compare_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("run_report_compare_failed", context), exc_info=exc)
        raise HTTPException(500, "Run report comparison failed") from exc


@router.get("/compare/summary")
async def compare_materialized_reports_summary(
    left_run_id: str = Query(..., alias="left_run_id"),
    right_run_id: str = Query(..., alias="right_run_id"),
    include_golden: bool = Query(True, description="Read existing golden comparison evidence when available."),
    require_golden: bool = Query(False, description="Block comparison when existing golden evidence is unavailable."),
) -> Dict[str, Any]:
    """Return a compact materialized report comparison for CLI/research workflows."""

    context = build_log_context(left_run_id=left_run_id, right_run_id=right_run_id, include_golden=include_golden, require_golden=require_golden)
    logger.info(with_log_context("run_report_compare_summary_request", context))
    try:
        result = await _run_report_task(
            _compare_materialized_run_reports,
            left_run_id,
            right_run_id,
            include_golden=include_golden,
            require_golden=require_golden,
        )
        payload = _summarize_run_report_comparison(result)
        logger.info(
            with_log_context(
                "run_report_compare_summary_success",
                context
                | {
                    "comparison_status": payload.get("comparison_status"),
                    "comparison_verdict": payload.get("comparison_verdict"),
                    "blocked_reason": payload.get("blocked_reason"),
                },
            )
        )
        return payload
    except KeyError as exc:
        logger.warning(with_log_context("run_report_compare_summary_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("run_report_compare_summary_failed", context), exc_info=exc)
        raise HTTPException(500, "Run report comparison summary failed") from exc


@router.get("/{run_id}/readiness", response_model=ReportReadinessResponse)
async def get_report_readiness(run_id: str) -> ReportReadinessResponse:
    """Return report readiness and readiness-impacting diagnostics."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_readiness_request", context))
    try:
        payload = await _run_report_task(_get_report_readiness, run_id)
        logger.info(with_log_context("report_readiness_success", context))
        return ReportReadinessResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_readiness_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_readiness_failed", context), exc_info=exc)
        raise HTTPException(500, "Report readiness build failed") from exc


@router.get("/{run_id}/summary", response_model=RunReportSummaryResponse)
async def get_run_report_summary(run_id: str) -> RunReportSummaryResponse:
    """Return a compact summary for a run report."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_summary_request", context))
    try:
        payload = await _run_report_task(_get_run_report_summary, run_id)
        logger.info(with_log_context("report_summary_success", context))
        return RunReportSummaryResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_summary_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_summary_failed", context), exc_info=exc)
        raise HTTPException(500, "Report summary build failed") from exc


@router.get("/{run_id}/research-summary")
async def get_run_research_summary(run_id: str) -> Dict[str, Any]:
    """Return a compact report summary for CLI/research workflows."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_research_summary_request", context))
    try:
        payload = await _run_report_task(_get_run_research_summary, run_id)
        logger.info(with_log_context("report_research_summary_success", context))
        return payload
    except KeyError as exc:
        logger.warning(with_log_context("report_research_summary_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_research_summary_failed", context), exc_info=exc)
        raise HTTPException(500, "Report research summary build failed") from exc


@router.get("/{run_id}/run-report/status", response_model=RunReportMaterializationResponse)
async def get_run_report_materialization_status(run_id: str) -> RunReportMaterializationResponse:
    """Return materialized Run Report DTO v2 artifact status."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("run_report_materialization_status_request", context))
    try:
        payload = await _run_report_task(_report_materialization_status, run_id)
        return RunReportMaterializationResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("run_report_materialization_status_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("run_report_materialization_status_failed", context), exc_info=exc)
        raise HTTPException(500, "Run report status query failed") from exc


@router.post("/{run_id}/run-report/build", response_model=RunReportMaterializationResponse)
async def build_run_report_materialization(
    run_id: str,
    async_build: bool = Query(False, description="Build in the background instead of returning the final status."),
    force_rebuild: bool = Query(False, description="Force a new materialized report build."),
) -> RunReportMaterializationResponse:
    """Build or enqueue a materialized RunReportDTO v2 artifact without returning the artifact."""

    context = build_log_context(run_id=run_id, async_build=async_build, force_rebuild=force_rebuild)
    logger.info(with_log_context("run_report_materialization_build_request", context))
    try:
        payload = await _run_report_task(
            _ensure_report_materialization,
            run_id,
            force=force_rebuild,
            async_build=async_build,
        )
        logger.info(
            with_log_context(
                "run_report_materialization_build_success",
                context | {"status": dict(payload.get("report_status") or {}).get("status")},
            )
        )
        return RunReportMaterializationResponse.model_validate(payload)
    except RunReportMaterializationNotTerminal as exc:
        logger.warning(with_log_context("run_report_materialization_build_not_terminal", context | {"run_status": exc.status}))
        raise HTTPException(409, f"Run {run_id} is not terminal: {exc.status}") from exc
    except KeyError as exc:
        logger.warning(with_log_context("run_report_materialization_build_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("run_report_materialization_build_failed", context), exc_info=exc)
        raise HTTPException(500, "Run report build failed") from exc


@router.get(
    "/{run_id}/run-report",
    responses={
        200: {"model": RunReportDTO},
        202: {"model": RunReportMaterializationResponse},
    },
)
async def get_run_report(
    run_id: str,
    build: bool = Query(True, description="Enqueue materialization for terminal runs when no ready artifact exists."),
    force_rebuild: bool = Query(False, description="Force a new materialized report build."),
) -> Any:
    """Return a materialized Run Report DTO v2 contract for a terminal run."""

    context = build_log_context(run_id=run_id, build=build, force_rebuild=force_rebuild)
    logger.info(with_log_context("run_report_v2_request", context))
    try:
        if not force_rebuild:
            materialized = await _run_report_task(_materialized_run_report, run_id)
            if materialized is not None:
                logger.info(with_log_context("run_report_v2_materialized_success", context))
                return RunReportDTO.model_validate(materialized)

        status_payload = await _run_report_task(
            _report_materialization_status,
            run_id,
            require_terminal=True,
        )
        if not build and not force_rebuild:
            return JSONResponse(status_code=202, content=status_payload)

        status_payload = await _run_report_task(
            _ensure_report_materialization,
            run_id,
            force=force_rebuild,
            async_build=True,
        )
        status = dict(status_payload.get("report_status") or {})
        if status.get("can_view") and not force_rebuild:
            materialized = await _run_report_task(_materialized_run_report, run_id)
            if materialized is not None:
                logger.info(with_log_context("run_report_v2_materialized_success", context))
                return RunReportDTO.model_validate(materialized)
        if status.get("status") == "failed":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "report_materialization_failed",
                    "message": "Run report materialization failed.",
                    "report_status": status,
                },
            )
        return JSONResponse(status_code=202, content=status_payload)
    except KeyError as exc:
        logger.warning(with_log_context("run_report_v2_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except RunReportMaterializationNotTerminal as exc:
        logger.info(with_log_context("run_report_v2_not_terminal", context | {"run_status": exc.status}))
        raise HTTPException(
            status_code=409,
            detail={
                "code": "run_not_terminal",
                "message": "Run report is only available after the run reaches a terminal status.",
                "run_id": run_id,
                "run_status": exc.status,
            },
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("run_report_v2_failed", context), exc_info=exc)
        raise HTTPException(500, "Run report materialization failed") from exc


@router.get("/{run_id}/sections", response_model=ReportSectionsResponse)
async def get_report_sections(run_id: str) -> ReportSectionsResponse:
    """Return available report sections and section-level states."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_sections_request", context))
    try:
        payload = await _run_report_task(_get_report_sections, run_id)
        logger.info(with_log_context("report_sections_success", context))
        return ReportSectionsResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_sections_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_sections_failed", context), exc_info=exc)
        raise HTTPException(500, "Report sections build failed") from exc


@router.get("/{run_id}/trades", response_model=DatasetPageResponse)
async def get_trade_dataset(
    run_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = Query(None),
    instrument_id: Optional[str] = Query(None, alias="instrumentId"),
) -> DatasetPageResponse:
    """Return a paged trade dataset for a run."""

    context = build_log_context(run_id=run_id, limit=limit, offset=offset, symbol=symbol, instrument_id=instrument_id)
    logger.info(with_log_context("report_trades_request", context))
    try:
        payload = await _run_report_task(
            _get_trade_dataset,
            run_id,
            limit=limit,
            offset=offset,
            symbol=symbol,
            instrument_id=instrument_id,
        )
        logger.info(with_log_context("report_trades_success", context | {"total": payload.get("total")}))
        return DatasetPageResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_trades_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_trades_failed", context), exc_info=exc)
        raise HTTPException(500, "Trade dataset build failed") from exc


@router.get("/{run_id}/decisions", response_model=DatasetPageResponse)
async def get_decision_dataset(
    run_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    state: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    instrument_id: Optional[str] = Query(None, alias="instrumentId"),
) -> DatasetPageResponse:
    """Return a paged decision dataset for a run."""

    context = build_log_context(
        run_id=run_id,
        limit=limit,
        offset=offset,
        state=state,
        symbol=symbol,
        instrument_id=instrument_id,
    )
    logger.info(with_log_context("report_decisions_request", context))
    try:
        payload = await _run_report_task(
            _get_decision_dataset,
            run_id,
            limit=limit,
            offset=offset,
            state=state,
            symbol=symbol,
            instrument_id=instrument_id,
        )
        logger.info(with_log_context("report_decisions_success", context | {"total": payload.get("total")}))
        return DatasetPageResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_decisions_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_decisions_failed", context), exc_info=exc)
        raise HTTPException(500, "Decision dataset build failed") from exc


@router.get("/{run_id}/signals", response_model=DatasetPageResponse)
async def get_signal_dataset(
    run_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = Query(None),
    instrument_id: Optional[str] = Query(None, alias="instrumentId"),
) -> DatasetPageResponse:
    """Return a paged signal dataset for a run."""

    context = build_log_context(run_id=run_id, limit=limit, offset=offset, symbol=symbol, instrument_id=instrument_id)
    logger.info(with_log_context("report_signals_request", context))
    try:
        payload = await _run_report_task(
            _get_signal_dataset,
            run_id,
            limit=limit,
            offset=offset,
            symbol=symbol,
            instrument_id=instrument_id,
        )
        logger.info(with_log_context("report_signals_success", context | {"total": payload.get("total")}))
        return DatasetPageResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_signals_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_signals_failed", context), exc_info=exc)
        raise HTTPException(500, "Signal dataset build failed") from exc


@router.get("/{run_id}/timeseries/{section}", response_model=DatasetPageResponse)
async def get_timeseries_dataset(
    run_id: str,
    section: str,
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> DatasetPageResponse:
    """Return a canonical timeseries section for a run."""

    context = build_log_context(run_id=run_id, section=section, limit=limit, offset=offset)
    logger.info(with_log_context("report_timeseries_request", context))
    try:
        payload = await _run_report_task(_get_timeseries_dataset, run_id, section, limit=limit, offset=offset)
        logger.info(with_log_context("report_timeseries_success", context | {"total": payload.get("total")}))
        return DatasetPageResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_timeseries_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_timeseries_failed", context), exc_info=exc)
        raise HTTPException(500, "Timeseries dataset build failed") from exc


@router.get("/{run_id}/context", response_model=DatasetPageResponse)
async def get_context_dataset(
    run_id: str,
    section: Optional[str] = Query("decision_context"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> DatasetPageResponse:
    """Return a canonical context/world-state section for a run."""

    context = build_log_context(run_id=run_id, section=section, limit=limit, offset=offset)
    logger.info(with_log_context("report_context_request", context))
    try:
        payload = await _run_report_task(_get_context_dataset, run_id, section=section, limit=limit, offset=offset)
        logger.info(with_log_context("report_context_success", context | {"total": payload.get("total")}))
        return DatasetPageResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_context_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_context_failed", context), exc_info=exc)
        raise HTTPException(500, "Context dataset build failed") from exc


@router.get("/{run_id}/candles/catalog", response_model=CandleCatalogResponse)
async def get_candle_catalog(run_id: str) -> CandleCatalogResponse:
    """Return the run-scoped candle catalog without full candle rows."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_candle_catalog_request", context))
    try:
        payload = await _run_report_task(_get_candle_catalog, run_id)
        logger.info(with_log_context("report_candle_catalog_success", context | {"rows": len(payload.get("items") or [])}))
        return CandleCatalogResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_candle_catalog_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_candle_catalog_failed", context), exc_info=exc)
        raise HTTPException(500, "Candle catalog build failed") from exc


@router.get("/{run_id}/candles", response_model=CandleDatasetResponse)
async def get_candle_dataset(
    run_id: str,
    instrument_id: str = Query(..., alias="instrument_id"),
    timeframe: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> CandleDatasetResponse:
    """Return a bounded reporting candle dataset for a run."""

    context = build_log_context(run_id=run_id, instrument_id=instrument_id, timeframe=timeframe, start=start, end=end)
    logger.info(with_log_context("report_candles_request", context))
    try:
        payload = await _run_report_task(
            _get_candle_dataset,
            run_id,
            instrument_id=instrument_id,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=limit,
            offset=offset,
        )
        logger.info(with_log_context("report_candles_success", context | {"total": payload.get("total")}))
        return CandleDatasetResponse.model_validate(payload)
    except ValueError as exc:
        logger.warning(with_log_context("report_candles_invalid", context))
        raise HTTPException(400, str(exc)) from exc
    except KeyError as exc:
        logger.warning(with_log_context("report_candles_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_candles_failed", context), exc_info=exc)
        raise HTTPException(500, "Candle dataset build failed") from exc


@router.get("/{run_id}/trades/{trade_id}/candle-window", response_model=CandleDatasetResponse)
async def get_trade_candle_window(
    run_id: str,
    trade_id: str,
    anchor: str = Query("entry"),
    before: int = Query(20, ge=0, le=500),
    after: int = Query(20, ge=0, le=500),
) -> CandleDatasetResponse:
    """Return candles around a trade entry or exit."""

    context = build_log_context(run_id=run_id, trade_id=trade_id, anchor=anchor)
    logger.info(with_log_context("report_trade_candle_window_request", context))
    try:
        payload = await _run_report_task(_get_trade_candle_window, run_id, trade_id, anchor=anchor, before=before, after=after)
        return CandleDatasetResponse.model_validate(payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{run_id}/decisions/{decision_id}/candle-window", response_model=CandleDatasetResponse)
async def get_decision_candle_window(
    run_id: str,
    decision_id: str,
    before: int = Query(20, ge=0, le=500),
    after: int = Query(20, ge=0, le=500),
) -> CandleDatasetResponse:
    """Return candles around a decision bar."""

    context = build_log_context(run_id=run_id, decision_id=decision_id)
    logger.info(with_log_context("report_decision_candle_window_request", context))
    try:
        payload = await _run_report_task(_get_decision_candle_window, run_id, decision_id, before=before, after=after)
        return CandleDatasetResponse.model_validate(payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{run_id}/signals/{signal_id}/candle-window", response_model=CandleDatasetResponse)
async def get_signal_candle_window(
    run_id: str,
    signal_id: str,
    before: int = Query(20, ge=0, le=500),
    after: int = Query(20, ge=0, le=500),
) -> CandleDatasetResponse:
    """Return candles around a signal bar."""

    context = build_log_context(run_id=run_id, signal_id=signal_id)
    logger.info(with_log_context("report_signal_candle_window_request", context))
    try:
        payload = await _run_report_task(_get_signal_candle_window, run_id, signal_id, before=before, after=after)
        return CandleDatasetResponse.model_validate(payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{run_id}/diagnostics", response_model=ReportDiagnosticsResponse)
async def get_report_diagnostics(run_id: str) -> ReportDiagnosticsResponse:
    """Return normalized report diagnostics for a run."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_diagnostics_request", context))
    try:
        payload = await _run_report_task(_get_report_diagnostics, run_id)
        logger.info(with_log_context("report_diagnostics_success", context))
        return ReportDiagnosticsResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_diagnostics_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_diagnostics_failed", context), exc_info=exc)
        raise HTTPException(500, "Report diagnostics build failed") from exc


@router.get("/{run_id}/metrics")
async def get_report_metrics(run_id: str) -> Dict[str, Any]:
    """Return canonical metrics and accounting sections for a run."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_metrics_request", context))
    try:
        payload = await _run_report_task(_get_report_metrics, run_id)
        logger.info(with_log_context("report_metrics_success", context))
        return payload
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_metrics_failed", context), exc_info=exc)
        raise HTTPException(500, "Report metrics build failed") from exc


@router.get("/{run_id}/operational-health")
async def get_operational_health(run_id: str) -> Dict[str, Any]:
    """Return operational and scale-health reporting data for a run."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_operational_health_request", context))
    try:
        payload = await _run_report_task(_get_operational_health, run_id)
        logger.info(with_log_context("report_operational_health_success", context))
        return payload
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_operational_health_failed", context), exc_info=exc)
        raise HTTPException(500, "Operational health build failed") from exc


@router.get("/{run_id}/metrics/{metric_name}/explanation", response_model=MetricExplanationResponse)
async def explain_metric(run_id: str, metric_name: str) -> MetricExplanationResponse:
    """Return formula and source references for a report metric."""

    context = build_log_context(run_id=run_id, metric_name=metric_name)
    logger.info(with_log_context("report_metric_explanation_request", context))
    try:
        payload = await _run_report_task(_get_metric_explanation, run_id, metric_name)
        logger.info(with_log_context("report_metric_explanation_success", context))
        return MetricExplanationResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_metric_explanation_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_metric_explanation_failed", context), exc_info=exc)
        raise HTTPException(500, "Metric explanation build failed") from exc


@router.get("/{run_id}/export/manifest", response_model=ExportManifestResponse)
async def get_export_manifest(
    run_id: str,
    include_candles: bool = Query(False, alias="include_candles"),
) -> ExportManifestResponse:
    """Return the export manifest that would be included in the report bundle."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_export_manifest_request", context))
    try:
        payload = await _run_report_task(build_export_manifest, run_id, include_candles=include_candles)
        logger.info(with_log_context("report_export_manifest_success", context))
        return ExportManifestResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("report_export_manifest_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_export_manifest_failed", context), exc_info=exc)
        raise HTTPException(500, "Report export manifest build failed") from exc


@router.get("/{run_id}", response_model=RunResearchDatasetResponse)
async def get_run_research_dataset(run_id: str) -> RunResearchDatasetResponse:
    """Return the canonical RunResearchDataset v1 payload for *run_id*."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("run_research_dataset_get_request", context))
    try:
        payload = await _run_report_task(_get_run_research_dataset, run_id)
        logger.info(with_log_context("run_research_dataset_get_success", context))
        return RunResearchDatasetResponse.model_validate(payload)
    except KeyError as exc:
        logger.warning(with_log_context("run_research_dataset_get_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("run_research_dataset_get_failed", context), exc_info=exc)
        raise HTTPException(500, "Run research dataset build failed") from exc


@router.post("/{run_id}/export")
async def export_report(run_id: str, payload: ReportExportRequest) -> StreamingResponse:
    """Export report data as a contract-backed zip archive."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_export_request", context))
    try:
        archive, filename = await _run_report_task(
            build_export_archive,
            run_id,
            include_json=payload.include_json,
            include_csv=payload.include_csv,
            include_candles=payload.include_candles,
        )
        logger.info(with_log_context("report_export_success", context))
        return StreamingResponse(
            io.BytesIO(archive),
            media_type="application/zip",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    except KeyError as exc:
        logger.warning(with_log_context("report_export_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        logger.warning(with_log_context("report_export_invalid", context))
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_export_failed", context), exc_info=exc)
        raise HTTPException(500, "Report export failed") from exc
