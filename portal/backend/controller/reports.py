"""FastAPI router for report listing and retrieval."""

from __future__ import annotations

import io
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..service.reports import compare_reports as _compare_reports, get_report as _get_report, list_reports as _list_reports
from ..service.reports.export import build_run_export
from utils.log_context import build_log_context, with_log_context


router = APIRouter()
logger = logging.getLogger(__name__)


class ReportCompareRequest(BaseModel):
    run_ids: List[str]


class ReportExportRequest(BaseModel):
    pre_roll_hours: Optional[int] = 48
    post_roll_hours: Optional[int] = 48
    stats_versions: Optional[List[str]] = None
    stats_key_limit: Optional[int] = 20


@router.get("/")
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
        payload = _list_reports(
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


@router.post("/compare")
async def compare_reports(payload: ReportCompareRequest) -> Dict[str, Any]:
    """Return a report comparison payload for multiple runs."""

    context = build_log_context(run_ids=payload.run_ids, runs=len(payload.run_ids))
    logger.info(with_log_context("report_compare_request", context))
    try:
        result = _compare_reports(payload.run_ids)
        logger.info(with_log_context("report_compare_success", context))
        return result
    except ValueError as exc:
        logger.warning(with_log_context("report_compare_invalid", context))
        raise HTTPException(400, str(exc)) from exc
    except KeyError as exc:
        logger.warning(with_log_context("report_compare_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_compare_failed", context), exc_info=exc)
        raise HTTPException(500, "Report comparison failed") from exc


@router.get("/{run_id}")
async def get_report(run_id: str) -> Dict[str, Any]:
    """Return a full report for *run_id*."""

    context = build_log_context(run_id=run_id)
    logger.info(with_log_context("report_get_request", context))
    try:
        payload = _get_report(run_id)
        logger.info(with_log_context("report_get_success", context))
        return payload
    except KeyError as exc:
        logger.warning(with_log_context("report_get_missing", context))
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - convert to API error
        logger.error(with_log_context("report_get_failed", context), exc_info=exc)
        raise HTTPException(500, "Report build failed") from exc


@router.post("/{run_id}/export")
async def export_report(run_id: str, payload: ReportExportRequest) -> StreamingResponse:
    """Export run data as a bounded LLM-ready archive."""

    context = build_log_context(
        run_id=run_id,
        pre_roll_hours=payload.pre_roll_hours,
        post_roll_hours=payload.post_roll_hours,
        stats_versions=payload.stats_versions or [],
        stats_key_limit=payload.stats_key_limit,
    )
    logger.info(with_log_context("report_export_request", context))
    try:
        archive, filename = build_run_export(
            run_id,
            pre_roll_hours=payload.pre_roll_hours if payload.pre_roll_hours is not None else 48,
            post_roll_hours=payload.post_roll_hours if payload.post_roll_hours is not None else 48,
            stats_versions=payload.stats_versions,
            stats_key_limit=payload.stats_key_limit if payload.stats_key_limit is not None else 20,
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
