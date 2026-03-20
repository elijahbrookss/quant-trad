from __future__ import annotations

import logging
import os
import signal
import socket
import time
from typing import Any, Dict

from core.settings import get_settings
import indicators  # noqa: F401
import signals  # noqa: F401
from signals.overlays.builtins import ensure_builtin_overlays_registered

from portal.backend.service.async_jobs import (
    claim_next_job,
    complete_job,
    fail_job,
    wait_for_database_ready,
)
from portal.backend.service.indicators.async_dispatch import JOB_TYPE_OVERLAYS, JOB_TYPE_SIGNALS
from portal.backend.service.indicators.indicator_service.api import (
    generate_signals_for_instance,
    overlays_for_instance,
)
from portal.backend.service.indicators.indicator_service.context import IndicatorServiceContext
from portal.backend.service.indicators.indicator_service.runtime_contract import (
    assert_engine_signal_runtime_path,
)


logger = logging.getLogger(__name__)
_STOP = False
_SETTINGS = get_settings()
_INDICATOR_WORKER_SETTINGS = _SETTINGS.workers.indicators
_SUPPORTED_JOB_TYPES = [JOB_TYPE_OVERLAYS, JOB_TYPE_SIGNALS]


def _configure_logging() -> None:
    level = _SETTINGS.logging.level
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _on_signal(signum: int, _frame: Any) -> None:
    global _STOP
    _STOP = True
    logger.info("indicator_worker_shutdown_signal | signum=%s", signum)


def _worker_identity() -> tuple[str, int, int]:
    host = socket.gethostname()
    pid = os.getpid()
    worker_id = f"indicator:{host}:{pid}"
    index = _INDICATOR_WORKER_SETTINGS.index
    total = _INDICATOR_WORKER_SETTINGS.total
    return worker_id, index, total


def _process_signals(payload: Dict[str, Any], *, ctx: IndicatorServiceContext) -> Dict[str, Any]:
    response = generate_signals_for_instance(
        inst_id=str(payload["inst_id"]),
        start=str(payload["start"]),
        end=str(payload["end"]),
        interval=str(payload["interval"]),
        symbol=payload.get("symbol"),
        datasource=payload.get("datasource"),
        exchange=payload.get("exchange"),
        config=payload.get("config") if isinstance(payload.get("config"), dict) else None,
        ctx=ctx,
    )
    assert_engine_signal_runtime_path(
        response,
        context="indicator_worker_signal_runtime_path_mismatch",
        indicator_id=str(payload["inst_id"]),
    )
    return response


def _process_overlays(payload: Dict[str, Any], *, ctx: IndicatorServiceContext) -> Dict[str, Any]:
    return overlays_for_instance(
        inst_id=str(payload["inst_id"]),
        start=str(payload["start"]),
        end=str(payload["end"]),
        interval=str(payload["interval"]),
        symbol=payload.get("symbol"),
        datasource=payload.get("datasource"),
        exchange=payload.get("exchange"),
        instrument_id=payload.get("instrument_id"),
        overlay_options={"visibility_epoch": payload.get("visibility_epoch")}
        if payload.get("visibility_epoch") is not None
        else None,
        ctx=ctx,
    )


def main() -> int:
    _configure_logging()
    ensure_builtin_overlays_registered()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    worker_id, partition_index, partition_total = _worker_identity()
    idle_sleep = _INDICATOR_WORKER_SETTINGS.idle_sleep_seconds
    db_wait_timeout = _INDICATOR_WORKER_SETTINGS.db_wait_timeout_seconds

    if not wait_for_database_ready(timeout_seconds=db_wait_timeout, poll_interval_seconds=0.5):
        logger.error(
            "indicator_worker_db_timeout | worker_id=%s timeout_seconds=%s",
            worker_id,
            db_wait_timeout,
        )
        return 2

    indicator_ctx = IndicatorServiceContext.for_indicator_worker(cache_scope_id=worker_id)
    logger.info(
        "indicator_worker_ready | worker_id=%s partition_index=%s partition_total=%s job_types=%s | cache_owner=%s | cache_scope_id=%s",
        worker_id,
        partition_index,
        partition_total,
        ",".join(_SUPPORTED_JOB_TYPES),
        indicator_ctx.cache_owner,
        indicator_ctx.cache_scope_id,
    )

    while not _STOP:
        try:
            job = claim_next_job(
                worker_id=worker_id,
                job_types=_SUPPORTED_JOB_TYPES,
                partition_index=partition_index,
                partition_total=partition_total,
            )
        except RuntimeError as exc:
            logger.warning("indicator_worker_claim_retry | worker_id=%s error=%s", worker_id, exc)
            time.sleep(max(0.05, idle_sleep))
            continue
        if job is None:
            time.sleep(max(0.05, idle_sleep))
            continue

        started = time.monotonic()
        logger.info(
            "indicator_worker_job_started | worker_id=%s job_id=%s job_type=%s indicator_id=%s instrument_id=%s symbol=%s interval=%s timeframe_seconds=%s start=%s end=%s datasource=%s exchange=%s",
            worker_id,
            job.id,
            job.job_type,
            job.payload.get("inst_id"),
            job.payload.get("instrument_id"),
            job.payload.get("symbol"),
            job.payload.get("interval"),
            job.payload.get("timeframe_seconds"),
            job.payload.get("start"),
            job.payload.get("end"),
            job.payload.get("datasource"),
            job.payload.get("exchange"),
        )
        try:
            if job.job_type == JOB_TYPE_OVERLAYS:
                result = _process_overlays(job.payload, ctx=indicator_ctx)
            elif job.job_type == JOB_TYPE_SIGNALS:
                result = _process_signals(job.payload, ctx=indicator_ctx)
            else:
                raise RuntimeError(f"unknown_job_type: {job.job_type}")
            complete_job(job.id, result=result if isinstance(result, dict) else {"result": result})
            signals_count = (
                len(result.get("signals"))
                if isinstance(result, dict) and isinstance(result.get("signals"), list)
                else None
            )
            overlays_count = (
                len(result.get("overlays"))
                if isinstance(result, dict) and isinstance(result.get("overlays"), list)
                else None
            )
            logger.info(
                "indicator_worker_job_succeeded | worker_id=%s job_id=%s job_type=%s duration_ms=%s overlays=%s signals=%s runtime_path=%s",
                worker_id,
                job.id,
                job.job_type,
                int((time.monotonic() - started) * 1000),
                overlays_count,
                signals_count,
                result.get("runtime_path") if isinstance(result, dict) else None,
            )
        except Exception as exc:
            fail_job(
                job.id,
                error=f"{exc.__class__.__name__}: {exc}",
                retry_delay_seconds=0.5,
            )
            logger.exception(
                "indicator_worker_job_failed | worker_id=%s job_id=%s job_type=%s duration_ms=%s",
                worker_id,
                job.id,
                job.job_type,
                int((time.monotonic() - started) * 1000),
            )

    logger.info("indicator_worker_stopped | worker_id=%s", worker_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
