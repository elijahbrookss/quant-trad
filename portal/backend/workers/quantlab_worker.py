from __future__ import annotations

import logging
import os
import signal
import socket
import time
from typing import Any, Dict

import indicators  # noqa: F401
import signals  # noqa: F401
from engines.bot_runtime.core.indicator_state import ensure_builtin_indicator_plugins_registered
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


logger = logging.getLogger(__name__)
_STOP = False


def _configure_logging() -> None:
    level_name = os.getenv("PORTAL_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _on_signal(signum: int, _frame: Any) -> None:
    global _STOP
    _STOP = True
    logger.info("quantlab_worker_shutdown_signal | signum=%s", signum)


def _worker_identity() -> tuple[str, int, int]:
    host = socket.gethostname()
    pid = os.getpid()
    worker_id = f"quantlab:{host}:{pid}"
    index = int(os.getenv("QUANTLAB_WORKER_INDEX", "0") or 0)
    total = max(1, int(os.getenv("QUANTLAB_WORKER_TOTAL", "1") or 1))
    return worker_id, index, total


def _process_overlay(payload: Dict[str, Any], *, ctx: IndicatorServiceContext) -> Dict[str, Any]:
    return overlays_for_instance(
        inst_id=str(payload["inst_id"]),
        start=str(payload["start"]),
        end=str(payload["end"]),
        interval=str(payload["interval"]),
        symbol=payload.get("symbol"),
        datasource=payload.get("datasource"),
        exchange=payload.get("exchange"),
        instrument_id=payload.get("instrument_id"),
        overlay_options=payload.get("overlay_options") if isinstance(payload.get("overlay_options"), dict) else None,
        ctx=ctx,
    )


def _process_signals(payload: Dict[str, Any], *, ctx: IndicatorServiceContext) -> Dict[str, Any]:
    return generate_signals_for_instance(
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


def main() -> int:
    _configure_logging()
    ensure_builtin_overlays_registered()
    ensure_builtin_indicator_plugins_registered()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    worker_id, partition_index, partition_total = _worker_identity()
    idle_sleep = float(os.getenv("QUANTLAB_WORKER_IDLE_SLEEP_SECONDS", "0.2"))
    db_wait_timeout = float(os.getenv("QUANTLAB_WORKER_DB_WAIT_TIMEOUT_SECONDS", "120"))

    if not wait_for_database_ready(timeout_seconds=db_wait_timeout, poll_interval_seconds=0.5):
        logger.error(
            "quantlab_worker_db_timeout | worker_id=%s timeout_seconds=%s",
            worker_id,
            db_wait_timeout,
        )
        return 2

    indicator_ctx = IndicatorServiceContext.for_quantlab_worker(cache_scope_id=worker_id)
    logger.info(
        "quantlab_worker_ready | worker_id=%s partition_index=%s partition_total=%s | cache_owner=%s | cache_scope_id=%s",
        worker_id,
        partition_index,
        partition_total,
        indicator_ctx.cache_owner,
        indicator_ctx.cache_scope_id,
    )

    while not _STOP:
        try:
            job = claim_next_job(
                worker_id=worker_id,
                job_types=[JOB_TYPE_OVERLAYS, JOB_TYPE_SIGNALS],
                partition_index=partition_index,
                partition_total=partition_total,
            )
        except RuntimeError as exc:
            logger.warning("quantlab_worker_claim_retry | worker_id=%s error=%s", worker_id, exc)
            time.sleep(max(0.05, idle_sleep))
            continue
        if job is None:
            time.sleep(max(0.05, idle_sleep))
            continue

        started = time.monotonic()
        try:
            if job.job_type == JOB_TYPE_OVERLAYS:
                result = _process_overlay(job.payload, ctx=indicator_ctx)
            elif job.job_type == JOB_TYPE_SIGNALS:
                result = _process_signals(job.payload, ctx=indicator_ctx)
            else:
                raise RuntimeError(f"unknown_job_type: {job.job_type}")
            complete_job(job.id, result=result if isinstance(result, dict) else {"result": result})
            logger.info(
                "quantlab_worker_job_succeeded | worker_id=%s job_id=%s job_type=%s duration_ms=%s",
                worker_id,
                job.id,
                job.job_type,
                int((time.monotonic() - started) * 1000),
            )
        except Exception as exc:
            fail_job(
                job.id,
                error=f"{exc.__class__.__name__}: {exc}",
                retry_delay_seconds=0.5,
            )
            logger.exception(
                "quantlab_worker_job_failed | worker_id=%s job_id=%s job_type=%s duration_ms=%s",
                worker_id,
                job.id,
                job.job_type,
                int((time.monotonic() - started) * 1000),
            )

    logger.info("quantlab_worker_stopped | worker_id=%s", worker_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
