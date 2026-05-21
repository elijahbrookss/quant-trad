"""RunReportDTO v2 materialization service.

Report materialization is deliberately separate from run lifecycle truth. A run
may complete successfully even if its report artifact later fails to build.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, Optional, Tuple

from utils.log_context import build_log_context, with_log_context

from . import report_data
from .contract import build_run_report
from .run_research_dataset import DATASET_SCHEMA_VERSION


logger = logging.getLogger(__name__)

REPORT_CONTRACT_VERSION = "run_report_v2"
REPORT_SCHEMA_VERSION = "run_report.v2"
REPORT_MATERIALIZATION_SCHEMA_VERSION = "run_report_materialization_status.v1"
REPORT_TERMINAL_STATUSES = frozenset(
    {
        "completed",
        "degraded_terminal",
        "failed",
        "error",
        "startup_failed",
        "crashed",
        "stopped",
        "cancelled",
        "canceled",
    }
)

_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="report-materializer")
_INFLIGHT: Dict[Tuple[str, str], Future] = {}
_INFLIGHT_LOCK = threading.RLock()


class RunReportMaterializationNotTerminal(RuntimeError):
    """Raised when a terminal report is requested for an active run."""

    def __init__(self, run_id: str, status: str) -> None:
        super().__init__(f"Run {run_id} is not terminal: {status or 'unknown'}")
        self.run_id = run_id
        self.status = status or "unknown"


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def is_terminal_run_status(value: Any) -> bool:
    return _normalize_status(value) in REPORT_TERMINAL_STATUSES


def _cache_key(run_id: str) -> str:
    return f"{run_id}:{REPORT_CONTRACT_VERSION}:{REPORT_SCHEMA_VERSION}:{DATASET_SCHEMA_VERSION}"


def _status_response(run_id: str, status: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "contract_version": REPORT_CONTRACT_VERSION,
        "schema_version": REPORT_MATERIALIZATION_SCHEMA_VERSION,
        "run_id": run_id,
        "report_status": dict(status),
    }


def report_materialization_status(run_id: str, *, require_terminal: bool = False) -> Dict[str, Any]:
    """Return a stable report materialization status payload."""

    run = report_data.get_run(run_id)
    if not run:
        raise KeyError(run_id)
    observed_status = _normalize_status(run.get("status"))
    if require_terminal and not is_terminal_run_status(observed_status):
        raise RunReportMaterializationNotTerminal(run_id, observed_status)
    status = report_data.get_report_materialization_status(run_id)
    return _status_response(run_id, status)


def materialized_run_report(run_id: str) -> Optional[Dict[str, Any]]:
    """Return a persisted RunReportDTO v2 artifact if it is ready."""

    return report_data.get_materialized_run_report(run_id)


def _build_and_store(run_id: str, cache_key: str) -> Dict[str, Any]:
    started = time.perf_counter()
    logger.info(
        with_log_context(
            "report_materialization_started",
            build_log_context(run_id=run_id, contract_version=REPORT_CONTRACT_VERSION),
        )
    )
    try:
        payload = build_run_report(run_id)
        duration_ms = (time.perf_counter() - started) * 1000.0
        status = report_data.store_materialized_run_report(
            run_id,
            payload,
            cache_key=cache_key,
            duration_ms=duration_ms,
        )
        logger.info(
            with_log_context(
                "report_materialization_completed",
                build_log_context(
                    run_id=run_id,
                    contract_version=REPORT_CONTRACT_VERSION,
                    duration_ms=round(duration_ms, 3),
                    status=status.get("status"),
                ),
            )
        )
        return status
    except Exception as exc:  # noqa: BLE001 - materialization failure must not fail run lifecycle.
        duration_ms = (time.perf_counter() - started) * 1000.0
        status = report_data.mark_report_materialization_failed(
            run_id,
            error=str(exc),
            cache_key=cache_key,
            duration_ms=duration_ms,
        )
        logger.exception(
            with_log_context(
                "report_materialization_failed",
                build_log_context(
                    run_id=run_id,
                    contract_version=REPORT_CONTRACT_VERSION,
                    duration_ms=round(duration_ms, 3),
                    status=status.get("status"),
                    error=str(exc),
                ),
            )
        )
        return status


def _submit_build(run_id: str, cache_key: str) -> Future:
    key = (run_id, REPORT_CONTRACT_VERSION)
    with _INFLIGHT_LOCK:
        existing = _INFLIGHT.get(key)
        if existing is not None and not existing.done():
            logger.info(
                with_log_context(
                    "report_materialization_in_flight_joined",
                    build_log_context(run_id=run_id, contract_version=REPORT_CONTRACT_VERSION),
                )
            )
            return existing

        future = _EXECUTOR.submit(_build_and_store, run_id, cache_key)
        _INFLIGHT[key] = future

        def _cleanup(done_future: Future) -> None:
            with _INFLIGHT_LOCK:
                if _INFLIGHT.get(key) is done_future:
                    _INFLIGHT.pop(key, None)

        future.add_done_callback(_cleanup)
        return future


def ensure_report_materialization(
    run_id: str,
    *,
    force: bool = False,
    async_build: bool = True,
    terminal_status: Optional[str] = None,
) -> Dict[str, Any]:
    """Ensure a terminal run has a RunReportDTO v2 materialization in flight."""

    run = report_data.get_run(run_id)
    if not run:
        raise KeyError(run_id)
    observed_status = _normalize_status(terminal_status or run.get("status"))
    if not is_terminal_run_status(observed_status):
        raise RunReportMaterializationNotTerminal(run_id, observed_status)

    existing = report_data.get_report_materialization_status(run_id)
    if existing.get("can_view") and not force:
        logger.info(
            with_log_context(
                "report_materialization_cache_hit",
                build_log_context(run_id=run_id, contract_version=REPORT_CONTRACT_VERSION),
            )
        )
        return _status_response(run_id, existing)

    cache_key = _cache_key(run_id)
    status, claimed, joined = report_data.claim_report_materialization_build(
        run_id,
        cache_key=cache_key,
        force=force,
    )
    if not claimed:
        if joined:
            logger.info(
                with_log_context(
                    "report_materialization_in_flight_joined",
                    build_log_context(run_id=run_id, contract_version=REPORT_CONTRACT_VERSION),
                )
            )
        return _status_response(run_id, status)

    if async_build:
        try:
            _submit_build(run_id, cache_key)
        except Exception as exc:  # noqa: BLE001 - persist enqueue failure as report failure, not run failure.
            failed_status = report_data.mark_report_materialization_failed(
                run_id,
                error=f"report_materialization_submit_failed: {exc}",
                cache_key=cache_key,
                duration_ms=0.0,
            )
            logger.exception(
                with_log_context(
                    "report_materialization_submit_failed",
                    build_log_context(run_id=run_id, contract_version=REPORT_CONTRACT_VERSION, error=str(exc)),
                )
            )
            return _status_response(run_id, failed_status)
        return _status_response(run_id, status)

    final_status = _build_and_store(run_id, cache_key)
    return _status_response(run_id, final_status)


def enqueue_report_materialization_for_terminal_run(
    run_id: str,
    *,
    terminal_status: Optional[str] = None,
    source_reason: str = "terminal_lifecycle",
) -> Dict[str, Any]:
    """Start report materialization after a terminal lifecycle event."""

    try:
        result = ensure_report_materialization(
            run_id,
            terminal_status=terminal_status,
            async_build=True,
        )
        logger.info(
            with_log_context(
                "report_materialization_enqueue_done",
                build_log_context(
                    run_id=run_id,
                    contract_version=REPORT_CONTRACT_VERSION,
                    source_reason=source_reason,
                    status=result.get("report_status", {}).get("status"),
                ),
            )
        )
        return result
    except RunReportMaterializationNotTerminal:
        raise
    except Exception as exc:  # noqa: BLE001 - enqueue diagnostics must not perturb runtime truth.
        logger.warning(
            with_log_context(
                "report_materialization_enqueue_failed",
                build_log_context(
                    run_id=run_id,
                    contract_version=REPORT_CONTRACT_VERSION,
                    source_reason=source_reason,
                    error=str(exc),
                ),
            )
        )
        raise


__all__ = [
    "REPORT_CONTRACT_VERSION",
    "REPORT_MATERIALIZATION_SCHEMA_VERSION",
    "REPORT_TERMINAL_STATUSES",
    "RunReportMaterializationNotTerminal",
    "enqueue_report_materialization_for_terminal_run",
    "ensure_report_materialization",
    "is_terminal_run_status",
    "materialized_run_report",
    "report_materialization_status",
]
