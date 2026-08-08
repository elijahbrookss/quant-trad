from __future__ import annotations

import logging
import os
import signal
import socket
import time
from typing import Any, Dict

from core.settings import get_settings
import indicators  # noqa: F401
from overlays.builtins import ensure_builtin_overlays_registered

from portal.backend.service.async_jobs import (
    AsyncJobOwnershipError,
    ClaimedJob,
    claim_next_job,
    complete_job,
    complete_job_with_owned_effect,
    fail_job,
    maintain_job_heartbeat,
    wait_for_database_ready,
)
from portal.backend.service.research import service as research_service
from portal.backend.service.research.async_dispatch import (
    JOB_TYPE_RESEARCH_CHECK_RUN,
    JOB_TYPE_RESEARCH_CHECK_SWEEP,
)


logger = logging.getLogger(__name__)
_STOP = False
_SETTINGS = get_settings()
_RESEARCH_WORKER_SETTINGS = _SETTINGS.workers.research
_SUPPORTED_JOB_TYPES = [JOB_TYPE_RESEARCH_CHECK_RUN, JOB_TYPE_RESEARCH_CHECK_SWEEP]


def _configure_logging() -> None:
    logging.basicConfig(
        level=_SETTINGS.logging.level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _on_signal(signum: int, _frame: Any) -> None:
    global _STOP
    _STOP = True
    logger.info("research_worker_shutdown_signal | signum=%s", signum)


def _worker_identity() -> tuple[str, int, int]:
    host = socket.gethostname()
    pid = os.getpid()
    worker_id = f"research:{host}:{pid}"
    return worker_id, _RESEARCH_WORKER_SETTINGS.index, _RESEARCH_WORKER_SETTINGS.total


def process_research_job(job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    request = payload.get("request")
    if not isinstance(request, dict):
        raise ValueError("research async job payload requires request object")
    if job_type == JOB_TYPE_RESEARCH_CHECK_SWEEP:
        return research_service.sweep_research_checks(request)
    if job_type == JOB_TYPE_RESEARCH_CHECK_RUN:
        raise RuntimeError(
            "research_check_run_requires_claimed_transactional_execution"
        )
    raise RuntimeError(f"unknown_research_job_type: {job_type}")


def execute_claimed_research_job(job: ClaimedJob) -> Dict[str, Any]:
    request = job.payload.get("request")
    if not isinstance(request, dict):
        raise ValueError("research async job payload requires request object")

    with maintain_job_heartbeat(job):
        if job.job_type != JOB_TYPE_RESEARCH_CHECK_RUN:
            result = process_research_job(job.job_type, job.payload)
        else:
            result = None

    if job.job_type == JOB_TYPE_RESEARCH_CHECK_RUN:
        return complete_job_with_owned_effect(
            job,
            lambda session: research_service.run_research_check(
                request, session=session
            ),
        )

    normalized_result = (
        result if isinstance(result, dict) else {"result": result}
    )
    complete_job(job, result=normalized_result)
    return normalized_result


def main() -> int:
    _configure_logging()
    ensure_builtin_overlays_registered()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    worker_id, partition_index, partition_total = _worker_identity()
    idle_sleep = float(_RESEARCH_WORKER_SETTINGS.idle_sleep_seconds)
    idle_sleep_max = max(float(_RESEARCH_WORKER_SETTINGS.idle_sleep_max_seconds), idle_sleep)
    current_idle_sleep = idle_sleep
    db_wait_timeout = _RESEARCH_WORKER_SETTINGS.db_wait_timeout_seconds

    if not wait_for_database_ready(timeout_seconds=db_wait_timeout, poll_interval_seconds=0.5):
        logger.error(
            "research_worker_db_timeout | worker_id=%s timeout_seconds=%s",
            worker_id,
            db_wait_timeout,
        )
        return 2

    logger.info(
        "research_worker_ready | worker_id=%s partition_index=%s partition_total=%s job_types=%s",
        worker_id,
        partition_index,
        partition_total,
        ",".join(_SUPPORTED_JOB_TYPES),
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
            logger.warning("research_worker_claim_retry | worker_id=%s error=%s", worker_id, exc)
            time.sleep(max(0.05, current_idle_sleep))
            current_idle_sleep = min(idle_sleep_max, current_idle_sleep * 2)
            continue
        if job is None:
            time.sleep(max(0.05, current_idle_sleep))
            current_idle_sleep = min(idle_sleep_max, current_idle_sleep * 2)
            continue

        current_idle_sleep = idle_sleep
        started = time.monotonic()
        request = job.payload.get("request") if isinstance(job.payload.get("request"), dict) else {}
        scope = request.get("scope") if isinstance(request.get("scope"), dict) else {}
        logger.info(
            "research_worker_job_started | worker_id=%s job_id=%s job_type=%s check_family=%s indicator_id=%s instrument_id=%s symbol=%s timeframe=%s start=%s end=%s",
            worker_id,
            job.id,
            job.job_type,
            request.get("check_family"),
            scope.get("indicator_id"),
            scope.get("instrument_id"),
            scope.get("symbol"),
            scope.get("timeframe"),
            scope.get("start"),
            scope.get("end"),
        )
        try:
            result = execute_claimed_research_job(job)
            logger.info(
                "research_worker_job_succeeded | worker_id=%s job_id=%s job_type=%s duration_ms=%s",
                worker_id,
                job.id,
                job.job_type,
                int((time.monotonic() - started) * 1000),
            )
        except AsyncJobOwnershipError:
            logger.exception(
                "research_worker_job_ownership_lost | worker_id=%s job_id=%s job_type=%s generation=%s duration_ms=%s",
                worker_id,
                job.id,
                job.job_type,
                job.claim_generation,
                int((time.monotonic() - started) * 1000),
            )
        except Exception as exc:
            try:
                fail_job(
                    job,
                    error=f"{exc.__class__.__name__}: {exc}",
                    retry_delay_seconds=0.5,
                )
            except AsyncJobOwnershipError:
                logger.exception(
                    "research_worker_job_failure_not_committed_ownership_lost | worker_id=%s job_id=%s job_type=%s generation=%s original_error=%s",
                    worker_id,
                    job.id,
                    job.job_type,
                    job.claim_generation,
                    exc,
                )
                continue
            except Exception:
                logger.exception(
                    "research_worker_job_failure_persist_failed | worker_id=%s job_id=%s job_type=%s generation=%s original_error=%s",
                    worker_id,
                    job.id,
                    job.job_type,
                    job.claim_generation,
                    exc,
                )
                continue
            logger.exception(
                "research_worker_job_failed | worker_id=%s job_id=%s job_type=%s duration_ms=%s",
                worker_id,
                job.id,
                job.job_type,
                int((time.monotonic() - started) * 1000),
            )

    logger.info("research_worker_stopped | worker_id=%s", worker_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
