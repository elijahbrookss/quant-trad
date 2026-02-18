from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import signal
import socket
import time
from typing import Any, Dict, Optional
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine

from core.logger import logger
from data_providers.config.runtime import runtime_config_from_env

from portal.backend.service.async_jobs import (
    claim_next_job,
    complete_job,
    enqueue_job,
    fail_job,
    wait_for_database_ready,
)

from .candle_stats_service import CandleStatsService
from .regime_stats_service import RegimeStatsService


STATS_VERSION = "v1"
REGIME_VERSION = "v1"
MAX_RETRIES = 3
JOB_TYPE_STATS_COMPUTE = "stats_compute"


@dataclass(frozen=True)
class StatsJob:
    job_id: str
    instrument_id: str
    timeframe_seconds: int
    time_min: pd.Timestamp
    time_max: pd.Timestamp
    stats_version: str
    regime_version: str
    attempts: int = 0


_STOP = False


def _on_signal(signum: int, _frame: Any) -> None:
    global _STOP
    _STOP = True
    logger.info("stats_worker_shutdown_signal | signum=%s", signum)



def _partition_key(instrument_id: str, timeframe_seconds: int) -> str:
    return f"{instrument_id}|{int(timeframe_seconds)}"



def enqueue_stats_job(
    *,
    instrument_id: str,
    timeframe_seconds: int,
    time_min: pd.Timestamp,
    time_max: pd.Timestamp,
) -> None:
    payload = {
        "instrument_id": instrument_id,
        "timeframe_seconds": int(timeframe_seconds),
        "time_min": pd.to_datetime(time_min, utc=True).isoformat(),
        "time_max": pd.to_datetime(time_max, utc=True).isoformat(),
        "stats_version": STATS_VERSION,
        "regime_version": REGIME_VERSION,
    }
    enqueue_job(
        job_type=JOB_TYPE_STATS_COMPUTE,
        payload=payload,
        partition_key=_partition_key(instrument_id, timeframe_seconds),
        max_attempts=MAX_RETRIES,
    )
    logger.info(
        "stats_job_enqueued | instrument_id=%s timeframe_seconds=%s time_min=%s time_max=%s stats_version=%s regime_version=%s",
        instrument_id,
        timeframe_seconds,
        payload["time_min"],
        payload["time_max"],
        STATS_VERSION,
        REGIME_VERSION,
    )


class StatsWorker:
    """Dedicated worker process for candle/regime stats jobs."""

    def __init__(self) -> None:
        config = runtime_config_from_env().persistence
        self._engine = create_engine(config.dsn) if config.dsn else None
        self._config = config
        self._stats_service = CandleStatsService(config=config, engine=self._engine)
        self._regime_service = RegimeStatsService(config=config, engine=self._engine)
        self._worker_id, self._partition_index, self._partition_total = self._worker_identity()
        self._idle_sleep = float(os.getenv("STATS_WORKER_IDLE_SLEEP_SECONDS", "0.25"))

    @staticmethod
    def _worker_identity() -> tuple[str, int, int]:
        host = socket.gethostname()
        pid = os.getpid()
        worker_id = f"stats:{host}:{pid}"
        index = int(os.getenv("STATS_WORKER_INDEX", "0") or 0)
        total = max(1, int(os.getenv("STATS_WORKER_TOTAL", "1") or 1))
        return worker_id, index, total

    def run_forever(self) -> None:
        global _STOP
        if self._engine is None:
            raise RuntimeError("stats_worker_engine_unavailable")

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)

        db_wait_timeout = float(os.getenv("STATS_WORKER_DB_WAIT_TIMEOUT_SECONDS", "120"))
        if not wait_for_database_ready(timeout_seconds=db_wait_timeout, poll_interval_seconds=0.5):
            logger.error(
                "stats_worker_db_timeout | worker_id=%s timeout_seconds=%s",
                self._worker_id,
                db_wait_timeout,
            )
            return

        logger.info(
            "stats_worker_ready | worker_id=%s partition_index=%s partition_total=%s",
            self._worker_id,
            self._partition_index,
            self._partition_total,
        )

        while not _STOP:
            try:
                job = claim_next_job(
                    worker_id=self._worker_id,
                    job_types=[JOB_TYPE_STATS_COMPUTE],
                    partition_index=self._partition_index,
                    partition_total=self._partition_total,
                )
            except RuntimeError as exc:
                logger.warning(
                    "stats_worker_claim_retry | worker_id=%s error=%s",
                    self._worker_id,
                    exc,
                )
                time.sleep(max(0.05, self._idle_sleep))
                continue
            if job is None:
                time.sleep(max(0.05, self._idle_sleep))
                continue

            started = time.monotonic()
            try:
                result = self._process_job(job.payload)
                complete_job(job.id, result=result)
                logger.info(
                    "stats_worker_job_succeeded | worker_id=%s job_id=%s duration_ms=%s",
                    self._worker_id,
                    job.id,
                    int((time.monotonic() - started) * 1000),
                )
            except Exception as exc:
                fail_job(
                    job.id,
                    error=f"{exc.__class__.__name__}: {exc}",
                    retry_delay_seconds=0.5,
                )
                logger.exception(
                    "stats_worker_job_failed | worker_id=%s job_id=%s duration_ms=%s",
                    self._worker_id,
                    job.id,
                    int((time.monotonic() - started) * 1000),
                )

        logger.info("stats_worker_stopped | worker_id=%s", self._worker_id)

    def _process_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        instrument_id = str(payload["instrument_id"])
        timeframe_seconds = int(payload["timeframe_seconds"])
        time_min = pd.to_datetime(payload["time_min"], utc=True)
        time_max = pd.to_datetime(payload["time_max"], utc=True)
        stats_version = str(payload.get("stats_version") or STATS_VERSION)
        regime_version = str(payload.get("regime_version") or REGIME_VERSION)

        stats_result = self._stats_service.compute_range(
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
            time_min=time_min,
            time_max=time_max,
            stats_version=stats_version,
        )
        regime_result = self._regime_service.compute_range(
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
            time_min=time_min,
            time_max=time_max,
            stats_version=stats_version,
            regime_version=regime_version,
        )
        return {
            "instrument_id": instrument_id,
            "timeframe_seconds": timeframe_seconds,
            "stats_version": stats_version,
            "regime_version": regime_version,
            "stats": {
                "rows_upserted": stats_result.rows_upserted,
                "gaps": stats_result.gaps,
                "last_candle_time": stats_result.last_candle_time,
            },
            "regime": {
                "rows_upserted": regime_result.rows_upserted,
                "gaps": regime_result.gaps,
                "last_candle_time": regime_result.last_candle_time,
            },
        }


def start_pipeline() -> None:
    logger.info("stats_pipeline_start_noop | mode=queue_backed")


def stop_pipeline() -> None:
    logger.info("stats_pipeline_stop_noop | mode=queue_backed")


__all__ = [
    "JOB_TYPE_STATS_COMPUTE",
    "REGIME_VERSION",
    "STATS_VERSION",
    "StatsJob",
    "StatsWorker",
    "enqueue_stats_job",
    "start_pipeline",
    "stop_pipeline",
]
