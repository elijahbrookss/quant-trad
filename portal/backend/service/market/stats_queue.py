from __future__ import annotations

from dataclasses import dataclass, replace
from queue import Empty, Queue
import threading
import time
from typing import Optional
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine

from core.logger import logger
from data_providers.config.runtime import runtime_config_from_env

from .candle_stats_service import CandleStatsService
from .regime_stats_service import RegimeStatsService


STATS_VERSION = "v1"
REGIME_VERSION = "v1"
MAX_RETRIES = 3


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


class StatsPipeline:
    """Background pipeline for candle/regime stats computation."""

    def __init__(self) -> None:
        config = runtime_config_from_env().persistence
        self._engine = create_engine(config.dsn) if config.dsn else None
        self._config = config
        self._stats_queue: Queue[Optional[StatsJob]] = Queue()
        self._regime_queue: Queue[Optional[StatsJob]] = Queue()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._stats_service = CandleStatsService(config=config, engine=self._engine)
        self._regime_service = RegimeStatsService(config=config, engine=self._engine)

    @property
    def engine_available(self) -> bool:
        return self._engine is not None

    def start(self) -> None:
        if not self._engine:
            logger.error("stats_pipeline_engine_unavailable")
            return
        if self._threads:
            return
        self._stop_event.clear()
        self._threads = [
            threading.Thread(target=self._run_stats_worker, name="candle-stats-worker", daemon=True),
            threading.Thread(target=self._run_regime_worker, name="regime-stats-worker", daemon=True),
        ]
        for thread in self._threads:
            thread.start()
        logger.info("stats_pipeline_started")

    def stop(self) -> None:
        if not self._threads:
            return
        self._stop_event.set()
        for _ in self._threads:
            self._stats_queue.put(None)
            self._regime_queue.put(None)
        for thread in self._threads:
            thread.join(timeout=2.0)
        self._threads = []
        logger.info("stats_pipeline_stopped")

    def enqueue(self, job: StatsJob) -> None:
        if not self._engine:
            logger.error(
                "stats_pipeline_enqueue_failed | instrument_id=%s timeframe_seconds=%s stats_version=%s regime_version=%s",
                job.instrument_id,
                job.timeframe_seconds,
                job.stats_version,
                job.regime_version,
            )
            return
        self._stats_queue.put(job)
        logger.info(
            "stats_job_enqueued | job_id=%s instrument_id=%s timeframe_seconds=%s time_min=%s time_max=%s stats_version=%s regime_version=%s",
            job.job_id,
            job.instrument_id,
            job.timeframe_seconds,
            job.time_min.isoformat(),
            job.time_max.isoformat(),
            job.stats_version,
            job.regime_version,
        )

    def _run_stats_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                job = self._stats_queue.get(timeout=0.5)
            except Empty:
                continue
            if job is None:
                self._stats_queue.task_done()
                continue
            started = time.monotonic()
            try:
                result = self._stats_service.compute_range(
                    instrument_id=job.instrument_id,
                    timeframe_seconds=job.timeframe_seconds,
                    time_min=job.time_min,
                    time_max=job.time_max,
                    stats_version=job.stats_version,
                )
                duration_ms = int((time.monotonic() - started) * 1000)
                logger.info(
                    "candle_stats_upserted | job_id=%s instrument_id=%s timeframe_seconds=%s stats_version=%s rows_upserted=%s gaps=%s last_candle_time=%s duration_ms=%s",
                    job.job_id,
                    job.instrument_id,
                    job.timeframe_seconds,
                    job.stats_version,
                    result.rows_upserted,
                    result.gaps,
                    result.last_candle_time,
                    duration_ms,
                )
                self._regime_queue.put(job)
            except Exception as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                logger.exception(
                    "candle_stats_failed | job_id=%s instrument_id=%s timeframe_seconds=%s stats_version=%s duration_ms=%s error=%s",
                    job.job_id,
                    job.instrument_id,
                    job.timeframe_seconds,
                    job.stats_version,
                    duration_ms,
                    exc,
                )
                if job.attempts < MAX_RETRIES:
                    self._stats_queue.put(replace(job, attempts=job.attempts + 1))
            finally:
                self._stats_queue.task_done()

    def _run_regime_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                job = self._regime_queue.get(timeout=0.5)
            except Empty:
                continue
            if job is None:
                self._regime_queue.task_done()
                continue
            started = time.monotonic()
            try:
                result = self._regime_service.compute_range(
                    instrument_id=job.instrument_id,
                    timeframe_seconds=job.timeframe_seconds,
                    time_min=job.time_min,
                    time_max=job.time_max,
                    stats_version=job.stats_version,
                    regime_version=job.regime_version,
                )
                duration_ms = int((time.monotonic() - started) * 1000)
                logger.info(
                    "regime_stats_upserted | job_id=%s instrument_id=%s timeframe_seconds=%s regime_version=%s rows_upserted=%s gaps=%s last_candle_time=%s duration_ms=%s",
                    job.job_id,
                    job.instrument_id,
                    job.timeframe_seconds,
                    job.regime_version,
                    result.rows_upserted,
                    result.gaps,
                    result.last_candle_time,
                    duration_ms,
                )
            except Exception as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                logger.exception(
                    "regime_stats_failed | job_id=%s instrument_id=%s timeframe_seconds=%s regime_version=%s duration_ms=%s error=%s",
                    job.job_id,
                    job.instrument_id,
                    job.timeframe_seconds,
                    job.regime_version,
                    duration_ms,
                    exc,
                )
                if job.attempts < MAX_RETRIES:
                    time.sleep(0.5)
                    self._regime_queue.put(replace(job, attempts=job.attempts + 1))
            finally:
                self._regime_queue.task_done()


_PIPELINE: Optional[StatsPipeline] = None


def get_pipeline() -> StatsPipeline:
    global _PIPELINE
    if _PIPELINE is None:
        _PIPELINE = StatsPipeline()
    return _PIPELINE


def start_pipeline() -> None:
    pipeline = get_pipeline()
    pipeline.start()


def stop_pipeline() -> None:
    pipeline = get_pipeline()
    pipeline.stop()


def enqueue_stats_job(
    *,
    instrument_id: str,
    timeframe_seconds: int,
    time_min: pd.Timestamp,
    time_max: pd.Timestamp,
) -> None:
    pipeline = get_pipeline()
    if not pipeline.engine_available:
        logger.error(
            "stats_pipeline_unavailable | instrument_id=%s timeframe_seconds=%s time_min=%s time_max=%s",
            instrument_id,
            timeframe_seconds,
            pd.to_datetime(time_min, utc=True).isoformat(),
            pd.to_datetime(time_max, utc=True).isoformat(),
        )
        return
    job = StatsJob(
        job_id=str(uuid4()),
        instrument_id=instrument_id,
        timeframe_seconds=timeframe_seconds,
        time_min=pd.to_datetime(time_min, utc=True),
        time_max=pd.to_datetime(time_max, utc=True),
        stats_version=STATS_VERSION,
        regime_version=REGIME_VERSION,
    )
    pipeline.enqueue(job)
