from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, Mapping, Optional

from portal.backend.service.async_jobs import enqueue_job, get_job


logger = logging.getLogger(__name__)


JOB_TYPE_OVERLAYS = "quantlab_overlay"
JOB_TYPE_SIGNALS = "quantlab_signals"


DEFAULT_WAIT_TIMEOUT_SECONDS = float(os.getenv("QUANTLAB_JOB_WAIT_TIMEOUT_SECONDS", "180"))
DEFAULT_POLL_INTERVAL_SECONDS = float(os.getenv("QUANTLAB_JOB_POLL_INTERVAL_SECONDS", "0.2"))


class AsyncJobTimeoutError(RuntimeError):
    pass


class AsyncJobFailedError(RuntimeError):
    pass


class AsyncJobNotFoundError(RuntimeError):
    pass



def _series_partition_key(payload: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(payload.get("datasource") or ""),
            str(payload.get("exchange") or ""),
            str(payload.get("symbol") or ""),
            str(payload.get("interval") or ""),
        ]
    )



def enqueue_overlay_job(
    *,
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str],
    datasource: Optional[str],
    exchange: Optional[str],
    instrument_id: Optional[str],
    overlay_options: Optional[Mapping[str, Any]],
) -> str:
    payload: Dict[str, Any] = {
        "inst_id": inst_id,
        "start": start,
        "end": end,
        "interval": interval,
        "symbol": symbol,
        "datasource": datasource,
        "exchange": exchange,
        "instrument_id": instrument_id,
        "overlay_options": dict(overlay_options or {}),
    }
    job_id = enqueue_job(
        job_type=JOB_TYPE_OVERLAYS,
        payload=payload,
        partition_key=_series_partition_key(payload),
        max_attempts=2,
    )
    return job_id



def enqueue_signal_job(
    *,
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str],
    datasource: Optional[str],
    exchange: Optional[str],
    config: Optional[Mapping[str, Any]],
) -> str:
    payload: Dict[str, Any] = {
        "inst_id": inst_id,
        "start": start,
        "end": end,
        "interval": interval,
        "symbol": symbol,
        "datasource": datasource,
        "exchange": exchange,
        "config": dict(config or {}),
    }
    job_id = enqueue_job(
        job_type=JOB_TYPE_SIGNALS,
        payload=payload,
        partition_key=_series_partition_key(payload),
        max_attempts=2,
    )
    return job_id


async def wait_for_job(
    job_id: str,
    *,
    timeout_seconds: float = DEFAULT_WAIT_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> Dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + max(0.1, float(timeout_seconds))
    while asyncio.get_running_loop().time() < deadline:
        job = await asyncio.to_thread(get_job, job_id)
        if job is None:
            raise AsyncJobNotFoundError(f"async_job_not_found: {job_id}")
        status = str(job.get("status") or "")
        if status == "succeeded":
            result = job.get("result")
            if not isinstance(result, dict):
                return {}
            return dict(result)
        if status == "failed":
            raise AsyncJobFailedError(str(job.get("error") or "async_job_failed"))
        await asyncio.sleep(max(0.05, float(poll_interval_seconds)))
    raise AsyncJobTimeoutError(f"async_job_timeout: {job_id}")


__all__ = [
    "AsyncJobFailedError",
    "AsyncJobNotFoundError",
    "AsyncJobTimeoutError",
    "JOB_TYPE_OVERLAYS",
    "JOB_TYPE_SIGNALS",
    "enqueue_overlay_job",
    "enqueue_signal_job",
    "wait_for_job",
]
