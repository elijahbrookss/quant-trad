from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Any, Dict, Mapping, Optional

from core.settings import get_settings
from portal.backend.service.async_jobs import enqueue_job, get_job


logger = logging.getLogger(__name__)


JOB_TYPE_SIGNALS = "quantlab_signals"


_SETTINGS = get_settings().async_jobs
DEFAULT_WAIT_TIMEOUT_SECONDS = float(_SETTINGS.quantlab_job_wait_timeout_seconds)
DEFAULT_POLL_INTERVAL_SECONDS = float(_SETTINGS.quantlab_job_poll_interval_seconds)


class AsyncJobTimeoutError(RuntimeError):
    pass


class AsyncJobFailedError(RuntimeError):
    pass


class AsyncJobNotFoundError(RuntimeError):
    pass



def _series_partition_key(payload: Mapping[str, Any]) -> str:
    partition_key = "|".join(
        [
            str(payload.get("datasource") or ""),
            str(payload.get("exchange") or ""),
            str(payload.get("symbol") or ""),
            str(payload.get("interval") or ""),
            str(payload.get("inst_id") or ""),
        ]
    )
    key_len = len(partition_key)
    partition_key_hashed = False
    if key_len > 255:
        digest = hashlib.sha256(partition_key.encode("utf-8")).hexdigest()
        partition_key = f"v1|{digest}"
        partition_key_hashed = True
        logger.warning(
            "quantlab_partition_key_hashed | partition_key_len=%s | partition_key_hashed=%s | inst_id=%s | symbol=%s | interval=%s",
            key_len,
            partition_key_hashed,
            payload.get("inst_id"),
            payload.get("symbol"),
            payload.get("interval"),
        )
    else:
        logger.info(
            "quantlab_partition_key_ready | partition_key_len=%s | partition_key_hashed=%s | inst_id=%s | symbol=%s | interval=%s",
            key_len,
            partition_key_hashed,
            payload.get("inst_id"),
            payload.get("symbol"),
            payload.get("interval"),
        )
    return partition_key

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
    "JOB_TYPE_SIGNALS",
    "enqueue_signal_job",
    "wait_for_job",
]
