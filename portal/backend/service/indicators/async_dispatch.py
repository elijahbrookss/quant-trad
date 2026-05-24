from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Any, Dict, Mapping, Optional

from core.settings import get_settings
from portal.backend.service.async_jobs import enqueue_job, find_reusable_job, get_job


logger = logging.getLogger(__name__)


JOB_TYPE_SIGNALS = "quantlab_signals"
JOB_TYPE_OVERLAYS = "quantlab_overlays"


_SETTINGS = get_settings().async_jobs
DEFAULT_WAIT_TIMEOUT_SECONDS = float(_SETTINGS.quantlab_job_wait_timeout_seconds)
DEFAULT_POLL_INTERVAL_SECONDS = float(_SETTINGS.quantlab_job_poll_interval_seconds)
DEFAULT_RESULT_CACHE_TTL_SECONDS = float(_SETTINGS.quantlab_result_cache_ttl_seconds)


class AsyncJobTimeoutError(RuntimeError):
    pass


class AsyncJobFailedError(RuntimeError):
    pass


class AsyncJobNotFoundError(RuntimeError):
    pass


def _canonical_request_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _canonical_request_value(value[key]) for key in sorted(value.keys(), key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonical_request_value(item) for item in value]
    if isinstance(value, set):
        return [_canonical_request_value(item) for item in sorted(value, key=repr)]
    return str(value)


def _request_fingerprint(parts: Mapping[str, Any]) -> str:
    canonical = _canonical_request_value(parts)
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def resolve_overlay_cursor_epoch(
    *,
    cursor_epoch: Optional[Any] = None,
    cursor_time: Optional[Any] = None,
) -> Optional[int]:
    if cursor_epoch is not None:
        try:
            numeric = float(cursor_epoch)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid cursor_epoch: {cursor_epoch}") from None
        if not numeric or not float(numeric).is_integer():
            raise ValueError(f"Invalid cursor_epoch: {cursor_epoch}")
        return int(numeric)

    if cursor_time is None:
        return None
    raw = str(cursor_time).strip()
    if not raw:
        return None
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise ValueError(f"Invalid cursor_time: {cursor_time}") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _series_partition_key(payload: Mapping[str, Any]) -> str:
    partition_key = "|".join(
        [
            str(payload.get("datasource") or ""),
            str(payload.get("exchange") or ""),
            str(payload.get("symbol") or ""),
            str(payload.get("instrument_id") or ""),
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
        logger.debug(
            "quantlab_partition_key_ready | partition_key_len=%s | partition_key_hashed=%s | inst_id=%s | symbol=%s | interval=%s",
            key_len,
            partition_key_hashed,
            payload.get("inst_id"),
            payload.get("symbol"),
            payload.get("interval"),
        )
    return partition_key


def quantlab_partition_key(payload: Mapping[str, Any]) -> str:
    return _series_partition_key(payload)


def quantlab_request_fingerprint(
    *,
    job_type: str,
    indicator_id: str,
    indicator_updated_at: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str],
    datasource: Optional[str],
    exchange: Optional[str],
    instrument_id: Optional[str],
    config: Optional[Mapping[str, Any]] = None,
    visibility_epoch: Optional[Any] = None,
    cursor_epoch: Optional[Any] = None,
    cursor_time: Optional[Any] = None,
) -> str:
    resolved_cursor_epoch = resolve_overlay_cursor_epoch(
        cursor_epoch=cursor_epoch,
        cursor_time=cursor_time,
    )
    return _request_fingerprint(
        {
            "request_contract_version": "quantlab_request_v2",
            "job_type": str(job_type),
            "indicator_id": str(indicator_id),
            "indicator_updated_at": str(indicator_updated_at or ""),
            "start": str(start),
            "end": str(end),
            "interval": str(interval),
            "symbol": str(symbol or ""),
            "datasource": str(datasource or ""),
            "exchange": str(exchange or ""),
            "instrument_id": str(instrument_id or ""),
            "config": dict(config or {}),
            "visibility_epoch": visibility_epoch,
            "cursor_epoch": resolved_cursor_epoch,
        }
    )


def reuse_quantlab_job(
    *,
    job_type: str,
    partition_key: str,
    request_fingerprint: str,
    result_ttl_seconds: float = DEFAULT_RESULT_CACHE_TTL_SECONDS,
) -> Optional[Dict[str, Any]]:
    return find_reusable_job(
        job_type=job_type,
        partition_key=partition_key,
        request_fingerprint=request_fingerprint,
        result_ttl_seconds=result_ttl_seconds,
    )

def enqueue_signal_job(
    *,
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str],
    datasource: Optional[str],
    exchange: Optional[str],
    instrument_id: str,
    config: Optional[Mapping[str, Any]],
    request_fingerprint: Optional[str] = None,
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
        "config": dict(config or {}),
    }
    if request_fingerprint:
        payload["request_fingerprint"] = str(request_fingerprint)
    job_id = enqueue_job(
        job_type=JOB_TYPE_SIGNALS,
        payload=payload,
        partition_key=_series_partition_key(payload),
        max_attempts=2,
    )
    return job_id


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
    visibility_epoch: Optional[int],
    cursor_epoch: Optional[int],
    request_fingerprint: Optional[str] = None,
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
        "visibility_epoch": visibility_epoch,
        "cursor_epoch": cursor_epoch,
    }
    if request_fingerprint:
        payload["request_fingerprint"] = str(request_fingerprint)
    job_id = enqueue_job(
        job_type=JOB_TYPE_OVERLAYS,
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
    "quantlab_partition_key",
    "quantlab_request_fingerprint",
    "resolve_overlay_cursor_epoch",
    "reuse_quantlab_job",
    "wait_for_job",
]
