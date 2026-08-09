from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Mapping, Optional

from portal.backend.service.async_jobs import (
    enqueue_or_reuse_job,
    get_job,
)


logger = logging.getLogger(__name__)


JOB_TYPE_RESEARCH_CHECK_RUN = "research_check_run"
JOB_TYPE_RESEARCH_CHECK_SWEEP = "research_check_sweep"
RESEARCH_JOB_TYPES = {JOB_TYPE_RESEARCH_CHECK_RUN, JOB_TYPE_RESEARCH_CHECK_SWEEP}


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


def research_request_fingerprint(*, job_type: str, request: Mapping[str, Any]) -> str:
    canonical = _canonical_request_value(
        {
            "request_contract_version": "research_async_job.v1",
            "job_type": str(job_type),
            "request": dict(request or {}),
        }
    )
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _request_partition_key(*, job_type: str, request: Mapping[str, Any], request_fingerprint: str) -> str:
    scope = request.get("scope") if isinstance(request.get("scope"), Mapping) else {}
    scopes = request.get("scopes") if isinstance(request.get("scopes"), list) else []
    if isinstance(scope, Mapping) and scope:
        parts = [
            job_type,
            str(request.get("check_family") or ""),
            str(scope.get("indicator_id") or ""),
            str(scope.get("run_id") or ""),
            str(scope.get("instrument_id") or ""),
            str(scope.get("symbol") or ""),
            str(scope.get("datasource") or ""),
            str(scope.get("exchange") or ""),
            str(scope.get("timeframe") or ""),
            str(scope.get("start") or ""),
            str(scope.get("end") or ""),
        ]
        key = "|".join(parts)
    elif scopes:
        digest = hashlib.sha256(
            json.dumps(_canonical_request_value(scopes), sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        key = f"{job_type}|{request.get('check_family') or ''}|scopes:{digest}"
    else:
        key = f"{job_type}|{request.get('check_family') or ''}|{request_fingerprint}"
    if len(key) <= 255:
        return key
    return f"{job_type}|{request.get('check_family') or ''}|{request_fingerprint}"


def _result_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    schema_version = str(result.get("schema_version") or "")
    if schema_version == "research_check_sweep.v1":
        leaderboard = result.get("leaderboard") if isinstance(result.get("leaderboard"), Mapping) else {}
        rows = leaderboard.get("rows") if isinstance(leaderboard.get("rows"), list) else []
        return {
            "schema_version": "research_job_result_summary.v1",
            "result_type": "research_check_sweep",
            "check_family": result.get("check_family"),
            "evaluation_count": result.get("evaluation_count"),
            "rank_by": leaderboard.get("rank_by"),
            "top_rows": rows[:5],
        }
    if schema_version in {
        "research_check_run.v1",
        "research_check_run.v2",
        "research_check_preview.v2",
    }:
        check = result.get("check") if isinstance(result.get("check"), Mapping) else {}
        run_result = result.get("result") if isinstance(result.get("result"), Mapping) else {}
        semantic_result = (
            run_result.get("result")
            if isinstance(run_result.get("result"), Mapping)
            else run_result
        )
        return {
            "schema_version": "research_job_result_summary.v1",
            "result_type": (
                "research_check_preview"
                if schema_version == "research_check_preview.v2"
                else "research_check_run"
            ),
            "check_id": check.get("id"),
            "symbol": check.get("symbol"),
            "timeframe": check.get("timeframe"),
            "status": result.get("status"),
            "sample_count": semantic_result.get("sample_count"),
            "recommendation": semantic_result.get("recommendation"),
            "result_hash": run_result.get("result_hash"),
        }
    return {
        "schema_version": "research_job_result_summary.v1",
        "result_type": schema_version or "unknown",
    }


def _job_payload(job: Mapping[str, Any], *, include_result: bool = False) -> dict[str, Any]:
    result = job.get("result") if isinstance(job.get("result"), Mapping) else None
    status = str(job.get("status") or "")
    payload: dict[str, Any] = {
        "schema_version": "research_job_status.v1",
        "job_id": job.get("id"),
        "job_type": job.get("job_type"),
        "status": status,
        "attempts": job.get("attempts"),
        "max_attempts": job.get("max_attempts"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "error": job.get("error"),
        "result_available": status == "succeeded" and isinstance(result, Mapping),
    }
    if isinstance(result, Mapping):
        payload["result_summary"] = _result_summary(result)
        if include_result:
            payload["result"] = dict(result)
    return payload


def dispatch_research_job(*, job_type: str, request: Mapping[str, Any]) -> dict[str, Any]:
    if job_type not in RESEARCH_JOB_TYPES:
        raise ValueError(f"unsupported research job type: {job_type}")
    normalized_request = dict(request or {})
    request_fingerprint = research_request_fingerprint(job_type=job_type, request=normalized_request)
    partition_key = _request_partition_key(
        job_type=job_type,
        request=normalized_request,
        request_fingerprint=request_fingerprint,
    )
    outcome = enqueue_or_reuse_job(
        job_type=job_type,
        payload={
            "schema_version": "research_async_job_request.v1",
            "request": normalized_request,
            "request_fingerprint": request_fingerprint,
        },
        partition_key=partition_key,
        request_fingerprint=request_fingerprint,
        max_attempts=2,
    )
    job_id = outcome.id
    status = outcome.status
    reused = outcome.reused
    logger.info(
        "research_job_dispatched | job_id=%s job_type=%s status=%s reused=%s check_family=%s",
        job_id,
        job_type,
        status,
        reused,
        normalized_request.get("check_family"),
    )
    return {
        "schema_version": "research_job_dispatch.v1",
        "job_id": job_id,
        "job_type": job_type,
        "status": status,
        "reused": reused,
        "request_fingerprint": request_fingerprint,
        "status_url": f"/api/research/jobs/{job_id}",
        "result_url": f"/api/research/jobs/{job_id}/result",
    }


def dispatch_research_check_run(request: Mapping[str, Any]) -> dict[str, Any]:
    if str(request.get("mode") or "").strip().lower() != "evidence":
        raise ValueError(
            "check_evidence_mode_required: async Check run accepts durable evidence only"
        )
    return dispatch_research_job(job_type=JOB_TYPE_RESEARCH_CHECK_RUN, request=request)


def dispatch_research_check_sweep(request: Mapping[str, Any]) -> dict[str, Any]:
    return dispatch_research_job(job_type=JOB_TYPE_RESEARCH_CHECK_SWEEP, request=request)


def get_research_job_status(job_id: str) -> dict[str, Any]:
    job = get_job(str(job_id))
    if job is None or str(job.get("job_type") or "") not in RESEARCH_JOB_TYPES:
        raise KeyError(f"research_job_not_found: {job_id}")
    return _job_payload(job)


def get_research_job_result(job_id: str) -> dict[str, Any]:
    job = get_job(str(job_id))
    if job is None or str(job.get("job_type") or "") not in RESEARCH_JOB_TYPES:
        raise KeyError(f"research_job_not_found: {job_id}")
    status = str(job.get("status") or "")
    if status != "succeeded":
        raise ValueError(f"research_job_not_succeeded: {job_id} status={status}")
    return _job_payload(job, include_result=True)


__all__ = [
    "JOB_TYPE_RESEARCH_CHECK_RUN",
    "JOB_TYPE_RESEARCH_CHECK_SWEEP",
    "RESEARCH_JOB_TYPES",
    "dispatch_research_check_run",
    "dispatch_research_check_sweep",
    "get_research_job_result",
    "get_research_job_status",
    "research_request_fingerprint",
]
