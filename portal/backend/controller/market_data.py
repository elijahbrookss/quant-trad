"""Typed market-data collection and causal read operator API."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from core.settings import get_settings
from market_data.contracts import (
    FUNDING_RATE_FACT_TYPE,
    OPEN_INTEREST_FACT_TYPE,
    FundingRateRecord,
    OpenInterestRecord,
)
from ..service.market.normalization_service import market_normalization_service
from market_data.requirements import UnavailableMarketData

from ..service.market.collector_service import market_data_collector
from ..service.market.market_storage_lifecycle import market_storage_lifecycle_service
from ..service.market.market_structure_service import market_structure_service
from ..service.storage.repos.market_lifecycle import (
    MarketStorageLifecycleBusyError,
    market_storage_lifecycle_repository,
)
from ..service.storage.repos.market_structure import market_structure_repository


router = APIRouter()
logger = logging.getLogger(__name__)


def _collector_material(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "collectors": snapshot.get("collectors", []),
        "workers": snapshot.get("workers", []),
        "worker_health": {
            key: value
            for key, value in dict(snapshot.get("worker_health") or {}).items()
            if key != "observed_at"
        },
    }


def _collector_fingerprint(snapshot: dict[str, Any]) -> str:
    payload = json.dumps(
        _collector_material(snapshot), sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_market_structure_component_errors: dict[str, str] = {}


def _read_market_structure_component(
    component: str,
    label: str,
    reader: Callable[[], Any],
    fallback: Any,
) -> tuple[Any, Optional[dict[str, str]]]:
    try:
        value = reader()
    except Exception as exc:
        signature = f"{type(exc).__name__}:{exc}"
        if _market_structure_component_errors.get(component) != signature:
            logger.warning(
                "market_structure_snapshot_component_unavailable component=%s error=%s",
                component,
                exc,
            )
            _market_structure_component_errors[component] = signature
        return fallback, {
            "code": f"market_structure_{component}_unavailable",
            "message": f"{label} are unavailable.",
            "details": str(exc),
        }
    if _market_structure_component_errors.pop(component, None) is not None:
        logger.info(
            "market_structure_snapshot_component_recovered component=%s", component
        )
    return value, None


def _market_structure_snapshot(*, session_limit: int = 250) -> dict[str, Any]:
    limit = max(1, min(int(session_limit or 250), 500))
    component_errors: dict[str, dict[str, str]] = {}
    definitions, error = _read_market_structure_component(
        "definitions",
        "Stream definitions",
        market_structure_repository.list_stream_definitions,
        [],
    )
    if error:
        component_errors["definitions"] = error
    sessions, error = _read_market_structure_component(
        "sessions",
        "Stream sessions",
        lambda: market_structure_repository.list_sessions(limit=limit),
        [],
    )
    if error:
        component_errors["sessions"] = error
    normalization_specs, error = _read_market_structure_component(
        "normalization_specs",
        "Normalization specifications",
        market_normalization_service.list_specs,
        [],
    )
    if error:
        component_errors["normalization_specs"] = error
    status_by_definition, error = _read_market_structure_component(
        "status_by_definition",
        "Archive and quality summaries",
        market_structure_repository.list_archive_status_summaries,
        {},
    )
    if error:
        component_errors["status_by_definition"] = error
    return {
        "schema_version": "market_structure_operator_snapshot.v1",
        "definitions": definitions,
        "sessions": sessions,
        "normalization_specs": normalization_specs,
        "status_by_definition": status_by_definition,
        "component_errors": component_errors,
        "observed_at": datetime.now(UTC).isoformat(),
    }


def _market_structure_fingerprint(snapshot: dict[str, Any]) -> str:
    material = {
        key: value
        for key, value in snapshot.items()
        if key != "observed_at"
    }
    payload = json.dumps(
        material, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _format_sse(event: str, payload: dict[str, Any], *, event_id: int) -> str:
    body = json.dumps(payload, separators=(",", ":"), default=str)
    return f"id: {event_id}\nevent: {event}\ndata: {body}\n\n"



def _time(value: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _record_payload(
    record: OpenInterestRecord | FundingRateRecord,
) -> dict[str, Any]:
    fact = record.fact.to_dict()
    for key, value in tuple(fact.items()):
        if isinstance(value, datetime):
            fact[key] = value.astimezone(UTC).isoformat()
    return {
        "available": True,
        "series_id": record.series_id,
        "revision": record.revision,
        "market_commit_seq": record.market_commit_seq,
        "ingestion_run_id": record.ingestion_run_id,
        "source_identity_key": record.source_identity_key,
        "source": {
            "provider": record.source.provider,
            "venue": record.source.venue,
            "source_kind": record.source.source_kind,
            "adapter_version": record.source.adapter_version,
        },
        "provenance": dict(record.provenance),
        "fact": fact,
    }


class CollectorCreateRequest(BaseModel):
    instrument_id: str
    provider_product_id: str
    fact_type: str = OPEN_INTEREST_FACT_TYPE
    poll_interval_seconds: int = 60
    max_attempts: int = 3
    minimum_spacing_seconds: float = 1.0
    enabled: bool = False


class CollectorToggleRequest(BaseModel):
    enabled: bool


class MarketNormalizationSpecInstallRequest(BaseModel):
    approved_by: str


class MarketNormalizationMaterializeRequest(BaseModel):
    spec_id: str
    source_series_id: int
    start: str
    end: str
    known_at: str
    as_of_commit_seq: Optional[int] = None

class MarketStructurePairRequest(BaseModel):
    pair_id: str = "bip_btc"
    auth_mode: str = "authenticated"
    max_spool_bytes: int = 8 * 1024**3
    max_segment_bytes: int = 128 * 1024**2
    enable_production: bool = False


class MarketStructureMaterializeRequest(BaseModel):
    start: str
    end: str
    known_at: str


class MarketStructureCaptureRequest(BaseModel):
    duration_seconds: float = 60.0
    storage_root: Optional[str] = None
    owner_id: Optional[str] = None


class MarketStructureContinuousValidationRequest(BaseModel):
    duration_seconds: float = 24 * 3600
    requested_by: str
    policy: Optional[dict[str, Any]] = None


class MarketStructureContinuousStartRequest(BaseModel):
    requested_by: str
    policy: Optional[dict[str, Any]] = None


class MarketStructureContinuousStopRequest(BaseModel):
    requested_by: str


class MarketStructureAdmissionRequest(BaseModel):
    admitted: bool
    approved_by: str
    evidence: dict[str, Any]
    storage_budget: dict[str, Any]


class MarketStructureReplayRequest(BaseModel):
    storage_root: Optional[str] = None


class MarketStructureCompactionRequest(BaseModel):
    source_manifest_ids: list[str]
    storage_root: Optional[str] = None
    owner_id: Optional[str] = None


class MarketStorageLifecycleRunRequest(BaseModel):
    execute: bool = False
    storage_root: Optional[str] = None
    owner_id: Optional[str] = None


class MarketStructureRetentionPinRequest(BaseModel):
    owner_kind: str
    owner_id: str
    active: bool = True
    reason: str


@router.post("/collectors")
def create_collector(req: CollectorCreateRequest) -> dict[str, Any]:
    creators = {
        OPEN_INTEREST_FACT_TYPE: (
            market_data_collector.create_coinbase_open_interest_definition
        ),
        FUNDING_RATE_FACT_TYPE: (
            market_data_collector.create_coinbase_funding_rate_definition
        ),
    }
    creator = creators.get(req.fact_type)
    if creator is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "market_collection_handler_missing: supported fact types are "
                f"{', '.join(sorted(creators))}"
            ),
        )
    try:
        definition = creator(
            instrument_id=req.instrument_id,
            provider_product_id=req.provider_product_id,
            poll_interval_seconds=req.poll_interval_seconds,
            max_attempts=req.max_attempts,
            minimum_spacing_seconds=req.minimum_spacing_seconds,
            enabled=req.enabled,
        )
    except (KeyError, ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "schema_version": "market_collection_definition_response.v1",
        "definition": definition,
    }


@router.get("/collectors")
def list_collectors(definition_id: Optional[str] = None) -> dict[str, Any]:
    return {
        "schema_version": "market_collection_catalog.v1",
        "definitions": market_data_collector.list_definitions(
            definition_id=definition_id
        ),
    }


@router.get("/collectors/snapshot")
def get_collector_snapshot(
    attempt_limit: int = Query(default=5, ge=1, le=100),
) -> dict[str, Any]:
    return market_data_collector.collector_snapshot(attempt_limit=attempt_limit)


@router.get("/collectors/stream")
async def stream_collectors(
    attempt_limit: int = Query(default=5, ge=1, le=100),
) -> StreamingResponse:
    async def event_iterator():
        event_id = 1
        snapshot = market_data_collector.collector_snapshot(
            attempt_limit=attempt_limit
        )
        fingerprint = _collector_fingerprint(snapshot)
        yield _format_sse("snapshot", snapshot, event_id=event_id)
        keepalive_ticks = 0
        while True:
            try:
                await asyncio.sleep(2.0)
                current = await asyncio.to_thread(
                    market_data_collector.collector_snapshot,
                    attempt_limit=attempt_limit,
                )
            except asyncio.CancelledError:
                break
            current_fingerprint = _collector_fingerprint(current)
            if current_fingerprint != fingerprint:
                event_id += 1
                fingerprint = current_fingerprint
                yield _format_sse("delta", current, event_id=event_id)
                keepalive_ticks = 0
                continue
            keepalive_ticks += 1
            if keepalive_ticks >= 8:
                yield ": keepalive\n\n"
                keepalive_ticks = 0

    return StreamingResponse(
        event_iterator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/collectors/{definition_id}/enabled")
def set_collector_enabled(
    definition_id: str, req: CollectorToggleRequest
) -> dict[str, Any]:
    try:
        definition = market_data_collector.set_enabled(
            definition_id, enabled=req.enabled
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {
        "schema_version": "market_collection_definition_response.v1",
        "definition": definition,
    }


@router.get("/collectors/{definition_id}/facts")
def collector_fact_history(
    definition_id: str,
    hours: int = 24,
    limit: int = 240,
) -> dict[str, Any]:
    try:
        return market_data_collector.fact_history(
            definition_id=definition_id,
            hours=max(1, min(int(hours or 24), 24 * 7)),
            limit=max(1, min(int(limit or 240), 1000)),
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/collectors/{definition_id}/attempts")
def list_collector_attempts(
    definition_id: str, limit: int = 100
) -> dict[str, Any]:
    try:
        attempts = market_data_collector.list_attempts(
            definition_id=definition_id, limit=limit
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "schema_version": "market_collection_attempt_catalog.v1",
        "definition_id": definition_id,
        "attempts": attempts,
    }


@router.get("/open-interest/latest")
def latest_open_interest(
    instrument_id: str,
    decision_time: str,
    max_staleness_seconds: int,
    required: bool = True,
) -> dict[str, Any]:
    try:
        result = market_data_collector.latest_open_interest(
            instrument_id=instrument_id,
            decision_time=_time(decision_time),
            max_staleness_seconds=max_staleness_seconds,
            required=required,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(result, UnavailableMarketData):
        return result.to_dict()
    return _record_payload(result)


@router.get("/funding-rate/latest")
def latest_funding_rate(
    instrument_id: str,
    decision_time: str,
    max_staleness_seconds: int,
    required: bool = True,
) -> dict[str, Any]:
    try:
        result = market_data_collector.latest_funding_rate(
            instrument_id=instrument_id,
            decision_time=_time(decision_time),
            max_staleness_seconds=max_staleness_seconds,
            required=required,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(result, UnavailableMarketData):
        return result.to_dict()
    return _record_payload(result)


@router.post("/market-structure/pairs")
def configure_market_structure_pair(
    req: MarketStructurePairRequest,
) -> dict[str, Any]:
    try:
        return market_structure_service.configure_pair(
            pair_id=req.pair_id,
            auth_mode=req.auth_mode,
            max_spool_bytes=req.max_spool_bytes,
            max_segment_bytes=req.max_segment_bytes,
            enable_production=req.enable_production,
        )
    except (KeyError, ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/market-structure/pairs/{pair_id}/materialize")
def materialize_market_structure_pair(
    pair_id: str,
    req: MarketStructureMaterializeRequest,
) -> dict[str, Any]:
    try:
        return market_structure_service.materialize_pair_features(
            pair_id=pair_id,
            start=_time(req.start),
            end=_time(req.end),
            known_at=_time(req.known_at),
        )
    except (KeyError, ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc



@router.post("/market-structure/normalization/specs/install")
def install_market_normalization_specs(
    req: MarketNormalizationSpecInstallRequest,
) -> dict[str, Any]:
    try:
        specs = market_normalization_service.install_builtin_specs(
            approved_by=req.approved_by
        )
        return {
            "schema_version": "market.normalization_spec_catalog.v1",
            "specs": specs,
        }
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/market-structure/normalization/specs")
def list_market_normalization_specs() -> dict[str, Any]:
    try:
        return {
            "schema_version": "market.normalization_spec_catalog.v1",
            "specs": market_normalization_service.list_specs(),
        }
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _normalization_request_kwargs(
    req: MarketNormalizationMaterializeRequest,
) -> dict[str, Any]:
    return {
        "spec_id": req.spec_id,
        "source_series_id": req.source_series_id,
        "start": _time(req.start),
        "end": _time(req.end),
        "known_at": _time(req.known_at),
        "as_of_commit_seq": req.as_of_commit_seq,
    }


@router.post("/market-structure/normalization/materialize")
def materialize_market_normalization(
    req: MarketNormalizationMaterializeRequest,
) -> dict[str, Any]:
    try:
        return market_normalization_service.materialize(
            **_normalization_request_kwargs(req)
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/market-structure/normalization/compare")
def compare_market_normalization(
    req: MarketNormalizationMaterializeRequest,
) -> dict[str, Any]:
    try:
        return market_normalization_service.compare_persisted(
            **_normalization_request_kwargs(req)
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
@router.get("/market-structure/snapshot")
def market_structure_operator_snapshot(session_limit: int = 250) -> dict[str, Any]:
    return _market_structure_snapshot(session_limit=session_limit)


@router.get("/market-structure/stream")
async def stream_market_structure_operator_snapshot(
    session_limit: int = 250,
) -> StreamingResponse:
    async def event_iterator():
        event_id = 1
        snapshot = await asyncio.to_thread(
            _market_structure_snapshot, session_limit=session_limit
        )
        fingerprint = _market_structure_fingerprint(snapshot)
        yield _format_sse("snapshot", snapshot, event_id=event_id)
        idle_ticks = 0
        while True:
            try:
                await asyncio.sleep(5)
                next_snapshot = await asyncio.to_thread(
                    _market_structure_snapshot, session_limit=session_limit
                )
            except asyncio.CancelledError:
                break
            next_fingerprint = _market_structure_fingerprint(next_snapshot)
            if next_fingerprint != fingerprint:
                event_id += 1
                fingerprint = next_fingerprint
                idle_ticks = 0
                yield _format_sse("delta", next_snapshot, event_id=event_id)
                continue
            idle_ticks += 1
            if idle_ticks >= 4:
                idle_ticks = 0
                yield ": keepalive\n\n"

    return StreamingResponse(
        event_iterator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/market-structure/definitions")
def list_market_structure_definitions(
    definition_id: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "schema_version": "market_structure_definition_catalog.v1",
        "definitions": market_structure_repository.list_stream_definitions(
            definition_id=definition_id
        ),
    }


@router.get("/market-structure/sessions")
def list_market_structure_sessions(
    definition_id: Optional[str] = None,
    limit: int = 100,
) -> dict[str, Any]:
    return {
        "schema_version": "market_structure_session_catalog.v1",
        "sessions": market_structure_repository.list_sessions(
            definition_id=definition_id,
            limit=limit,
        ),
    }


@router.get("/market-structure/definitions/{definition_id}/status")
def market_structure_status(definition_id: str) -> dict[str, Any]:
    try:
        return market_structure_repository.archive_status(
            definition_id=definition_id
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/market-structure/definitions/{definition_id}/capture")
def capture_market_structure(
    definition_id: str,
    req: MarketStructureCaptureRequest,
) -> dict[str, Any]:
    import asyncio

    try:
        kwargs: dict[str, Any] = {
            "definition_id": definition_id,
            "duration_seconds": req.duration_seconds,
            "owner_id": req.owner_id,
        }
        if req.storage_root:
            kwargs["storage_root"] = Path(req.storage_root)
        return asyncio.run(market_structure_service.capture_bounded(**kwargs))
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/market-structure/definitions/{definition_id}/continuous/validate"
)
def validate_continuous_market_structure(
    definition_id: str,
    req: MarketStructureContinuousValidationRequest,
) -> dict[str, Any]:
    try:
        return market_structure_service.start_continuous_validation(
            definition_id=definition_id,
            duration_seconds=req.duration_seconds,
            requested_by=req.requested_by,
            policy=req.policy,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/market-structure/definitions/{definition_id}/continuous/start"
)
def start_continuous_market_structure(
    definition_id: str,
    req: MarketStructureContinuousStartRequest,
) -> dict[str, Any]:
    try:
        return market_structure_service.start_continuous_production(
            definition_id=definition_id,
            requested_by=req.requested_by,
            policy=req.policy,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/market-structure/definitions/{definition_id}/continuous/stop"
)
def stop_continuous_market_structure(
    definition_id: str,
    req: MarketStructureContinuousStopRequest,
) -> dict[str, Any]:
    try:
        return market_structure_service.stop_continuous(
            definition_id=definition_id,
            requested_by=req.requested_by,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/market-structure/definitions/{definition_id}/continuous/admission"
)
def set_continuous_market_structure_admission(
    definition_id: str,
    req: MarketStructureAdmissionRequest,
) -> dict[str, Any]:
    try:
        return market_structure_service.set_production_admission(
            definition_id=definition_id,
            admitted=req.admitted,
            approved_by=req.approved_by,
            evidence=req.evidence,
            storage_budget=req.storage_budget,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get(
    "/market-structure/definitions/{definition_id}/continuous/validation/{session_id}"
)
def get_continuous_market_structure_validation_evidence(
    definition_id: str,
    session_id: str,
) -> dict[str, Any]:
    try:
        return market_structure_repository.continuous_validation_evidence(
            definition_id=definition_id,
            session_id=session_id,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/market-structure/manifests/{manifest_id}/replay")
def replay_market_structure_manifest(
    manifest_id: str,
    req: MarketStructureReplayRequest,
) -> dict[str, Any]:
    try:
        kwargs: dict[str, Any] = {"manifest_id": manifest_id}
        if req.storage_root:
            kwargs["storage_root"] = Path(req.storage_root)
        return market_structure_service.replay_manifest(**kwargs)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/market-structure/definitions/{definition_id}/sessions/{session_id}/replay-book"
)
def replay_market_structure_book_session(
    definition_id: str,
    session_id: str,
    req: MarketStructureReplayRequest,
) -> dict[str, Any]:
    try:
        kwargs: dict[str, Any] = {
            "definition_id": definition_id,
            "session_id": session_id,
        }
        if req.storage_root:
            kwargs["storage_root"] = Path(req.storage_root)
        return market_structure_service.replay_book_session(**kwargs)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/market-structure/definitions/{definition_id}/sessions/{session_id}/compact"
)
def compact_market_structure_session(
    definition_id: str,
    session_id: str,
    req: MarketStructureCompactionRequest,
) -> dict[str, Any]:
    try:
        kwargs: dict[str, Any] = {
            "definition_id": definition_id,
            "source_session_id": session_id,
            "source_manifest_ids": req.source_manifest_ids,
            "owner_id": req.owner_id,
        }
        if req.storage_root:
            kwargs["storage_root"] = Path(req.storage_root)
        return market_structure_service.compact_session_archives(**kwargs)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/market-structure/archive-retention/{target_kind}/{target_id}/pin")
def append_market_structure_retention_pin(
    target_kind: str,
    target_id: str,
    req: MarketStructureRetentionPinRequest,
) -> dict[str, Any]:
    try:
        version_id = market_structure_repository.append_archive_retention_pin_version(
            target_kind=target_kind,
            target_id=target_id,
            owner_kind=req.owner_kind,
            owner_id=req.owner_id,
            active=req.active,
            reason=req.reason,
        )
        return {
            "schema_version": "market.archive_retention_pin_operation.v1",
            "version_id": version_id,
            "status": market_structure_repository.archive_retention_status(
                target_kind=target_kind,
                target_id=target_id,
            ),
        }
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/market-structure/archive-retention/{target_kind}/{target_id}")
def get_market_structure_retention_status(
    target_kind: str,
    target_id: str,
) -> dict[str, Any]:
    try:
        return market_structure_repository.archive_retention_status(
            target_kind=target_kind,
            target_id=target_id,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/market-structure/storage-lifecycle/plan")
def plan_market_storage_lifecycle() -> dict[str, Any]:
    try:
        policy = get_settings().market_data_lifecycle
        return market_storage_lifecycle_service.plan(policy=policy)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/market-structure/storage-lifecycle/run")
def run_market_storage_lifecycle(
    req: MarketStorageLifecycleRunRequest,
) -> dict[str, Any]:
    try:
        kwargs: dict[str, Any] = {
            "policy": get_settings().market_data_lifecycle,
            "execute": req.execute,
            "owner_id": req.owner_id,
        }
        if req.storage_root:
            kwargs["storage_root"] = Path(req.storage_root)
        return market_storage_lifecycle_service.run(**kwargs)
    except MarketStorageLifecycleBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/market-structure/storage-lifecycle/events")
def list_market_storage_lifecycle_events(
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    try:
        rows = market_storage_lifecycle_repository.list_recent_events(limit=limit)
        return {
            "schema_version": "market.storage_lifecycle_event_list.v1",
            "events": rows,
            "count": len(rows),
            "observed_at": datetime.now(UTC).isoformat(),
        }
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/market-structure/definitions/{definition_id}/reconcile-recent")
def reconcile_recent_market_structure_trades(
    definition_id: str, limit: int = 100
) -> dict[str, Any]:
    try:
        return market_structure_service.reconcile_recent_trades(
            definition_id=definition_id,
            limit=limit,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


__all__ = ["router"]
