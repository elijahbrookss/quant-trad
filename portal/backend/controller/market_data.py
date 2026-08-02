"""Typed market-data collection and causal read operator API."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from market_data.contracts import (
    FUNDING_RATE_FACT_TYPE,
    OPEN_INTEREST_FACT_TYPE,
    FundingRateRecord,
    OpenInterestRecord,
)
from market_data.requirements import UnavailableMarketData

from ..service.market.collector_service import market_data_collector
from ..service.market.market_structure_service import market_structure_service
from ..service.storage.repos.market_structure import market_structure_repository


router = APIRouter()


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


class MarketStructurePairRequest(BaseModel):
    pair_id: str = "bip_btc"
    auth_mode: str = "authenticated"
    max_spool_bytes: int = 8 * 1024**3
    max_segment_bytes: int = 128 * 1024**2
    enable_production: bool = False


class MarketStructureCaptureRequest(BaseModel):
    duration_seconds: float = 60.0
    storage_root: Optional[str] = None
    owner_id: Optional[str] = None


class MarketStructureReplayRequest(BaseModel):
    storage_root: Optional[str] = None


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
