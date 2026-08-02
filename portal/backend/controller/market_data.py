"""Typed market-data collection and causal read operator API."""

from __future__ import annotations

from datetime import UTC, datetime
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


__all__ = ["router"]
