"""FastAPI router exposing instrument metadata CRUD endpoints."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field
from datetime import datetime

from ..service.market import instrument_service

router = APIRouter()


class InstrumentPayload(BaseModel):
    """Shared instrument attributes."""

    symbol: str = Field(..., description="Symbol ticker (e.g., LINKUSDT)")
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    instrument_type: Optional[str] = None
    tick_size: Optional[float] = Field(default=None, gt=0)
    tick_value: Optional[float] = None
    contract_size: Optional[float] = Field(default=None, gt=0)
    min_order_size: Optional[float] = Field(default=None, gt=0)
    qty_step: Optional[float] = Field(default=None, gt=0)
    max_qty: Optional[float] = Field(default=None, gt=0)
    min_notional: Optional[float] = Field(default=None, ge=0)
    base_currency: Optional[str] = None
    quote_currency: Optional[str] = None
    can_short: Optional[bool] = None
    short_requires_borrow: Optional[bool] = None
    has_funding: Optional[bool] = None
    expiry_ts: Optional[datetime] = None
    maker_fee_rate: Optional[float] = Field(default=None, ge=0)
    taker_fee_rate: Optional[float] = Field(default=None, ge=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class InstrumentResponse(InstrumentPayload):
    """Response payload enriched with identifiers and timestamps."""

    id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class InstrumentResolveRequest(BaseModel):
    symbol: str
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    force_refresh: bool = False


@router.get("/", response_model=List[InstrumentResponse])
async def list_instruments() -> List[Dict[str, Any]]:
    """Return all stored instruments."""

    return instrument_service.list_instruments()


@router.get("/health")
async def instrument_health(datasource: Optional[str] = None, exchange: Optional[str] = None) -> Dict[str, Any]:
    """Return spot instrument metadata health report."""

    return instrument_service.instrument_health_report(datasource=datasource, exchange=exchange)


@router.post("/resolve", response_model=InstrumentResponse)
async def resolve_instrument(request: InstrumentResolveRequest) -> Dict[str, Any]:
    """Validate provider/venue/symbol and return a canonical instrument record."""

    record, error = instrument_service.resolve_or_create_instrument(
        request.datasource,
        request.exchange,
        request.symbol,
        provider_id=request.provider_id,
        venue_id=request.venue_id,
        force_refresh=request.force_refresh,
    )
    if error:
        raise HTTPException(400, error)
    if not record:
        raise HTTPException(404, "Instrument could not be resolved.")
    return record


@router.post("/", response_model=InstrumentResponse, status_code=201)
async def create_instrument(payload: InstrumentPayload) -> Dict[str, Any]:
    """Create a new instrument definition."""

    try:
        return instrument_service.create_instrument(**payload.dict())
    except ValueError as exc:  # pragma: no cover - FastAPI plumbing
        raise HTTPException(400, str(exc)) from exc


@router.get("/{instrument_id}", response_model=InstrumentResponse)
async def get_instrument(instrument_id: str) -> Dict[str, Any]:
    """Return a single instrument."""

    try:
        return instrument_service.get_instrument_record(instrument_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/{instrument_id}", response_model=InstrumentResponse)
async def update_instrument(instrument_id: str, payload: InstrumentPayload) -> Dict[str, Any]:
    """Update an existing instrument."""

    try:
        return instrument_service.update_instrument(instrument_id, **payload.dict())
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{instrument_id}", status_code=204, response_class=Response)
async def delete_instrument(instrument_id: str) -> Response:
    """Delete an instrument record."""

    instrument_service.delete_instrument_record(instrument_id)

    return Response(status_code=204)
