"""FastAPI router exposing instrument metadata CRUD endpoints."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from ..service import instrument_service

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
    quote_currency: Optional[str] = None
    maker_fee_rate: Optional[float] = Field(default=None, ge=0)
    taker_fee_rate: Optional[float] = Field(default=None, ge=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class InstrumentResponse(InstrumentPayload):
    """Response payload enriched with identifiers and timestamps."""

    id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@router.get("/", response_model=List[InstrumentResponse])
async def list_instruments() -> List[Dict[str, Any]]:
    """Return all stored instruments."""

    return instrument_service.list_instruments()


@router.get("/health")
async def instrument_health(datasource: Optional[str] = None, exchange: Optional[str] = None) -> Dict[str, Any]:
    """Return spot instrument metadata health report."""

    return instrument_service.instrument_health_report(datasource=datasource, exchange=exchange)


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
