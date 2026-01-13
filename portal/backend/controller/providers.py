"""API surface for provider/venue registry and market validation."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..service.providers import provider_service

router = APIRouter()


class ProviderVenueRequest(BaseModel):
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    refresh: Optional[bool] = None


@router.get("/")
async def list_providers() -> Dict[str, Any]:
    """Return all providers and their venues."""

    return {"providers": provider_service.provider_payloads()}


@router.post("/validate")
async def validate_selection(body: ProviderVenueRequest) -> Dict[str, Any]:
    """Validate provider/venue pairing and optionally a symbol."""

    valid, errors, normalized = provider_service.validate_provider_venue(body.provider_id, body.venue_id)
    symbol_errors: Dict[str, str] = {}
    if body.symbol is not None and not provider_service.normalize_symbol(body.symbol):
        symbol_errors["symbol"] = "Symbol is required."
    all_errors = {**errors, **symbol_errors}
    return {"valid": valid and len(symbol_errors) == 0, "errors": all_errors, "normalized": normalized}


@router.post("/tick-metadata")
async def tick_metadata(body: ProviderVenueRequest) -> Dict[str, Any]:
    """Return tick metadata for a provider/venue/symbol combo."""

    try:
        return provider_service.tick_metadata(
            body.provider_id,
            body.venue_id,
            body.symbol,
            timeframe=body.timeframe,
            refresh=bool(body.refresh),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc
