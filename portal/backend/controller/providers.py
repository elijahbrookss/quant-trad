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
    strategy_id: Optional[str] = None


class ProviderCredentialsRequest(BaseModel):
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    credentials: Dict[str, str]


@router.get("/")
async def list_providers() -> Dict[str, Any]:
    """Return all providers and their venues."""

    payload = provider_service.provider_payloads()
    try:
        # Structured logging for tracing venue/provider payloads
        import logging

        logging.getLogger(__name__).info(
            "providers_list_served | providers=%s",
            [p.get("id") for p in payload],
        )
    except Exception:
        pass

    return {"providers": payload}


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
            strategy_id=body.strategy_id,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@router.post("/credentials")
async def upsert_credentials(body: ProviderCredentialsRequest) -> Dict[str, Any]:
    """Persist provider credentials and return the updated status."""

    try:
        return provider_service.upsert_provider_secrets(body.provider_id, body.venue_id, body.credentials or {})
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, str(exc)) from exc
