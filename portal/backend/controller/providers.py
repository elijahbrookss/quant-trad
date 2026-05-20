"""API surface for provider/venue registry and market validation."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..service.providers import provider_service
from ..service.providers.stream_smoke import run_provider_stream_smoke

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
    credential_ref: Optional[str] = None
    environment: Optional[str] = "paper"
    display_name: Optional[str] = None
    credentials: Dict[str, str]


class ProviderStreamSmokeRequest(BaseModel):
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    symbol: str
    product_id: Optional[str] = None
    channels: Optional[List[str]] = None
    timeframe: Optional[str] = None
    auth_mode: str = "public"
    duration_seconds: float = 10.0
    sample_limit: int = 10


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
        return provider_service.upsert_provider_secrets(
            body.provider_id,
            body.venue_id,
            body.credentials or {},
            credential_ref=body.credential_ref,
            environment=body.environment,
            display_name=body.display_name,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, str(exc)) from exc


@router.get("/credentials/schema")
async def credential_schema(
    provider_id: Optional[str] = Query(None),
    venue_id: Optional[str] = Query(None),
    environment: Optional[str] = Query("paper"),
) -> Dict[str, Any]:
    """Return accepted credential fields without returning secret values."""

    try:
        return provider_service.credential_schema(provider_id, venue_id, environment=environment)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, str(exc)) from exc


@router.get("/credentials")
async def list_credentials(
    provider_id: Optional[str] = Query(None),
    venue_id: Optional[str] = Query(None),
    include_revoked: bool = Query(False),
) -> Dict[str, Any]:
    """Return credential metadata only. Secret values are never returned."""

    try:
        return provider_service.list_provider_credentials(
            provider_id=provider_id,
            venue_id=venue_id,
            include_revoked=include_revoked,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, str(exc)) from exc


@router.post("/credentials/{credential_ref}/validate")
async def validate_credentials(credential_ref: str) -> Dict[str, Any]:
    """Validate stored credential payload structure without exposing secrets."""

    try:
        return provider_service.validate_provider_credentials(credential_ref)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, str(exc)) from exc


@router.delete("/credentials/{credential_ref}")
async def revoke_credentials(credential_ref: str) -> Dict[str, Any]:
    """Revoke a stored credential reference without deleting audit metadata."""

    try:
        return provider_service.revoke_provider_credentials(credential_ref)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, str(exc)) from exc


@router.post("/stream-smoke")
async def stream_smoke(body: ProviderStreamSmokeRequest) -> Dict[str, Any]:
    """Run a bounded read-only provider stream smoke check."""

    try:
        return await run_provider_stream_smoke(
            provider_id=body.provider_id or "COINBASE",
            venue_id=body.venue_id or "COINBASE_DIRECT",
            symbol=body.symbol,
            product_id=body.product_id,
            channels=body.channels,
            timeframe=body.timeframe,
            auth_mode=body.auth_mode,
            duration_seconds=body.duration_seconds,
            sample_limit=body.sample_limit,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, str(exc)) from exc
