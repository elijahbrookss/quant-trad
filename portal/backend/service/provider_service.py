"""Provider/venue registry helpers and validation for the portal API."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from data_providers.registry import (
    exchange_slug_for_venue,
    get_provider_config,
    get_venue_config,
    list_providers,
    list_venues,
    normalize_provider_id,
    normalize_venue_id,
    provider_for_venue,
)

from . import instrument_service

logger = logging.getLogger(__name__)


def provider_payloads() -> List[Dict[str, Any]]:
    """Return providers with nested venue metadata for the frontend."""

    venues_by_provider: Dict[str, List[Dict[str, Any]]] = {}
    for venue in list_venues():
        venues_by_provider.setdefault(venue.provider_id, []).append(
            {
                "id": venue.id,
                "label": venue.label,
                "provider_id": venue.provider_id,
                "adapter_id": venue.adapter_id,
                "asset_class": venue.asset_class,
                "symbols_format": venue.symbols_format,
                "metadata": venue.metadata,
            }
        )

    payload: List[Dict[str, Any]] = []
    for provider in list_providers():
        payload.append(
            {
                "id": provider.id,
                "label": provider.label,
                "capabilities": provider.capabilities,
                "supportedVenues": provider.supported_venues,
                "venues": venues_by_provider.get(provider.id, []),
            }
        )
    return payload


def validate_provider_venue(provider_id: Optional[str], venue_id: Optional[str]) -> Tuple[bool, Dict[str, str], Dict[str, Optional[str]]]:
    """Validate provider/venue pairing and return normalized identifiers."""

    errors: Dict[str, str] = {}
    normalized_provider = normalize_provider_id(provider_id)
    normalized_venue = normalize_venue_id(venue_id)

    if normalized_venue and not normalized_provider:
        normalized_provider = provider_for_venue(normalized_venue)

    provider_cfg = get_provider_config(normalized_provider)
    if not provider_cfg:
        errors["provider_id"] = "Select a valid data provider."

    venue_cfg = get_venue_config(normalized_venue) if normalized_venue else None
    if normalized_venue and not venue_cfg:
        errors["venue_id"] = "Select an exchange/venue supported by the provider."
    elif venue_cfg and provider_cfg and venue_cfg.provider_id != provider_cfg.id:
        errors["venue_id"] = "Venue is not supported by the chosen provider."

    if provider_cfg and not normalized_venue:
        venues = provider_cfg.supported_venues
        if len(venues) == 1:
            normalized_venue = venues[0]

    return len(errors) == 0, errors, {"provider_id": normalized_provider, "venue_id": normalized_venue}


def normalize_symbol(symbol: Optional[str]) -> str:
    if not symbol:
        return ""
    return (
        str(symbol)
        .strip()
        .upper()
        .replace(" ", "")
    )


def translate_market(provider_id: Optional[str], venue_id: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Translate provider/venue into datasource/exchange identifiers for downstream services."""

    valid, _, normalized = validate_provider_venue(provider_id, venue_id)
    if not valid:
        return None, None
    provider = normalized.get("provider_id")
    venue = normalized.get("venue_id")
    exchange = exchange_slug_for_venue(venue)
    return provider, exchange


def venue_from_exchange_slug(exchange: Optional[str]) -> Optional[str]:
    if not exchange:
        return None
    slug = str(exchange).strip().lower()
    for venue in list_venues():
        venue_slug = exchange_slug_for_venue(venue.id)
        if venue_slug and venue_slug.lower() == slug:
            return venue.id
    return None


def tick_metadata(provider_id: Optional[str], venue_id: Optional[str], symbol: Optional[str], timeframe: Optional[str] = None) -> Dict[str, Any]:
    """Return tick metadata for the given market selection."""

    _, errors, normalized = validate_provider_venue(provider_id, venue_id)
    if errors:
        return {"errors": errors}

    datasource, exchange = translate_market(normalized.get("provider_id"), normalized.get("venue_id"))
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        return {"errors": {"symbol": "Symbol is required."}}

    instrument = instrument_service.resolve_instrument(datasource, exchange, normalized_symbol)
    if not instrument and datasource == "CCXT":
        instrument, _ = instrument_service.auto_sync_instrument(datasource, exchange, normalized_symbol)

    if not instrument:
        return {"errors": {"symbol": "Tick metadata not found for this symbol."}}

    metadata = {"tick_size": instrument.get("tick_size"), "tick_value": instrument.get("tick_value"), "contract_size": instrument.get("contract_size")}
    metadata["datasource"] = datasource
    metadata["exchange"] = exchange
    metadata["symbol"] = normalized_symbol
    metadata["timeframe"] = timeframe
    return {"metadata": metadata}
