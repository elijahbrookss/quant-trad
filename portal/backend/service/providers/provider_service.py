"""Provider/venue registry helpers and validation for the portal API."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional, Tuple

from data_providers.providers.factory import get_provider
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

from . import persistence_bootstrap  # noqa: F401

from ..market import instrument_service

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


def tick_metadata(
    provider_id: Optional[str],
    venue_id: Optional[str],
    symbol: Optional[str],
    timeframe: Optional[str] = None,
    refresh: bool = False,
    strategy_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Return tick metadata for the given market selection."""

    _, errors, normalized = validate_provider_venue(provider_id, venue_id)
    if errors:
        return {"errors": errors}

    datasource, exchange = translate_market(normalized.get("provider_id"), normalized.get("venue_id"))
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        return {"errors": {"symbol": "Symbol is required."}}

    instrument = instrument_service.resolve_instrument(datasource, exchange, normalized_symbol)
    error: Optional[str] = None
    # Always re-validate Alpaca instruments so asset-class checks (e.g., futures vs. equities)
    # surface immediately during the symbol step.
    force_validate = (datasource or "").upper() in {"ALPACA", "CCXT"}
    if refresh or not instrument or force_validate:
        instrument, error = instrument_service.validate_instrument(
            datasource,
            exchange,
            normalized_symbol,
            provider_id=normalized.get("provider_id"),
            venue_id=normalized.get("venue_id"),
            force_refresh=refresh,
        )

    if error:
        return {"errors": {"symbol": error}}
    if not instrument:
        return {"errors": {"symbol": "Tick metadata not found for this symbol."}}

    if strategy_id and instrument.get("id"):
        try:
            from ..strategies.strategy_service import persistence as strategy_persistence

            removed_orphans = strategy_persistence.delete_orphan_strategy_instrument_links(strategy_id)
            if removed_orphans:
                logger.info(
                    "strategy_instrument_orphans_removed | strategy=%s | removed=%s",
                    strategy_id,
                    removed_orphans,
                )
            links = strategy_persistence.list_strategy_instrument_links(strategy_id)
            for link in links:
                if (link.get("symbol") or "").upper() == normalized_symbol and link.get("instrument_id") != instrument.get("id"):
                    strategy_persistence.delete_strategy_instrument(strategy_id, link.get("instrument_id"))
            strategy_persistence.upsert_strategy_instrument(
                strategy_id=strategy_id,
                instrument_id=instrument.get("id"),
                snapshot=instrument,
            )
        except Exception as exc:
            logger.warning(
                "strategy_instrument_link_refresh_failed | strategy=%s symbol=%s error=%s",
                strategy_id,
                normalized_symbol,
                exc,
            )

    metadata = {
        "tick_size": instrument.get("tick_size"),
        "tick_value": instrument.get("tick_value"),
        "contract_size": instrument.get("contract_size"),
        "can_short": instrument.get("can_short"),
        "short_requires_borrow": instrument.get("short_requires_borrow"),
        "has_funding": instrument.get("has_funding"),
        "expiry_ts": instrument.get("expiry_ts"),
        "margin_rates": instrument.get("margin_rates"),
        "currency": instrument.get("quote_currency"),
        "quote_currency": instrument.get("quote_currency"),
        "base_currency": instrument.get("base_currency"),
    }
    if instrument.get("instrument_type"):
        metadata["instrument_type"] = instrument.get("instrument_type")
    metadata["datasource"] = datasource
    metadata["exchange"] = exchange
    metadata["symbol"] = normalized_symbol
    metadata["timeframe"] = timeframe
    return {"metadata": metadata}
