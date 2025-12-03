from __future__ import annotations

"""Provider and venue registry for market data integrations."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ProviderConfig:
    id: str
    label: str
    supported_venues: List[str] = field(default_factory=list)
    capabilities: Dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class VenueConfig:
    id: str
    label: str
    provider_id: str
    adapter_id: Optional[str] = None
    asset_class: Optional[str] = None
    symbols_format: Optional[str] = None
    metadata: Dict[str, object] = field(default_factory=dict)


PROVIDER_CONFIGS: List[ProviderConfig] = [
    ProviderConfig(
        id="ALPACA",
        label="Alpaca API",
        supported_venues=["ALPACA"],
        capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["equities"]},
    ),
    ProviderConfig(
        id="YAHOO",
        label="Yahoo Finance",
        supported_venues=["YAHOO"],
        capabilities={"supportsHistorical": True, "supportsLive": False, "supportsOrders": False, "assetClasses": ["equities", "etf"]},
    ),
    ProviderConfig(
        id="INTERACTIVE_BROKERS",
        label="Interactive Brokers",
        supported_venues=["INTERACTIVE_BROKERS"],
        capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["equities", "futures", "options"]},
    ),
    ProviderConfig(
        id="CCXT",
        label="CCXT (multi-exchange)",
        supported_venues=["KRAKEN_PRO", "BINANCE_US"],
        capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["crypto"]},
    ),
]


VENUE_CONFIGS: List[VenueConfig] = [
    VenueConfig(id="ALPACA", label="Alpaca", provider_id="ALPACA", adapter_id=None, asset_class="equities"),
    VenueConfig(id="YAHOO", label="Yahoo Finance", provider_id="YAHOO", adapter_id=None, asset_class="equities"),
    VenueConfig(id="INTERACTIVE_BROKERS", label="Interactive Brokers", provider_id="INTERACTIVE_BROKERS", adapter_id=None),
    VenueConfig(id="KRAKEN_PRO", label="Kraken Pro", provider_id="CCXT", adapter_id="kraken", asset_class="crypto"),
    VenueConfig(id="BINANCE_US", label="Binance US", provider_id="CCXT", adapter_id="binanceus", asset_class="crypto"),
]


_PROVIDER_MAP: Dict[str, ProviderConfig] = {cfg.id: cfg for cfg in PROVIDER_CONFIGS}
_VENUE_MAP: Dict[str, VenueConfig] = {cfg.id: cfg for cfg in VENUE_CONFIGS}


def normalize_provider_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip().upper()
    return text or None


def normalize_venue_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip().upper()
    return text or None


def list_providers() -> List[ProviderConfig]:
    return list(PROVIDER_CONFIGS)


def list_venues(provider_id: Optional[str] = None) -> List[VenueConfig]:
    pid = normalize_provider_id(provider_id)
    if not pid:
        return list(VENUE_CONFIGS)
    return [venue for venue in VENUE_CONFIGS if venue.provider_id == pid]


def provider_for_venue(venue_id: Optional[str]) -> Optional[str]:
    venue = _VENUE_MAP.get(normalize_venue_id(venue_id))
    return venue.provider_id if venue else None


def get_provider_config(provider_id: Optional[str]) -> Optional[ProviderConfig]:
    return _PROVIDER_MAP.get(normalize_provider_id(provider_id))


def get_venue_config(venue_id: Optional[str]) -> Optional[VenueConfig]:
    return _VENUE_MAP.get(normalize_venue_id(venue_id))


def venues_by_provider() -> Dict[str, List[VenueConfig]]:
    mapping: Dict[str, List[VenueConfig]] = {cfg.id: [] for cfg in PROVIDER_CONFIGS}
    for venue in VENUE_CONFIGS:
        mapping.setdefault(venue.provider_id, []).append(venue)
    return mapping


def exchange_slug_for_venue(venue_id: Optional[str]) -> Optional[str]:
    venue = get_venue_config(venue_id)
    if not venue:
        return None
    slug = venue.adapter_id or venue.id
    return slug.lower() if isinstance(slug, str) else None
