from __future__ import annotations

"""Provider and venue registry for market data integrations.

This module now supports dynamic registration via decorators to make it easy
for developers to add new providers/venues without editing static lists.
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional


@dataclass(frozen=True)
class ProviderConfig:
    id: str
    label: str
    supported_venues: List[str] = field(default_factory=list)
    capabilities: Dict[str, object] = field(default_factory=dict)
    required_secrets: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class VenueConfig:
    id: str
    label: str
    provider_id: str
    adapter_id: Optional[str] = None
    asset_class: Optional[str] = None
    symbols_format: Optional[str] = None
    metadata: Dict[str, object] = field(default_factory=dict)
    required_secrets: List[str] = field(default_factory=list)


class _Registry:
    def __init__(self) -> None:
        self._providers: Dict[str, ProviderConfig] = {}
        self._venues: Dict[str, VenueConfig] = {}

    def register_provider(self, cfg: ProviderConfig) -> ProviderConfig:
        if cfg.id in self._providers:
            # Avoid duplicate registrations when decorators and bootstrap both run.
            return self._providers[cfg.id]
        self._providers[cfg.id] = cfg
        return cfg

    def register_venue(self, cfg: VenueConfig) -> VenueConfig:
        if cfg.id in self._venues:
            return self._venues[cfg.id]
        self._venues[cfg.id] = cfg
        return cfg

    # Decorators to allow @register_provider syntax
    def provider(self, **kwargs):
        def decorator(obj: Callable):
            cfg = ProviderConfig(**kwargs)
            self.register_provider(cfg)
            return obj
        return decorator

    def venue(self, **kwargs):
        def decorator(obj: Callable):
            cfg = VenueConfig(**kwargs)
            self.register_venue(cfg)
            return obj
        return decorator

    @property
    def providers(self) -> List[ProviderConfig]:
        return list(self._providers.values())

    @property
    def venues(self) -> List[VenueConfig]:
        return list(self._venues.values())

    def get_provider(self, provider_id: Optional[str]) -> Optional[ProviderConfig]:
        return self._providers.get(normalize_provider_id(provider_id))

    def get_venue(self, venue_id: Optional[str]) -> Optional[VenueConfig]:
        return self._venues.get(normalize_venue_id(venue_id))


_REGISTRY = _Registry()


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
    return _REGISTRY.providers


def list_venues(provider_id: Optional[str] = None) -> List[VenueConfig]:
    pid = normalize_provider_id(provider_id)
    if not pid:
        return _REGISTRY.venues
    return [venue for venue in _REGISTRY.venues if venue.provider_id == pid]


def provider_for_venue(venue_id: Optional[str]) -> Optional[str]:
    venue = _REGISTRY.get_venue(venue_id)
    return venue.provider_id if venue else None


def get_provider_config(provider_id: Optional[str]) -> Optional[ProviderConfig]:
    return _REGISTRY.get_provider(provider_id)


def get_venue_config(venue_id: Optional[str]) -> Optional[VenueConfig]:
    return _REGISTRY.get_venue(venue_id)


def venues_by_provider() -> Dict[str, List[VenueConfig]]:
    mapping: Dict[str, List[VenueConfig]] = {cfg.id: [] for cfg in _REGISTRY.providers}
    for venue in _REGISTRY.venues:
        mapping.setdefault(venue.provider_id, []).append(venue)
    return mapping


def exchange_slug_for_venue(venue_id: Optional[str]) -> Optional[str]:
    venue = get_venue_config(venue_id)
    if not venue:
        return None
    slug = venue.adapter_id or venue.id
    return slug.lower() if isinstance(slug, str) else None


# --- Pre-register built-in providers/venues to keep existing behaviour ---
_REGISTRY.register_provider(
    ProviderConfig(
        id="ALPACA",
        label="Alpaca API",
        supported_venues=["ALPACA"],
        capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["equities"]},
    )
)
_REGISTRY.register_provider(
    ProviderConfig(
        id="YAHOO",
        label="Yahoo Finance",
        supported_venues=["YAHOO"],
        capabilities={"supportsHistorical": True, "supportsLive": False, "supportsOrders": False, "assetClasses": ["equities", "etf"]},
    )
)
_REGISTRY.register_provider(
    ProviderConfig(
        id="INTERACTIVE_BROKERS",
        label="Interactive Brokers",
        supported_venues=["INTERACTIVE_BROKERS"],
        capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["equities", "futures", "options"]},
    )
)
_REGISTRY.register_provider(
    ProviderConfig(
        id="CCXT",
        label="CCXT (multi-exchange)",
        supported_venues=["KRAKEN_PRO", "BINANCE_US", "COINBASE"],
        capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["crypto"]},
    )
)
_REGISTRY.register_provider(
    ProviderConfig(
        id="COINBASE",
        label="Coinbase Direct API",
        supported_venues=["COINBASE_DIRECT"],
        capabilities={"supportsHistorical": True, "supportsLive": True, "supportsOrders": True, "assetClasses": ["crypto"]},
    )
)

_REGISTRY.register_venue(VenueConfig(id="ALPACA", label="Alpaca", provider_id="ALPACA", adapter_id=None, asset_class="equities"))
_REGISTRY.register_venue(VenueConfig(id="YAHOO", label="Yahoo Finance", provider_id="YAHOO", adapter_id=None, asset_class="equities"))
_REGISTRY.register_venue(VenueConfig(id="INTERACTIVE_BROKERS", label="Interactive Brokers", provider_id="INTERACTIVE_BROKERS", adapter_id=None))
_REGISTRY.register_venue(VenueConfig(id="KRAKEN_PRO", label="Kraken Pro", provider_id="CCXT", adapter_id="kraken", asset_class="crypto"))
_REGISTRY.register_venue(VenueConfig(id="BINANCE_US", label="Binance US", provider_id="CCXT", adapter_id="binanceus", asset_class="crypto"))
_REGISTRY.register_venue(VenueConfig(id="COINBASE", label="Coinbase Advanced (CCXT)", provider_id="CCXT", adapter_id="coinbase", asset_class="crypto", required_secrets=[]))
_REGISTRY.register_venue(VenueConfig(id="COINBASE_DIRECT", label="Coinbase Direct API", provider_id="COINBASE", adapter_id=None, asset_class="crypto", required_secrets=["COINBASE_API_KEY", "COINBASE_API_SECRET"]))


__all__ = [
    "ProviderConfig",
    "VenueConfig",
    "list_providers",
    "list_venues",
    "get_provider_config",
    "get_venue_config",
    "provider_for_venue",
    "exchange_slug_for_venue",
    "normalize_provider_id",
    "normalize_venue_id",
    "venues_by_provider",
    "_REGISTRY",
]
