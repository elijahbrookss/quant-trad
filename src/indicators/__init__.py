"""
Indicators package with automatic indicator discovery.

Each indicator is organized as a subpackage containing:
- indicator.py: Core indicator implementation (pure computation)
- overlays.py: Optional overlay adapters for charting
- domain.py: Domain types (Profile, ValueArea, etc.) when needed

Signal rules live separately in signals/rules/ to maintain proper layering.

Example:
    from indicators.market_profile import MarketProfileIndicator
"""

import importlib
import pkgutil
import logging

from .registry import (
    create_indicator,
    get_indicator,
    indicator,
    list_indicators,
)

logger = logging.getLogger(__name__)


def _discover_indicators():
    """
    Auto-discover and import all indicator modules.

    Walks through indicators/ and imports indicator.py files to make them available.
    Does NOT import signal rules (those are in signals/ layer).
    """
    import indicators
    discovered_count = 0

    for importer, modname, ispkg in pkgutil.walk_packages(
        path=indicators.__path__,
        prefix=f"{indicators.__name__}.",
    ):
        # Only import indicator.py modules, skip signals/overlays
        if 'indicator' in modname and not ispkg:
            try:
                importlib.import_module(modname)
                discovered_count += 1
                logger.debug(f"Discovered indicator module: {modname}")
            except Exception as e:
                logger.warning(f"Failed to import indicator module {modname}: {e}")

    logger.info(f"Auto-discovered {discovered_count} indicator modules")


# Trigger auto-discovery
_discover_indicators()

# Explicit exports for commonly used indicators and registry helpers
from .market_profile import MarketProfileIndicator, market_profile_overlay_adapter
from .pivot_level import PivotLevelIndicator
from .trendline import TrendlineIndicator, trendline_overlay_adapter
from .vwap import VWAPIndicator, vwap_overlay_adapter

__all__ = [
    "MarketProfileIndicator",
    "PivotLevelIndicator",
    "TrendlineIndicator",
    "VWAPIndicator",
    "indicator",
    "get_indicator",
    "create_indicator",
    "list_indicators",
    "market_profile_overlay_adapter",
    "trendline_overlay_adapter",
    "vwap_overlay_adapter",
]
