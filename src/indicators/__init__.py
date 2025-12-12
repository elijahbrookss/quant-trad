"""
Indicators package with automatic indicator discovery.

Each indicator is organized as a subpackage containing:
- indicator.py: Core indicator implementation (pure computation)
- domain.py: Domain types (Profile, ValueArea, etc.)
- _internal/: Internal implementation details

Signal rules live separately in signals/rules/ to maintain proper layering.

Example:
    from indicators.market_profile import MarketProfileIndicator
"""

import importlib
import pkgutil
import logging

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

# Explicit exports for commonly used indicators
from .market_profile import MarketProfileIndicator

__all__ = [
    "MarketProfileIndicator",
]
