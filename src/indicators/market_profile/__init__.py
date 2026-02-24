"""
Market Profile Indicator Package

Every indicator needs 3 core components:
1. compute/      - Compute indicator state and domain models
2. signals/      - Runtime signal logic
3. overlays/     - Overlay adapters/projectors

Everything else is internal implementation details.
"""

# ============================================================================
# CORE COMPONENTS (what every indicator needs)
# ============================================================================

# 1. Indicator - Computation
from .compute import MarketProfileIndicator, Profile, ValueArea

# 2. Signals - Runtime signal evaluation logic
# See src/indicators/market_profile/signals/

# 3. Overlays - Visualization
from .overlays import market_profile_overlay_adapter

# ============================================================================
# EXPORTS (only expose what users need)
# ============================================================================

__all__ = [
    # Indicator computation
    "MarketProfileIndicator",

    # Domain types
    "Profile",
    "ValueArea",

    # Visualization
    "market_profile_overlay_adapter",
]
