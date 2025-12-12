"""
Market Profile Indicator Package

Every indicator needs 3 core components:
1. indicator.py  - Compute the indicator values
2. signals.py    - Define signal rules for trade detection
3. overlays.py   - Visualize signals on charts

Everything else is internal implementation details.
"""

# ============================================================================
# CORE COMPONENTS (what every indicator needs)
# ============================================================================

# 1. Indicator - Computation
from .indicator import MarketProfileIndicator

# Domain types (outputs from indicator)
from .domain import Profile, ValueArea

# NOTE: Signals moved to signals/rules/market_profile/
# They are auto-discovered when you import signals package

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
