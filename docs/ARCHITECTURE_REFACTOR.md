# Market Profile Architecture Refactoring

## Problem Statement

Current implementation violates separation of concerns:
- **Indicator** knows about matplotlib, mplfinance, signal decorators
- **Signals** are inside indicator package (wrong dependency direction)
- **Overlays** are inside indicator package (wrong dependency direction)
- **Cache** is mixed with computation logic
- **UI concerns** (`extend_to_chart_end`, `market_profile_breakout_confirmation_bars`) leak into domain

## Proposed Layered Architecture

```
src/
├── indicators/
│   └── market_profile/
│       ├── __init__.py              # Pure computation exports
│       ├── indicator.py             # MarketProfileIndicator (pure)
│       ├── domain.py                # Profile, ValueArea, TPOHistogram (domain types)
│       └── _internal/               # Hidden implementation details
│           ├── __init__.py
│           ├── computation.py       # TPO histogram, value area extraction
│           ├── bin_size.py          # Bin size inference
│           └── merging.py           # Profile merging logic
│
├── signals/
│   └── rules/
│       └── market_profile/
│           ├── __init__.py          # Signal rule exports
│           ├── breakout.py          # Breakout detection
│           ├── retest.py            # Retest detection
│           └── _evaluators/         # Pattern evaluation helpers
│               ├── __init__.py
│               ├── breakout_eval.py
│               └── retest_eval.py
│
└── visualization/
    └── overlays/
        └── market_profile/
            ├── __init__.py          # Overlay adapter exports
            ├── value_area.py        # Value area boxes
            ├── poc_lines.py         # POC lines
            └── touch_markers.py     # Touch markers
```

## Layer Responsibilities

### Layer 1: Indicators (Pure Computation)
**Location**: `src/indicators/market_profile/`

**Responsibilities**:
- Compute TPO histograms
- Extract value areas (VAH, VAL, POC)
- Merge overlapping profiles
- Return structured domain objects

**Dependencies**:
- pandas, numpy (data)
- Own domain types

**NO dependencies on**:
- matplotlib, mplfinance
- signals package
- visualization package

**Exports**:
```python
from indicators.market_profile import MarketProfileIndicator
from indicators.market_profile.domain import Profile, ValueArea
```

### Layer 2: Signals (Trade Detection)
**Location**: `src/signals/rules/market_profile/`

**Responsibilities**:
- Detect breakout patterns
- Detect retest patterns
- Evaluate signal conditions
- Return signal metadata

**Dependencies**:
- indicators.market_profile (reads outputs)
- signals.engine.signal_generator (@signal_rule decorator)

**Exports**:
```python
from signals.rules.market_profile import (
    market_profile_breakout_rule,
    market_profile_retest_rule,
)
```

### Layer 3: Visualization (UI Adapters)
**Location**: `src/visualization/overlays/market_profile/`

**Responsibilities**:
- Transform domain objects → UI payloads
- Generate chart overlays (boxes, lines, markers)
- Handle UI-specific options (colors, extensions, etc.)

**Dependencies**:
- indicators.market_profile.domain (domain types)
- signals.base (BaseSignal)
- visualization libraries (if needed)

**Exports**:
```python
from visualization.overlays.market_profile import market_profile_overlay_adapter
```

## Key Design Decisions

### 1. Domain Types Package

Create `domain.py` with pure data classes:

```python
# src/indicators/market_profile/domain.py
from dataclasses import dataclass
from typing import Optional
import pandas as pd

@dataclass(frozen=True)
class ValueArea:
    """Value area for a trading session."""
    vah: float  # Value Area High
    val: float  # Value Area Low
    poc: float  # Point of Control

@dataclass(frozen=True)
class Profile:
    """Market profile for a trading session or merged period."""
    start: pd.Timestamp
    end: pd.Timestamp
    value_area: ValueArea
    session_count: int = 1
    tpo_histogram: Optional[dict] = None  # price -> count

    @property
    def vah(self) -> float:
        return self.value_area.vah

    @property
    def val(self) -> float:
        return self.value_area.val

    @property
    def poc(self) -> float:
        return self.value_area.poc
```

**Benefits**:
- Immutable, hashable, type-safe
- Clear API contract
- Easy to test
- No UI concerns

### 2. Pure Indicator Class

```python
# src/indicators/market_profile/indicator.py
class MarketProfileIndicator(BaseIndicator):
    """
    Computes TPO-based market profiles.

    Returns structured Profile objects with value areas.
    No knowledge of plotting or signal detection.
    """
    NAME = "market_profile"

    def __init__(self, df: pd.DataFrame, bin_size: Optional[float] = None):
        super().__init__(df)
        self.bin_size = bin_size or self._infer_bin_size()
        self._profiles = self._compute_profiles()

    def get_profiles(self) -> List[Profile]:
        """Return all computed profiles."""
        return self._profiles

    def get_merged_profiles(self, threshold: float = 0.6, min_sessions: int = 3) -> List[Profile]:
        """Return merged profiles based on overlap."""
        return merge_profiles(self._profiles, threshold, min_sessions)

    # NO: to_overlays(), to_lightweight(), extend_to_chart_end
    # NO: market_profile_breakout_confirmation_bars
    # These are UI/signal concerns!
```

### 3. Signal Rules in Separate Package

```python
# src/signals/rules/market_profile/breakout.py
from signals.engine.signal_generator import signal_rule
from indicators.market_profile import MarketProfileIndicator

@signal_rule(
    "market_profile",
    rule_id="market_profile_breakout",
    label="Value Area Breakout",
    description="Price closes outside value area"
)
def market_profile_breakout_rule(context, payload):
    """
    Detect breakouts from indicator outputs.

    Signal rule depends ON indicator, not vice versa.
    """
    indicator: MarketProfileIndicator = context.get("market_profile")
    profiles = indicator.get_profiles()

    # Detection logic here
    return signals
```

### 4. Overlay Adapters in Separate Package

```python
# src/visualization/overlays/market_profile/value_area.py
from visualization.base import OverlayAdapter
from indicators.market_profile.domain import Profile

class ValueAreaOverlay(OverlayAdapter):
    """Renders value area boxes on charts."""

    def render(self, profiles: List[Profile], plot_df: pd.DataFrame, **options) -> List[dict]:
        """
        Transform profiles into chart boxes.

        UI options (extend_to_end, colors) live here, not in indicator.
        """
        extend_to_end = options.get("extend_to_chart_end", True)

        overlays = []
        for profile in profiles:
            overlays.append({
                "kind": "rect",
                "x1": profile.start,
                "x2": profile.end if not extend_to_end else plot_df.index[-1],
                "y1": profile.val,
                "y2": profile.vah,
                # ... styling
            })
        return overlays
```

## Migration Path

### Phase 1: Create Domain Types
1. Create `domain.py` with Profile, ValueArea classes
2. No breaking changes yet

### Phase 2: Refactor Indicator (Pure Computation)
1. Remove matplotlib/mplfinance imports
2. Remove `to_overlays()`, `to_lightweight()` methods
3. Remove UI-related __init__ params
4. Return Profile objects instead of dicts
5. Move helpers to `_internal/`

### Phase 3: Extract Signals
1. Create `src/signals/rules/market_profile/`
2. Move signal rules from `indicators/market_profile/signals.py`
3. Move evaluators to `_evaluators/`
4. Update imports

### Phase 4: Extract Overlays
1. Create `src/visualization/overlays/market_profile/`
2. Move overlay adapter from `indicators/market_profile/overlays.py`
3. Move UI logic (extend_to_end, touch markers, etc.)
4. Update imports

### Phase 5: Clean Up
1. Remove old files
2. Update `portal/backend/main.py` imports
3. Update documentation
4. Test end-to-end

## Tradeoffs

### Benefits
✅ **Clear separation of concerns**: Indicator doesn't know about UI or signals
✅ **Testability**: Each layer can be tested independently
✅ **Reusability**: Indicator outputs can be used in different contexts
✅ **Dependency flow**: Correct direction (signals/viz depend on indicators)
✅ **Explicit imports**: No hidden side-effects in __init__.py
✅ **Scalability**: Pattern works for all future indicators

### Costs
⚠️ **More packages**: 3 packages instead of 1
⚠️ **Import updates**: Existing code needs to update imports
⚠️ **Initial work**: Refactoring takes time

### Not Changed
✅ Existing behavior preserved
✅ Same functionality, better structure
✅ No new abstractions, just better organization

## Example: End-to-End Flow

```python
# 1. User creates indicator (pure computation)
from indicators.market_profile import MarketProfileIndicator

indicator = MarketProfileIndicator(df, bin_size=0.25)
profiles = indicator.get_profiles()  # Returns List[Profile]

# 2. Backend registers signal rules (depends on indicator)
import signals.rules.market_profile  # Registers @signal_rule decorators

# 3. Chart requests overlays (depends on indicator outputs)
from visualization.overlays.market_profile import render_value_areas

overlays = render_value_areas(
    profiles=profiles,
    plot_df=df,
    extend_to_chart_end=True,  # UI concern lives in viz layer
)
```

## Success Criteria

1. ✅ MarketProfileIndicator has zero imports of matplotlib/mplfinance
2. ✅ MarketProfileIndicator has zero knowledge of @signal_rule decorator
3. ✅ Signal rules depend on indicator, not vice versa
4. ✅ Overlay adapters depend on indicator, not vice versa
5. ✅ `indicators/market_profile/__init__.py` has no @decorator side-effects
6. ✅ Domain types are immutable and type-safe
7. ✅ Each layer has <500 lines per file (current: 669 in indicator.py)
8. ✅ Existing functionality works identically

## Next Steps

1. Create `domain.py` with Profile and ValueArea types
2. Show refactored indicator.py structure
3. Show signals package structure
4. Show visualization package structure
5. Execute migration incrementally with tests
