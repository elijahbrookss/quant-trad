# Indicator Development Standard

**Simple Rule: Every indicator needs 3 files.**

1. **`indicator.py`** - Compute the indicator values
2. **`signals.py`** - Define signal rules for trade detection
3. **`overlays.py`** - Visualize signals on charts

Everything else is optional internal implementation.

---

## Minimal Directory Structure

```
src/indicators/<indicator_name>/
├── __init__.py         # Simple re-exports (see template below)
├── indicator.py        # REQUIRED: Core computation
├── signals.py          # REQUIRED: Signal detection rules
└── overlays.py         # REQUIRED: Chart visualization
```

## Optional Files (for complex indicators)

```
src/indicators/<indicator_name>/
├── ... (3 required files above)
├── _internals.py       # Internal helpers (hidden from users)
├── config.py           # Configuration classes
├── evaluators.py       # Pattern matching logic
├── formatters.py       # Formatting utilities
├── cache.py            # Caching logic
└── utils.py            # Local utility functions
```

**Note**: Prefix internal modules with `_` to signal they're not part of the public API.

---

## Template: `__init__.py`

Keep it simple - just re-export the 3 core components:

```python
"""
<Indicator Name> Indicator Package

Every indicator needs 3 core components:
1. indicator.py  - Compute the indicator values
2. signals.py    - Define signal rules for trade detection
3. overlays.py   - Visualize signals on charts

Everything else is internal implementation details.
"""

# 1. Indicator - Computation
from .indicator import MyIndicator

# 2. Signals - Trade detection (auto-registers via @signal_rule)
from .signals import (
    my_indicator_signal_rule,
)

# 3. Overlays - Visualization
from .overlays import my_indicator_overlay_adapter

__all__ = [
    "MyIndicator",
    "my_indicator_signal_rule",
    "my_indicator_overlay_adapter",
]
```

---

## Template: `indicator.py`

The core indicator class that computes values:

```python
"""Core indicator computation."""

from typing import Dict, List, Any
import pandas as pd
from indicators.base import BaseIndicator

class MyIndicator(BaseIndicator):
    """
    Brief description of what this indicator computes.
    """
    NAME = "my_indicator"  # Used for registration

    def __init__(self, df: pd.DataFrame, **kwargs):
        super().__init__(df)
        # Initialize your indicator
        self.results = self._compute()

    def _compute(self) -> Any:
        """Main computation logic."""
        # Your indicator logic here
        pass
```

---

## Template: `signals.py`

Signal rules that detect trading opportunities:

```python
"""Signal rules for detecting trade setups."""

from typing import Any, Dict, List, Mapping
from signals.engine.signal_generator import signal_rule

@signal_rule(
    "my_indicator",  # Use string to avoid circular imports
    rule_id="my_signal_rule",
    label="My Signal",
    description="Description of what this signal detects"
)
def my_indicator_signal_rule(
    context: Mapping[str, Any],
    payload: Any
) -> List[Dict[str, Any]]:
    """
    Detect trading signals based on indicator data.

    Args:
        context: Strategy context with indicator data
        payload: Additional context data

    Returns:
        List of signal dictionaries with metadata
    """
    signals = []

    # Your signal detection logic here
    # Example:
    # if some_condition:
    #     signals.append({
    #         "bar_index": idx,
    #         "direction": "long",
    #         "metadata": {...}
    #     })

    return signals
```

---

## Template: `overlays.py`

Visualization adapter for displaying signals on charts:

```python
"""Overlay adapter for visualizing signals on charts."""

from typing import Any, Dict, List, Sequence
import pandas as pd
from signals.base import BaseSignal
from signals.engine.signal_generator import overlay_adapter

@overlay_adapter("my_indicator")
def my_indicator_overlay_adapter(
    signals: Sequence[BaseSignal],
    plot_df: pd.DataFrame,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """
    Convert signals into chart overlays (shapes, markers, etc).

    Args:
        signals: List of signals to visualize
        plot_df: DataFrame with OHLCV data
        **kwargs: Additional visualization options

    Returns:
        List of overlay specifications for chart rendering
    """
    overlays = []

    for signal in signals:
        # Convert signal to visual overlay
        # Example:
        # overlays.append({
        #     "kind": "marker",
        #     "time": signal.bar_timestamp,
        #     "price": signal.price,
        #     "shape": "arrow",
        #     "color": "#00ff00" if signal.direction == "long" else "#ff0000"
        # })
        pass

    return overlays
```

---

## Quick Start: Creating a New Indicator

### 1. Create the package structure

```bash
mkdir src/indicators/my_indicator
touch src/indicators/my_indicator/__init__.py
touch src/indicators/my_indicator/indicator.py
touch src/indicators/my_indicator/signals.py
touch src/indicators/my_indicator/overlays.py
```

### 2. Implement the 3 required files

Use the templates above as starting points.

### 3. Register the indicator

Add to `src/indicators/__init__.py`:
```python
from .my_indicator import MyIndicator
```

Add to `portal/backend/main.py`:
```python
import indicators.my_indicator  # noqa: F401
```

### 4. Test

```python
from indicators.my_indicator import MyIndicator

indicator = MyIndicator(df)
# Signals are automatically registered and available
```

---

## Benefits of This Pattern

✅ **Simple**: Only 3 files required, easy to understand
✅ **Collocated**: Everything for one indicator in one folder
✅ **Auto-registration**: Import triggers signal registration
✅ **Scalable**: Add complexity only when needed with `_internals.py`
✅ **Clean imports**: Users only see what matters

---

## Advanced: Internal Implementation Details

If your indicator needs complex helpers, cache management, or utilities:

1. Create `_internals.py` with all helper functions
2. Import from `_internals.py` in your 3 core files
3. Don't export `_internals` from `__init__.py`

Users never see the complexity - they only see the 3 core components.

---

## Example: Market Profile Structure

```
src/indicators/market_profile/
├── __init__.py              # Simple: exports 3 components
├── indicator.py             # Core: Profile computation
├── signals.py               # Core: Breakout/retest detection
├── overlays.py              # Core: Value area visualization
│
├── _internals.py            # Hidden: Re-exports internal modules
├── cache.py                 # Hidden: Data fetching cache
├── config.py                # Hidden: Configuration classes
├── evaluators.py            # Hidden: Pattern evaluation
├── formatters.py            # Hidden: Formatting helpers
└── utils.py                 # Hidden: Touch marker detection
```

**What users see**: Just 3 components (indicator, signals, overlays)
**What actually exists**: 10 files with complex logic
**Result**: Simple mental model, powerful implementation

---

## FAQs

**Q: Do I need all the optional files?**
A: No! Start with just the 3 required files. Add complexity only when needed.

**Q: Can I use helper functions?**
A: Yes! Put them in `_internals.py` or directly in your 3 core files.

**Q: What about configuration?**
A: Simple config can go in `indicator.py`. Complex config goes in `config.py`.

**Q: How do I avoid circular imports?**
A: Use string identifiers in decorators: `@signal_rule("my_indicator", ...)` instead of `@signal_rule(MyIndicator, ...)`

**Q: Where do I put tests?**
A: Follow the same pattern: `tests/indicators/my_indicator/test_*.py`
