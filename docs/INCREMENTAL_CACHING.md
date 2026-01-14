## Incremental Caching for Indicators

A **zero-setup, plug-and-play** caching system that lets your indicator cache anything it needs for efficient incremental updates.

---

## Quick Start (30 seconds)

**1. Add the decorator:**

```python
from indicators.runtime import incremental_cacheable

@incremental_cacheable("my_indicator")
@indicator(name="my_indicator", inputs=["ohlc"], outputs=["data"])
class MyIndicator(ComputeIndicator):
    NAME = "my_indicator"
```

**2. Implement the method:**

```python
    @classmethod
    def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
        # Check cache
        cached = cache.get_range(inst_id, ctx.symbol, my_keys)

        if all_data_cached:
            return cls._from_cached(cached, **kwargs)

        # Compute missing data
        df = provider.get_ohlcv(ctx)
        instance = cls(df, **kwargs)

        # Store in cache
        for key, value in instance.get_data():
            cache.set(inst_id, ctx.symbol, key, value)

        return instance
```

**Done!** The framework automatically uses your cache when available.

---

## What Can You Cache?

**Anything you want:**
- ✅ Daily/weekly/monthly calculations
- ✅ Session-based computations
- ✅ Pivot levels
- ✅ Volume profiles
- ✅ Anchored calculations
- ✅ Expensive aggregations
- ✅ ML model predictions
- ✅ Custom data structures

**The cache is a simple key-value store:**
```python
cache.set(inst_id, symbol, key, value)
value = cache.get(inst_id, symbol, key)
```

**Keys can be anything hashable:**
- Date strings: `"2025-01-13"`
- Timestamps: `1736726400`
- Session IDs: `"session_morning_2025-01-13"`
- Custom identifiers: `"pivot_weekly_W1_2025"`

---

## Complete Examples

### Example 1: Daily Profiles (Market Profile Style)

```python
from datetime import datetime
from indicators.runtime import incremental_cacheable

@incremental_cacheable("my_profile")
@indicator(name="my_profile", inputs=["ohlc"], outputs=["profiles"])
class MyProfileIndicator(ComputeIndicator):
    NAME = "my_profile"

    @classmethod
    def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
        from datetime import timedelta

        # Calculate expected date keys
        start = datetime.fromisoformat(ctx.start)
        end = datetime.fromisoformat(ctx.end)
        date_keys = []
        current = start
        while current <= end:
            date_keys.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        # Check cache
        cached_profiles = cache.get_range(inst_id, ctx.symbol, date_keys)

        # If fully cached, skip data fetching
        if len(cached_profiles) >= len(date_keys):
            return cls._create_from_cached(cached_profiles, **kwargs)

        # Fetch data and compute
        df = provider.get_ohlcv(ctx)
        instance = cls(df, **kwargs)

        # Cache newly computed profiles
        for profile in instance.profiles:
            date_key = profile.date.strftime("%Y-%m-%d")
            cache.set(inst_id, ctx.symbol, date_key, profile)

        return instance

    @classmethod
    def _create_from_cached(cls, cached_profiles, **kwargs):
        """Create instance from cached data without fetching/computing."""
        instance = cls.__new__(cls)
        instance.profiles = sorted(cached_profiles.values(), key=lambda p: p.date)
        # Set other attributes as needed
        return instance
```

### Example 2: Session-Based Levels (Pivot Points)

```python
@incremental_cacheable("pivot_levels")
@indicator(name="pivot_levels", inputs=["ohlc"], outputs=["levels"])
class PivotLevelIndicator(ComputeIndicator):
    NAME = "pivot_levels"

    @classmethod
    def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
        session_type = kwargs.get("session_type", "daily")  # daily, weekly, monthly

        # Build session keys based on timeframe
        session_keys = cls._generate_session_keys(ctx.start, ctx.end, session_type)

        # Check cache
        cached_levels = cache.get_range(inst_id, ctx.symbol, session_keys)

        # Compute missing sessions only
        if len(cached_levels) < len(session_keys):
            df = provider.get_ohlcv(ctx)
            instance = cls(df, session_type=session_type, **kwargs)

            # Cache computed levels
            for session_id, levels in instance.session_levels.items():
                cache.set(inst_id, ctx.symbol, session_id, levels)

            return instance
        else:
            # Fully cached
            return cls._from_cached(cached_levels, session_type=session_type, **kwargs)
```

### Example 3: Time-Independent Caching (ML Predictions)

```python
@incremental_cacheable("ml_indicator")
@indicator(name="ml_indicator", inputs=["ohlc"], outputs=["signals"])
class MLIndicator(ComputeIndicator):
    NAME = "ml_indicator"

    @classmethod
    def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
        model_version = kwargs.get("model_version", "v1")

        # Cache key could be bar timestamp
        df = provider.get_ohlcv(ctx)

        predictions = []
        for timestamp, row in df.iterrows():
            ts_key = int(timestamp.timestamp())

            # Check cache for this bar's prediction
            cached_pred = cache.get(inst_id, ctx.symbol, ts_key)

            if cached_pred is not None:
                predictions.append(cached_pred)
            else:
                # Compute prediction
                pred = cls._run_model(row, model_version)
                cache.set(inst_id, ctx.symbol, ts_key, pred)
                predictions.append(pred)

        instance = cls(df, **kwargs)
        instance.predictions = predictions
        return instance
```

---

## API Reference

### IncrementalCache Methods

```python
# Single item operations
cache.set(inst_id: str, symbol: str, key: Hashable, value: Any) -> None
cache.get(inst_id: str, symbol: str, key: Hashable) -> Optional[Any]
cache.has(inst_id: str, symbol: str, key: Hashable) -> bool

# Batch operations
cache.set_many(inst_id: str, symbol: str, items: Dict[Hashable, Any]) -> None
cache.get_range(inst_id: str, symbol: str, keys: List[Hashable]) -> Dict[Hashable, Any]

# Cleanup operations
cache.purge_indicator(inst_id: str) -> None  # Clear all for this indicator
cache.purge_symbol(inst_id: str, symbol: str) -> None  # Clear for indicator+symbol
cache.clear() -> None  # Clear everything

# Stats
cache.get_stats() -> Dict[str, int]  # Get cache statistics
```

### Cache Behavior

- **Deep copying**: Values are deep copied on get/set to prevent mutations
- **LRU eviction**: Oldest entries removed when max_entries reached (default: 10,000)
- **Auto-purge**: Cache cleared when indicator parameters change
- **Thread-safe**: OrderedDict provides basic thread safety for Python GIL

---

## Design Patterns

### Pattern 1: Skip Data Fetch on Full Cache Hit

```python
@classmethod
def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
    cached_data = cache.get_range(inst_id, ctx.symbol, expected_keys)

    if len(cached_data) >= len(expected_keys):
        # Fully cached - no need to fetch data!
        return cls._minimal_instance_from_cache(cached_data, **kwargs)

    # Partial cache - fetch and compute
    df = provider.get_ohlcv(ctx)
    ...
```

### Pattern 2: Incremental Append

```python
@classmethod
def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
    all_keys = cls._generate_keys(ctx.start, ctx.end)
    cached = cache.get_range(inst_id, ctx.symbol, all_keys)

    # Identify missing keys
    missing_keys = [k for k in all_keys if k not in cached]

    if missing_keys:
        # Fetch only the missing date range
        missing_ctx = cls._build_context_for_keys(missing_keys, ctx)
        missing_df = provider.get_ohlcv(missing_ctx)

        # Compute and cache missing data
        new_data = cls._compute_for_keys(missing_df, missing_keys)
        for key, value in new_data.items():
            cache.set(inst_id, ctx.symbol, key, value)

        # Combine cached + new
        all_data = {**cached, **new_data}
    else:
        all_data = cached

    return cls._from_data(all_data, **kwargs)
```

### Pattern 3: Hierarchical Caching

```python
@classmethod
def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
    # Cache daily calculations
    daily_data = cls._get_or_compute_daily(cache, provider, ctx, inst_id)

    # Derive weekly/monthly from cached daily (not cached themselves)
    weekly_data = cls._aggregate_to_weekly(daily_data)
    monthly_data = cls._aggregate_to_monthly(daily_data)

    instance = cls(**kwargs)
    instance.daily = daily_data
    instance.weekly = weekly_data
    instance.monthly = monthly_data
    return instance
```

---

## Testing Your Implementation

```python
def test_my_indicator_incremental_cache():
    from indicators.runtime import IncrementalCache

    cache = IncrementalCache()

    # First run - compute everything
    indicator1 = MyIndicator.from_context_with_incremental_cache(
        provider=provider,
        ctx=DataContext(symbol="BTC", start="2025-01-01", end="2025-01-05", interval="1h"),
        cache=cache,
        inst_id="test-1"
    )

    # Verify data was cached
    stats = cache.get_stats()
    assert stats["total_entries"] > 0

    # Second run - extend range (should use cache for 1-5, compute 6-10)
    provider_mock.reset_mock()
    indicator2 = MyIndicator.from_context_with_incremental_cache(
        provider=provider,
        ctx=DataContext(symbol="BTC", start="2025-01-01", end="2025-01-10", interval="1h"),
        cache=cache,
        inst_id="test-1"
    )

    # Should have more data now
    assert len(indicator2.data) > len(indicator1.data)

    # Cache purge test
    cache.purge_indicator("test-1")
    assert cache.get_stats()["total_entries"] == 0
```

---

## Best Practices

### ✅ DO:
1. **Cache atomic units** (single days, sessions, bars)
2. **Use stable, predictable keys** (dates, timestamps)
3. **Handle partial cache gracefully** (compute only missing data)
4. **Log cache hit/miss** for debugging
5. **Return early if fully cached** (skip data fetch)

### ❌ DON'T:
1. **Cache final overlay payloads** (use `overlay_cache` for that)
2. **Cache configuration-dependent results** (cache is auto-purged on param changes)
3. **Mutate cached values** (cache uses deepcopy to prevent this)
4. **Over-cache** (don't cache if computation is trivial)

---

## Migration from Profile Cache

If you're currently using `ProfileCache`:

```python
# Old (profile-specific)
from indicators.runtime import profile_cacheable

@profile_cacheable("market_profile")
class MarketProfileIndicator:
    @classmethod
    def from_context_with_cache(cls, provider, ctx, profile_cache, inst_id, **kwargs):
        profiles = profile_cache.get_range(inst_id, symbol, start, end)
        ...

# New (generic)
from indicators.runtime import incremental_cacheable

@incremental_cacheable("market_profile")
class MarketProfileIndicator:
    @classmethod
    def from_context_with_incremental_cache(cls, provider, ctx, cache, inst_id, **kwargs):
        # Same logic, different method name and cache param
        date_keys = ["2025-01-10", "2025-01-11", ...]
        profiles = cache.get_range(inst_id, ctx.symbol, date_keys)
        ...
```

**ProfileCache is now legacy** - use `IncrementalCache` for all new indicators.

---

## Debugging

Enable debug logging:

```python
import logging
logging.getLogger("core.logger").setLevel(logging.DEBUG)
```

Check cache stats:

```python
stats = ctx.incremental_cache.get_stats()
print(f"Entries: {stats['total_entries']}/{stats['max_entries']}")
print(f"Indicators: {stats['indicators']}, Symbols: {stats['symbols']}")
```

---

## Framework Integration

The framework automatically:
- ✅ Passes `incremental_cache` to your method when available
- ✅ Purges cache when indicator parameters change
- ✅ Falls back to regular `from_context` if cache unavailable
- ✅ Handles cache concurrency (Python GIL protection)
- ✅ Enforces LRU eviction at configured limit

**You just write the caching logic - the framework does the rest.**

---

## Questions?

- **When should I use this?** For computationally expensive, period-based indicators
- **What shouldn't I cache?** Final rendered overlays (use `overlay_cache`)
- **How much can I cache?** Default 10,000 entries (configurable)
- **Is it thread-safe?** Yes, within Python GIL constraints
- **Does it persist across restarts?** No, it's in-memory only

See also:
- [Complete example: MarketProfileIndicator](../src/indicators/market_profile/indicator.py)
- [Tests](../tests/test_indicators/test_market_profile_cache_integration.py)
- [API docs](../src/indicators/runtime/incremental_cache.py)
