"""Integration test for Market Profile incremental caching."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

pd = pytest.importorskip("pandas")
np = pytest.importorskip("numpy")

from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator
from indicators.runtime.incremental_cache import IncrementalCache


def create_mock_ohlcv_data(start_date, days=10):
    """Create mock OHLCV data for testing."""
    # Generate 30-minute data for the specified number of days
    date_range = pd.date_range(
        start=start_date,
        periods=days * 48,  # 48 30-min intervals per day
        freq="30min"
    )

    df = pd.DataFrame({
        "open": np.random.uniform(90, 110, len(date_range)),
        "high": np.random.uniform(95, 115, len(date_range)),
        "low": np.random.uniform(85, 105, len(date_range)),
        "close": np.random.uniform(90, 110, len(date_range)),
        "volume": np.random.uniform(1000, 5000, len(date_range)),
    }, index=date_range)

    return df


def test_market_profile_with_cache_first_run():
    """Test that profiles are computed and cached on first run."""
    cache = IncrementalCache(max_entries=1000)
    inst_id = "test-mp-1"
    symbol = "BTCUSD"

    # Create mock data provider
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 10)
    mock_df = create_mock_ohlcv_data(start_date, days=10)

    provider = MagicMock()
    provider.get_ohlcv.return_value = mock_df

    ctx = DataContext(
        symbol=symbol,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        interval="30m"
    )

    # First run - should compute all profiles
    indicator = MarketProfileIndicator.from_context_with_incremental_cache(
        provider=provider,
        ctx=ctx,
        cache=cache,
        inst_id=inst_id,
        days_back=10,
    )

    # Verify profiles were computed
    profiles = indicator.get_profiles()
    assert len(profiles) > 0

    # Verify profiles were cached (build date keys)
    date_keys = []
    current = start_date
    while current <= end_date:
        date_keys.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    cached_range = cache.get_range(inst_id, symbol, date_keys)
    assert len(cached_range) > 0


def test_market_profile_with_cache_incremental():
    """Test incremental computation with partially cached profiles."""
    cache = IncrementalCache(max_entries=1000)
    inst_id = "test-mp-2"
    symbol = "ETHUSD"

    # First run: compute profiles for days 1-5
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 5)
    mock_df_1 = create_mock_ohlcv_data(start_date, days=5)

    provider = MagicMock()
    provider.get_ohlcv.return_value = mock_df_1

    ctx_1 = DataContext(
        symbol=symbol,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        interval="30m"
    )

    indicator_1 = MarketProfileIndicator.from_context_with_incremental_cache(
        provider=provider,
        ctx=ctx_1,
        cache=cache,
        inst_id=inst_id,
        days_back=5,
    )

    profiles_1 = indicator_1.get_profiles()
    initial_profile_count = len(profiles_1)
    assert initial_profile_count > 0

    # Track how many times get_ohlcv was called
    call_count_1 = provider.get_ohlcv.call_count

    # Second run: request days 1-10 (5 days cached, 5 days new)
    end_date_2 = datetime(2025, 1, 10)
    mock_df_2 = create_mock_ohlcv_data(start_date, days=10)
    provider.get_ohlcv.return_value = mock_df_2

    ctx_2 = DataContext(
        symbol=symbol,
        start=start_date.isoformat(),
        end=end_date_2.isoformat(),
        interval="30m"
    )

    indicator_2 = MarketProfileIndicator.from_context_with_incremental_cache(
        provider=provider,
        ctx=ctx_2,
        cache=cache,
        inst_id=inst_id,
        days_back=10,
    )

    profiles_2 = indicator_2.get_profiles()
    # Should have more profiles now (covering 10 days instead of 5)
    assert len(profiles_2) >= initial_profile_count

    # Verify cache was used (new profiles were added)
    date_keys_2 = []
    current = start_date
    while current <= end_date_2:
        date_keys_2.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    cached_range = cache.get_range(inst_id, symbol, date_keys_2)
    assert len(cached_range) >= len(profiles_2)


def test_market_profile_cache_purge():
    """Test that purging works correctly."""
    cache = IncrementalCache(max_entries=1000)
    inst_id = "test-mp-3"
    symbol = "XAUUSD"

    # Create and cache some profiles
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 5)
    mock_df = create_mock_ohlcv_data(start_date, days=5)

    provider = MagicMock()
    provider.get_ohlcv.return_value = mock_df

    ctx = DataContext(
        symbol=symbol,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        interval="30m"
    )

    indicator = MarketProfileIndicator.from_context_with_incremental_cache(
        provider=provider,
        ctx=ctx,
        cache=cache,
        inst_id=inst_id,
        days_back=5,
    )

    # Verify profiles were cached
    date_keys = []
    current = start_date
    while current <= end_date:
        date_keys.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    cached_range = cache.get_range(inst_id, symbol, date_keys)
    assert len(cached_range) > 0

    # Purge the cache for this indicator
    cache.purge_indicator(inst_id)

    # Verify cache is empty for this indicator
    cached_range_after = cache.get_range(inst_id, symbol, date_keys)
    assert len(cached_range_after) == 0
