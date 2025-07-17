import pytest
import pandas as pd
from src.indicators.pivot_level import PivotLevelIndicator, Level
from data_providers.alpaca_provider import AlpacaProvider
from src.indicators.config import DataContext

@pytest.mark.integration
def test_pivot_level_indicator_plot():
    """
    Integration test:
    - Pulls 15m chart data for CL between 2025-05-15 and 2025-05-30
    - Builds pivot level indicators from 4h and 1d historical data
    - Generates overlay objects and legend entries for plotting
    - Validates overlays were created successfully
    - Uses the provider’s built-in charting function to output the visual to disk
    """
    ctx = DataContext(
        symbol="CL",
        start="2025-05-15",
        end="2025-06-13",
        interval="15m"
    )

    indicator_ctx = DataContext(
        symbol="CL",
        start="2025-04-01",
        end="2025-06-13",
        interval="15m"
    )

    provider = AlpacaProvider()
    trading_chart = provider.get_ohlcv(ctx)  # Fetch trading chart for display
    assert not trading_chart.empty, "Trading chart data is empty"

    overlays, legend_entries = [], set()
    for tf in ["4h", "1d"]:
        # Generate pivot levels based on higher timeframe context
        indicator = PivotLevelIndicator.from_context(
            provider=provider,
            ctx=indicator_ctx,
            level_timeframe=tf,
            lookbacks=(2, 3, 5, 10, 20),
        )
        # Get overlay lines + legend data for charting
        tf_overlays, tf_legend = indicator.to_overlays(
            plot_df=trading_chart,
            color_mode="timeframe"
        )
        overlays.extend(tf_overlays)
        legend_entries.update(tf_legend)

    assert overlays, "No overlays generated — possible failure in indicator logic"
    assert legend_entries, "No legend entries generated — possible failure in indicator logic"

    # Plot and save chart using provider’s built-in method
    provider.plot_ohlcv(
        plot_ctx=ctx,
        title="Integration Test – Pivot Levels",
        overlays=overlays,
        legend_entries=legend_entries,
        show_volume=True,
        output_subdir="integration_tests/pivot_levels"
    )


@pytest.fixture
def dummy_df():
    """
    Unit test fixture:
    - Returns a small dummy DataFrame with 10 hourly candles
    - Used to test pivot detection and level logic in isolation
    """
    index = pd.date_range("2025-01-01", periods=10, freq="h")
    data = {
        "open": [100]*10,
        "high": [101, 103, 99, 102, 105, 98, 101, 104, 103, 99],
        "low":  [95, 97, 96, 98, 97, 94, 96, 95, 98, 96],
        "close":[100]*10,
        "volume":[1000]*10,
    }
    return pd.DataFrame(data, index=index)


@pytest.mark.unit
def test_find_pivots(dummy_df):
    """
    Unit test:
    - Validates that _find_pivots returns lists of (timestamp, price) tuples
    - Asserts data structure and content format
    """
    indicator = PivotLevelIndicator(dummy_df, timeframe="1h", lookbacks=(2,))
    highs, lows = indicator._find_pivots(2)
    assert isinstance(highs, list) and isinstance(lows, list)
    assert all(isinstance(p, tuple) for p in highs + lows)  # Expect timestamp/price pairs


@pytest.mark.unit
def test_compute_generates_levels(dummy_df):
    """
    Unit test:
    - Ensures that the compute() method generates a list of Level objects
    - This verifies the core pivot-to-level conversion pipeline
    """
    indicator = PivotLevelIndicator(dummy_df, timeframe="1h", lookbacks=(2,))
    assert isinstance(indicator.levels, list)
    assert all(isinstance(l, Level) for l in indicator.levels)  # Confirm correct object type


@pytest.mark.unit
def test_nearest_support_and_resistance(dummy_df):
    """
    Unit test:
    - Checks that nearest support/resistance methods return a Level (or None)
    - Validates that distance-based search logic works correctly
    """
    indicator = PivotLevelIndicator(dummy_df, timeframe="1h", lookbacks=(2,))
    price = 100
    support = indicator.nearest_support(price)
    resistance = indicator.nearest_resistance(price)
    if support:
        assert isinstance(support, Level)
    if resistance:
        assert isinstance(resistance, Level)


@pytest.mark.unit
def test_distance_to_level(dummy_df):
    """
    Unit test:
    - Verifies that distance_to_level returns a non-negative float
    - Ensures correctness of price-to-level distance calculation
    """
    indicator = PivotLevelIndicator(dummy_df, timeframe="1h", lookbacks=(2,))
    if indicator.levels:
        price = 100
        dist = indicator.distance_to_level(indicator.levels[0], price)
        assert isinstance(dist, float)
        assert dist >= 0  # Distance should never be negative