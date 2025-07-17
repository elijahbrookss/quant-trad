import pytest
import pandas as pd
import numpy as np
from indicators.trendline import TrendlineIndicator, Trendline
from data_providers.alpaca_provider import AlpacaProvider
from indicators.config import DataContext

@pytest.mark.integration
def test_trendline_indicator_plot():
    """
    Integration test:
    - Pulls 15m chart data for CL between 2025-05-15 and 2025-06-13
    - Builds TrendlineIndicator from 4h context
    - Generates overlay objects and legend entries
    - Asserts overlays/legend non-empty and plots to disk
    """
    # chart for plotting
    plot_ctx = DataContext(
        symbol="CL",
        start="2025-05-15",
        end="2025-06-13",
        interval="15m"
    )
    provider = AlpacaProvider()
    plot_df = provider.get_ohlcv(plot_ctx)
    assert not plot_df.empty, "15m price data is empty"

    # indicator context: feed the same timeframe or a higher one
    ind_ctx = DataContext(
        symbol="CL",
        start="2025-05-01",
        end="2025-06-13",
        interval="15m"
    )

    # build and plot trendlines
    indicator = TrendlineIndicator.from_context(
        provider=provider,
        ctx=ind_ctx,
        lookbacks=[5,10,20],
        tolerance=0.0025,
        min_touches=2,
        slope_tol=1e-4,
        intercept_tol=0.01
    )
    overlays, legend_entries = indicator.to_overlays(
        plot_df=plot_df,
        color_mode="role",
        top_n=5
    )

    assert overlays, "No trendline overlays generated"
    assert legend_entries, "No legend entries generated"

    # write out a plot so you can inspect manually
    provider.plot_ohlcv(
        plot_ctx=plot_ctx,
        title="Integration Test – Trendlines",
        overlays=overlays,
        legend_entries=legend_entries,
        show_volume=False,
        output_subdir="integration_tests/trendlines"
    )

@pytest.fixture
def dummy_df():
    """
    Unit fixture: 30-min bars over 30 points tracing a linear uptrend + noise.
    """
    idx = pd.date_range("2025-01-01 09:30", periods=30, freq="30min")
    # a line y=100+0.5*i plus some noise
    base = np.array([100 + 0.5*i for i in range(30)])
    noise = np.random.normal(scale=0.1, size=30)
    close = base + noise
    df = pd.DataFrame({
        "open": close,
        "high": close + 0.2,
        "low":  close - 0.2,
        "close":close,
        "volume":[1000]*30
    }, index=idx)
    return df

@pytest.mark.unit
def test_find_pivots(dummy_df):
    """
    _find_pivots should return lists of (timestamp, price) tuples.
    """
    ind = TrendlineIndicator(dummy_df, lookbacks=[2], tolerance=0.01, min_touches=1)
    highs, lows = ind._find_pivots(2)
    assert isinstance(highs, list) and isinstance(lows, list)
    assert all(isinstance(tp, tuple) for tp in highs + lows)

@pytest.mark.unit
def test_compute_generates_trendlines(dummy_df):
    """
    After init, .trendlines should be a list of Trendline objects.
    """
    ind = TrendlineIndicator(dummy_df, lookbacks=[2,4], tolerance=0.01, min_touches=2)
    assert isinstance(ind.trendlines, list)
    assert all(isinstance(tl, Trendline) for tl in ind.trendlines)

@pytest.mark.unit
def test_to_overlays_and_handles(dummy_df):
    """
    to_overlays must return (overlays, legend_entries) and build_legend_handles must
    convert legend_entries into Patch handles.
    """
    ind = TrendlineIndicator(dummy_df, lookbacks=[3], tolerance=0.01, min_touches=2)
    overlays, legend = ind.to_overlays(plot_df=dummy_df, color_mode="role")
    assert isinstance(overlays, list)
    assert isinstance(legend, set)
    # at least one overlay/legend when trend exists
    if ind.trendlines:
        assert overlays, "Expected overlays for detected trendlines"
        assert legend,   "Expected legend entries"
        handles = TrendlineIndicator.build_legend_handles(legend)
        from matplotlib.patches import Patch
        assert all(isinstance(h, Patch) for h in handles)

@pytest.mark.unit
def test_score_and_r2(dummy_df):
    """
    Each Trendline should have r2 between 0 and 1, and score defaulting to 0.
    """
    ind = TrendlineIndicator(dummy_df, lookbacks=[2], tolerance=0.01, min_touches=2)
    for tl in ind.trendlines:
        assert 0.0 <= tl.r2 <= 1.0
        assert hasattr(tl, "score") and tl.score == 0.0
