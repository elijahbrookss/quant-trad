import pytest
import pandas as pd
from data_providers.alpaca_provider import AlpacaProvider
from src.indicators.VWAPIndicator import VWAPIndicator
from src.indicators.config import DataContext
from matplotlib.patches import Patch

@pytest.mark.integration
def test_vwap_indicator_from_context_and_plot(tmp_path):
    """
    Integration Test for VWAPIndicator:
    - Fetches 15m OHLCV data for AAPL over a date range.
    - Instantiates VWAPIndicator via from_context.
    - Generates overlays and legend entries.
    - Invokes provider.plot_ohlcv to save a chart and asserts output.
    """
    ctx = DataContext(
        symbol="CL",
        start="2025-05-15",
        end="2025-05-30",
        interval="15m"
    )

    provider = AlpacaProvider()
    trading_chart = provider.get_ohlcv(ctx)  # Fetch trading chart for display
    assert not trading_chart.empty, "Trading chart data is empty"

    overlays, legend_entries = [], set()

    vwap_indicator = VWAPIndicator.from_context(
        provider=provider,
        ctx=ctx,
        stddev_window=20,
        stddev_multipliers=[1.0, 2.0],
        reset_by="D"  # Reset VWAP daily
    )

    vwap_overlays, vwap_legend_keys = vwap_indicator.to_overlays(trading_chart)
    overlays.extend(vwap_overlays)
    legend_entries.update(vwap_legend_keys)

    provider.plot_ohlcv(
        plot_ctx=ctx,
        title=f"{ctx.symbol} | {ctx.interval} - VWAP Bands",
        overlays=overlays,
        legend_entries=legend_entries,
        output_subdir="integration_tests/vwap_bands",
    )

    assert overlays, "No overlays generated — possible failure in indicator logic"
    assert legend_entries, "No legend entries generated — possible failure in indicator logic"

@pytest.fixture
def constant_price_df():
    """
    Fixture: 10 daily bars with constant price and volume
    """
    idx = pd.date_range("2025-01-01", periods=10, freq="D")
    df = pd.DataFrame({
        "high": [100]*10,
        "low": [100]*10,
        "close": [100]*10,
        "volume": [1000]*10
    }, index=idx)
    return df

@pytest.mark.unit
def test_compute_constant_price(constant_price_df):
    ind = VWAPIndicator(
        df=constant_price_df,
        stddev_window=5,
        stddev_multipliers=[1.0, 2.0],
        reset_by="D"
    )
    df = ind.df

    # VWAP should be 100 throughout
    assert (df['vwap'] == 100).all()

    # Bands: some NaNs may appear due to stddev on first few rows — drop them for test
    for m in ind.stddev_multipliers:
        assert (df[f'upper_{int(m)}std'].dropna() == 100).all()
        assert (df[f'lower_{int(m)}std'].dropna() == 100).all()


@pytest.mark.unit
def test_to_overlays_and_legend_handles(constant_price_df):
    """
    to_overlays returns correct overlay count and legend handles
    """
    ind = VWAPIndicator(
        df=constant_price_df,
        stddev_window=5,
        stddev_multipliers=[1.0, 2.0],
        reset_by="D"
    )
    overlays, legend_entries = ind.to_overlays(plot_df=constant_price_df)
    # 1 VWAP + 2 multipliers*2 bands each = 5 overlays
    expected = 1 + len(ind.stddev_multipliers)*2
    assert len(overlays) == expected
    # Legend labels
    labels = {lbl for lbl, _ in legend_entries}
    expected_labels = {"VWAP"} | {f"VWAP + {m}\u03c3" for m in ind.stddev_multipliers} | {f"VWAP - {m}\u03c3" for m in ind.stddev_multipliers}
    assert labels == expected_labels
    # Legend handles
    handles = VWAPIndicator.build_legend_handles(legend_entries)
    assert all(isinstance(h, Patch) for h in handles)
    assert {h.get_label() for h in handles} == expected_labels

@pytest.mark.unit
def test_from_context_missing_data():
    """
    from_context should raise ValueError when provider returns empty DF
    """
    class DummyProvider:
        def get_ohlcv(self, ctx):
            return pd.DataFrame()
    ctx = DataContext(symbol="XYZ", start="2025-01-01", end="2025-01-02", interval="1d")
    with pytest.raises(ValueError):
        VWAPIndicator.from_context(provider=DummyProvider(), ctx=ctx)
