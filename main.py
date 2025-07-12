import pandas as pd
from pathlib import Path
from classes.Logger import logger

from classes.ChartPlotter import ChartPlotter
from classes.indicators.MarketProfileIndicator import MarketProfileIndicator
from classes.indicators.PivotLevelIndicator import PivotLevelIndicator
from classes.indicators.VWAPIndicator import VWAPIndicator
from classes.indicators.TrendlineIndicator import TrendlineIndicator

from data_providers.alpaca_provider import AlpacaProvider
from classes.indicators.config import DataContext


symbol = "CL"
provider = AlpacaProvider()

ctx = DataContext(
    symbol="CL",
    start="2025-06-01",
    end="2025-06-30",
    interval="15m"
)

def get_overlays(plot_df: pd.DataFrame):
    mpi = MarketProfileIndicator.from_context(
        provider=provider,
        ctx=ctx,
        bin_size=0.5,      # can adjust bin size as desired
        mode="tpo",
        interval="30m"
    )
    merged = mpi.merge_value_areas()

    return mpi.to_overlays(plot_df, use_merged=True)


def show():
    plot_df = provider.get_ohlcv(ctx)
    overlays, legend_keys = get_overlays(plot_df)

    logger.debug([overlay["kind"] for overlay in overlays])

    provider.plot_ohlcv(
        plot_ctx=ctx,
        title="Integration Test – Market Profile (CL 30m)",
        overlays=overlays,
        legend_entries=legend_keys,
        show_volume=True
    )

show()