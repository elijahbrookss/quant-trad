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

lower_timespan_ctx = DataContext(
    symbol="CL",
    start="2025-06-01",
    end="2025-06-30",
    interval="15m"
)

higher_timespan_ctx = DataContext(
    symbol="CL",
    start="2025-05-10",
    end="2025-07-12",
    interval="30m"
)

def get_overlays(plot_df: pd.DataFrame):
    logger.info("Creating MarketProfileIndicator with higher timeframe context: %s %s-%s (%s)", 
                higher_timespan_ctx.symbol, higher_timespan_ctx.start, higher_timespan_ctx.end, higher_timespan_ctx.interval)
    mpi = MarketProfileIndicator.from_context(
        provider=provider,
        ctx=higher_timespan_ctx,
        bin_size=0.5,
        mode="tpo",
        interval="30m"
    )
    logger.info("Merging value areas for MarketProfileIndicator")
    merged = mpi.merge_value_areas()
    logger.debug("Merged value areas count: %d", len(merged))

    overlays, legend_keys = mpi.to_overlays(plot_df, use_merged=True)
    logger.info("Generated overlays: %d, legend entries: %d", len(overlays), len(legend_keys))
    logger.debug("Overlay kinds: %s", [overlay.get("kind", "addplot") for overlay in overlays])
    return overlays, legend_keys

def show():
    logger.info("Fetching lower timeframe OHLCV data for plotting: %s %s-%s (%s)",
                lower_timespan_ctx.symbol, lower_timespan_ctx.start, lower_timespan_ctx.end, lower_timespan_ctx.interval)
    plot_df = provider.get_ohlcv(lower_timespan_ctx)
    logger.info("Fetched plot DataFrame shape: %s", plot_df.shape if plot_df is not None else None)

    overlays, legend_keys = get_overlays(plot_df)

    logger.info("Plotting chart with overlays and legend")
    provider.plot_ohlcv(
        plot_ctx=lower_timespan_ctx,
        title="Integration Test – Market Profile (CL 30m)",
        overlays=overlays,
        legend_entries=legend_keys,
        show_volume=False
    )
    logger.info("Chart plotting complete.")

show()