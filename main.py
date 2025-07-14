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

from classes.signals.market_profile_signal import MarketProfileSignalGenerator

symbol = "CL"
provider = AlpacaProvider()

lower_timespan_ctx = DataContext(
    symbol=symbol,
    start="2025-06-01",
    end="2025-07-13",
    interval="15m"
)

higher_timespan_ctx = DataContext(
    symbol=symbol,
    start="2025-05-10",
    end="2025-07-12",
    interval="30m"
)


def get_overlays_and_value_areas(plot_df: pd.DataFrame):
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

    # Prepare value area schema for signal generator
    value_areas = []
    for va in merged:
        value_areas.append({
            "start": va["start_date"],
            "end": va["end_date"],
            "VAL": va["VAL"],
            "VAH": va["VAH"],
            "POC": va["POC"]
        })

    overlays, legend_keys = mpi.to_overlays(plot_df, use_merged=True)
    logger.info("Generated overlays: %d, legend entries: %d", len(overlays), len(legend_keys))
    logger.debug("Overlay kinds: %s", [overlay.get("kind", "addplot") for overlay in overlays])

    return overlays, legend_keys, value_areas


def show():
    logger.info("Fetching lower timeframe OHLCV data for plotting: %s %s-%s (%s)",
                lower_timespan_ctx.symbol, lower_timespan_ctx.start, lower_timespan_ctx.end, lower_timespan_ctx.interval)
    plot_df = provider.get_ohlcv(lower_timespan_ctx)
    logger.info("Fetched plot DataFrame shape: %s", plot_df.shape if plot_df is not None else None)

    overlays, legend_keys, value_areas = get_overlays_and_value_areas(plot_df)

    logger.info("Plotting chart with overlays and legend")
    provider.plot_ohlcv(
        plot_ctx=lower_timespan_ctx,
        title="Integration Test – Market Profile (CL 30m)",
        overlays=overlays,
        legend_entries=legend_keys,
        show_volume=False
    )
    logger.info("Chart plotting complete.")

    logger.info("Running MarketProfileSignalGenerator for symbol: %s", symbol)
    signal_generator = MarketProfileSignalGenerator(symbol=symbol)
    signals = signal_generator.generate_signals(plot_df, value_areas)

    logger.info("Generated %d breakout signals", len(signals))
    for sig in signals:
        print(sig.to_dict())

    signal_overlays = MarketProfileSignalGenerator.to_overlays(signals, plot_df)

    full_overlays = overlays + signal_overlays
    legend_keys.add(("MarketProfile breakout", "red"))

    # full_legend_keys = legend_keys + ["MarketProfile breakout"]

    logger.info("Plotting chart with breakout overlays")
    provider.plot_ohlcv(
        plot_ctx=lower_timespan_ctx,
        title="Market Profile Breakouts – 15m Chart",
        overlays=full_overlays,
        legend_entries=legend_keys,
        show_volume=False
    )



show()
