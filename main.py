import pandas as pd
from pathlib import Path
from classes.Logger import logger

from classes.ChartPlotter import ChartPlotter
from classes.indicators.MarketProfileIndicator import MarketProfileIndicator
from classes.indicators.PivotLevelIndicator import PivotLevelIndicator

from data_providers.alpaca_provider import AlpacaProvider
from classes.indicators.config import DataContext


symbol = "CL"
provider = AlpacaProvider()

trading_data_context = DataContext(
    symbol=symbol,
    start="2025-01-30",
    end="2025-05-23",
    interval="1h"  # timeframe for trading data
)

def show_market_profile():
    trading_chart = provider.get_ohlcv(trading_data_context)

    merged_profile = MarketProfileIndicator.from_context(provider, ctx=trading_data_context, interval="30m")
    merged_profile.merge_value_areas(
        threshold=0.7,
        min_merge=3
    )
    overlays, legend_keys = merged_profile.to_overlays(trading_chart, use_merged=True)

    provider.plot_ohlcv(
        plot_ctx=trading_data_context,
        title=f"{symbol} | {trading_data_context.interval} - {merged_profile.NAME} - Merged VAs",
        overlays=overlays,
        legend_entries=legend_keys,
        file_name=f"{symbol}_merged_market_profile_{trading_data_context.interval}"
    )

    unmerged_profile = MarketProfileIndicator.from_context(provider, ctx=trading_data_context, interval="30m")
    overlays_unmerged, legend_keys_unmerged = unmerged_profile.to_overlays(trading_chart, use_merged=False)

    provider.plot_ohlcv(
        plot_ctx=trading_data_context,
        title=f"{symbol} | {trading_data_context.interval} - {unmerged_profile.NAME} - Unmerged VAs",
        overlays=overlays_unmerged,
        legend_entries=legend_keys_unmerged,
        file_name=f"{symbol}_unmerged_market_profile_{trading_data_context.interval}"
    )

    
show_market_profile()