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
    start="2025-04-20",
    end="2025-05-23",
    interval="15m"  # timeframe for trading data
)

def show_market_profile():
    trading_chart = provider.get_ohlcv(trading_data_context)

    market_profile = MarketProfileIndicator.from_context(provider, ctx=trading_data_context, interval="30m")
    overlays, legend_keys = market_profile.to_overlays(trading_chart)

    provider.plot_ohlcv(
        plot_ctx=trading_data_context,
        title=f"{symbol} | {trading_data_context.interval} - {market_profile.NAME}",
        overlays=overlays,
        legend_entries=legend_keys
    )

    
show_market_profile()