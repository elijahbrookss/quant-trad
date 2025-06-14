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

trading_data_context = DataContext(
    symbol=symbol,
    start="2025-04-01",
    end="2025-05-30",
    interval="1h"  # timeframe for trading data
)

def get_overlays(plot_df: pd.DataFrame):
    trendline = TrendlineIndicator.from_context(
        provider=provider,
        ctx=trading_data_context,
        lookbacks=[20, 50, 100],
        tolerance=10,
        min_touches=3,
        slope_tol=1,
        intercept_tol=1
    )

    return trendline.to_overlays(plot_df)


def show():
    trading_chart = provider.get_ohlcv(trading_data_context)
    overlays, legend_keys = get_overlays(trading_chart)

    provider.plot_ohlcv(
        plot_ctx=trading_data_context,
        title=f"{symbol} | {trading_data_context.interval} - Trendline",
        overlays=overlays,
        legend_entries=legend_keys,
        file_name=f"{symbol}_trendline_{trading_data_context.interval}"
    )

show()