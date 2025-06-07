import pandas as pd
from pathlib import Path
from classes.Logger import logger

from classes.ChartPlotter import ChartPlotter
from classes.indicators.MarketProfileIndicator import MarketProfileIndicator
from classes.indicators.PivotLevelIndicator import PivotLevelIndicator
from classes.indicators.VWAPIndicator import VWAPIndicator

from data_providers.alpaca_provider import AlpacaProvider
from classes.indicators.config import DataContext


symbol = "CL"
provider = AlpacaProvider()

trading_data_context = DataContext(
    symbol=symbol,
    start="2025-05-01",
    end="2025-05-30",
    interval="1h"  # timeframe for trading data
)

def get_overlays(plot_df: pd.DataFrame):

    logger.info(f"VWAP overlays: {vwap_overlays}")
    return vwap_overlays, vwap_legend_keys


def show_vwap():
    trading_chart = provider.get_ohlcv(trading_data_context)
    overlays, legend_keys = get_overlays(trading_chart)

    provider.plot_ohlcv(
        plot_ctx=trading_data_context,
        title=f"{symbol} | {trading_data_context.interval} - VWAP Bands",
        overlays=overlays,
        legend_entries=legend_keys,
        file_name=f"{symbol}_vwap_bands_{trading_data_context.interval}"
    )

show_vwap()