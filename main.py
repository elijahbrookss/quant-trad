import pandas as pd
from pathlib import Path
from classes.Logger import logger

from classes.ChartPlotter import ChartPlotter
from classes.indicators.MarketProfileIndicator import MarketProfileIndicator
from classes.indicators.PivotLevelIndicator import PivotLevelIndicator

from data_providers.alpaca import AlpacaProvider

symbol = "CL"
provider = AlpacaProvider()
start_date = "2025-03-23"
end_date = "2025-05-23"
trading_start_date = "2025-03-20"
trading_end_date = "2025-05-23"
color_mode = "role"  # role or timeframe

def show_market_profile():
    interval = "30m"
    # provider.ingest_history(symbol=symbol, interval=interval, days=365)
    df_30m = provider.get_ohlcv(symbol, start=start_date, end=end_date, interval=interval)
    indicator = MarketProfileIndicator(df_30m)
    overlays = indicator.to_overlays()

    ChartPlotter.plot_ohlc(
        df_30m,
        title=f"{symbol} Market Profile (30min)",
        symbol=symbol,
        datasource=provider.get_datasource(),
        start=start_date,
        end=end_date,
        overlays=overlays
    )

def show_pivot_levels():
    trading_chart = provider.get_ohlcv(symbol, start=trading_start_date, end=trading_end_date, interval="1h")
    indicators = PivotLevelIndicator(trading_chart, timeframe="1h")

    overlays, legend_entries = indicators.to_overlays(plot_df=trading_chart, color_mode=color_mode)

    ChartPlotter.plot_ohlc(
        trading_chart,
        title="CL | Support & Resistance Levels",
        symbol=symbol,
        datasource=provider.get_datasource(),
        start=trading_start_date,
        end=trading_end_date,
        overlays=overlays,
        legend_entries=legend_entries,
        show_volume=True
    )

show_market_profile()
# show_pivot_levels()
