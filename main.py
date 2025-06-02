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
start_date = "2025-03-23"
end_date = "2025-05-23"
trading_start_date = "2025-03-20"
trading_end_date = "2025-05-23"
color_mode = "timeframe"  # role or timeframe

trading_data_context = DataContext(
    symbol=symbol,
    start="2025-03-20",
    end="2025-05-23",
    interval="15m"  # timeframe for trading data
)

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
    trading_chart = provider.get_ohlcv(trading_data_context)

    overlays = []
    legend_entries = set()
    level_timeframes = ["4h", "1d"]

    for tf in level_timeframes:
        indicator = PivotLevelIndicator.from_context(
            provider=provider,
            ctx=trading_data_context,
            level_timeframe=tf,
            lookbacks=(2, 3, 5, 10, 20),
        )
        tf_overlays, tf_legends = indicator.to_overlays(plot_df=trading_chart, color_mode=color_mode)
        overlays.extend(tf_overlays)
        legend_entries.update(tf_legends)

    provider.plot_ohlcv(
        ctx=trading_data_context,
        title=f"{symbol} Pivot Levels",
        overlays=overlays,
        legend_entries=legend_entries,
        show_volume=False,
        chart_type="candle"
    )

    

    
# show_market_profile()
show_pivot_levels()
