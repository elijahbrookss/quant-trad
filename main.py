# grid_search.py  ──────────────────────────────────────────────────────────
import itertools
import pandas as pd
from classes.DataLoader import DataLoader
from classes.engines.Backtester   import Backtester
from classes.engines.StrategyEngine import StrategyEngine
from pathlib import Path
from classes.Logger import logger
import yaml

# ---------- indicator imports ----------
from classes.indicators.LevelsIndicator        import LevelsIndicator      # daily & H4
from classes.indicators.MarketProfileIndicator import DailyMarketProfileIndicator
from classes.indicators.VWAPIndicator          import VWAPIndicator
from classes.indicators.PivotLevelIndicator import PivotLevelIndicator

# add your TrendlineIndicator etc. here
# ---------------------------------------

from classes.ChartPlotter import ChartPlotter

from data_providers.yahoo import YahooFinanceProvider
from data_providers.alpaca import AlpacaProvider 
from classes.DataLoader import DataLoader

symbol = "CL"
start_date = "2024-05-23"
end_date = "2025-05-23"

trading_start_date = "2025-03-20"
trading_end_date = "2025-05-23"

color_mode = "role"  # timeframe or role

provider = AlpacaProvider()

intervals = ["15m"]

# for interval in intervals:
    # provider.ingest_history(symbol=symbol, interval=interval, days=15)

# Get daily and hourly OHLCV data
df_daily = provider.get_ohlcv(symbol, start=start_date, end=end_date, interval="1d")
df_hourly = provider.get_ohlcv(symbol, start=start_date, end=end_date, interval="1h")

trading_chart = provider.get_ohlcv(symbol, start=trading_start_date, end=trading_end_date, interval="1h")

# Compute daily and hourly levels
daily_indicator = PivotLevelIndicator(df_daily, timeframe="1d")
hourly_indicator = PivotLevelIndicator(df_hourly, timeframe="1h")

indicators = PivotLevelIndicator(trading_chart, timeframe="1h")

# Generate overlays (aligned to trading_chart index)
overlays = []
legend_entries = set()

# daily_overlays, daily_legend = daily_indicator.to_overlays(
#     plot_index=trading_chart.index,
#     color_mode=color_mode
# )

# hourly_overlays, hourly_legend = hourly_indicator.to_overlays(
#     plot_index=trading_chart.index,
#     color_mode=color_mode
# )

# overlays += daily_overlays + hourly_overlays
# legend_entries |= daily_legend | hourly_legend

indcator_overlays, indicator_legend = indicators.to_overlays(
    plot_index=trading_chart.index,
    color_mode=color_mode
)

overlays += indcator_overlays
legend_entries |= indicator_legend

logger.info("legend_entries: %s", legend_entries)

# Plot the combined chart
ChartPlotter.plot_ohlc(
    trading_chart,
    title="CL | Support & Resistance Levels",
    symbol=symbol,
    datasource=provider.get_datasource(),
    start=start_date,
    end=end_date,
    overlays=overlays,
    legend_entries=legend_entries,
    show_volume=True
)


# ─── hyper‑parameter grids ────────────────────────────────────────────────
# THRESHOLDS   = [0.40, 0.45, 0.50, 0.55, 0.60]
# ATR_FACTORS  = [0.8, 1.0, 1.2, 1.5, 2.0]

# # ─── load your price frame once ───────────────────────────────────────────
# config_path = Path("configs/simple_strategy.yaml")
# conf = yaml.safe_load(config_path.read_text())

# df = DataLoader.ensure_schema()
# DataLoader.ingest_history("AAPL", days=conf.get("ingest_days", 365), interval="1m")
# df = DataLoader.get('AAPL', tf="15m", lookback_days=conf.get("exec_days", 180))

# def build_indicators(df: pd.DataFrame):
#     """Fresh indicator objects so state doesn’t leak run‑to‑run."""
#     inds = [
#         LevelsIndicator(df, lookbacks=(20, 50, 100)),
#         LevelsIndicator(df, lookbacks=(10, 20, 50)),
#         DailyMarketProfileIndicator(df),           # gives 'merged_va' clusters
#         VWAPIndicator(df),                         # gives 'vwap'
#         # TrendlineIndicator(df, timeframe="4h"),  # example
#     ]
#     for ind in inds:
#         ind.compute()
#     return inds

# records = []
# for th, af in itertools.product(THRESHOLDS, ATR_FACTORS):
#     indicators = build_indicators(df)
    
#     engine      = StrategyEngine(indicators, atr_factor=af)
#     signals     = engine.run(df)

#     # 2) send those signals to the back‑tester
#     bt = Backtester(
#         signals,
#         engine,
#         entry_threshold=th,
#         stop_loss=1.0,
#         take_profit=2.0,
#     )

#     bt.run()                                           # prints perf summary
#     stats       = bt.stats

#     records.append({
#         "threshold":   th,
#         "atr_factor":  af,
#         "n_trades":    stats.n_trades,
#         "win_rate":    stats.win_rate,
#         "expectancy":  stats.expectancy,
#         "max_dd":      stats.max_dd
#     })

# # ─── dump & view results ──────────────────────────────────────────────────
# results = pd.DataFrame(records).sort_values("expectancy", ascending=False)
# print("\n=== grid‑search results (top 10) ===")
# print(results.head(10).to_string(index=False))

# results.to_csv("grid_search_results.csv", index=False)
# print("\nFull grid saved to grid_search_results.csv")
