import mplfinance as mpf
import pandas as pd

# 1) load your 15 m DataFrame with OHLC and signals
df = pd.read_csv("artifacts/simple_strategy_signals.csv", index_col=0, parse_dates=True)
df_ohlc = df[["Open","High","Low","Close","Volume"]]

# 2) collect trade entry/exits
trades = pd.read_csv("strategy1/trades_AAPL.csv", parse_dates=["entry_time","exit_time"])
apds = []

# 3) add VWAP line
apds.append(mpf.make_addplot(df["VWAP"], color="yellow"))

# 4) mark entries and exits
entry_markers = trades.set_index("entry_time")["entry_price"]
exit_markers  = trades.set_index("exit_time")["exit_price"]
apds.append(mpf.make_addplot(entry_markers, type="scatter", markersize=100, marker="^", color="lime"))
apds.append(mpf.make_addplot(exit_markers,  type="scatter", markersize=100, marker="v", color="red"))

# 5) render
mpf.plot(
    df_ohlc,
    type="candle",
    addplot=apds,
    volume=True,
    style="nightclouds",
    title="AAPL 15m with Trades & VWAP",
    figsize=(12,8),
)
