# scripts/cli.py
"""
Command‑line interface for Quant‑Trad framework.

Usage examples:
  quant-trad backtest --config configs/simple_strategy.yaml
  quant-trad ingest   --symbol AAPL --days 365
"""
import yaml
import typer
from pathlib import Path
import pandas as pd
import mplfinance as mpf
import numpy as np

from classes.DataLoader import DataLoader
from classes.indicators.LevelsIndicator import DailyLevelsIndicator, H4LevelsIndicator
from classes.indicators.MarketProfileIndicator import DailyMarketProfileIndicator, MergedValueAreaIndicator
from classes.indicators.TrendlineIndicator import TrendlineIndicator
from classes.indicators.VWAPIndicator import VWAPIndicator
from classes.engines.StrategyEngine import StrategyEngine
from classes.engines.Backtester import Backtester

app = typer.Typer()

# ---------------------------------------------------------
@app.command()
def ingest(
    symbol: str = typer.Option(..., help="Ticker symbol e.g. AAPL"),
    days: int   = typer.Option(365, help="History depth in days to ingest"),
    intervals: list[str] = typer.Option(["15m","60m"], help="List of intervals to ingest"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-critical output"),
):
    """Pull historical data into TimescaleDB."""
    DataLoader.ensure_schema()
    for iv in intervals:
        count = DataLoader.ingest_history(symbol, days=days, interval=iv)
        typer.echo(f"Ingested {count} rows of {iv} data for {symbol}")

# ---------------------------------------------------------
@app.command()
def backtest(
    config: Path = typer.Option(..., exists=True, help="Path to YAML config file"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-critical output")
):
    """Run backtest per config and output signals & trades CSVs."""
    conf = yaml.safe_load(config.read_text())
    # prepare output directory
    out_dir = Path(conf.get("output_dir", "artifacts"))
    out_dir.mkdir(parents=True, exist_ok=True)

    symbol = conf["symbol"]
    # Ingest as needed
    DataLoader.ensure_schema()
    for iv in conf.get("ingest_intervals", ["15m","60m"]):
        DataLoader.ingest_history(symbol, days=conf.get("history_days",365), interval=iv)

    # Load DataFrames
    df15 = DataLoader.get(symbol, tf="15m", lookback_days=conf.get("exec_days",120))
    df4h = DataLoader.get(symbol, tf="4h",  lookback_days=conf.get("higher_days",365))
    dfd  = DataLoader.get(symbol, tf="1d",  lookback_days=conf.get("higher_days",365))

    # Instantiate & compute indicators
    daily_lv  = DailyLevelsIndicator(dfd); daily_lv.compute()
    h4_lv     = H4LevelsIndicator(df4h);   h4_lv.compute()
    daily_mp  = DailyMarketProfileIndicator(dfd); daily_mp.compute()
    merged_va = MergedValueAreaIndicator(daily_mp.result, min_cluster=conf.get("min_va_cluster",3)); merged_va.compute()
    tl4h      = TrendlineIndicator(df4h,   tf_label="4h");  tl4h.compute()
    tl15      = TrendlineIndicator(df15,   tf_label="15m"); tl15.compute()
    vwap15    = VWAPIndicator(df15,        session_tf='D', band_k=conf.get("vwap_k",2.0)); vwap15.compute()

    # Strategy & signals
    engine = StrategyEngine([
        daily_lv, h4_lv, merged_va,
        tl4h, tl15, vwap15
    ], atr_factor=conf.get("atr_factor",0.15))
    signals = engine.run(df15)

    # Save engine state for reuse
    import pickle
    engine_file = out_dir / "engine.pkl"
    with open(engine_file, 'wb') as f:
        pickle.dump(engine, f)
    typer.echo(f"Engine state saved to {engine_file}")

        # Save signals
    signals_csv = out_dir / f"signals_{symbol}.csv"
    signals.to_csv(signals_csv)
    typer.echo(f"Signals written to {signals_csv}")
    # Also save legacy filename
    legacy_csv = out_dir / "simple_strategy_signals.csv"
    signals.to_csv(legacy_csv)
    typer.echo(f"Signals also written to {legacy_csv}")

    # Backtest

    bt = Backtester(signals, engine,
                     entry_threshold=conf.get("entry_threshold",0.8),
                     stop_loss=conf.get("stop_loss",1.0),
                     take_profit=conf.get("take_profit",2.0))
    trades = bt.run()

    trades_csv = out_dir / f"trades_{symbol}.csv"
    trades.to_csv(trades_csv, index=False)
    typer.echo(f"Trades written to {trades_csv}")

    # Auto-generate plot immediately after backtest
    typer.echo("Generating plot...")
    # Reuse the existing plot command logic
    plot(config, show=False)

# ---------------------------------------------------------
@app.command()
def plot(
    config: Path = typer.Option(..., exists=True, help="Path to YAML config file"),
    show: bool = typer.Option(False, help="Display plot interactively"),
):
    """Generate a composite chart of price, indicators, and trade markers."""
    # Load config and prepare
    conf = yaml.safe_load(config.read_text())
    symbol = conf["symbol"]
    out_dir = Path(conf.get("output_dir","artifacts"))

    # Load or reconstruct engine and signals
    engine_file = out_dir / "engine.pkl"
    if engine_file.exists():
        import pickle
        with open(engine_file, 'rb') as f:
            engine = pickle.load(f)
        # load signals and trades
        signals = pd.read_csv(out_dir / f"signals_{symbol}.csv", parse_dates=True, index_col=0)
        trades  = pd.read_csv(out_dir / f"trades_{symbol}.csv", parse_dates=["entry_time","exit_time"])
    else:
        # fallback to full recompute if no engine pickle
        # Ingest/load same dataframes as backtest
        df15 = DataLoader.get(symbol, tf="15m", lookback_days=conf.get("exec_days",120))
        df4h = DataLoader.get(symbol, tf="4h",  lookback_days=conf.get("higher_days",365))
        dfd  = DataLoader.get(symbol, tf="1d",  lookback_days=conf.get("higher_days",365))

        # Instantiate & compute indicators
        daily_lv  = DailyLevelsIndicator(dfd); daily_lv.compute()
        h4_lv     = H4LevelsIndicator(df4h);   h4_lv.compute()
        daily_mp  = DailyMarketProfileIndicator(dfd); daily_mp.compute()
        merged_va = MergedValueAreaIndicator(daily_mp.result, min_cluster=conf.get("min_va_cluster",3)); merged_va.compute()
        tl4h      = TrendlineIndicator(df4h,   tf_label="4h");  tl4h.compute()
        tl15      = TrendlineIndicator(df15,   tf_label="15m"); tl15.compute()
        vwap15    = VWAPIndicator(df15,        session_tf='D', band_k=conf.get("vwap_k",2.0)); vwap15.compute()

        engine = StrategyEngine([
            daily_lv, h4_lv, merged_va,
            tl4h, tl15, vwap15
        ], atr_factor=conf.get("atr_factor",0.15))
        signals = engine.run(df15)
        trades  = Backtester(signals, engine,
                             entry_threshold=conf.get("entry_threshold",0.8),
                             stop_loss=conf.get("stop_loss",1.0),
                             take_profit=conf.get("take_profit",2.0)).run()
    indicators = []
    daily_lv  = DailyLevelsIndicator(dfd); daily_lv.compute(); indicators.append(daily_lv)
    h4_lv     = H4LevelsIndicator(df4h);   h4_lv.compute();    indicators.append(h4_lv)
    daily_mp  = DailyMarketProfileIndicator(dfd); daily_mp.compute(); indicators.append(daily_mp)
    merged_va = MergedValueAreaIndicator(daily_mp.result, min_cluster=conf.get("min_va_cluster",3)); merged_va.compute(); indicators.append(merged_va)
    tl4h      = TrendlineIndicator(df4h,   tf_label="4h");  tl4h.compute();  indicators.append(tl4h)
    tl15      = TrendlineIndicator(df15,   tf_label="15m"); tl15.compute(); indicators.append(tl15)
    vwap15    = VWAPIndicator(df15,        session_tf='D', band_k=conf.get("vwap_k",2.0)); vwap15.compute(); indicators.append(vwap15)

    # Run strategy to get signals and trades
    engine = StrategyEngine(indicators, atr_factor=conf.get("atr_factor",0.15))
    signals = engine.run(df15)
    trades  = Backtester(signals, engine,
                         entry_threshold=conf.get("entry_threshold",0.8),
                         stop_loss=conf.get("stop_loss",1.0),
                         take_profit=conf.get("take_profit",2.0)).run()

    # Prepare df for plotting
    df_plot = df15.copy()
    df_plot['score'] = signals['score']
    df_plot['direction'] = signals['direction']

    # Build mplfinance addplot list dynamically from engine.indicators
    apds = []
    # Pull indicator instances directly
    for ind in engine.indicators:
        # VWAP
        if hasattr(ind, 'get_vwap'):
            v, std = ind.get_vwap(df_plot.index[-1])
            df_plot['VWAP'] = ind.result['VWAP']
            apds.append(mpf.make_addplot(df_plot['VWAP'], color='yellow', width=1))
        # Levels
        if hasattr(ind, 'get_levels'):
            for lvl in ind.get_levels():
                apds.append(mpf.make_addplot(pd.Series(lvl, index=df_plot.index), linestyle='--', linewidth=0.5))
        # Clusters
        if hasattr(ind, 'get_clusters'):
            for val, poc, vah, cnt in ind.get_clusters():
                apds.append(mpf.make_addplot(pd.Series(val, index=df_plot.index), linestyle=':', linewidth=0.5, alpha=0.4))
                apds.append(mpf.make_addplot(pd.Series(vah, index=df_plot.index), linestyle=':', linewidth=0.5, alpha=0.4))
        # Trendlines
        if hasattr(ind, 'get_lines'):
            for tl in ind.get_lines():
                xs = df_plot.index
                idx_nums = np.arange(len(xs))
                yvals = tl.intercept + tl.slope * idx_nums
                apds.append(mpf.make_addplot(pd.Series(yvals, index=xs), color='cyan', width=0.7))
    # Entry / exit markers
    entry_s = pd.Series(trades.set_index('entry_time')['entry_price'])
    exit_s  = pd.Series(trades.set_index('exit_time')['exit_price'])
    apds.append(mpf.make_addplot(entry_s, type='scatter', marker='^', markersize=100, color='green'))
    apds.append(mpf.make_addplot(exit_s,  type='scatter', marker='v', markersize=100, color='red'))
    mpf.plot(
        df_plot[['Open','High','Low','Close']],
        type='candle',
        addplot=apds,
        volume=True,
        style='nightclouds',
        title=f"{symbol} 15m Backtest Overview",
        savefig=out_dir / f"plot_{symbol}.png",
        block=show
    )


if __name__ == "__main__":
    app()
