import yaml
from pathlib import Path
import pandas as pd
import numpy as np
import mplfinance as mpf

from classes.DataLoader import DataLoader
from classes.indicators.LevelsIndicator import DailyLevelsIndicator, H4LevelsIndicator
from classes.indicators.MarketProfileIndicator import DailyMarketProfileIndicator, MergedValueAreaIndicator
from classes.indicators.TrendlineIndicator import TrendlineIndicator
from classes.indicators.VWAPIndicator import VWAPIndicator
from classes.engines.StrategyEngine import StrategyEngine
from classes.engines.Backtester import Backtester


def run_backtest():
    # Load configuration
    config_path = Path("configs/simple_strategy.yaml")
    conf = yaml.safe_load(config_path.read_text())
    symbol = conf["symbol"]
    out_dir = Path(conf.get("output_dir", "artifacts"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # Ingest & load data
    DataLoader.ensure_schema()
    # for iv in conf.get("ingest_intervals", ["15m", "60m"]):
    #     DataLoader.ingest_history(symbol, days=conf.get("history_days", 365), interval=iv)

    df15 = DataLoader.get(symbol, tf="15m", lookback_days=conf.get("exec_days", 120))
    df4h = DataLoader.get(symbol, tf="4h",  lookback_days=conf.get("higher_days", 365))
    dfd  = DataLoader.get(symbol, tf="1d",  lookback_days=conf.get("higher_days", 365))

    # Compute indicators
    daily_lv   = DailyLevelsIndicator(dfd);   daily_lv.compute()
    h4_lv      = H4LevelsIndicator(df4h);     h4_lv.compute()
    daily_mp   = DailyMarketProfileIndicator(dfd); daily_mp.compute()
    merged_va  = MergedValueAreaIndicator(daily_mp.result, min_cluster=conf.get("min_va_cluster", 3)); merged_va.compute()
    tl4h       = TrendlineIndicator(df4h, tf_label="4h");  tl4h.compute()
    tl15       = TrendlineIndicator(df15, tf_label="15m"); tl15.compute()
    vwap15     = VWAPIndicator(df15, session_tf='D', band_k=conf.get("vwap_k", 2.0)); vwap15.compute()

    # Run strategy & backtest
    engine = StrategyEngine([daily_lv, h4_lv, merged_va, tl4h, tl15, vwap15],
                             atr_factor=conf.get("atr_factor", 0.15))
    signals = engine.run(df15)
    bt = Backtester(signals, engine,
                    entry_threshold=conf.get("entry_threshold", 0.8),
                    stop_loss=conf.get("stop_loss", 1.0),
                    take_profit=conf.get("take_profit", 2.0))
    trades = bt.run()

    # Save trades
    trades_csv = out_dir / f"trades_{symbol}.csv"
    trades.to_csv(trades_csv, index=False)
    print(f"Trades written to {trades_csv}")

    # --- CHART EACH TRADE ---
    df_plot = df15[['Open','High','Low','Close','Volume']].copy()

    for idx, trade in trades.iterrows():
        entry     = pd.to_datetime(trade['entry_time'])
        exit_     = pd.to_datetime(trade['exit_time'])
        ep        = trade['entry_price']
        xp        = trade['exit_price']
        direction = trade['direction']

        # Window around the trade
        window_start = entry - pd.Timedelta(minutes=5 * 15)
        window_end   = exit_  + pd.Timedelta(minutes=5 * 15)
        df_w = df_plot.loc[window_start:window_end]
        if df_w.empty:
            continue

        # Determine price range in window (to filter levels)
        price_min, price_max = df_w['Low'].min(), df_w['High'].max()

        addplots = []

        # 1) VWAP overlay
        if hasattr(vwap15, 'result'):
            vwap_series = vwap15.result['VWAP'].reindex(df_w.index)
            addplots.append(mpf.make_addplot(vwap_series, color='yellow', width=1))

        # 2) Levels (only plot those near current price)
        for lvl in daily_lv.get_levels():
            if price_min <= lvl <= price_max:
                lvl_s = pd.Series(lvl, index=df_w.index)
                addplots.append(mpf.make_addplot(lvl_s, linestyle='--', width=0.5, alpha=0.7))
        for lvl in h4_lv.get_levels():
            if price_min <= lvl <= price_max:
                lvl_s = pd.Series(lvl, index=df_w.index)
                addplots.append(mpf.make_addplot(lvl_s, linestyle='--', width=0.5, alpha=0.7))

        # 3) Merged Value Areas (filter by price range)
        for val, poc, vah, cnt in merged_va.get_clusters():
            if price_min <= val <= price_max:
                addplots.append(mpf.make_addplot(pd.Series(val, index=df_w.index), linestyle=':', width=0.7, alpha=0.4))
            if price_min <= vah <= price_max:
                addplots.append(mpf.make_addplot(pd.Series(vah, index=df_w.index), linestyle=':', width=0.7, alpha=0.4))

        # 4) Trendlines (only those whose pivot points lie within window)
        start_pos = df15.index.get_loc(df_w.index[0])
        end_pos   = df15.index.get_loc(df_w.index[-1])
        for tl in tl4h.get_lines() + tl15.get_lines():
            i1, _ = tl.p1
            i2, _ = tl.p2
            if (start_pos <= i1 <= end_pos) and (start_pos <= i2 <= end_pos):
                xs = np.arange(len(df_w)) + start_pos
                ys = tl.intercept + tl.slope * xs
                series = pd.Series(ys, index=df_w.index)
                addplots.append(mpf.make_addplot(series, color='cyan', width=0.8, alpha=0.8))

        # 5) Entry & Exit markers
        marker_entry = pd.Series(np.nan, index=df_w.index)
        marker_exit  = pd.Series(np.nan, index=df_w.index)
        if entry in df_w.index:
            marker_entry.loc[entry] = ep
        if exit_ in df_w.index:
            marker_exit.loc[exit_] = xp
        addplots.append(mpf.make_addplot(marker_entry, type='scatter', marker='^', markersize=50, color='green'))
        addplots.append(mpf.make_addplot(marker_exit,  type='scatter', marker='v', markersize=50, color='red'))

        # 6) Highlight volume bars at entry/exit
        vol_entry = pd.Series(np.nan, index=df_w.index)
        vol_exit  = pd.Series(np.nan, index=df_w.index)
        if entry in df_w.index:
            vol_entry.loc[entry] = df_w.loc[entry, 'Volume']
        if exit_ in df_w.index:
            vol_exit.loc[exit_] = df_w.loc[exit_,  'Volume']
        addplots.append(mpf.make_addplot(vol_entry, type='bar', panel=1, width=0.7, color='green', alpha=0.8))
        addplots.append(mpf.make_addplot(vol_exit,  type='bar', panel=1, width=0.7, color='red',   alpha=0.8))

        title = f"{symbol} Trade #{idx+1} ({direction}): {ep:.2f} → {xp:.2f}"
        out_png = out_dir / f"trade_{idx+1}_{symbol}.png"
        mpf.plot(
            df_w,
            type='candle',
            addplot=addplots,
            volume=True,
            style='nightclouds',
            title=title,
            savefig=out_png
        )
        print(f"Chart saved to {out_png}")
        entry    = pd.to_datetime(trade['entry_time'])
        exit_    = pd.to_datetime(trade['exit_time'])
        ep       = trade['entry_price']
        xp       = trade['exit_price']
        direction= trade['direction']

        # Window around the trade
        window_start = entry - pd.Timedelta(minutes=5 * 15)
        window_end   = exit_  + pd.Timedelta(minutes=5 * 15)
        df_w = df_plot.loc[window_start:window_end]
        if df_w.empty:
            continue

        addplots = []
        # VWAP
        if hasattr(vwap15, 'result'):
            vwap_series = vwap15.result['VWAP'].reindex(df_w.index)
            addplots.append(mpf.make_addplot(vwap_series, color='yellow', width=1))

        # Levels
        for lvl in daily_lv.get_levels():
            lvl_s = pd.Series(lvl, index=df_w.index)
            addplots.append(mpf.make_addplot(lvl_s, linestyle='--', width=0.5, alpha=0.7))
        for lvl in h4_lv.get_levels():
            lvl_s = pd.Series(lvl, index=df_w.index)
            addplots.append(mpf.make_addplot(lvl_s, linestyle='--', width=0.5, alpha=0.7))

        # Merged Value Areas
        for val, poc, vah, cnt in merged_va.get_clusters():
            addplots.append(mpf.make_addplot(pd.Series(val, index=df_w.index), linestyle=':', width=0.7, alpha=0.4))
            addplots.append(mpf.make_addplot(pd.Series(vah, index=df_w.index), linestyle=':', width=0.7, alpha=0.4))

        # Trendlines
        base_idx = df15.index.get_indexer_for([df_w.index[0]])[0]
        for tl in tl4h.get_lines() + tl15.get_lines():
            xs = np.arange(len(df_w)) + base_idx
            ys = tl.intercept + tl.slope * xs
            series = pd.Series(ys, index=df_w.index)
            addplots.append(mpf.make_addplot(series, color='cyan', width=0.8, alpha=0.8))

        # Entry & Exit markers as full-length series
        marker_entry = pd.Series(np.nan, index=df_w.index)
        if entry in df_w.index:
            marker_entry.loc[entry] = ep
        marker_exit  = pd.Series(np.nan, index=df_w.index)
        if exit_ in df_w.index:
            marker_exit.loc[exit_] = xp
        addplots.append(mpf.make_addplot(marker_entry, type='scatter', marker='^', markersize=50, color='green'))
        addplots.append(mpf.make_addplot(marker_exit,  type='scatter', marker='v', markersize=50, color='red'))

        title = f"{symbol} Trade #{idx+1} ({direction}): {ep:.2f} → {xp:.2f}"
        out_png = out_dir / f"trade_{idx+1}_{symbol}.png"
        mpf.plot(
            df_w,
            type='candle',
            addplot=addplots,
            volume=True,
            style='nightclouds',
            title=title,
            savefig=out_png
        )
        print(f"Chart saved to {out_png}")


def main():
    run_backtest()


if __name__ == "__main__":
    main()
