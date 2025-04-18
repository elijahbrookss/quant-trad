
from classes.DataLoader import DataLoader
from classes.indicators.LevelsIndicator import DailyLevelsIndicator, H4LevelsIndicator
from classes.indicators.MarketProfileIndicator import DailyMarketProfileIndicator, MergedValueAreaIndicator
from classes.indicators.TrendlineIndicator import TrendlineIndicator
from classes.indicators.VWAPIndicator import VWAPIndicator
from classes.engines.StrategyEngine import StrategyEngine
from classes.engines.Backtester import Backtester


if __name__ == "__main__":
    symbol = "AAPL"

    # 1) Ensure the DB schema exists and back‑fill your data
    DataLoader.ensure_schema()

    # 2) Load DataFrames for each timeframe
    df_15 = DataLoader.get(symbol, tf="15m", lookback_days=120)   # execution TF
    df_h4 = DataLoader.get(symbol, tf="4h",  lookback_days=365)   # H4 for levels & trendlines
    df_day= DataLoader.get(symbol, tf="1d",  lookback_days=365)   # daily for levels & MP

    # 3) Instantiate & compute each indicator
    daily_lv   = DailyLevelsIndicator(df_day); daily_lv.compute()
    h4_lv      = H4LevelsIndicator(df_h4);    h4_lv.compute()

    daily_mp   = DailyMarketProfileIndicator(df_day)
    daily_mp.compute()
    merged_va  = MergedValueAreaIndicator(daily_mp.result, min_cluster=3)
    merged_va.compute()

    tl_h4      = TrendlineIndicator(df_h4, tf_label="4h");  tl_h4.compute()
    tl_15      = TrendlineIndicator(df_15, tf_label="15m"); tl_15.compute()

    vwap_15    = VWAPIndicator(df_15, session_tf="D", band_k=2.0)
    vwap_15.compute()

    # 4) Wire up the StrategyEngine
    indicators = [
        daily_lv,
        h4_lv,
        merged_va,
        tl_h4,
        tl_15,
        vwap_15,
    ]
    engine = StrategyEngine(indicators, atr_factor=0.15)

    # 5) Run the strategy over every 15‑min bar
    signals_df = engine.run(df_15, price_col="Close")

    # 6) Inspect and save
    print(signals_df[["score", "direction"]].tail(10))
    signals_df.to_csv("artifacts/simple_strategy_signals.csv")
    print("Signals written to artifacts/simple_strategy_signals.csv")

    bt = Backtester(signals_df, engine, entry_threshold=0.8, stop_loss=1.0, take_profit=2.0)
    trades_df = bt.run()
    print(trades_df)