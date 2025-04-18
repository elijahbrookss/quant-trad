from datetime import datetime
from classes.StockData import StockData
from classes.ChartPlotter import ChartPlotter
from classes.PivotDetector import PivotDetector
from classes.TrendlineAnalyzer import TrendlineAnalyzer

# from classes.strategies.SimpleStrategy import SimpleStrategy
from classes.DataLoader import DataLoader
from classes.indicators.LevelsIndicator import DailyLevelsIndicator, H4LevelsIndicator
from classes.indicators.MarketProfileIndicator import DailyMarketProfileIndicator, MergedValueAreaIndicator
from classes.indicators.TrendlineIndicator import TrendlineIndicator

if __name__ == "__main__":

    # symbol = "AAPL"
    # start_date = "2024-01-01"
    # end_date = "2025-01-01"
    # timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # stock_data = StockData(symbol, start_date, end_date)
    # detector = PivotDetector(stock_data.df, lookbacks=[5, 10, 15, 20, 25])
    # all_pivots = detector.detect_all()

    # combined_pivots = []
    # for high_pivots, low_pivots in all_pivots.values():
    #     combined_pivots.extend(high_pivots + low_pivots)

    # plotter = ChartPlotter(stock_data.df, combined_pivots)
    # trendlines_by_threshold = {}
    # for min_pts in range(3, 8):
    #     analyzer = TrendlineAnalyzer(stock_data.df, combined_pivots, min_points=min_pts)
    #     trendlines_by_threshold[min_pts] = analyzer.analyze()

    # plot_filename = f"trendlines_regression_{timestamp}.png"
    # plotter.plot_trendlines(trendlines_by_threshold, filename=plot_filename)

    # strat = SimpleStrategy()
    # print("Strategy confidence:", strat.run())

    # Seed database with 1 year of 15‑minute data
    # DataLoader.ensure_schema()
    # rows = DataLoader.ingest_history("AAPL", days=365, interval="60m")
    # print("inserted", rows, "rows of 15‑minute data.")

    df_h4  = DataLoader.get("AAPL", tf="4h",   lookback_days=365)
    df_m30 = DataLoader.get("AAPL", tf="30min", lookback_days=120)

    tl_h4  = TrendlineIndicator(df_h4,  tf_label="4h")
    tl_m30 = TrendlineIndicator(df_m30, tf_label="30min")

    for tl in (tl_h4, tl_m30):
        tl.compute()
        tl.plot()
        print("Top line score", tl.score)