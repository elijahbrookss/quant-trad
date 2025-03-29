from datetime import datetime
from classes.StockData import StockData
from classes.ChartPlotter import ChartPlotter
from classes.PivotDetector import PivotDetector
from classes.TrendlineAnalyzer import TrendlineAnalyzer

if __name__ == "__main__":
    symbol = "AAPL"
    start_date = "2024-01-01"
    end_date = "2025-01-01"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    stock_data = StockData(symbol, start_date, end_date)
    detector = PivotDetector(stock_data.df, lookbacks=[5, 10, 15, 20, 25])
    all_pivots = detector.detect_all()

    combined_pivots = []
    for high_pivots, low_pivots in all_pivots.values():
        combined_pivots.extend(high_pivots + low_pivots)

    plotter = ChartPlotter(stock_data.df, combined_pivots)
    trendlines_by_threshold = {}
    for min_pts in range(3, 8):
        analyzer = TrendlineAnalyzer(stock_data.df, combined_pivots, min_points=min_pts)
        trendlines_by_threshold[min_pts] = analyzer.analyze()

    plot_filename = f"trendlines_regression_{timestamp}.png"
    plotter.plot_trendlines(trendlines_by_threshold, filename=plot_filename)
