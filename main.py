import os
from datetime import datetime
from classes.StockData import StockData
from classes.ChartPlotter import ChartPlotter
from classes.PivotDetector import PivotDetector
from classes.TrendlineAnalyzer import TrendlineAnalyzer
from classes.Logger import logger

# Stock Data Configurations
STOCK_SYMBOL = "AAPL"
START_DATE = "2024-01-01"
END_DATE = "2025-01-01"

# Pivot Detection Configurations
LOOKBACKS = [3, 5, 7, 10]
MIN_PRICE_DISTANCE = 5.0
PIVOT_FILENAME_SUFFIX = "pivot_levels"
PIVOT_SUBDIR = "artifacts/levels/"
PLOT_PIVOTS = True  # Set to False if you don't want to plot the pivot points

# Trendline Analysis Configurations
TRENDLINE_MIN_SCORE = 0
TRENDLINE_FILENAME_SUFFIX = "trendlines_regression"
TRENDLINE_SUBDIR = "artifacts/trendlines/"
TRENDLINE_THRESHOLDS = range(3, 8)


def ensure_directory(directory: str) -> None:
    """Ensure that the directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def get_timestamp() -> str:
    """Return a timestamp string for file naming."""
    return datetime.now().strftime("%Y%m%d%H%M%S")


def load_stock_data():
    """Load and return the stock data."""
    logger.info(f"Loading stock data for {STOCK_SYMBOL} from {START_DATE} to {END_DATE}.")
    return StockData(STOCK_SYMBOL, START_DATE, END_DATE)


def detect_pivots(stock_data):
    """Detect and return pivot points."""
    detector = PivotDetector(stock_data.df, lookbacks=LOOKBACKS)
    return detector.detect_all()


def plot_pivots(stock_data, all_pivots):
    """Plot the pivot levels if desired."""
    timestamp = get_timestamp()
    filename = f"{PIVOT_FILENAME_SUFFIX}_{timestamp}.png"
    ensure_directory(PIVOT_SUBDIR)

    plotter = ChartPlotter(stock_data.df, all_pivots)
    plotter.plot_levels(all_pivots, filename=filename, subdirectory=PIVOT_SUBDIR, min_price_distance=MIN_PRICE_DISTANCE)
    logger.info(f"Pivot levels plotted and saved to {os.path.join(PIVOT_SUBDIR, filename)}")


def analyze_and_plot_trendlines(stock_data, all_pivots):
    """Analyze trendlines for various thresholds and plot the regression lines."""
    # Combine high and low pivots into a single list
    combined_pivots = [p for highs, lows in all_pivots.values() for p in (highs + lows)]
    
    trendlines_by_threshold = {}
    for min_pts in TRENDLINE_THRESHOLDS:
        analyzer = TrendlineAnalyzer(
            stock_data.df, combined_pivots, min_points=min_pts, trendline_min_score=TRENDLINE_MIN_SCORE
        )
        logger.info(f"Analyzing trendlines with minimum {min_pts} points.")
        trendlines_by_threshold[min_pts] = analyzer.analyze()

    timestamp = get_timestamp()
    filename = f"{TRENDLINE_FILENAME_SUFFIX}_{timestamp}.png"
    ensure_directory(TRENDLINE_SUBDIR)

    plotter = ChartPlotter(stock_data.df, combined_pivots)
    plotter.plot_trendlines(trendlines_by_threshold, filename=filename, subdirectory=TRENDLINE_SUBDIR)
    logger.info(f"Trendlines plotted and saved to {os.path.join(TRENDLINE_SUBDIR, filename)}")


def main():
    stock_data = load_stock_data()
    all_pivots = detect_pivots(stock_data)

    # Optionally plot pivot points
    if PLOT_PIVOTS:
        plot_pivots(stock_data, all_pivots)

    analyze_and_plot_trendlines(stock_data, all_pivots)


if __name__ == "__main__":
    main()
