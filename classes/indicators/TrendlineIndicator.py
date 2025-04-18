from classes.indicators.BaseIndicator import BaseIndicator
from classes.PivotDetector import PivotDetector
from classes.TrendlineAnalyzer import TrendlineAnalyzer
from classes.ChartPlotter import ChartPlotter
from pathlib import Path
from typing import Dict, List
import pandas as pd

ARTIFACT_ROOT = Path("artifacts")

class TrendlineIndicator(BaseIndicator):
    NAME = "trendlines"

    def __init__(self, df: pd.DataFrame, min_points_range=range(3, 7)):
        super().__init__(df)
        self.min_points_range = min_points_range
        self.trendlines_by_thresh: Dict[int, List] = {}

    # ------------------------------------------------------------------
    def compute(self):
        # Generate a single list of pivots first (using median lookback)
        pivots_detector = PivotDetector(self.df, lookbacks=[10])
        highs, lows = pivots_detector._find_pivots(10)  # detector API nuance
        all_pivots = sorted(highs + lows, key=lambda x: x[0])

        for n in self.min_points_range:
            analyzer = TrendlineAnalyzer(self.df, all_pivots, min_points=n)
            self.trendlines_by_thresh[n] = analyzer.analyze()

        self.result = self.trendlines_by_thresh
        # Quick heuristic score – more confirmed lines ⇒ higher confidence
        self.score = sum(bool(v) for v in self.trendlines_by_thresh.values()) / len(
            self.trendlines_by_thresh
        )
        return self.result

    # ------------------------------------------------------------------
    def plot(self) -> Path:
        if self.result is None:
            self.compute()
        # Delegates plotting back to the existing helper
        chart = ChartPlotter(self.df, pivots=[])
        chart.plot_trendlines(self.result, filename="trendlines.png", subdirectory=str(ARTIFACT_ROOT / self.NAME))
        return ARTIFACT_ROOT / self.NAME / "trendlines.png"
