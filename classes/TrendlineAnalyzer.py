import pandas as pd

from classes.Logger import logger
from classes.Trendline import Trendline

class TrendlineAnalyzer:
    def __init__(self, df, pivots, min_points=3, tolerance=0.001, max_trendlines=500, trendline_min_score=0):
        self.df = df
        self.pivots = pivots
        self.min_points = min_points
        self.tolerance = tolerance
        self.max_trendlines = max_trendlines
        self.trendline_min_score = trendline_min_score

    def analyze(self):
        if len(self.pivots) < self.min_points:
            return []

        trendlines = []
        n = len(self.pivots)

        for i in range(n - self.min_points + 1):
            for j in range(i + 1, n):
                t1, p1 = self.pivots[i]
                t2, p2 = self.pivots[j]
                if pd.Timestamp(t2) == pd.Timestamp(t1):
                    continue

                slope = (p2 - p1) / (pd.Timestamp(t2).timestamp() - pd.Timestamp(t1).timestamp())
                intercept = p1 - slope * pd.Timestamp(t1).timestamp()

                points_on_line = []
                for date, price in self.pivots:
                    x = pd.Timestamp(date).timestamp()
                    if abs((slope * x + intercept - price) / price) <= self.tolerance:
                        points_on_line.append((date, price))

                if len(points_on_line) >= self.min_points:
                    points_on_line.sort(key=lambda x: x[0])
                    tl = Trendline(slope, intercept, points_on_line[0][0], points_on_line[-1][0], points_on_line, self.df)
                    
                    if all(len(set(tl.points) & set(e.points)) / len(tl.points) <= 0.1 for e in trendlines):
                        if tl.score > self.trendline_min_score:
                            trendlines.append(tl)

        logger.info("Trendline analysis completed. Found %d trendlines", len(trendlines))
        return sorted(trendlines, key=lambda t: t.score, reverse=True)[:self.max_trendlines]
