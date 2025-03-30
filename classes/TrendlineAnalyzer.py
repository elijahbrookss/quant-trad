import pandas as pd
from typing import List, Tuple, Optional
from classes.Logger import logger
from classes.Trendline import Trendline

class TrendlineAnalyzer:
    def __init__(
        self,
        df,
        pivots: List[Tuple[str, float]],
        min_points: int = 3,
        tolerance: float = 0.001,
        max_trendlines: int = 500,
        trendline_min_score: float = 0
    ):
        """
        Analyze trendlines from a list of pivot points in stock data.

        Parameters:
            df: DataFrame containing the stock data.
            pivots: A list of tuples (date, price).
            min_points: Minimum number of pivot points required to form a trendline.
            tolerance: Allowed relative deviation for a point to be considered on the line.
            max_trendlines: Maximum number of trendlines to return.
            trendline_min_score: Minimum score threshold for a trendline to be accepted.
        """
        self.df = df
        self.pivots = pivots
        self.min_points = min_points
        self.tolerance = tolerance
        self.max_trendlines = max_trendlines
        self.trendline_min_score = trendline_min_score

    def _calculate_line_parameters(self, t1: str, p1: float, t2: str, p2: float) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate slope and intercept for a line through two pivot points.
        
        Returns:
            A tuple (slope, intercept) or (None, None) if the two dates are equal.
        """
        x1 = pd.Timestamp(t1).timestamp()
        x2 = pd.Timestamp(t2).timestamp()
        if x2 == x1:
            return None, None  # Cannot compute if the timestamps are identical
        slope = (p2 - p1) / (x2 - x1)
        intercept = p1 - slope * x1
        return slope, intercept

    def _point_on_line(self, slope: float, intercept: float, date: str, price: float) -> bool:
        """
        Check if a given pivot point (date, price) lies on the line defined by slope and intercept within tolerance.
        """
        x = pd.Timestamp(date).timestamp()
        relative_error = abs((slope * x + intercept - price) / price)
        return relative_error <= self.tolerance

    def analyze(self) -> List[Trendline]:
        """
        Analyze pivot points to detect valid trendlines and return them sorted by score.
        """
        if len(self.pivots) < self.min_points:
            return []

        trendlines = []
        n = len(self.pivots)
        
        # Precompute timestamps to avoid repeated conversions
        pivots_with_ts = [
            (pd.Timestamp(date).timestamp(), date, price) for date, price in self.pivots
        ]

        # Iterate over pairs of pivot points to form candidate lines
        for i in range(n - self.min_points + 1):
            for j in range(i + 1, n):
                _, t1, p1 = pivots_with_ts[i]
                _, t2, p2 = pivots_with_ts[j]
                
                slope, intercept = self._calculate_line_parameters(t1, p1, t2, p2)
                if slope is None:
                    continue

                # Find all pivots that lie on the candidate line within tolerance
                points_on_line = [
                    (date, price)
                    for _, date, price in pivots_with_ts
                    if self._point_on_line(slope, intercept, date, price)
                ]

                if len(points_on_line) >= self.min_points:
                    # Sort points chronologically using their timestamps
                    points_on_line.sort(key=lambda pt: pd.Timestamp(pt[0]).timestamp())
                    tl = Trendline(
                        slope,
                        intercept,
                        points_on_line[0][0],
                        points_on_line[-1][0],
                        points_on_line,
                        self.df
                    )
                    
                    # Avoid adding trendlines with significant overlap (more than 10%)
                    if all(len(set(tl.points) & set(existing.points)) / len(tl.points) <= 0.1 for existing in trendlines):
                        if tl.score > self.trendline_min_score:
                            trendlines.append(tl)

        logger.info("Trendline analysis completed. Found %d trendlines", len(trendlines))
        
        # Return trendlines sorted by score (highest first) and limited to max_trendlines
        return sorted(trendlines, key=lambda t: t.score, reverse=True)[:self.max_trendlines]
