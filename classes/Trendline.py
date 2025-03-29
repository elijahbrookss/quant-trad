from scipy.stats import linregress
import numpy as np
import pandas as pd

class Trendline:
    def __init__(self, slope, intercept, start_date, end_date, points, df):
        self.slope = slope
        self.intercept = intercept
        self.start_date = start_date
        self.end_date = end_date
        self.points = points
        self.df = df

        self.r_squared = self._calculate_r_squared()
        self.length = self._calculate_length()
        self.angle = self._calculate_angle()
        self.proximity = self._calculate_proximity()
        self.violations = self._calculate_violations()
        self.violation_ratio = self.violations / max(1, self.length)
        self.score = self._calculate_score()

    def _calculate_r_squared(self):
        x = np.array([pd.Timestamp(date).timestamp() for date, _ in self.points])
        y = np.array([price for _, price in self.points])
        _, _, r_value, _, _ = linregress(x, y)
        return r_value**2

    def _calculate_length(self):
        return (pd.Timestamp(self.end_date) - pd.Timestamp(self.start_date)).days

    def _calculate_angle(self):
        return abs(np.degrees(np.arctan(self.slope)))

    def _calculate_proximity(self):
        current_price = self.df['Close'].iloc[-1]
        current_x = pd.Timestamp(self.df.index[-1]).timestamp()
        predicted_price = self.slope * current_x + self.intercept
        return abs(current_price - predicted_price) / current_price

    def _calculate_score(self):
        weights = {'r_squared': 0.1, 'points': 0.4, 'length': 0.3, 'angle': 0.2}
        norm_len = min(1.0, self.length / 365)
        norm_angle = 1.0 - min(abs(self.angle - 45) / 45, 1.0)
        score = (
            weights['r_squared'] * self.r_squared +
            weights['points'] * (len(self.points) / 10) +
            weights['length'] * norm_len +
            weights['angle'] * norm_angle
        )
        if self.proximity > 0.01:
            score *= 0.5
        if self.violation_ratio > 0.25:
            score *= 0.5
        if self.violation_ratio > 0.5:
            score = 0
        return score

    def _calculate_violations(self, tolerance=0.01):
        count = 0
        for date in self.df.index:
            if self.start_date <= date <= self.end_date:
                x = pd.Timestamp(date).timestamp()
                predicted = self.slope * x + self.intercept
                actual = self.df.at[pd.Timestamp(date), 'Close']
                if abs(predicted - actual) / actual > tolerance:
                    count += 1
        return count