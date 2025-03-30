from scipy.stats import linregress
import numpy as np
import pandas as pd
from typing import List, Tuple, Any

class Trendline:
    """
    Represents a trendline computed from pivot points in stock data.

    Attributes:
        slope (float): The slope of the trendline.
        intercept (float): The intercept of the trendline.
        start_date (Any): The starting date of the trendline.
        end_date (Any): The ending date of the trendline.
        points (List[Tuple[Any, float]]): A list of pivot points (date, price) used to compute the trendline.
        df (pd.DataFrame): The DataFrame containing stock data.
        r_squared (float): The R-squared value of the trendline fit.
        length (int): The duration (in days) between start_date and end_date.
        angle (float): The angle of the trendline in degrees.
        proximity (float): The relative difference between the current price and the trendline's prediction.
        violations (int): The count of data points that deviate from the trendline beyond a set tolerance.
        violation_ratio (float): Ratio of violations relative to the length of the trendline.
        score (float): A composite score representing the quality of the trendline.
    """
    def __init__(
        self,
        slope: float,
        intercept: float,
        start_date: Any,
        end_date: Any,
        points: List[Tuple[Any, float]],
        df: pd.DataFrame
    ) -> None:
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

    def _calculate_r_squared(self) -> float:
        """
        Calculate the R-squared value for the trendline using the pivot points.

        Returns:
            float: The R-squared value.
        """
        x = np.array([pd.Timestamp(date).timestamp() for date, _ in self.points])
        y = np.array([price for _, price in self.points])
        _, _, r_value, _, _ = linregress(x, y)
        return r_value ** 2

    def _calculate_length(self) -> int:
        """
        Calculate the duration of the trendline in days.

        Returns:
            int: Number of days between start_date and end_date.
        """
        return (pd.Timestamp(self.end_date) - pd.Timestamp(self.start_date)).days

    def _calculate_angle(self) -> float:
        """
        Calculate the absolute angle of the trendline in degrees.

        Returns:
            float: The angle in degrees.
        """
        return abs(np.degrees(np.arctan(self.slope)))

    def _calculate_proximity(self) -> float:
        """
        Calculate the relative difference between the current closing price and the trendline's prediction.

        Returns:
            float: The proximity as a relative error.
        """
        current_price = self.df['Close'].iloc[-1]
        current_x = pd.Timestamp(self.df.index[-1]).timestamp()
        predicted_price = self.slope * current_x + self.intercept
        return abs(current_price - predicted_price) / current_price

    def _calculate_score(self) -> float:
        """
        Compute a composite score for the trendline based on several quality metrics.

        Returns:
            float: The computed score.
        """
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

    def _calculate_violations(self, tolerance: float = 0.01) -> int:
        """
        Count the number of times the actual closing price deviates from the trendline beyond the specified tolerance.

        Args:
            tolerance (float): The relative tolerance above which a deviation is counted as a violation.

        Returns:
            int: The total count of violations.
        """
        count = 0
        for date in self.df.index:
            # Only consider dates within the trendline period
            if self.start_date <= date <= self.end_date:
                x = pd.Timestamp(date).timestamp()
                predicted = self.slope * x + self.intercept
                actual = self.df.at[pd.Timestamp(date), 'Close']
                if abs(predicted - actual) / actual > tolerance:
                    count += 1
        return count
