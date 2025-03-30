import pandas as pd
from typing import List, Tuple, Dict, Any

from classes.Logger import logger

class PivotDetector:
    """
    Detect pivot points in stock data over multiple lookback periods.

    Attributes:
        df (pd.DataFrame): DataFrame containing stock data with at least 'High' and 'Low' columns.
        lookbacks (List[int]): A list of lookback periods for pivot detection.
        threshold (float): Relative threshold to treat pivots as distinct.
    """
    def __init__(self, df: pd.DataFrame, lookbacks: List[int], threshold: float = 0.005) -> None:
        self.df = df
        self.lookbacks = lookbacks
        self.threshold = threshold

    def detect_all(self) -> Dict[int, Tuple[List[Tuple[Any, float]], List[Tuple[Any, float]]]]:
        """
        Detects pivot points for each lookback period.

        Returns:
            Dict[int, Tuple[List[Tuple[Any, float]], List[Tuple[Any, float]]]]:
                A dictionary mapping each lookback period to a tuple containing:
                - List of high pivot tuples (date, high price)
                - List of low pivot tuples (date, low price)
        """
        all_pivots = {}
        for lookback in self.lookbacks:
            all_pivots[lookback] = self._find_pivots(lookback)
        return all_pivots

    def _find_pivots(self, lookback: int) -> Tuple[List[Tuple[Any, float]], List[Tuple[Any, float]]]:
        """
        Detect high and low pivot points for a given lookback period.

        Args:
            lookback (int): The lookback period to consider.

        Returns:
            Tuple[List[Tuple[Any, float]], List[Tuple[Any, float]]]:
                A tuple containing:
                - A list of high pivots (date, high price)
                - A list of low pivots (date, low price)
        """
        highs: List[Tuple[Any, float]] = []
        lows: List[Tuple[Any, float]] = []

        def is_near_existing(price: float) -> bool:
            # Check if the given price is near any already detected pivot using relative threshold
            return any(abs(price - existing_price) / existing_price < self.threshold 
                       for _, existing_price in (highs + lows))

        for i in range(lookback, len(self.df) - lookback):
            current_date = self.df.index[i]
            current_high = self.df.at[current_date, 'High']
            current_low = self.df.at[current_date, 'Low']

            # Get the surrounding range excluding the current data point
            high_range = self.df['High'].iloc[i - lookback:i + lookback + 1].drop(labels=[current_date])
            low_range = self.df['Low'].iloc[i - lookback:i + lookback + 1].drop(labels=[current_date])

            # Check for high pivot
            if current_high > high_range.max() and not is_near_existing(current_high):
                highs.append((current_date, current_high))
            # Check for low pivot (separate if to allow both high and low detection in same iteration)
            if current_low < low_range.min() and not is_near_existing(current_low):
                lows.append((current_date, current_low))

        logger.info(f"Detected {len(highs)} high pivots and {len(lows)} low pivots for lookback period {lookback}")
        return highs, lows