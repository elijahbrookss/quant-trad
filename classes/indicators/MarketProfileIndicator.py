from collections import defaultdict
import numpy as np
import pandas as pd
from typing import Dict, List
from classes.indicators.BaseIndicator import BaseIndicator
from mplfinance.plotting import make_addplot


class MarketProfileIndicator(BaseIndicator):
    NAME = "market_profile"

    def __init__(self, df: pd.DataFrame, bin_size: float = 0.1):
        super().__init__(df)
        self.bin_size = bin_size
        self.daily_profiles: List[Dict[str, float]] = []
        self.df.index = pd.to_datetime(self.df.index, utc=True)

    def compute(self) -> List[Dict[str, float]]:
        df = self.df.copy()
        df.index = pd.to_datetime(df.index, utc=True)
        grouped = df.groupby(df.index.date)
        profiles = []

        for date, group in grouped:
            volume_hist = self._build_volume_histogram(group)
            profile = self._extract_value_area(volume_hist)
            profile["date"] = pd.to_datetime(date)
            profiles.append(profile)

        self.daily_profiles = profiles
        return profiles

    def _build_volume_histogram(self, df: pd.DataFrame) -> Dict[float, float]:
        volume_by_price = defaultdict(float)

        for _, row in df.iterrows():
            low = row["low"]
            high = row["high"]
            volume = row["volume"]

            price_range = np.arange(low, high + self.bin_size, self.bin_size)
            vol_per_price = volume / len(price_range)

            for price in price_range:
                bucket = round(price / self.bin_size) * self.bin_size
                volume_by_price[bucket] += vol_per_price

        return dict(volume_by_price)

    def _extract_value_area(self, volume_hist: Dict[float, float]) -> Dict[str, float]:
        total_volume = sum(volume_hist.values())
        sorted_buckets = sorted(volume_hist.items(), key=lambda x: x[1], reverse=True)

        poc = sorted_buckets[0][0]
        cumulative_volume = 0
        value_area_prices = []

        for price, vol in sorted_buckets:
            cumulative_volume += vol
            value_area_prices.append(price)
            if cumulative_volume >= 0.7 * total_volume:
                break

        return {
            "POC": poc,
            "VAH": max(value_area_prices),
            "VAL": min(value_area_prices),
        }

    def to_overlays(self) -> List:
        if not self.daily_profiles:
            self.compute()

        overlays = []
        for profile in self.daily_profiles:
            date = profile["date"]
            session_index = self.df[self.df.index.date == date.date()].index
            if session_index.empty:
                continue
            overlays.append(make_addplot(pd.Series(profile["POC"], index=session_index), color="orange", width=1.2, linestyle="--", label="POC"))
            overlays.append(make_addplot(pd.Series(profile["VAH"], index=session_index), color="gray", width=1, linestyle="--", label="VAH"))
            overlays.append(make_addplot(pd.Series(profile["VAL"], index=session_index), color="gray", width=1, linestyle="--", label="VAL"))

        return overlays