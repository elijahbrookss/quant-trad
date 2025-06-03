from collections import defaultdict
import numpy as np
import pandas as pd
from typing import Dict, List
from classes.indicators.BaseIndicator import BaseIndicator
from mplfinance.plotting import make_addplot
from classes.indicators.config import DataContext
from classes.Logger import logger
from typing import Tuple, Set


class MarketProfileIndicator(BaseIndicator):
    NAME = "market_profile"

    def __init__(self, df: pd.DataFrame, bin_size: float = 0.1, mode: str = "tpo"):
        super().__init__(df)
        self.bin_size = bin_size
        self.mode = mode  # "tpo" for now
        self.daily_profiles: List[Dict[str, float]] = self.compute() 

    @classmethod
    def from_context(cls, provider, ctx: DataContext, bin_size: float = 0.1, mode: str = "tpo", interval: str = "30m") -> "MarketProfileIndicator":
        """
        Instantiate MarketProfileIndicator from a provider and a DataContext.
        """
        ctx = DataContext(
            symbol=ctx.symbol,
            start=ctx.start,
            end=ctx.end,
            interval=interval
        )
        ctx.validate()
        
        df = provider.get_ohlcv(ctx)

        if df is None or df.empty:
            logger.warning("No data found for MarketProfile [%s] from %s to %s", ctx.symbol, ctx.start, ctx.end)
            rows_ingested = provider.ingest_history(ctx)
            if rows_ingested == 0:
                raise ValueError(f"Failed to ingest data for {ctx.symbol} ({ctx.interval}) from {ctx.start} to {ctx.end}")
            df = provider.get_ohlcv(ctx)

        if df.empty:
            raise ValueError(f"MarketProfileIndicator: No data to compute after ingest for {ctx.symbol} [{ctx.interval}]")

        return cls(df=df, bin_size=bin_size, mode=mode)

    def compute(self) -> List[Dict[str, float]]:
        """
        Computes TPO-based market profile for each daily session.
        """
    
        df = self.df.copy()
        df.index = pd.to_datetime(df.index, utc=True)        
        grouped = df.groupby(df.index.date)

        profiles = []

        for date, group in grouped:
            tpo_hist = self._build_tpo_histogram(group)
            profile = self._extract_value_area(tpo_hist)
            profile["date"] = pd.to_datetime(str(date)).tz_localize("UTC")
            profiles.append(profile)
            logger.debug("Profile for %s: POC=%.2f, VAH=%.2f, VAL=%.2f", profile["date"], profile["POC"], profile["VAH"], profile["VAL"])


        

        self.daily_profiles = profiles
        return profiles

    def _build_tpo_histogram(self, df: pd.DataFrame) -> Dict[float, int]:
        """
        Builds a TPO histogram: counts how many 30m bars visited each price bucket.
        """
        tpo_count = defaultdict(int)

        for _, row in df.iterrows():
            low = row["low"]
            high = row["high"]

            # Round each price into buckets
            price_range = np.arange(low, high + self.bin_size, self.bin_size)

            for price in price_range:
                bucket = round(price / self.bin_size) * self.bin_size
                tpo_count[bucket] += 1

        return dict(tpo_count)

    def _extract_value_area(self, tpo_hist: Dict[float, int]) -> Dict[str, float]:
        """
        Given a TPO histogram, computes:
        - POC (most visited price)
        - VAH, VAL (top and bottom of 70% of TPOs)
        """
        total_tpos = sum(tpo_hist.values())
        sorted_buckets = sorted(tpo_hist.items(), key=lambda x: x[1], reverse=True)

        poc = sorted_buckets[0][0]
        cumulative = 0
        value_area_prices = []

        for price, count in sorted_buckets:
            cumulative += count
            value_area_prices.append(price)
            if cumulative >= 0.7 * total_tpos:
                break

        return {
            "POC": poc,
            "VAH": max(value_area_prices),
            "VAL": min(value_area_prices),
        }

    def to_overlays(self, plot_df: pd.DataFrame) -> Tuple[List, Set[Tuple[str, str]]]:
        """
        Returns chart overlays (POC, VAH, VAL lines) aligned to the plot_df index.
        Each value area line is aligned with the daily session window within plot_df.
        Logs session matching details for debugging.

        
        """

        if not self.daily_profiles:
            logger.warning("No daily profiles available to generate overlays.")
            return []

        overlays = []
        legend_entries = set()

        full_index = plot_df.index
        logger.info("Generating overlays for %d sessions", len(self.daily_profiles))

        style_map = {
            "POC": {"color": "orange", "width": 1.2},
            "VAH": {"color": "gray", "width": 1},
            "VAL": {"color": "gray", "width": 1},
        }

        for profile in self.daily_profiles:
            date = profile["date"]
            session_index = plot_df[plot_df.index.date == date.date()].index

            if session_index.empty:
                logger.warning("No candles matched for session date: %s", date.date())
                continue

            logger.debug("Matched %d candles for session date: %s", len(session_index), date.date())

            for key, style in style_map.items():
                session_series = pd.Series([profile[key]] * len(session_index), index=session_index)
                aligned_line = session_series.reindex(full_index, fill_value=pd.NA)

                overlays.append(make_addplot(
                    aligned_line,
                    color=style["color"],
                    width=style["width"],
                    linestyle="--",
                    label=""
                ))

                legend_entries.add((key, style["color"]))

        logger.info("Generated %d overlays total", len(overlays))
        return overlays, legend_entries