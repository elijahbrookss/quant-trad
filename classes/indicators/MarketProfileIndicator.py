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
        Computes TPO‐based market profile for each daily session.
        Now also records each session’s real start/end intraday timestamps.
        """
        df = self.df.copy()
        df.index = pd.to_datetime(df.index, utc=True)

        # Group by calendar date (UTC) so that 'date' is the midnight of each day
        grouped = df.groupby(df.index.date)

        profiles: List[Dict[str, float]] = []

        for date, group in grouped:
            # Build your TPO histogram and extract VAL/VAH/POC as before
            tpo_hist = self._build_tpo_histogram(group)
            profile = self._extract_value_area(tpo_hist)

            # 2) Record the actual first‐bar and last‐bar timestamps for this session:
            first_ts = group.index.min()  # e.g. 2025-05-15 13:30:00+00:00
            last_ts  = group.index.max()  # e.g. 2025-05-15 20:00:00+00:00
            profile["start_date"] = first_ts
            profile["end_date"]   = last_ts

            # Now you have:
            #   profile["POC"], profile["VAH"], profile["VAL"]
            #   profile["date"]   (calendar midnight if you still want it)
            #   profile["start_ts"], profile["end_ts"]  (real intraday bounds)

            profiles.append(profile)
            logger.debug("Computed profile for %s: POC=%.2f, VAH=%.2f, VAL=%.2f",
                         date, profile["POC"], profile["VAH"], profile["VAL"])

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
    
    def _value_area_overlap(self, val1, vah1, val2, vah2) -> float:
        overlap_low = max(val1, val2)
        overlap_high = min(vah1, vah2)

        overlap_range = max(0.0, overlap_high - overlap_low)
        target_range = vah2 - val2  # use the *next* VA's range for % basis

        if target_range == 0:
            return 0.0

        return overlap_range / target_range

    
    def merge_value_areas(self, threshold: float = 0.6, min_merge: float = 2) -> List[Dict]:
        merged_profiles = []
        i = 0
        n = len(self.daily_profiles)

        while i < n:
            base = self.daily_profiles[i]
            merged_val = base['VAL']
            merged_vah = base['VAH']
            start_date = base['start_date']
            end_date = base['end_date']
            poc_values = [base['POC']] if base['POC'] is not None else []
            merge_count = 1
            j = i + 1

            while j < n:
                next_va = self.daily_profiles[j]
                overlap = self._value_area_overlap(merged_val, merged_vah, next_va['VAL'], next_va['VAH'])

                if overlap >= threshold:
                    merged_val = min(merged_val, next_va['VAL'])
                    merged_vah = max(merged_vah, next_va['VAH'])
                    end_date = next_va['end_date']
                    if next_va['POC'] is not None:
                        poc_values.append(next_va['POC'])

                    merge_count += 1
                    j += 1
                else:
                    break
                
            if merge_count >= min_merge:
                merged_poc = sum(poc_values) / len(poc_values) if poc_values else None
                merged_profiles.append({
                    "start_date": start_date,
                    "end_date": end_date,
                    "VAL": merged_val,
                    "VAH": merged_vah,
                    "POC": merged_poc,
                })
            i = j

        self.merged_profiles = merged_profiles
        return merged_profiles


    def to_overlays(self, plot_df: pd.DataFrame, merged_vas: bool=True) -> Tuple[List, Set[Tuple[str, str]]]:
        """
        Returns chart overlays (POC, VAH, VAL lines) aligned to the plot_df index.
        Each value area line is aligned with the daily session window within plot_df.
        Logs session matching details for debugging.
        """
        if not self.daily_profiles:
            logger.warning("No daily profiles available to generate overlays.")
            return []

        if merged_vas and not hasattr(self, 'merged_profiles'):
            raise ValueError("Merged VAs not computed. Call merge_value_areas() first.")

        profiles = self.merged_profiles if merged_vas else self.daily_profiles

        overlays = []
        legend_entries = set()

        full_index = plot_df.index
        logger.info("Generating overlays for %d sessions", len(profiles))

        style_map = {
            "POC": {"color": "orange", "width": 1.2},
            "VAH": {"color": "gray", "width": 1},
            "VAL": {"color": "gray", "width": 1},
        }

        for profile in profiles:
            start = profile["start_date"]
            end   = profile["end_date"]
            session_index = plot_df[(plot_df.index >= start) & (plot_df.index <= end)].index
            logger.debug("Merged block: start_date=%s, end_date=%s", start, end)

            if session_index.empty:
                logger.warning("No candles matched for session: %s", profile)
                continue

            for key, style in style_map.items():
                session_series = pd.Series([profile[key]] * len(session_index), index=session_index)
                aligned_line = session_series.reindex(full_index, fill_value=np.nan)

                overlays.append(make_addplot(
                    aligned_line,
                    color=style["color"],
                    width=style["width"],
                    linestyle="--",
                    label=""
                ))

                legend_entries.add((key, style["color"]))

        logger.debug("Generated %d overlays for %d sessions", len(overlays), len(profiles))
        return overlays, legend_entries