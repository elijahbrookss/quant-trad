import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Set

from mplfinance.plotting import make_addplot

from classes.Logger import logger
from classes.indicators.BaseIndicator import BaseIndicator
from classes.indicators.config import DataContext


class MarketProfileIndicator(BaseIndicator):
    NAME = "market_profile"

    def __init__(
        self,
        df: pd.DataFrame,
        bin_size: float = 0.1,
        mode: str = "tpo"
    ):
        super().__init__(df)
        self.bin_size = bin_size
        self.mode = mode
        self.daily_profiles: List[Dict[str, float]] = self._compute_daily_profiles()
        self.merged_profiles: List[Dict[str, float]] = []

    @classmethod
    def from_context(
        cls,
        provider,
        ctx: DataContext,
        bin_size: float = 0.1,
        mode: str = "tpo",
        interval: str = "30m"
    ) -> "MarketProfileIndicator":
        """
        Create an instance using a data provider and context. Ingests history if needed.
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
            raise ValueError(
                f"MarketProfileIndicator: No data available for {ctx.symbol} [{ctx.interval}] after ingest"
            )

        return cls(df=df, bin_size=bin_size, mode=mode)

    def _compute_daily_profiles(self) -> List[Dict[str, float]]:
        """
        Build daily TPO profiles with POC, VAH, VAL, and session timestamps.
        """
        df = self.df.copy()
        df.index = pd.to_datetime(df.index, utc=True)
        profiles: List[Dict[str, float]] = []

        # Group rows by calendar date
        grouped = df.groupby(df.index.date)
        for session_date, group in grouped:
            tpo_hist = self._build_tpo_histogram(group)
            value_area = self._extract_value_area(tpo_hist)

            first_ts = group.index.min()
            last_ts = group.index.max()
            value_area.update({
                "date": pd.to_datetime(session_date),
                "start_date": first_ts,
                "end_date": last_ts
            })

            profiles.append(value_area)
            logger.debug(
                "Profile for %s: POC=%.2f, VAH=%.2f, VAL=%.2f",
                session_date,
                value_area["POC"],
                value_area["VAH"],
                value_area["VAL"]
            )

        return profiles

    def _build_tpo_histogram(self, data: pd.DataFrame) -> Dict[float, int]:
        """
        Count how many bars visit each price bucket (by bin_size).
        """
        tpo_counts: Dict[float, int] = {}
        for _, row in data.iterrows():
            low, high = row["low"], row["high"]
            # Create price buckets from low to high (inclusive)
            prices = np.arange(low, high + self.bin_size, self.bin_size)
            for price in prices:
                bucket = round(price / self.bin_size) * self.bin_size
                tpo_counts[bucket] = tpo_counts.get(bucket, 0) + 1
        return tpo_counts

    def _extract_value_area(self, tpo_hist: Dict[float, int]) -> Dict[str, float]:
        """
        From TPO histogram, compute POC, VAH, VAL (70% value area).
        """
        total = sum(tpo_hist.values())
        if total == 0:
            return {"POC": None, "VAH": None, "VAL": None}

        # Sort buckets by count descending
        sorted_buckets = sorted(tpo_hist.items(), key=lambda item: item[1], reverse=True)
        poc_price = sorted_buckets[0][0]

        cumulative = 0
        va_prices: List[float] = []
        threshold = 0.7 * total
        for price, count in sorted_buckets:
            cumulative += count
            va_prices.append(price)
            if cumulative >= threshold:
                break

        return {
            "POC": poc_price,
            "VAH": max(va_prices),
            "VAL": min(va_prices)
        }

    @staticmethod
    def _calculate_overlap(
        val1: float,
        vah1: float,
        val2: float,
        vah2: float
    ) -> float:
        """
        Compute overlap ratio between two value areas, relative to second VA's range.
        """
        low = max(val1, val2)
        high = min(vah1, vah2)
        overlap = max(0.0, high - low)
        range2 = vah2 - val2
        return overlap / range2 if range2 > 0 else 0.0

    def merge_value_areas(
        self,
        threshold: float = 0.6,
        min_merge: int = 2
    ) -> List[Dict[str, float]]:
        """
        Merge consecutive daily profiles if their value areas overlap by threshold.
        """
        merged: List[Dict[str, float]] = []
        profiles = self.daily_profiles
        i, n = 0, len(profiles)

        while i < n:
            base = profiles[i]
            merged_val = base["VAL"]
            merged_vah = base["VAH"]
            start_ts = base["start_date"]
            end_ts = base["end_date"]
            poc_list = [base["POC"]] if base.get("POC") is not None else []
            count = 1
            j = i + 1

            while j < n:
                next_prof = profiles[j]
                overlap = self._calculate_overlap(
                    merged_val,
                    merged_vah,
                    next_prof["VAL"],
                    next_prof["VAH"]
                )
                if overlap < threshold:
                    break

                merged_val = min(merged_val, next_prof["VAL"])
                merged_vah = max(merged_vah, next_prof["VAH"])
                end_ts = next_prof["end_date"]
                if next_prof.get("POC") is not None:
                    poc_list.append(next_prof["POC"])
                count += 1
                j += 1

            if count >= min_merge:
                avg_poc = sum(poc_list) / len(poc_list) if poc_list else None
                merged.append({
                    "start_date": start_ts,
                    "end_date": end_ts,
                    "VAL": merged_val,
                    "VAH": merged_vah,
                    "POC": avg_poc
                })

            i = j

        self.merged_profiles = merged
        return merged

    def to_overlays(
        self,
        plot_df: pd.DataFrame,
        use_merged: bool = True
    ) -> Tuple[List, Set[Tuple[str, str]]]:
        """
        Generate chart overlays (POC/VAH/VAL) aligned to plot_df's index.
        Returns a list of addplot objects and legend entries.
        """
        profiles = self.merged_profiles if use_merged else self.daily_profiles
        if not profiles:
            logger.warning("No profiles to generate overlays.")
            return [], set()

        overlays = []
        legend_entries: Set[Tuple[str, str]] = set()
        full_idx = plot_df.index

        styles = {
            "POC": {"color": "orange", "width": 1.2},
            "VAH": {"color": "gray", "width": 1.0},
            "VAL": {"color": "gray", "width": 1.0}
        }

        for prof in profiles:
            start_ts, end_ts = prof["start_date"], prof["end_date"]
            session_idx = full_idx[(full_idx >= start_ts) & (full_idx <= end_ts)]
            if session_idx.empty:
                logger.warning("No data in plot for session: %s", prof)
                continue

            for key, style in styles.items():
                values = pd.Series(
                    [prof[key]] * len(session_idx),
                    index=session_idx
                )
                aligned = values.reindex(full_idx, fill_value=np.nan)
                overlays.append(
                    make_addplot(
                        aligned,
                        color=style["color"],
                        width=style["width"],
                        linestyle="--",
                        label=""
                    )
                )
                legend_entries.add((key, style["color"]))

        return overlays, legend_entries