import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Set
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle

from mplfinance.plotting import make_addplot

from classes.Logger import logger
from classes.indicators.BaseIndicator import BaseIndicator
from classes.indicators.config import DataContext


class MarketProfileIndicator(BaseIndicator):
    """
    Computes daily market profile (TPO) to identify Point of Control (POC),
    Value Area High (VAH), and Value Area Low (VAL), and provides plotting overlays.
    """
    NAME = "market_profile"

    def __init__(
        self,
        df: pd.DataFrame,
        bin_size: float = 0.1,
        mode: str = "tpo"
    ):
        """
        :param df: OHLCV DataFrame indexed by timestamp.
        :param bin_size: price bucket size for TPO histogram.
        :param mode: profile mode (only 'tpo' supported today).
        """
        super().__init__(df)
        self.bin_size = bin_size
        self.mode = mode
        # Compute raw daily profiles on initialization
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
        Fetches OHLCV from provider and constructs the indicator.
        Raises ValueError if no data is available.
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
        Build daily TPO profiles: POC, VAH, VAL, plus session timestamps.
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
        Count how many bars visit each price bucket defined by bin_size.
        :param data: intraday DataFrame for one session.
        :return: mapping of price bucket -> count of TPO occurrences.
        """
        tpo_counts: Dict[float, int] = {}
        for _, row in data.iterrows():
            low, high = row["low"], row["high"]
            prices = np.arange(low, high + self.bin_size, self.bin_size)
            for price in prices:
                bucket = round(price / self.bin_size) * self.bin_size
                tpo_counts[bucket] = tpo_counts.get(bucket, 0) + 1
        return tpo_counts

    def _extract_value_area(self, tpo_hist: Dict[float, int]) -> Dict[str, float]:
        """
        From the TPO histogram, compute:
          - POC: price with highest count
          - VAH: upper bound of 70% cumulative TPO
          - VAL: lower bound of 70% cumulative TPO
        """
        total = sum(tpo_hist.values())
        if total == 0:
            return {"POC": None, "VAH": None, "VAL": None}

        # sort buckets by descending count
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
        Compute the overlap ratio between two value areas,
        normalized by the range of the second area.
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
        Combine consecutive daily profiles whose value areas overlap
        at least `threshold` fraction, requiring at least `min_merge` days.
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
        Create persistent value area overlays using horizontal lines extending from
        each merged profile's start to the end of the chart.

        Returns:
            - overlays: list of mplfinance addplots
            - legend_entries: set of (label, color) tuples
        """
        profiles = self.merged_profiles if use_merged else self.daily_profiles
        if not profiles:
            logger.warning("No profiles to generate overlays.")
            return [], set()

        overlays = []
        legend_entries: Set[Tuple[str, str]] = set()

        full_idx = plot_df.index
        end_ts = full_idx[-1]

        for prof in profiles:
            start_ts = prof["start_date"]
            vah = prof["VAH"]
            val = prof["VAL"]
            poc = prof.get("POC")

            # Masked series for VAH
            vah_series = pd.Series(index=full_idx, dtype=float)
            vah_series[(full_idx >= start_ts) & (full_idx <= end_ts)] = vah
            overlays.append(make_addplot(vah_series, color="gray", width=0.9, linestyle="--"))
            legend_entries.add(("VAH", "gray"))

            # Masked series for VAL
            val_series = pd.Series(index=full_idx, dtype=float)
            val_series[(full_idx >= start_ts) & (full_idx <= end_ts)] = val
            overlays.append(make_addplot(val_series, color="gray", width=0.9, linestyle="--"))
            legend_entries.add(("VAL", "gray"))

            # Optional: POC line
            if poc is not None:
                poc_series = pd.Series(index=full_idx, dtype=float)
                poc_series[(full_idx >= start_ts) & (full_idx <= end_ts)] = poc
                overlays.append(make_addplot(poc_series, color="orange", width=1.0, linestyle="--"))
                legend_entries.add(("POC", "orange"))

        return overlays, legend_entries
