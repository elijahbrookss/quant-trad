from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from classes.indicators.BaseIndicator import BaseIndicator

ARTIFACT_ROOT = Path("artifacts/market_profile")

# ------------------------------------------------------------------
# Helper to compute a day's POC / VAH / VAL with simple histogram method
# ------------------------------------------------------------------

def _value_area(highs: np.ndarray, lows: np.ndarray, vols: np.ndarray, bins=24):
    prices = np.concatenate([highs, lows])  # simplistic approximation
    v = np.concatenate([vols / 2, vols / 2])
    hist, edges = np.histogram(prices, bins=bins, weights=v)
    centers = (edges[:-1] + edges[1:]) / 2
    poc_idx = np.argmax(hist)
    poc = centers[poc_idx]
    total_vol = hist.sum()
    sorted_idx = np.argsort(hist)[::-1]
    cume = 0
    included = []
    for idx in sorted_idx:
        cume += hist[idx]
        included.append(idx)
        if cume >= 0.7 * total_vol:
            break
    in_bin = centers[included]
    return float(in_bin.min()), float(poc), float(in_bin.max())


class DailyMarketProfileIndicator(BaseIndicator):
    """Computes daily VAL/POC/VAH triplets for the last N days."""

    NAME = "daily_mp"

    def __init__(self, df: pd.DataFrame):
        super().__init__(df)
        self.daily_va: pd.DataFrame | None = None  # columns: date, val, poc, vah

    def compute(self):
        groups = self.df.groupby(self.df.index.date)
        records = []
        for date, g in groups:
            val, poc, vah = _value_area(g["High"].values, g["Low"].values, g["Volume"].values)
            records.append((pd.Timestamp(date), val, poc, vah))
        self.daily_va = pd.DataFrame(records, columns=["date", "val", "poc", "vah"])
        self.daily_va.set_index("date", inplace=True)
        self.result = self.daily_va
        self.score = 0  # not used here
        return self.result

    def plot(self) -> Path:
        if self.result is None:
            self.compute()
        fig, ax = plt.subplots(figsize=(12, 6))
        x = np.arange(len(self.daily_va))
        ax.fill_between(x, self.daily_va["val"], self.daily_va["vah"], color="gray", alpha=0.2)
        ax.plot(x, self.daily_va["poc"], color="yellow", linewidth=1)
        ax.set_title("Daily Value Areas")
        folder = ARTIFACT_ROOT
        folder.mkdir(parents=True, exist_ok=True)
        file = folder / "daily_va.png"
        fig.savefig(file, dpi=300, bbox_inches="tight")
        plt.close(fig)
        return file
    
    def get_daily_va(self, date: pd.Timestamp) -> Tuple[float, float, float]:
        """
        Return (VAL, POC, VAH) for the given session (date).
        """
        if self.result is None:
            self.compute()
        row = self.daily_va.loc[date.date()]
        return float(row.val), float(row.poc), float(row.vah)

class MergedValueAreaIndicator(BaseIndicator):
    """Merge consecutive daily value areas where their price ranges overlap.

    A cluster is kept only if **≥ `min_cluster`** daily VAs share an
    intersection band.  For each cluster we output:
        (cluster_VAL, cluster_POC, cluster_VAH, count)
    where POC is the *mean* of included POCs.
    """

    NAME = "merged_va"

    def __init__(self, daily_va_df: pd.DataFrame, min_cluster: int = 3):
        super().__init__(daily_va_df)  # df columns: val, poc, vah
        self.min_cluster = min_cluster
        self.merged: List[Tuple[float, float, float, int]] = []

    # ------------------------------------------------------------------
    def _intervals(self):
        return [
            (row.val, row.poc, row.vah) for row in self.df.itertuples()
        ]

    # ------------------------------------------------------------------
    def compute(self):
        intervals = self._intervals()
        if not intervals:
            self.result, self.score = [], 0
            return self.result

        clusters: dict[Tuple[float, float], List[Tuple[float, float, float]]] = {}
        for i, (low_i, poc_i, high_i) in enumerate(intervals):
            overlappers = [
                (low_j, poc_j, high_j)
                for (low_j, poc_j, high_j) in intervals
                if not (high_j < low_i or low_j > high_i)  # overlap condition
            ]
            if len(overlappers) >= self.min_cluster:
                inter_low = max(o[0] for o in overlappers)
                inter_high = min(o[2] for o in overlappers)
                if inter_low < inter_high:
                    inter_poc = float(np.mean([o[1] for o in overlappers]))
                    key = (round(inter_low, 2), round(inter_high, 2))
                    clusters[key] = (inter_low, inter_poc, inter_high, len(overlappers))

        self.merged = list(clusters.values())
        self.result = self.merged
        self.score = len(self.merged)
        return self.result
    
    def get_clusters(self) -> List[Tuple[float, float, float, int]]:
        """
        Return list of merged clusters: 
        each is (VAL, POC, VAH, count_of_days).
        """
        if self.result is None:
            self.compute()
        return self.merged
    
    # ------------------------------------------------------------------
    def plot(self) -> Path:
        if self.result is None:
            self.compute()

        plt.style.use("dark_background")
        fig, ax = plt.subplots(figsize=(8, 6))
        for val, poc, vah, cnt in self.merged:
            ax.axhspan(val, vah, alpha=0.3, color="purple")
            ax.axhline(poc, color="yellow", linewidth=1)
            ax.text(
                0.01,
                poc,
                f"{cnt}d",
                color="white",
                va="center",
                transform=ax.get_yaxis_transform(),
                fontsize=8,
            )
        ax.set_xlim(0, 1)  # dummy x‑axis; focus on price levels only
        ax.set_xticks([])
        ax.set_title("Merged Value Areas (≥3 overlapping days)")
        folder = ARTIFACT_ROOT
        folder.mkdir(parents=True, exist_ok=True)
        file = folder / "merged_va.png"
        fig.savefig(file, dpi=300, bbox_inches="tight")
        plt.close(fig)
        return file