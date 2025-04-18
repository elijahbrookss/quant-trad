from classes.indicators.BaseIndicator import BaseIndicator
from pathlib import Path

import numpy as np
import pandas as pd

class MarketProfileIndicator(BaseIndicator):
    NAME = "market_profile"

    def __init__(self, df: pd.DataFrame, bins: int = 30):
        super().__init__(df)
        self.bins = bins
        self.profile_df: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    def compute(self):
        # Bin closes into equal‑width buckets & sum volume per bucket
        hist, bin_edges = np.histogram(
            self.df["Close"], bins=self.bins, weights=self.df["Volume"]
        )
        centres = (bin_edges[:-1] + bin_edges[1:]) / 2

        # Keep rows ordered by price so the Y‑axis is monotonic
        self.profile_df = (
            pd.DataFrame({"price": centres, "volume": hist})
            .sort_values("price")
            .reset_index(drop=True)
        )
        self.result = self.profile_df

        # Score – relative volume in the upper vs lower half of price range
        mid_price = self.df["Close"].median()
        upper = self.profile_df[self.profile_df["price"] >= mid_price]["volume"].sum()
        lower = self.profile_df[self.profile_df["price"] < mid_price]["volume"].sum()
        self.score = upper / (upper + lower + 1e-9)
        return self.result

    # ------------------------------------------------------------------
    def plot(self) -> Path:
        if self.result is None:
            self.compute()

        fig, ax = self._init_price_ax(self.df, "Market Profile (Volume by Price)")

        # Secondary **X** axis (shares Y), so huge volume numbers stay off the date axis
        ax_profile = ax.twiny()
        ax_profile.barh(
            self.profile_df["price"],
            self.profile_df["volume"],
            align="center",
            alpha=0.4,
        )
        ax_profile.set_xlabel("Volume", color="white")
        ax_profile.tick_params(axis="x", colors="white")
        ax_profile.spines["top"].set_color("white")

        # Tighten layout to prevent label cut‑off
        fig.tight_layout()

        return self._save_fig(fig, "market_profile.png")
