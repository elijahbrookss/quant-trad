from classes.indicators.BaseIndicator import BaseIndicator
from classes.PivotDetector import PivotDetector
from classes.TrendlineAnalyzer import TrendlineAnalyzer

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ARTIFACT_ROOT = Path("artifacts/trendlines")

# ------------------------------------------------------------------
# In‑memory cache so repeated calls on the same DF / params are cheap
# ------------------------------------------------------------------
_CACHE: Dict[str, Tuple[datetime, dict]] = {}


def _cache_key(symbol: str, tf: str, last_idx: int, params_hash: str) -> str:
    return f"{symbol}-{tf}-{last_idx}-{params_hash}"


# ------------------------------------------------------------------
@dataclass
class TL:
    """Single trendline data structure."""

    p1: Tuple[int, float]  # (idx, price)
    p2: Tuple[int, float]
    slope: float
    intercept: float
    touches: int
    violations: int
    last_touch_idx: int

    @property
    def direction(self) -> str:  # up / down
        return "up" if self.slope > 0 else "down"

    @property
    def score(self) -> float:
        """Simple quality score (0‑1)."""
        return min(1.0, self.touches / (self.violations + 1))


# ------------------------------------------------------------------
class TrendlineIndicator(BaseIndicator):
    """Extracts & scores trendlines for a given timeframe."""

    NAME = "trendlines"

    def __init__(
        self,
        df: pd.DataFrame,
        tf_label: str = "4h",
        lookbacks: Tuple[int, ...] = (5, 10, 20, 40),
        max_pivots: int = 300,
    ):
        super().__init__(df)
        self.tf_label = tf_label
        self.lookbacks = lookbacks
        self.max_pivots = max_pivots
        self.lines: List[TL] = []

        # adaptive min_points based on volatility
        atr = (df["High"] - df["Low"]).rolling(14).mean().iloc[-1]
        self.min_points_range = range(max(3, int(atr / df["Close"].iloc[-1] * 1000)), 8)

    # ------------------------------------------------------------------
    def _get_pivots(self) -> List[Tuple[int, float]]:
        pivots: List[Tuple[int, float]] = []
        detector = PivotDetector(self.df, lookbacks=self.lookbacks)
        pivots_map = detector.detect_all()
        for highs, lows in pivots_map.values():
            pivots.extend(highs)
            pivots.extend(lows)
        pivots.sort(key=lambda t: t[0])
        return pivots[-self.max_pivots :]  # limit for performance

    # ------------------------------------------------------------------
    def compute(self):
        symbol = getattr(self.df, "symbol", "NA")
        last_idx = int(self.df.index[-1].timestamp())
        params_hash = hashlib.sha1(str((self.lookbacks, tuple(self.min_points_range))).encode()).hexdigest()[:8]
        key = _cache_key(symbol, self.tf_label, last_idx, params_hash)
        cached = _CACHE.get(key)
        if cached:
            self.result, self.lines = cached  # type: ignore
            self.score = max(l.score for l in self.lines) if self.lines else 0
            return self.result

        pivots = self._get_pivots()
        trendlines_by_thresh: Dict[int, List[TL]] = {}
        for n in self.min_points_range:
            analyzer = TrendlineAnalyzer(self.df, pivots, min_points=n)
            raw_lines = analyzer.analyze()
            trendlines_by_thresh[n] = []
            for (idx1, price1), (idx2, price2), touches, violations in raw_lines:
                slope = (price2 - price1) / (idx2 - idx1)
                intercept = price1 - slope * idx1
                tl = TL(
                    p1=(idx1, price1),
                    p2=(idx2, price2),
                    slope=slope,
                    intercept=intercept,
                    touches=touches,
                    violations=violations,
                    last_touch_idx=idx2,
                )
                trendlines_by_thresh[n].append(tl)
                self.lines.append(tl)

        self.result = trendlines_by_thresh
        self.score = max(l.score for l in self.lines) if self.lines else 0
        _CACHE[key] = (datetime.utcnow(), self.result)
        return self.result

    # ------------------------------------------------------------------
    def plot(self) -> Path:
        if self.result is None:
            self.compute()

        plt.style.use("dark_background")
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(self.df.index, self.df["Close"], color="cyan", alpha=0.6)

        cmap = plt.cm.get_cmap("plasma")
        for tl in self.lines:
            xs = np.array([tl.p1[0], tl.p2[0]])
            ys = tl.intercept + tl.slope * xs
            norm_score = tl.score  # 0‑1
            ax.plot(
                self.df.index[xs],
                ys,
                color=cmap(norm_score),
                linewidth=1 + 2 * norm_score,
                alpha=0.8,
            )
        ax.set_title(f"Trendlines ({self.tf_label})")
        folder = ARTIFACT_ROOT / self.tf_label
        folder.mkdir(parents=True, exist_ok=True)
        file = folder / f"trendlines_{self.tf_label}.png"
        fig.savefig(file, dpi=300, bbox_inches="tight")
        plt.close(fig)
        return file
    
    def get_lines(self) -> List[TL]:
        """
        Return all detected TL objects for this timeframe.
        """
        if self.result is None:
            self.compute()
        return self.lines