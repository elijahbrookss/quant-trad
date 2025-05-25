from typing import List
import pandas as pd
from classes.PivotDetector import PivotDetector
from mplfinance.plotting import make_addplot

class PivotLevelOverlay:
    def __init__(self, df: pd.DataFrame, lookbacks=(10, 20, 50), threshold=0.005):
        self.df = df
        self.lookbacks = lookbacks
        self.threshold = threshold
        self.levels = self._compute_levels()

    def _compute_levels(self) -> List[float]:
        detector = PivotDetector(self.df, self.lookbacks, self.threshold)
        pivot_map = detector.detect_all()

        # flatten and deduplicate within threshold
        raw = []
        for highs, lows in pivot_map.values():
            raw.extend([p for _, p in highs])
            raw.extend([p for _, p in lows])
        raw = sorted(raw)

        unique_levels = []
        for lvl in raw:
            if not unique_levels or abs(lvl - unique_levels[-1]) / lvl > self.threshold:
                unique_levels.append(lvl)

        return unique_levels

    def to_overlays(self, color: str = "gray") -> List:
        return [
            make_addplot(pd.Series(level, index=self.df.index), color=color, linestyle='--', width=1, alpha=0.7)
            for level in self.levels
        ]
