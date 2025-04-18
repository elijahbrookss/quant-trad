from matplotlib import pyplot as plt
from classes.indicators.BaseIndicator import BaseIndicator
from classes.PivotDetector import PivotDetector
from pathlib import Path
from typing import List
import numpy as np
import pandas as pd



class LevelsIndicator(BaseIndicator):
    NAME = "levels"

    def __init__(self, df: pd.DataFrame, lookbacks=(10, 20, 50)):
        super().__init__(df)
        self.lookbacks = lookbacks
        self.levels: List[float] = []  # flattened unique list

    # ------------------------------------------------------------------
    def compute(self):
        detector = PivotDetector(self.df, self.lookbacks)
        pivots_by_lb = detector.detect_all()
        # flatten & dedupe within 0.2 % bandwidth
        raw_levels = [p for hs_ls in pivots_by_lb.values() for pair in hs_ls for _, p in pair]
        raw_levels.sort()
        unique_levels: List[float] = []
        for level in raw_levels:
            if not unique_levels or abs(level - unique_levels[-1]) / level > 0.002:
                unique_levels.append(level)
        self.levels = unique_levels
        self.result = self.levels
        # Score – tighter cluster of recent closes to levels ⇒ higher score
        last_price = self.df.iloc[-1]["Close"]
        self.score = 1 - min(abs(last_price - np.array(unique_levels)) / last_price)
        return self.result

    # ------------------------------------------------------------------
    def plot(self) -> Path:
        if self.result is None:
            self.compute()
        fig, ax = self._init_price_ax(self.df, "Pivot Levels")
        colors = plt.cm.viridis(np.linspace(0, 1, len(self.levels)))
        for lvl, col in zip(self.levels, colors):
            ax.axhline(lvl, color=col, linestyle="--", linewidth=1, alpha=0.8)
        ax.legend(["Close"] + [f"Level {i+1}" for i in range(len(self.levels))], loc="upper left", fontsize=8)
        return self._save_fig(fig, "levels.png")

