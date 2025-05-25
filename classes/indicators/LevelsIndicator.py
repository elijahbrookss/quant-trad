from matplotlib import pyplot as plt
from classes.indicators.BaseIndicator import BaseIndicator
from classes.PivotDetector import PivotDetector
from pathlib import Path
from typing import List
import numpy as np
import pandas as pd
from typing import Dict, Tuple, List
from mplfinance.plotting import make_addplot


ARTIFACT_ROOT = Path("artifacts")

def _merge_band(df: pd.DataFrame, factor: float = 0.25) -> float:
    atr = (df["High"] - df["Low"]).rolling(14).mean().iloc[-1]
    return max(atr * factor, df["Close"].iloc[-1] * 0.001)  # floor at 0.1 %


class _BaseLevelsIndicator(BaseIndicator):
    NAME = "levels"

    def __init__(self, df: pd.DataFrame, lookbacks=(10, 20, 50), label: str = "level"):
        super().__init__(df)
        self.lookbacks = lookbacks
        self.label = label  # daily / h4 tag
        self.levels: List[float] = []

    # --------------------------------------------------------------
    def compute(self):
        detector = PivotDetector(self.df, self.lookbacks)
        pivot_map: Dict[int, List[Tuple[int, float]]] = detector.detect_all()

        raw_levels: List[float] = []
        for highs, lows in pivot_map.values():
            raw_levels.extend([p for _, p in highs])
            raw_levels.extend([p for _, p in lows])
        raw_levels.sort()


        band = _merge_band(self.df)
        uniq: List[float] = []
        for lvl in raw_levels:
            if not uniq or abs(lvl - uniq[-1]) > band:
                uniq.append(lvl)
        self.levels = uniq
        self.result = uniq

        # Score – distance of last close to nearest level (smaller = stronger)
        last_px = self.df.iloc[-1]["Close"]
        self.score = 1 - min(abs(last_px - np.array(uniq)) / last_px)
        return self.result
        
    def get_levels(self) -> List[float]:
        """Return the flattened list of pivot levels."""
        if self.result is None:
            self.compute()
        return self.levels
    
    # --------------------------------------------------------------
    def plot(self) -> Path:
        if self.result is None:
            self.compute()
        plt.style.use("dark_background")
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(self.df.index, self.df["Close"], color="cyan", alpha=0.6, label="Close")
        for lvl in self.levels:
            ax.axhline(lvl, linestyle="--", linewidth=1, alpha=0.7)
        ax.set_title(f"{self.label.capitalize()} Levels", color="white")
        ax.legend(facecolor="black", edgecolor="white")
        folder = ARTIFACT_ROOT / self.label
        folder.mkdir(parents=True, exist_ok=True)
        file = folder / f"levels_{self.label}.png"
        fig.savefig(file, dpi=300, bbox_inches="tight", facecolor="black")
        plt.close(fig)

    def to_overlay(self, color: str = "gray") -> List:
        if self.result is None:
            self.compute()
        overlays = []
        for lvl in self.levels:
            line = pd.Series(lvl, index=self.df.index)
            overlays.append(make_addplot(line, color=color, linestyle='--', linewidth=1, alpha=0.7))
        return overlays

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

# ------------------------------------------------------------------
# Public subclasses
# ------------------------------------------------------------------
class DailyLevelsIndicator(_BaseLevelsIndicator):
    NAME = "levels_daily"

    def __init__(self, df: pd.DataFrame):
        super().__init__(df, lookbacks=(5, 10, 20), label="daily")


class H4LevelsIndicator(_BaseLevelsIndicator):
    NAME = "levels_h4"

    def __init__(self, df: pd.DataFrame):
        super().__init__(df, lookbacks=(10, 25, 50), label="h4")
