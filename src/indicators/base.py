from pathlib import Path
from typing import Tuple

import pandas as pd

from core.logger import logger

ARTIFACT_ROOT = Path("artifacts")


class ComputeIndicator:
    """Lightweight base for compute-only indicators."""

    NAME: str = "base"

    def __init__(self, df: pd.DataFrame):
        logger.debug("Initializing %s with DataFrame of shape %s", self.NAME, df.shape)
        self.df = df.copy()


class PlotIndicatorMixin:
    """Optional plotting helpers for indicators that render overlays."""

    @staticmethod
    def _init_price_ax(df: pd.DataFrame, title: str) -> Tuple["plt.Figure", "plt.Axes"]:
        import matplotlib.pyplot as plt

        logger.debug("Initializing price axis for plot: %s (df shape: %s)", title, df.shape)
        plt.style.use("dark_background")
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df.index, df["Close"], label="Close", color="cyan", alpha=0.6)
        ax.set_title(title, color="white", fontsize=14)
        ax.set_xlabel("Date", color="white")
        ax.set_ylabel("Price", color="white")
        ax.grid(alpha=0.2, color="gray")
        logger.debug("Price axis initialized for: %s", title)
        return fig, ax

    def _save_fig(self, fig: "plt.Figure", filename: str) -> Path:
        import matplotlib.pyplot as plt

        folder = ARTIFACT_ROOT / self.NAME
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / filename
        logger.debug("Saving figure for %s to %s", self.NAME, path)
        fig.savefig(path, dpi=300, bbox_inches="tight", facecolor="black")
        plt.close(fig)
        logger.info("%s plot saved → %s", self.NAME, path)
        return path
