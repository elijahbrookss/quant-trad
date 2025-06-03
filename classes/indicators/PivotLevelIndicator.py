from dataclasses import dataclass, field
from classes.indicators.config import DataContext
from typing import List, Optional
import pandas as pd
from mplfinance.plotting import make_addplot
from classes.Logger import logger

from matplotlib import patches
from typing import Tuple, Set
from dataclasses import field


@dataclass
class Level:
    price: float
    kind: str  # "support" or "resistance"
    lookback: int
    first_touched: pd.Timestamp
    timeframe: str  # e.g., "1d", "1h", "4h"
    touches: List[pd.Timestamp] = field(default_factory=list)

    
    def get_touches(self, df: pd.DataFrame) -> List[pd.Timestamp]:
        """
        Returns timestamps where this level was touched in the given trading DataFrame.

        A 'touch' is defined as a candle where the level is between the Low and High.
        """
        # Wick-based mask: price falls between the candle range
        mask = (df["low"] <= self.price) & (df["high"] >= self.price)
        touches = df[mask].index.tolist()
        self.touches = touches

        logger.debug(f"Level {self.kind} at {self.price} touched {len(touches)} times.")

        return touches

class PivotLevelIndicator:
    def __init__(self, df, timeframe, lookbacks=(10, 20, 50), threshold=0.005):
        self.df = df
        self.lookbacks = lookbacks
        self.threshold = threshold
        self.timeframe = timeframe
        self.levels: List[Level] = []
        self._compute()

    def _compute(self):
        def detect_all():
            all_pivots = {}
            for lb in self.lookbacks:
                all_pivots[lb] = self._find_pivots(lb)
            return all_pivots

        pivot_map = detect_all()
        last_price = self.df["close"].iloc[-1]
        levels: List[Level] = []

        for lb, (highs, lows) in pivot_map.items():
            for t, p in highs + lows:
                # Deduplication within threshold
                if any(abs(p - existing.price) / p < self.threshold for existing in levels):
                    continue
                kind = "support" if last_price > p else "resistance"
                levels.append(Level(price=p, kind=kind, lookback=lb, first_touched=t, timeframe=self.timeframe))

        levels.sort(key=lambda l: l.price)
        self.levels = levels

    def _find_pivots(self, lookback):
        highs, lows = [], []

        def is_near_existing(price):
            for _, p in highs + lows:
                if abs(price - p) / p < self.threshold:
                    return True
            return False

        for i in range(lookback, len(self.df) - lookback):
            current = self.df.index[i]
            high = self.df.at[current, 'high']
            low = self.df.at[current, 'low']

            high_range = self.df['high'].iloc[i - lookback:i + lookback + 1].drop(labels=[current])
            low_range = self.df['low'].iloc[i - lookback:i + lookback + 1].drop(labels=[current])

            if high > high_range.max() and not is_near_existing(high):
                highs.append((current, high))
            elif low < low_range.min() and not is_near_existing(low):
                lows.append((current, low))

        return highs, lows

    def to_overlays(
        self,
        plot_df: pd.Index,
        color_mode: str = "role"
    ) -> Tuple[List, Set[Tuple[str, str]]]:
        
        plot_index = plot_df.index
        overlays = []
        legend_entries = set()

        tf_color = {
            "daily": "goldenrod",
            "4h": "magenta",
            "1h": "blue",
            "1d": "goldenrod"
        }

        role_color = {
            "support": "green",
            "resistance": "red"
        }

        for level in self.levels:
            if color_mode == "role":
                color = role_color.get(level.kind, "gray")
                label = f"{level.kind.capitalize()} Level"
                legend_key = (label, color)
            elif color_mode == "timeframe":
                color = tf_color.get(level.timeframe, "gray")
                label = f"{level.timeframe} Levels"
                legend_key = (label, color)
            else:
                color = "gray"
                label = "Level"
                legend_key = (label, color)

            if level.first_touched in plot_index:
                start_idx = plot_index.get_loc(level.first_touched)
            else:
                start_idx = 0

            ray_index = plot_index[start_idx:]
            line = pd.Series(level.price, index=ray_index)
            padded_line = line.reindex(plot_index, fill_value=pd.NA)

            if padded_line.dropna().empty:
                continue

            overlays.append(make_addplot(padded_line, color=color, linestyle="--", width=1, alpha=0.7))

            level.touches = level.get_touches(plot_df)
            # Only keep touches that occur on or after the level was discovered
            level.touches = [ts for ts in level.touches if ts >= level.first_touched]

            if level.touches:
                dot_series = pd.Series(level.price, index=level.touches)
                dot_line = dot_series.reindex(plot_index)

                overlays.append(make_addplot(
                    dot_line,
                    color=color,
                    marker='o',
                    markersize=4,
                    scatter=True,
                    label=""
            ))
            legend_entries.add(legend_key)

        return overlays, legend_entries

    # helper to convert legend entries to mpl handles
    def build_legend_handles(legend_entries: Set[Tuple[str, str]]):
        return [
            patches.Patch(color=color, label=label)
            for label, color in sorted(legend_entries)
        ]

    def nearest_support(self, price: float) -> Optional[Level]:
        supports = [lvl for lvl in self.levels if lvl.kind == "support"]
        return min(supports, key=lambda l: abs(l.price - price), default=None)

    def nearest_resistance(self, price: float) -> Optional[Level]:
        resistances = [lvl for lvl in self.levels if lvl.kind == "resistance"]
        return min(resistances, key=lambda l: abs(l.price - price), default=None)

    def distance_to_level(self, level: Level, price: float) -> float:
        return abs(level.price - price) / price
    
    @classmethod
    def from_context(cls, provider, ctx: DataContext, level_timeframe: str, **kwargs):
        """Create PivotLevelIndicator from provider-fetched timeframe with fallback ingestion."""

        level_ctx = DataContext(
            symbol=ctx.symbol,
            start=ctx.start,
            end=ctx.end,
            interval=level_timeframe,
        )

        df = provider.get_ohlcv(level_ctx)

        if df is None or df.empty:
            raise ValueError(
                f"Data missing after ingest for {level_ctx.symbol} [{level_timeframe}] from {level_ctx.start} to {level_ctx.end}"
            )

        return cls(df=df, timeframe=level_timeframe, **kwargs)