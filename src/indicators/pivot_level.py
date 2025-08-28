from dataclasses import dataclass, field
from .config import DataContext
from typing import List, Optional, Tuple, Set, Dict, Any
import pandas as pd
from mplfinance.plotting import make_addplot
from core.logger import logger
from matplotlib import patches
from .base import BaseIndicator

@dataclass
class Level:
    """
    Represents a horizontal support or resistance level detected from pivots.

    :param price: price of the level
    :param kind: 'support' or 'resistance'
    :param lookback: pivot lookback window used
    :param first_touched: timestamp of the pivot that created this level
    :param timeframe: timeframe label (e.g., '1d', '4h')
    :param touches: list of timestamps where price touched this level
    """
    price: float
    kind: str
    lookback: int
    first_touched: pd.Timestamp
    timeframe: str
    touches: List[pd.Timestamp] = field(default_factory=list)

    def get_touches(self, df: pd.DataFrame) -> List[pd.Timestamp]:
        """
        Find all bar timestamps where the level price falls between low and high.

        :param df: OHLC DataFrame
        :return: list of touch timestamps
        """
        mask = (df['low'] <= self.price) & (df['high'] >= self.price)
        touches = df[mask].index.tolist()
        self.touches = touches
        logger.debug(
            "Level %s at %.4f touched %d times.",
            self.kind, self.price, len(touches)
        )
        return touches

class PivotLevelIndicator(BaseIndicator):
    """
    Detects horizontal support and resistance levels by clustering pivot highs/lows,
    and provides mplfinance overlays with optional touch markers.
    """
    NAME = 'pivot_level'

    def __init__(
        self,
        df: pd.DataFrame,
        timeframe: str,
        lookbacks: Tuple[int, ...] = (10, 20, 50),
        threshold: float = 0.005
    ):
        """
        :param df: OHLC DataFrame indexed by timestamp
        :param timeframe: label for coloring (e.g., '1h', 'daily')
        :param lookbacks: tuple of pivot lookback sizes
        :param threshold: price dedup threshold (fractional)
        """
        self.df = df
        self.timeframe = timeframe
        self.lookbacks = lookbacks
        self.threshold = threshold
        self.levels: List[Level] = []
        self._compute()

    def _compute(self):
        """
        Find pivot highs/lows for each lookback, dedupe within threshold,
        and label each as support or resistance based on last price.
        Populates self.levels sorted by price.
        """
        # collect all pivots
        pivot_map = {lb: self._find_pivots(lb) for lb in self.lookbacks}
        last_price = self.df['close'].iloc[-1]
        levels: List[Level] = []

        for lb, (highs, lows) in pivot_map.items():
            for ts, price in highs + lows:
                # skip if within threshold of existing
                if any(abs(price - lvl.price) / price < self.threshold for lvl in levels):
                    continue
                kind = 'support' if last_price > price else 'resistance'
                levels.append(
                    Level(
                        price=price,
                        kind=kind,
                        lookback=lb,
                        first_touched=ts,
                        timeframe=self.timeframe
                    )
                )
        # sort levels and assign
        self.levels = sorted(levels, key=lambda lvl: lvl.price)

    def _find_pivots(self, lookback: int) -> Tuple[List[Tuple[pd.Timestamp, float]], List[Tuple[pd.Timestamp, float]]]:
        """
        Identify pivot highs and lows using a rolling window of size lookback.

        :param lookback: number of bars on each side for pivot detection
        :return: (highs, lows) lists of (timestamp, price)
        """
        highs, lows = [], []
        def near_existing(price):
            return any(abs(price - p) / p < self.threshold for _, p in highs + lows)

        for i in range(lookback, len(self.df) - lookback):
            ts = self.df.index[i]
            high, low = self.df.at[ts, 'high'], self.df.at[ts, 'low']
            window_high = self.df['high'].iloc[i-lookback:i+lookback+1].drop(labels=[ts])
            window_low  = self.df['low'].iloc[i-lookback:i+lookback+1].drop(labels=[ts])

            if high > window_high.max() and not near_existing(high):
                highs.append((ts, high))
            elif low < window_low.min() and not near_existing(low):
                lows.append((ts, low))
        return highs, lows

    def to_overlays(
        self,
        plot_df: pd.DataFrame,
        color_mode: str = 'role'
    ) -> Tuple[List, Set[Tuple[str, str]]]:
        """
        Generate addplot overlays for each level with touch dots.

        :param plot_df: DataFrame for plotting (must contain low/high index)
        :param color_mode: 'role' to color by support/resistance, 'timeframe' to color by timeframe
        :return: (overlays, legend_entries)
        """
        overlays: List[dict] = []
        legend_entries: Set[Tuple[str, str]] = set()

        idx = plot_df.index
        role_colors = {'support':'green','resistance':'red'}
        tf_colors   = {self.timeframe:'blue'}  # extend as needed

        for lvl in self.levels:
            # choose color and label
            if color_mode == 'role':
                color = role_colors[lvl.kind]
                label = f"{lvl.kind.capitalize()} Level"
            else:
                color = tf_colors.get(lvl.timeframe, 'gray')
                label = f"{lvl.timeframe} Levels"
            legend_entries.add((label, color))

            # draw the infinite ray from first touch onward
            start = lvl.first_touched if lvl.first_touched in idx else idx[0]
            ray = idx[idx.get_indexer([start])[0]:]
            series = pd.Series(lvl.price, index=ray).reindex(idx)

            ap = make_addplot(series, color=color, linestyle='--', width=1, alpha=0.7)
            overlays.append({
                "kind": "addplot",
                "plot": ap
            })

            # compute & plot touches
            touches = [ts for ts in lvl.get_touches(plot_df) if ts >= lvl.first_touched]
            if touches:
                dots = pd.Series(lvl.price, index=touches).reindex(idx)
                ap =  make_addplot(
                        dots,
                        scatter=True,
                        marker='o',
                        markersize=4,
                        color=color,
                        label=""
                    )
                
                overlays.append({
                    "kind": "addplot",
                    "plot": ap
                })
        return overlays, legend_entries

    @staticmethod
    def build_legend_handles(legend_entries: Set[Tuple[str, str]]):
        """
        Convert (label, color) pairs into matplotlib Patch handles.
        """
        return [patches.Patch(color=c, label=l) for l, c in sorted(legend_entries)]

    def nearest_support(self, price: float) -> Optional[Level]:
        """Return the support level closest to the given price."""
        supports = [lvl for lvl in self.levels if lvl.kind=='support']
        return min(supports, key=lambda l: abs(l.price-price), default=None)

    def nearest_resistance(self, price: float) -> Optional[Level]:
        """Return the resistance level closest to the given price."""
        resistances = [lvl for lvl in self.levels if lvl.kind=='resistance']
        return min(resistances, key=lambda l: abs(l.price-price), default=None)

    def distance_to_level(self, level: Level, price: float) -> float:
        """Compute fractional distance between price and a level."""
        return abs(level.price - price) / price

    @classmethod
    def from_context(
        cls,
        provider,
        ctx: DataContext,
        timeframe: str,
        **kwargs
    ):
        """
        Instantiate from a DataContext 
        :param provider: data provider with get_ohlcv method
        :param ctx: DataContext with symbol, start, end, interval
        :param level_timeframe: timeframe for the pivot levels (e.g., '1d', '4h')

        """
        level_ctx = DataContext(
            symbol=ctx.symbol,
            start=ctx.start,
            end=ctx.end,
            interval=timeframe
        )
        df = provider.get_ohlcv(level_ctx)
        if df is None or df.empty:
            raise ValueError(
                f"Data missing for {ctx.symbol} [{timeframe}]"
            )
        return cls(df=df, timeframe=timeframe, **kwargs)


    def to_lightweight(
        self,
        plot_df: pd.DataFrame,
        color_mode: str = 'role'
    ) -> Dict[str, Any]:
        """
        Produce TradingView Lightweight Charts overlays:
        - price_lines: [{ id, price, title, color, lineStyle, lineWidth, axisLabelVisible, originTime }]
        - markers:     [{ time, position, shape, color, text }]
        """
        role_colors = {'support': '#22c55e', 'resistance': '#ef4444'}  # green/red
        tf_colors   = {self.timeframe: '#60a5fa'}                      # blue fallback

        price_lines = []
        markers = []

        idx = plot_df.index

        for i, lvl in enumerate(self.levels):
            # color / label
            if color_mode == 'role':
                color = role_colors.get(lvl.kind, '#9ca3af')  # gray fallback
                title = f"{lvl.kind[:3].upper()} · lb={lvl.lookback}"
            else:
                color = tf_colors.get(lvl.timeframe, '#9ca3af')
                title = f"{lvl.timeframe} · lb={lvl.lookback}"

            # ensure originTime exists (first visible bar or first_touched)
            origin = lvl.first_touched if lvl.first_touched in idx else (idx[0] if len(idx) else lvl.first_touched)
            origin_ts = int(pd.Timestamp(origin).timestamp())

            price_lines.append({
                "id": f"pivotlvl-{i}-{lvl.lookback}",
                "price": float(lvl.price),
                "title": title,
                "color": color,
                "lineStyle": 2,             # Dashed
                "lineWidth": 1,
                "axisLabelVisible": True,
                "originTime": origin_ts     # for optional client logic
            })

            # touch markers on/after first_touched
            touches = [ts for ts in lvl.get_touches(plot_df) if ts >= lvl.first_touched]
            pos = "belowBar" if lvl.kind == "resistance" else "aboveBar"
            for ts in touches:
                markers.append({
                    "time": int(pd.Timestamp(ts).timestamp()),
                    "position": pos,
                    "shape": "circle",
                    "color": color,
                    "text": f"{lvl.kind}@{lvl.price:.2f}"
                })

        return {
            "type": "pivot-levels",
            "timeframe": self.timeframe,
            "price_lines": price_lines,
            "markers": markers
        }