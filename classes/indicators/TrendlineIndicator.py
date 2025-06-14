import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple, Dict, Set, Literal, Optional
from scipy.stats import linregress
from mplfinance.plotting import make_addplot
from matplotlib import patches
from classes.indicators.BaseIndicator import BaseIndicator
from classes.indicators.config import DataContext



@dataclass
class Trendline:
    slope: float
    intercept: float
    r2: float
    touches: List[pd.Timestamp]
    violations: int
    lookback: int
    score: float = 0.0


class TrendlineIndicator(BaseIndicator):
    """
    Detects trendlines from pivot points, clusters overlapping lines into averaged ones,
    and provides overlays.

    Supports two color modes:
      - 'role': green for support, red for resistance (determined at overlay time)
      - 'timeframe': custom colors per indicator timeframe
    """
    NAME = 'trendline'

    def __init__(
        self,
        df: pd.DataFrame,
        lookbacks: List[int],
        tolerance: float = 0.0025,
        min_touches: int = 2,
        slope_tol: float = 0.0001,
        intercept_tol: float = 0.01,
        timeframe: str = ''
    ):
        self.df = df.copy()
        self.lookbacks = lookbacks
        self.tolerance = tolerance
        self.min_touches = min_touches
        self.slope_tol = slope_tol
        self.intercept_tol = intercept_tol
        self.timeframe = timeframe
        self.trendlines: List[Trendline] = []
        self._compute()

    @classmethod
    def from_context(
        cls,
        provider,
        ctx: DataContext,
        lookbacks: List[int],
        tolerance: float = 0.0025,
        min_touches: int = 2,
        slope_tol: float = 0.0001,
        intercept_tol: float = 0.01
    ):
        df = provider.get_ohlcv(ctx)
        if df is None or df.empty:
            raise ValueError(f"Missing OHLCV for {ctx.symbol} from {ctx.start} to {ctx.end}")
        return cls(
            df=df,
            lookbacks=lookbacks,
            tolerance=tolerance,
            min_touches=min_touches,
            slope_tol=slope_tol,
            intercept_tol=intercept_tol,
            timeframe=ctx.interval
        )

    def _find_pivots(self, lookback: int) -> Tuple[List[Tuple[pd.Timestamp, float]], List[Tuple[pd.Timestamp, float]]]:
        highs, lows = [], []
        prices = self.df['close']
        for i in range(lookback, len(prices) - lookback):
            window = prices.iloc[i - lookback: i + lookback + 1]
            center = prices.iat[i]
            ts = prices.index[i]
            if center == window.max(): highs.append((ts, center))
            if center == window.min(): lows.append((ts, center))
        return highs, lows

    def _compute(self) -> None:
        raw_lines: List[Trendline] = []
        # generate raw trendlines
        for lb in self.lookbacks:
            highs, lows = self._find_pivots(lb)
            for pts in (lows, highs):
                if len(pts) < 2: continue
                for i in range(len(pts)):
                    for j in range(i + 1, len(pts)):
                        t1, p1 = pts[i]; t2, p2 = pts[j]
                        x = np.array([self.df.index.get_loc(t1), self.df.index.get_loc(t2)])
                        y = np.array([p1, p2])
                        slope, intercept, r_val, _, _ = linregress(x, y)
                        r2 = r_val**2
                        touches, violations = [], 0
                        for idx, ts in enumerate(self.df.index):
                            price = self.df.at[ts, 'close']
                            line_p = slope*idx + intercept
                            dist = abs(price - line_p)/price
                            if dist <= self.tolerance:
                                touches.append(ts)
                            else:
                                violations += 1
                        if len(touches) < self.min_touches: continue
                        raw_lines.append(Trendline(slope, intercept, r2, touches, violations, lb))

        # cluster lines that overlap (similar slope & intercept)
        clusters: List[List[Trendline]] = []
        for tl in raw_lines:
            placed = False
            for cluster in clusters:
                rep = cluster[0]
                if abs(tl.slope - rep.slope) <= self.slope_tol and abs(tl.intercept - rep.intercept) <= self.intercept_tol:
                    cluster.append(tl)
                    placed = True
                    break
            if not placed:
                clusters.append([tl])

        # average each cluster
        self.trendlines.clear()
        for cluster in clusters:
            slopes = [c.slope for c in cluster]
            inters = [c.intercept for c in cluster]
            r2s = [c.r2 for c in cluster]
            touches = sorted({ts for c in cluster for ts in c.touches})
            violations = sum(c.violations for c in cluster)
            lookback = min(c.lookback for c in cluster)
            avg_slope = float(np.mean(slopes))
            avg_intercept = float(np.mean(inters))
            avg_r2 = float(np.mean(r2s))
            self.trendlines.append(
                Trendline(
                    slope=avg_slope,
                    intercept=avg_intercept,
                    r2=avg_r2,
                    touches=touches,
                    violations=violations,
                    lookback=lookback
                )
            )

    def to_overlays(
        self,
        plot_df: pd.DataFrame,
        color_mode: Literal['role', 'timeframe'] = 'role',
        role_color_map: Dict[str, str] = None,
        timeframe_color_map: Dict[str, str] = None,
        width: float = 1.0,
        style: str = 'dashed',
        top_n: Optional[int] = None
    ) -> Tuple[List, Set[Tuple[str, str]]]:
        overlays, legend_entries = [], set()
        lines = sorted(self.trendlines, key=lambda tl: tl.r2, reverse=True)
        if top_n:
            lines = lines[:top_n]

        role_c = role_color_map or {'support':'green','resistance':'red'}
        tf_c   = timeframe_color_map or {self.timeframe:'blue'}
        last_idx   = len(plot_df.index) - 1
        last_price = plot_df['close'].iat[last_idx]

        for tl in lines:
            # choose color/label
            end_p = tl.slope * last_idx + tl.intercept
            kind  = 'support' if last_price > end_p else 'resistance'
            if color_mode=='role':
                color, label = role_c[kind], f"{kind.capitalize()} TL"
            else:
                color, label = tf_c.get(self.timeframe,'gray'), f"{self.timeframe} TL"
            legend_entries.add((label, color))

            # build the trendline series
            series = pd.Series(
                [tl.slope*i + tl.intercept for i in range(len(plot_df.index))],
                index=plot_df.index
            )
            overlays.append(make_addplot(series, color=color, linestyle=style, width=width))

            # 1) build an all‐NaN series
            dot_series = pd.Series(np.nan, index=plot_df.index)

            # 2) only fill in the actual touch points, based on wick crossing
            for ts in plot_df.index:
                idx = plot_df.index.get_loc(ts)
                line_p = tl.slope * idx + tl.intercept
                low, high = plot_df.at[ts, 'low'], plot_df.at[ts, 'high']
                if low <= line_p <= high:
                    dot_series.iat[idx] = line_p

            # 3) plot only those non‐NaN points as a scatter
            overlays.append(make_addplot(
                dot_series,
                type='scatter',        # <— this forces matplotlib.scatter
                marker='o',
                markersize=6,
                color=color,
                label=""               # no extra legend entry
            ))
        return overlays, legend_entries


    @staticmethod
    def build_legend_handles(legend_entries: Set[Tuple[str, str]]):
        return [patches.Patch(color=c,label=l) for l,c in sorted(legend_entries)]
