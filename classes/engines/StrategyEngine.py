from typing import List, Tuple
import pandas as pd

from classes.indicators.BaseIndicator import BaseIndicator

class StrategyEngine:
    """
    Unified strategy engine that combines multiple indicators into a single
    confidence score and recommended direction for each bar.
    """

    def __init__(
        self,
        indicators: List[BaseIndicator],
        atr_factor: float = 0.15,
    ) -> None:
        self.indicators = indicators
        self.atr_factor = atr_factor

        # unpack known indicators by type
        # order: DailyLevels, H4Levels, MergedVA, Trendline 4h, Trendline 15m, VWAP
        (
            self.daily_levels,
            self.h4_levels,
            self.merged_va,
            self.tl_h4,
            self.tl_15,
            self.vwap_15
        ) = indicators

    def score_bar(
        self,
        timestamp: pd.Timestamp,
        price: float,
    ) -> Tuple[float, str]:
        """
        Compute confidence score and direction for a single bar.
        """
        # ATR series assumed precomputed and stored
        atr = self.atr_series.loc[timestamp]
        score = 0.0
        votes = {"long": 0, "short": 0}

        # 1) Merged Value-Area clusters
        for val, poc, vah, count in self.merged_va.get_clusters():
            if abs(price - poc) <= atr * self.atr_factor:
                score += 2
                votes["long"] += 1
            elif val <= price <= vah:
                score += 1
                votes["long"] += 1
            if count >= 9:
                score += 1
                votes["long"] += 1

        # 2) Daily & H4 pivot levels
        for lvl in self.daily_levels.get_levels():
            if abs(price - lvl) <= atr * self.atr_factor:
                score += 1
                votes["long"] += 1
        for lvl in self.h4_levels.get_levels():
            if abs(price - lvl) / price <= 0.003:
                score += 2
                votes["long"] += 1

        # 3) VWAP bands
        v, std = self.vwap_15.get_vwap(timestamp)
        if abs(price - v) <= std * self.vwap_15.band_k:
            score += 1
            votes["long"] += 1

        # 4) Trendline proximity
        # use 1 ATR threshold
        for tl in self.tl_h4.get_lines() + self.tl_15.get_lines():
            # map timestamp to integer index
            try:
                idx = self.df_15.index.get_loc(timestamp)
            except KeyError:
                continue
            y_line = tl.intercept + tl.slope * idx
            dist = abs(price - y_line) / atr if atr > 0 else 0
            if tl.score >= 0.8 and dist <= self.atr_factor:
                score += 1
                votes["long"] += 1
                break

        # normalize to 0–1
        max_pts = 2 + 1 + 1 + 1 + 1 + 1  # adjust if you add more rules
        final_score = min(score / max_pts, 1.0)
        direction = "long" if votes["long"] >= votes["short"] else "short"
        return final_score, direction

    def run(
        self,
        df: pd.DataFrame,
        price_col: str = 'Close'
    ) -> pd.DataFrame:
        """
        Score every bar in the DataFrame.
        """
        # store 15m frame and ATR series
        self.df_15 = df.copy()
        self.atr_series = (self.df_15['High'] - self.df_15['Low']).rolling(14).mean()

        results = []
        for ts, row in self.df_15.iterrows():
            score, direction = self.score_bar(ts, float(row[price_col]))
            results.append((score, direction))

        scores, dirs = zip(*results) if results else ([], [])
        out = self.df_15.copy()
        out['score'] = scores
        out['direction'] = dirs
        return out