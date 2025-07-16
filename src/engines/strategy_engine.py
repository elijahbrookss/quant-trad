from typing import List, Tuple, Optional
import pandas as pd

from src.indicators.base import BaseIndicator


class StrategyEngine:
    """
    Unified strategy engine that combines any subset of indicators into a
    confidence score (0‒1) and a suggested trade direction for each bar.

    Only the indicators you supply will be evaluated. Rules tied to
    missing indicators are silently skipped, and the score is re‑scaled
    by the maximum achievable points for the *active* rules so that the
    final score remains in the 0‑1 range regardless of how many
    indicators you pass in.
    """

    def __init__(
        self,
        indicators: List[BaseIndicator],
        atr_factor: float = 0.15,
    ) -> None:
        self.indicators = indicators
        self.atr_factor = atr_factor
        # Map indicators by their declared NAME attribute for quick lookup
        self.ind_map = {ind.NAME: ind for ind in indicators}
        print("StrategyEngine loaded indicators:", ", ".join(self.ind_map))

    # ------------------------------------------------------------------ #
    #  Helper: safe TRUE RANGE Wilder ATR (used by some rules)           #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _wilder_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        hl  = df['High'] - df['Low']
        h_cp = (df['High'] - df['Close'].shift()).abs()
        l_cp = (df['Low']  - df['Close'].shift()).abs()
        tr   = pd.concat([hl, h_cp, l_cp], axis=1).max(axis=1)
        return tr.ewm(alpha=1 / period, adjust=False).mean()

    # ------------------------------------------------------------------ #
    #  Per‑bar scoring                                                   #
    # ------------------------------------------------------------------ #
    def score_bar(self, ts: pd.Timestamp, price: float, atr: float) -> Tuple[float, str]:
        score      = 0.0
        max_points = 0.0
        votes      = {"long": 0, "short": 0}

        # 1) ── Merged Value‑Area clusters ------------------------------ #
        mva = self.ind_map.get("merged_va")
        if mva is not None:
            for val, poc, vah, cnt in mva.get_clusters():
                if abs(price - poc) <= atr * self.atr_factor:
                    score += 2; votes["long"] += 1
                elif val <= price <= vah:
                    score += 1; votes["long"] += 1
                if cnt >= 9:  # dense cluster bonus
                    score += 1; votes["long"] += 1
            max_points += 4  # 2 + 1 + 1

        # 2) ── Daily levels ------------------------------------------- #
        lv_daily = self.ind_map.get("levels_daily")
        if lv_daily is not None:
            for lvl in lv_daily.get_levels():
                if abs(price - lvl) <= atr * self.atr_factor:
                    score += 1; votes["long"] += 1
            max_points += 1

        # 3) ── H4 levels ---------------------------------------------- #
        lv_h4 = self.ind_map.get("levels_h4")
        if lv_h4 is not None:
            for lvl in lv_h4.get_levels():
                if abs(price - lvl) / price <= 0.003:
                    score += 2; votes["long"] += 1
            max_points += 2

        # 4) ── VWAP band ---------------------------------------------- #
        vwap = self.ind_map.get("vwap")
        if vwap is not None:
            v, std = vwap.get_vwap(ts)
            if abs(price - v) <= std * getattr(vwap, "band_k", 2.0):
                score += 1; votes["long"] += 1
            max_points += 1

        # 5) ── Trendlines (optional) ---------------------------------- #
        tl_h4 = self.ind_map.get("tl_h4")
        tl_15 = self.ind_map.get("tl_15")
        trendlines = []
        if tl_h4 is not None:
            trendlines += tl_h4.get_lines()
        if tl_15 is not None:
            trendlines += tl_15.get_lines()
        if trendlines:
            # map ts to numeric index in the stored 15‑m frame
            try:
                idx = self.df_15.index.get_loc(ts)
            except KeyError:
                idx = None
            if idx is not None:
                for tl in trendlines:
                    y_line = tl.intercept + tl.slope * idx
                    dist   = abs(price - y_line) / atr if atr > 0 else 0
                    if getattr(tl, "score", 1.0) >= 0.8 and dist <= self.atr_factor:
                        score += 1; votes["long"] += 1
                        break
                max_points += 1

        # -------------------------------------------------------------- #
        #  Normalise score 0‑1                                           #
        # -------------------------------------------------------------- #
        final_score = 0.0 if max_points == 0 else min(score / max_points, 1.0)
        direction   = "long" if votes["long"] >= votes["short"] else "short"
        return final_score, direction

    # ------------------------------------------------------------------ #
    #  Bulk run over a DataFrame                                        #
    # ------------------------------------------------------------------ #
    def run(self, df: pd.DataFrame, price_col: str = "Close") -> pd.DataFrame:
        # cache DF and compute Wilder ATR once
        self.df_15     = df.copy()
        self.atr_series = self._wilder_atr(self.df_15)

        results: List[Tuple[float, str]] = []
        for ts, row in self.df_15.iterrows():
            price = float(row[price_col])
            atr   = float(self.atr_series.loc[ts])
            results.append(self.score_bar(ts, price, atr))

        scores, dirs = zip(*results) if results else ([], [])
        out = self.df_15.copy()
        out["score"]     = scores
        out["direction"] = dirs
        return out
