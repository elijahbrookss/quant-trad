from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import pandas as pd

from indicators.base import BaseIndicator


@dataclass(frozen=True)
class StrategyContext:
    """Lightweight identifier describing the strategy + symbol + timeframe."""

    strategy_id: str
    symbol: str
    timeframe: str


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

        # context -> timeframe -> DataFrame cache for downstream access
        self._frame_buffers: Dict[Tuple[str, str, str], pd.DataFrame] = {}
        self._atr_buffers: Dict[Tuple[str, str, str], pd.Series] = {}
        self._markers: MutableMapping[str, MutableMapping[str, MutableMapping[str, List[Dict[str, object]]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

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
    def score_bar(
        self,
        ts: pd.Timestamp,
        price: float,
        atr: float,
        context: StrategyContext,
    ) -> Tuple[float, str]:
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
            # map ts to numeric index within the cached timeframe frame
            frame = self._get_frame(context)
            try:
                idx = frame.index.get_loc(ts) if frame is not None else None
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
    def run(
        self,
        df: pd.DataFrame,
        context: StrategyContext,
        price_col: str = "Close",
        additional_frames: Optional[Mapping[str, pd.DataFrame]] = None,
    ) -> Tuple[pd.DataFrame, List[Dict[str, object]]]:
        """Score the supplied timeframe within the provided context."""

        frame = df.copy()
        self._store_frame(context, frame)
        atr_series = self._wilder_atr(frame)
        self._atr_buffers[self._context_key(context)] = atr_series

        if additional_frames:
            for name, extra in additional_frames.items():
                if extra is None:
                    continue
                self._store_frame(context, extra.copy(), timeframe=name)

        self.clear_markers(context)

        results: List[Tuple[float, str]] = []
        for ts, row in frame.iterrows():
            price = float(row[price_col])
            atr = float(atr_series.loc[ts])
            results.append(self.score_bar(ts, price, atr, context))

        scores, dirs = zip(*results) if results else ([], [])
        out = frame.copy()
        out["score"] = scores
        out["direction"] = dirs

        return out, self.get_markers_for_context(context)

    # ------------------------------------------------------------------ #
    #  Frame + marker helpers                                            #
    # ------------------------------------------------------------------ #
    def _context_key(self, context: StrategyContext, timeframe: Optional[str] = None) -> Tuple[str, str, str]:
        return (context.strategy_id, context.symbol, timeframe or context.timeframe)

    def _store_frame(
        self,
        context: StrategyContext,
        frame: pd.DataFrame,
        *,
        timeframe: Optional[str] = None,
    ) -> None:
        key = self._context_key(context, timeframe)
        self._frame_buffers[key] = frame

    def _get_frame(self, context: StrategyContext, timeframe: Optional[str] = None) -> Optional[pd.DataFrame]:
        return self._frame_buffers.get(self._context_key(context, timeframe))

    # ------------------------------------------------------------------ #
    #  Marker management                                                 #
    # ------------------------------------------------------------------ #
    def append_marker(self, context: StrategyContext, marker: Mapping[str, object]) -> None:
        payload = dict(marker)
        payload["strategy_id"] = context.strategy_id
        payload["symbol"] = context.symbol
        payload["timeframe"] = context.timeframe
        bucket = self._markers[context.strategy_id][context.symbol][context.timeframe]
        bucket.append(payload)

    def extend_markers(self, context: StrategyContext, markers: Iterable[Mapping[str, object]]) -> None:
        for marker in markers:
            self.append_marker(context, marker)

    def clear_markers(self, context: Optional[StrategyContext] = None) -> None:
        if context is None:
            self._markers.clear()
            return

        strategy_bucket = self._markers.get(context.strategy_id)
        if not strategy_bucket:
            return
        symbol_bucket = strategy_bucket.get(context.symbol)
        if not symbol_bucket:
            return
        symbol_bucket.pop(context.timeframe, None)
        if not symbol_bucket:
            strategy_bucket.pop(context.symbol, None)
        if not strategy_bucket:
            self._markers.pop(context.strategy_id, None)

    def get_markers_for_context(self, context: StrategyContext) -> List[Dict[str, object]]:
        strategy_bucket = self._markers.get(context.strategy_id, {})
        symbol_bucket = strategy_bucket.get(context.symbol, {})
        return [dict(marker) for marker in symbol_bucket.get(context.timeframe, [])]

    def get_markers_grouped(self) -> Dict[str, Dict[str, Dict[str, List[Dict[str, object]]]]]:
        grouped: Dict[str, Dict[str, Dict[str, List[Dict[str, object]]]]] = {}
        for strategy_id, symbol_map in self._markers.items():
            grouped[strategy_id] = {}
            for symbol, timeframe_map in symbol_map.items():
                grouped[strategy_id][symbol] = {}
                for timeframe, markers in timeframe_map.items():
                    grouped[strategy_id][symbol][timeframe] = [dict(marker) for marker in markers]
        return grouped
