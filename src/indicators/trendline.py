import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple, Dict, Literal, Optional
import logging

from .base import BaseIndicator
from .config import DataContext

log = logging.getLogger("TrendlineIndicator")

# ----------------------------
# Tunables (simple, readable)
# ----------------------------
LOOKBACKS_DEFAULT        = (5,)     # rolling window size(s) to detect pivots
PIVOT_DEDUPE_FRAC        = 0.005    # 0.5%: merge near-equal pivots
WINDOW_SIZE              = 3        # fit line on this many most-recent pivots per side
MAX_WINDOWS_PER_SIDE     = 2        # draw this many windows per side (recent first)
BREAK_TOL_FRAC           = 0.0015   # 0.15% tolerance for “decisive break”
TOUCH_TOL_FRAC           = 0.0015   # 0.15% tolerance for touch detection
ENFORCE_DIRECTION        = True     # slope must match local price move
SLOPE_EPS                = 1e-6
MIN_SPAN_BARS            = 12       # earliest vs latest pivot in window must be at least this far apart
MAX_PROJECTION_BARS      = 0        # keep 0 for no projection; keep simple
# --- Pivot RANSAC defaults ---
PROJECTION_BARS         = 40     # dashed projection after the solid segment
RANSAC_TRIALS           = 250    # random 2-point samples
RANSAC_TOL_FRAC         = 0.003  # ~0.3% relative error to count as inlier
RANSAC_MIN_INLIERS      = 3      # need at least this many pivot inliers
MAX_LINES_PER_SIDE      = 2      # sequentially extract up to N lines per side


@dataclass
class TL:
    """Simple trendline holder."""
    side: Literal["support", "resistance"]
    slope: float
    intercept: float
    i_from: int
    i_to: int
    touches: List[pd.Timestamp]

# ---------- small utilities ----------

def _to_utc_s(t: pd.Timestamp) -> int:
    t = pd.Timestamp(t)
    t = t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
    return int(t.timestamp())

def _nearest_pos(dt_index: pd.DatetimeIndex, ts: pd.Timestamp) -> int:
    """Nearest index position for ts (tz-safe, older pandas-safe)."""
    t = pd.Timestamp(ts)
    if dt_index.tz is not None:
        t = t.tz_localize(dt_index.tz) if t.tzinfo is None else t.tz_convert(dt_index.tz)
    else:
        t = t.tz_localize(None)
    try:
        pos = int(dt_index.get_indexer([t], method="nearest")[0])
        if pos != -1:
            return pos
    except TypeError:
        pass
    return int(np.clip(np.searchsorted(dt_index.values, t.to_datetime64()),
                       0, len(dt_index)-1))
def _ransac_line(x: np.ndarray, y: np.ndarray,
                 trials: int = RANSAC_TRIALS,
                 tol_frac: float = RANSAC_TOL_FRAC,
                 min_inliers: int = RANSAC_MIN_INLIERS) -> tuple[float, float, np.ndarray] | None:
    """
    Return (m, c, inlier_mask) for y ≈ m*x + c using simple 2-point RANSAC on pivots.
    Inlier rule: |y - (m x + c)| / max(|y|, 1e-9) <= tol_frac.
    """
    n = x.size
    if n < 2:
        return None
    best = (None, None, None)
    best_count = 0
    rng = np.random.default_rng()
    for _ in range(trials):
        i, j = rng.choice(n, size=2, replace=False)
        if x[j] == x[i]:
            continue
        m = (y[j] - y[i]) / (x[j] - x[i])
        c = y[i] - m * x[i]
        yhat = m * x + c
        resid = np.abs(y - yhat) / np.maximum(np.abs(yhat), 1e-9)
        mask = resid <= tol_frac
        cnt = int(mask.sum())
        if cnt > best_count:
            best = (m, c, mask)
            best_count = cnt
    if best_count < min_inliers:
        return None
    # refine with OLS on inliers
    m0, c0, mask = best
    xi, yi = x[mask], y[mask]
    m_ref = float(np.polyfit(xi, yi, 1)[0])
    c_ref = float(np.mean(yi - m_ref * xi))
    return m_ref, c_ref, mask


# ---------- the indicator ----------

class TrendlineIndicator(BaseIndicator):
    """
    Minimal, pivot-anchored trendlines:
      • find pivot highs / lows (rolling lookback)
      • for each side, use the last WINDOW_SIZE pivots to fit a line
      • adjust intercept to hug pivots (envelope)
      • draw from first pivot to the first decisive break
    """

    NAME = "trendline"

    def __init__(
        self,
        df: pd.DataFrame,
        lookbacks: List[int] | Tuple[int, ...] = LOOKBACKS_DEFAULT,
        tolerance: float = TOUCH_TOL_FRAC,
        timeframe: str = "1d",
        min_span_bars: int = MIN_SPAN_BARS,
        window_size: int = WINDOW_SIZE,
        max_windows_per_side: int = MAX_WINDOWS_PER_SIDE,
        pivot_dedupe_frac: float = PIVOT_DEDUPE_FRAC,
        enforce_direction: bool = ENFORCE_DIRECTION,
        algo: str = "pivot_ransac",
        projection_bars: int = PROJECTION_BARS,
        ransac_trials: int = RANSAC_TRIALS,
        ransac_tol_frac: float = RANSAC_TOL_FRAC,
        ransac_min_inliers: int = RANSAC_MIN_INLIERS,
        max_lines_per_side: int = MAX_LINES_PER_SIDE,
    ):
        self.df = df.copy()
        self.lookbacks = tuple(int(x) for x in lookbacks)
        self.tolerance = float(tolerance)
        self.timeframe = timeframe
        self.min_span_bars = int(min_span_bars)
        self.window_size = int(window_size)
        self.max_windows_per_side = int(max_windows_per_side)
        self.pivot_dedupe_frac = float(pivot_dedupe_frac)
        self.enforce_direction = bool(enforce_direction)
        self.algo = algo
        self.projection_bars = int(projection_bars)
        self.ransac_trials = int(ransac_trials)
        self.ransac_tol_frac = float(ransac_tol_frac)
        self.ransac_min_inliers = int(ransac_min_inliers)
        self.max_lines_per_side = int(max_lines_per_side)
        self.lines: List[TL] = []
        self._compute()

    @classmethod
    def from_context(
        cls,
        provider,
        ctx: DataContext,
        lookbacks: List[int] = list(LOOKBACKS_DEFAULT),
        tolerance: float = TOUCH_TOL_FRAC,
        timeframe: str = "1d",
        min_span_bars: int = MIN_SPAN_BARS,
        window_size: int = WINDOW_SIZE,
        max_windows_per_side: int = MAX_WINDOWS_PER_SIDE,
        pivot_dedupe_frac: float = PIVOT_DEDUPE_FRAC,
        enforce_direction: bool = ENFORCE_DIRECTION,
        algo: str = "pivot_ransac",
        projection_bars: int = PROJECTION_BARS,
        ransac_trials: int = RANSAC_TRIALS,
        ransac_tol_frac: float = RANSAC_TOL_FRAC,
        ransac_min_inliers: int = RANSAC_MIN_INLIERS,
        max_lines_per_side: int = MAX_LINES_PER_SIDE,
    ):
        log.info("[Trendline.from_context] Fetching OHLCV for %s [%s - %s] interval=%s",
                 ctx.symbol, ctx.start, ctx.end, ctx.interval)
        df = provider.get_ohlcv(ctx)
        if df is None or df.empty:
            raise ValueError("No OHLCV to compute trendlines.")
        return cls(
            df=df,
            lookbacks=lookbacks,
            tolerance=tolerance,
            timeframe=timeframe,
            min_span_bars=min_span_bars,
            window_size=window_size,
            max_windows_per_side=max_windows_per_side,
            pivot_dedupe_frac=pivot_dedupe_frac,
            enforce_direction=enforce_direction,
            algo=algo,
            projection_bars=projection_bars,
            ransac_trials=ransac_trials,
            ransac_tol_frac=ransac_tol_frac,
            ransac_min_inliers=ransac_min_inliers,
            max_lines_per_side=max_lines_per_side,
        )

    # ----- pivots -----

    def _find_pivots(self, lookback: int) -> Tuple[List[Tuple[pd.Timestamp, float]], List[Tuple[pd.Timestamp, float]]]:
        """Local max/min with simple dedupe by price distance."""
        highs, lows = [], []
        def near_any(px: float) -> bool:
            allp = [p for _, p in highs + lows]
            return any(abs(px - p) / max(p, 1e-9) < self.pivot_dedupe_frac for p in allp)

        hi_series = self.df["high"]
        lo_series = self.df["low"]
        idx = self.df.index

        for i in range(lookback, len(idx) - lookback):
            ts = idx[i]
            hi = hi_series.iat[i]
            lo = lo_series.iat[i]
            if hi > hi_series.iloc[i-lookback:i+lookback+1].drop(ts).max() and not near_any(hi):
                highs.append((ts, float(hi)))
            if lo < lo_series.iloc[i-lookback:i+lookback+1].drop(ts).min() and not near_any(lo):
                lows.append((ts, float(lo)))
        return highs, lows

    def _collect_pivots(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Merge pivots from all lookbacks and return (high_idx, high_px, low_idx, low_px)."""
        all_highs, all_lows = [], []
        for lb in self.lookbacks:
            h, l = self._find_pivots(lb)
            all_highs += h
            all_lows += l
        all_highs.sort(key=lambda x: x[0])
        all_lows.sort(key=lambda x: x[0])

        hi_idx = np.array([self.df.index.get_loc(ts) for ts, _ in all_highs], dtype=int)
        hi_px  = np.array([p for _, p in all_highs], dtype=float)
        lo_idx = np.array([self.df.index.get_loc(ts) for ts, _ in all_lows ], dtype=int)
        lo_px  = np.array([p for _, p in all_lows ], dtype=float)
        return hi_idx, hi_px, lo_idx, lo_px

    # ----- line building -----
    def _first_break(self, side: str, line: np.ndarray, low: np.ndarray, high: np.ndarray, start_i: int) -> int | None:
        tol = BREAK_TOL_FRAC
        if side == "resistance":
            hits = np.nonzero(high[start_i:] > line[start_i:] * (1 + tol))[0]
        else:
            hits = np.nonzero(low[start_i:]  < line[start_i:] * (1 - tol))[0]
        return start_i + int(hits[0]) if hits.size else None

    def _compute_pivot_ransac(self) -> None:
        """Detect up to N lines per side using RANSAC on pivot points only."""
        # 1) collect pivots
        hi_i, hi_p, lo_i, lo_p = self._collect_pivots()  # (indices, prices) arrays
        close = self.df["close"].to_numpy(float)
        low   = self.df["low"  ].to_numpy(float)
        high  = self.df["high" ].to_numpy(float)
        xs    = np.arange(len(self.df.index), dtype=float)
        last  = int(xs[-1])

        self.lines = []
        for side, piv_idx, piv_px in (("resistance", hi_i, hi_p), ("support", lo_i, lo_p)):
            # work on a copy we can whittle down (sequential RANSAC)
            X = piv_idx.astype(float).copy()
            Y = piv_px.astype(float).copy()
            used_mask = np.zeros_like(X, dtype=bool)

            lines_side = []
            for _ in range(self.max_lines_per_side):
                # available (not yet used) pivots
                avail = ~used_mask
                if avail.sum() < self.ransac_min_inliers:
                    break
                xw, yw = X[avail], Y[avail]

                # require span
                if (xw.max() - xw.min()) < self.min_span_bars:
                    break

                got = _ransac_line(xw, yw,
                                trials=self.ransac_trials,
                                tol_frac=self.ransac_tol_frac,
                                min_inliers=self.ransac_min_inliers)
                if not got:
                    break
                m_hat, c_hat, in_mask = got

                # adjust intercept to "envelope" pivots (stay under highs / over lows)
                c_vec = yw[in_mask] - m_hat * xw[in_mask]
                if side == "resistance":
                    c = float(np.min(c_vec))   # line <= highs
                else:
                    c = float(np.max(c_vec))   # line >= lows
                m = float(m_hat)

                line = m * xs + c

                # segment bounds
                in_idx = xw[in_mask].astype(int)
                seg_from = int(in_idx.min())
                break_i = self._first_break(side, line, low, high, seg_from)
                seg_to = int(break_i) if (break_i is not None and break_i > seg_from) else last

                # optional direction enforcement
                if self.enforce_direction and seg_to > seg_from:
                    local_m = float(np.polyfit(np.arange(seg_from, seg_to+1, dtype=float),
                                            close[seg_from:seg_to+1], 1)[0])
                    if m < -SLOPE_EPS and local_m >= -SLOPE_EPS:  # down line, but not a down move
                        # mark these inliers as used anyway to avoid reselecting the same cluster
                        used_mask[np.isin(X, in_idx)] = True
                        continue
                    if m >  SLOPE_EPS and local_m <=  SLOPE_EPS:  # up line, but not an up move
                        used_mask[np.isin(X, in_idx)] = True
                        continue

                # touches within the *solid* part: from seg_from to last inlier touch
                solid_to = int(min(seg_to, in_idx.max()))
                proj_to  = int(min(last, solid_to + self.projection_bars))

                # bar-range touches inside solid segment
                seg_slice = slice(seg_from, solid_to + 1)
                near = (low[seg_slice] <= line[seg_slice]) & (line[seg_slice] <= high[seg_slice])
                touch_is = (np.nonzero(near)[0] + seg_from).tolist()
                touches_ts = [self.df.index[i] for i in touch_is]

                lines_side.append(dict(
                    side=side, m=m, c=c,
                    i_from=seg_from, i_solid=solid_to, i_proj=proj_to,
                    touches=touches_ts
                ))

                # retire used inliers (by index equality)
                used_mask[np.isin(X, in_idx)] = True

            # keep the side’s lines ordered by recency (start index descending)
            lines_side.sort(key=lambda d: d["i_from"], reverse=True)
            self.lines.extend(lines_side)


    def _fit_envelope(self, side: str, x: np.ndarray, y: np.ndarray) -> Tuple[float, float]:
        """OLS slope, then shift intercept to hug pivots (envelope)."""
        m = float(np.polyfit(x.astype(float), y.astype(float), 1)[0])
        c_candidates = y - m * x
        if side == "resistance":
            c = float(np.min(c_candidates))  # line <= highs
        else:
            c = float(np.max(c_candidates))  # line >= lows
        return m, c

    def _first_break(self, side: str, line: np.ndarray, low: np.ndarray, high: np.ndarray, start_i: int) -> Optional[int]:
        tol = float(BREAK_TOL_FRAC)
        if side == "resistance":
            hits = np.nonzero(high[start_i:] > line[start_i:] * (1 + tol))[0]
        else:
            hits = np.nonzero(low[start_i:]  < line[start_i:] * (1 - tol))[0]
        return start_i + int(hits[0]) if hits.size else None

    def _build_side(self, side: Literal["support","resistance"], piv_idx: np.ndarray, piv_px: np.ndarray,
                    close: np.ndarray, low: np.ndarray, high: np.ndarray) -> List[TL]:
        """Use the last WINDOW_SIZE pivots (and optionally the previous block) to build lines."""
        out: List[TL] = []
        if piv_idx.size < self.window_size:
            return out

        xs = np.arange(len(close), dtype=float)

        # choose up to max_windows_per_side windows counting from the end
        windows = []
        end = piv_idx.size
        for _ in range(self.max_windows_per_side):
            start = max(0, end - self.window_size)
            if end - start < self.window_size:
                break
            windows.append((start, end))
            end = start
        # most recent first
        for (a, b) in windows:
            xw = piv_idx[a:b]
            yw = piv_px[a:b]
            if (xw[-1] - xw[0]) < self.min_span_bars:
                continue
            m, c = self._fit_envelope(side, xw, yw)
            line = m * xs + c

            seg_from = int(xw[0])
            br = self._first_break(side, line, low, high, seg_from)
            seg_to = int(br) if (br is not None and br > seg_from) else int(xs[-1])

            if self.enforce_direction and seg_to > seg_from:
                local_m = float(np.polyfit(np.arange(seg_from, seg_to+1, dtype=float),
                                           close[seg_from:seg_to+1], 1)[0])
                if m < -SLOPE_EPS and local_m >= -SLOPE_EPS:  # down line, not down move
                    continue
                if m >  SLOPE_EPS and local_m <=  SLOPE_EPS:  # up line, not up move
                    continue

            # touches only within the valid segment
            seg_slice = slice(seg_from, seg_to + 1)
            near = (low[seg_slice] <= line[seg_slice]) & (line[seg_slice] <= high[seg_slice])
            touch_i = (np.nonzero(near)[0] + seg_from).tolist()
            touches_ts = [self.df.index[i] for i in touch_i]

            out.append(TL(side=side, slope=float(m), intercept=float(c),
                          i_from=seg_from, i_to=seg_to, touches=touches_ts))
        return out

    # ----- orchestrate -----

    def _compute(self) -> None:
        
        if self.algo == "pivot_ransac":
            self._compute_pivot_ransac()
            return


        hi_i, hi_p, lo_i, lo_p = self._collect_pivots()
        close = self.df["close"].to_numpy(float)
        low   = self.df["low"  ].to_numpy(float)
        high  = self.df["high" ].to_numpy(float)

        self.lines = []
        self.lines += self._build_side("resistance", hi_i, hi_p, close, low, high)
        self.lines += self._build_side("support",    lo_i, lo_p, close, low, high)
        log.info("[Trendline] built %d lines", len(self.lines))

    # ----- outputs -----

    def to_lightweight(self, plot_df: pd.DataFrame, include_touches: bool = True, top_n: int | None = None):
        if plot_df is None or plot_df.empty:
            return {"segments": [], "markers": []}

        lines = self.lines[:top_n] if top_n else self.lines
        segs, markers = [], []
        n = len(plot_df.index)

        def tsec(i): return _to_utc_s(plot_df.index[max(0, min(i, n-1))])

        for L in lines:
            m, c, side = L["m"], L["c"], L["side"]
            i0   = int(L.get("i_from", 0))
            iS   = int(L.get("i_solid", L.get("i_to", n-1)))   # solid end
            iP   = int(L.get("i_proj",  iS))                   # projection end

            y0 = float(m * i0 + c); yS = float(m * iS + c); yP = float(m * iP + c)

            # solid
            segs.append({
                "x1": tsec(i0), "x2": tsec(iS), "y1": y0, "y2": yS,
                "lineStyle": 0, "lineWidth": 2, "color": "#6b7280",
            })
            # dashed projection (+40 bars by default)
            if iP > iS:
                segs.append({
                    "x1": tsec(iS), "x2": tsec(iP), "y1": yS, "y2": yP,
                    "lineStyle": 2, "lineWidth": 2, "color": "#6b7280",
                })

            if include_touches and L.get("touches"):
                pos = "belowBar" if side == "resistance" else "aboveBar"
                for ts in L["touches"]:
                    j = _nearest_pos(plot_df.index, ts)
                    if j < i0 or j > iS:  # show dots only on the solid segment
                        continue
                    markers.append({
                        "time": tsec(j), "position": pos, "shape": "circle",
                        "color": "#6b7280", "price": float(m * j + c),
                        "subtype": "touch",
                    })

        return {"segments": segs, "markers": markers}
