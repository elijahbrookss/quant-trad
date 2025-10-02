import math
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Set, Any, Optional, Mapping
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from mplfinance.plotting import make_addplot
import logging
from datetime import timezone

from core.logger import logger
from .base import BaseIndicator
from .config import DataContext

def _ts_iso(ts) -> str:
    # Lightweight markers/lines in your app are fine with ISO8601 strings
    stamp = pd.Timestamp(ts)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.isoformat().replace("+00:00", "Z")

def _to_business_day_str(ts):
    return pd.Timestamp(ts).tz_convert("UTC").date().isoformat()

def _to_unix_s(ts):
    return int(pd.Timestamp(ts).tz_convert("UTC").timestamp())

def _find_touch_markers(df, level: float, start_ts: pd.Timestamp, label: str, fmt_time):
    mks = []
    if df.index.tz is None:
        df = df.tz_localize("UTC")
    window = df.loc[df.index >= start_ts]
    for t, row in window.iterrows():
        lo, hi = float(row["low"]), float(row["high"])
        if lo <= level <= hi:
            mks.append({
                "time": int(pd.Timestamp(t).timestamp()),      # <-- 'YYYY-MM-DD'
                "shape": "circle",
                "color": "#6b7280", # default color, will be recolored on client,
                "subtype": "touch",
                "price": float(level)
            })
    return mks

class MarketProfileIndicator(BaseIndicator):
    """
    Computes daily market profile (TPO) to identify Point of Control (POC),
    Value Area High (VAH), and Value Area Low (VAL), and provides plotting overlays.
    """
    NAME = "market_profile"

    def __init__(
        self,
        df: pd.DataFrame,
        bin_size: float = 0.1,
        mode: str = "tpo",
        interval: str = "30m",
        extend_value_area_to_chart_end: bool = True,
        use_merged_value_areas: bool = True,
        merge_threshold: float = 0.6,
    ):
        super().__init__(df)
        self.bin_size = bin_size
        self.mode = mode
        # Compute raw daily profiles on initialization
        self.daily_profiles = self._compute_daily_profiles()
        self.merged_profiles = []
        self.interval = interval
        self.extend_value_area_to_chart_end = bool(extend_value_area_to_chart_end)
        self.use_merged_value_areas = bool(use_merged_value_areas)
        self.merge_threshold = float(merge_threshold) if merge_threshold is not None else 0.6

    @staticmethod
    def describe_profile(profile: Mapping[str, Any]) -> str:
        """Return a concise, human readable description for a market profile."""

        def _format_ts(value: Any) -> str:
            if value is None:
                return "n/a"
            try:
                return _ts_iso(value)
            except Exception:
                try:
                    return pd.Timestamp(value).isoformat()
                except Exception:
                    return str(value)

        def _format_price(value: Any) -> str:
            numeric = None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return "n/a"

            if math.isnan(numeric) or math.isinf(numeric):
                return "n/a"
            return f"{numeric:.2f}"

        start_ts = profile.get("start") or profile.get("start_date") or profile.get("date")
        end_ts = profile.get("end") or profile.get("end_date") or start_ts

        val = profile.get("VAL")
        vah = profile.get("VAH")
        poc = profile.get("POC")

        session_count = profile.get("session_count") or profile.get("sessions")
        if not session_count:
            session_count = profile.get("sessionCount")

        extra_bits = []
        if session_count:
            extra_bits.append(f"sessions={session_count}")

        return (
            f"start={_format_ts(start_ts)} | end={_format_ts(end_ts)} | "
            f"VAL={_format_price(val)} | VAH={_format_price(vah)} | "
            f"POC={_format_price(poc)}" + (" | " + ", ".join(extra_bits) if extra_bits else "")
        )

    @classmethod
    def from_context(
        cls,
        provider,
        ctx: DataContext,
        bin_size: float = 0.1,
        mode: str = "tpo",
        interval: str = "30m",
        extend_value_area_to_chart_end: bool = True,
        use_merged_value_areas: bool = True,
        merge_threshold: float = 0.6,
    ):
        """
        Fetches OHLCV from provider and constructs the indicator.
        Raises ValueError if no data is available.
        """
        ctx = DataContext(symbol=ctx.symbol, start=ctx.start, end=ctx.end, interval=interval)
        ctx.validate()

        df = provider.get_ohlcv(ctx)
        if df is None or df.empty:
            raise ValueError(f"MarketProfileIndicator: No data available for {ctx.symbol} [{ctx.interval}] after ingest")

        return cls(
            df=df,
            bin_size=bin_size,
            mode=mode,
            interval=interval,
            extend_value_area_to_chart_end=extend_value_area_to_chart_end,
            use_merged_value_areas=use_merged_value_areas,
            merge_threshold=merge_threshold,
        )

    def _compute_daily_profiles(self) -> List[Dict[str, float]]:
        """
        Build daily TPO profiles: POC, VAH, VAL, plus session timestamps.
        """
        df = self.df.copy()
        df.index = pd.to_datetime(df.index, utc=True)
        profiles = []

        logger.info("Starting daily profile computation for %d sessions", len(np.unique(df.index.date)))
        # Group rows by calendar date
        grouped = df.groupby(df.index.date)
        for session_date, group in grouped:
            logger.debug("Processing session: %s, bars: %d", session_date, len(group))
            tpo_hist = self._build_tpo_histogram(group)
            value_area = self._extract_value_area(tpo_hist)

            value_area.update({
                "date": pd.to_datetime(session_date),
                "start_date": group.index.min(),
                "end_date": group.index.max()
            })

            profiles.append(value_area)
            logger.info("Profile for %s: POC=%.2f, VAH=%.2f, VAL=%.2f", session_date, value_area["POC"], value_area["VAH"], value_area["VAL"])

        logger.info("Completed daily profile computation. Total profiles: %d", len(profiles))
        return profiles

    def _build_tpo_histogram(self, data: pd.DataFrame) -> Dict[float, int]:
        """
        Count how many bars visit each price bucket defined by bin_size.
        :param data: intraday DataFrame for one session.
        :return: mapping of price bucket -> count of TPO occurrences.
        """
        tpo_counts = {}
        logger.debug("Building TPO histogram for session with %d bars", len(data))
        for _, row in data.iterrows():
            low, high = row["low"], row["high"]
            prices = np.arange(low, high + self.bin_size, self.bin_size)
            for price in prices:
                bucket = round(price / self.bin_size) * self.bin_size
                tpo_counts[bucket] = tpo_counts.get(bucket, 0) + 1
        logger.debug("Built TPO histogram with %d buckets", len(tpo_counts))
        return tpo_counts

    def _extract_value_area(self, tpo_hist: Dict[float, int]) -> Dict[str, float]:
        """
        From the TPO histogram, compute:
          - POC: price with highest count
          - VAH: upper bound of 70% cumulative TPO
          - VAL: lower bound of 70% cumulative TPO
        """
        total = sum(tpo_hist.values())
        if total == 0:
            logger.warning("TPO histogram is empty, cannot extract value area.")
            return {"POC": None, "VAH": None, "VAL": None}

        # sort buckets by descending count
        sorted_buckets = sorted(tpo_hist.items(), key=lambda item: item[1], reverse=True)
        poc_price = sorted_buckets[0][0]

        cumulative = 0
        va_prices = []
        threshold = 0.7 * total
        for price, count in sorted_buckets:
            cumulative += count
            va_prices.append(price)
            if cumulative >= threshold:
                break

        logger.debug("Extracted value area: POC=%.2f, VAH=%.2f, VAL=%.2f, total TPO=%d", poc_price, max(va_prices), min(va_prices), total)
        return {"POC": poc_price, "VAH": max(va_prices), "VAL": min(va_prices)}

    def merge_value_areas(
        self,
        threshold: Optional[float] = None,
        min_merge: int = 2,
    ) -> List[Dict[str, float]]:
        """
        Combine consecutive daily profiles whose value areas overlap
        at least `threshold` fraction, requiring at least `min_merge` days.
        """
        if threshold is None:
            threshold = getattr(self, "merge_threshold", 0.6)
        threshold = float(threshold)

        merged = []
        profiles = self.daily_profiles
        i, n = 0, len(profiles)

        logger.info("Starting merge of value areas: threshold=%.2f, min_merge=%d", threshold, min_merge)

        while i < n:
            base = profiles[i]
            merged_val, merged_vah = base["VAL"], base["VAH"]
            start_ts, end_ts = base["start_date"], base["end_date"]
            poc_list = [base["POC"]] if base.get("POC") is not None else []
            count = 1
            j = i + 1

            logger.debug("Merging from profile %d (start: %s)", i, start_ts)
            while j < n:
                next_prof = profiles[j]
                overlap = self._calculate_overlap(merged_val, merged_vah, next_prof["VAL"], next_prof["VAH"])
                logger.debug("Checking overlap with profile %d: overlap=%.2f (threshold=%.2f)", j, overlap, threshold)
                if overlap < threshold:
                    logger.debug("Overlap below threshold, stopping merge at profile %d", j)
                    break
                merged_val = min(merged_val, next_prof["VAL"])
                merged_vah = max(merged_vah, next_prof["VAH"])
                end_ts = next_prof["end_date"]
                if next_prof.get("POC") is not None:
                    poc_list.append(next_prof["POC"])
                count += 1
                j += 1

            if count >= min_merge:
                avg_poc = sum(poc_list) / len(poc_list) if poc_list else None
                merged.append({
                    "start": start_ts,
                    "end": end_ts,
                    "VAL": merged_val,
                    "VAH": merged_vah,
                    "POC": avg_poc,
                    "session_count": count,
                })
                logger.info("Merged %d profiles: [%s → %s], VAL=%.2f, VAH=%.2f, avg POC=%.2f", count, start_ts, end_ts, merged_val, merged_vah, avg_poc if avg_poc else float('nan'))
            else:
                logger.debug("Merge group too small (%d < %d), skipping", count, min_merge)
            i = j

        self.merged_profiles = merged
        logger.info("Completed merging. Total merged profiles: %d", len(merged))

        if profiles:
            logger.info("Daily market profiles summary (%d):", len(profiles))
            for idx, prof in enumerate(profiles, start=1):
                logger.info("  [%d] %s", idx, self.describe_profile(prof))

        if merged:
            logger.info("Merged market profiles summary (%d):", len(merged))
            for idx, prof in enumerate(merged, start=1):
                logger.info("  [%d] %s", idx, self.describe_profile(prof))

        return merged

    def to_overlays(
        self,
        plot_df: pd.DataFrame,
        use_merged: Optional[bool] = None,
    ) -> Tuple[List, Set[Tuple[str, str]]]:
        """
        Emit two kinds of overlay specs:
        • kind="rect" → persistent VAH/VAL zones
        • kind="addplot" → POC horizontal line
        """
        if use_merged is None:
            use_merged = getattr(self, "use_merged_value_areas", True)

        profiles = self.merged_profiles if use_merged else self.daily_profiles
        if not profiles:
            logger.warning("No profiles to generate overlays.")
            return [], set()

        overlays = []
        legend_entries = set()
        full_idx = plot_df.index

        logger.info(
            "event=market_profile_overlay_start profiles=%d use_merged=%s",
            len(profiles),
            use_merged,
        )
        for idx, prof in enumerate(profiles):
            # Robustly get the start timestamp
            try:
                start_ts = prof.get("start")
                if start_ts is None:
                    start_ts = prof.get("start_date")
                if start_ts is None:
                    raise KeyError("Profile missing both 'start' and 'start_date'")
            except Exception as e:
                logger.error("Profile %d missing 'start'/'start_date': %s. Skipping overlay.", idx, str(e))
                continue

            val, vah = prof.get("VAL"), prof.get("VAH")
            poc = prof.get("POC")

            if val is None or vah is None:
                logger.warning("Profile %d missing VAL or VAH. Skipping overlay.", idx)
                continue

            plot_min = plot_df.index.min()
            if start_ts < plot_min:
                logger.debug("Adjusting rect start from %s to %s (plot start)", start_ts, plot_min)
                start_ts = plot_min

            # 1) persistent rectangle for VA zone
            overlays.append({
                "kind": "rect",
                "start": start_ts,
                "val": val,
                "vah": vah,
                "color": "gray",
                "alpha": 0.2
            })
            legend_entries.add(("Value Area", "gray"))
            logger.debug(
                "event=market_profile_rect start=%s val=%.4f vah=%.4f",
                start_ts,
                val,
                vah,
            )

            # 2) optional POC line as an addplot
            if poc is not None:
                poc_series = pd.Series(index=full_idx, dtype=float)
                poc_series.loc[full_idx >= start_ts] = poc

                ap = make_addplot(poc_series, color="orange", width=1.0, linestyle="--")
                overlays.append({"kind": "addplot", "plot": ap})
                legend_entries.add(("POC", "orange"))
                logger.debug(
                    "event=market_profile_poc start=%s poc=%.4f",
                    start_ts,
                    poc,
                )
        logger.info("event=market_profile_overlay_complete overlays=%d", len(overlays))
        return overlays, legend_entries

    @staticmethod
    def _calculate_overlap(val1: float, vah1: float, val2: float, vah2: float) -> float:
        """
        Compute the overlap ratio between two value areas,
        normalized by the range of the second area.
        """
        low = max(val1, val2)
        high = min(vah1, vah2)
        overlap = max(0.0, high - low)
        range2 = vah2 - val2
        ratio = overlap / range2 if range2 > 0 else 0.0
        logger.debug("Calculated overlap: [%.2f, %.2f] vs [%.2f, %.2f] => overlap=%.2f, ratio=%.2f", val1, vah1, val2, vah2, overlap, ratio)
        return ratio

    


    def to_lightweight(
        self,
        plot_df: pd.DataFrame,
        use_merged: Optional[bool] = None,
        merge_threshold: Optional[float] = None,
        min_merge: int = 3,
        include_touches: bool = True,
        time_fmt="business_day",
        extend_boxes_to_chart_end: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Return overlays in your portal's expected shape:
        {
            "price_lines": [ { price, title, time, lineStyle, lineWidth, extend, color, axisLabelVisible }, ... ],
            "markers":     [ { time, position, shape, text, color, size }, ... ]
        }
        Notes:
        • 'time' is when the line starts; renderer should extend to the right.
        • Color will be recolored uniformly per-indicator on the client.
        • Value-area boxes extend to the chart end unless extend_value_area_to_chart_end=False.
        """
        if plot_df is None or plot_df.empty:
            return {"price_lines": [], "markers": []}

        # Ensure tz-aware UTC index for robust comparisons
        plot_df = plot_df.copy()
        plot_df.index = pd.to_datetime(plot_df.index, utc=True)

        fmt_time = _to_business_day_str if time_fmt == "business_day" else _to_unix_s

        if extend_boxes_to_chart_end is None:
            extend_boxes_to_chart_end = getattr(self, "extend_value_area_to_chart_end", True)
        else:
            extend_boxes_to_chart_end = bool(extend_boxes_to_chart_end)

        if use_merged is None:
            use_merged = getattr(self, "use_merged_value_areas", True)

        if merge_threshold is None:
            merge_threshold = getattr(self, "merge_threshold", 0.6)

        if use_merged:
            # compute merged profiles once if needed
            if not getattr(self, "merged_profiles", None):
                self.merge_value_areas(threshold=merge_threshold, min_merge=min_merge)
            profiles = self.merged_profiles or []
        else:
            profiles = self.daily_profiles or []

        out_lines: List[Dict[str, Any]] = []
        out_markers: List[Dict[str, Any]] = []

        if not profiles:
            return {"price_lines": [], "markers": []}

        chart_start = plot_df.index.min()
        chart_end = plot_df.index.max()

        out_lines, out_markers, out_boxes = [], [], []

        logger.info(
            "event=market_profile_lightweight_start profiles=%d chart_start=%s chart_end=%s",
            len(profiles),
            chart_start,
            chart_end,
        )

        for prof in profiles:
            # accept either merged keys ('start') or daily keys ('start_date')
            start_ts = pd.to_datetime(prof.get("start") or prof.get("start_date"), utc=True)

            vah = prof.get("VAH")
            val = prof.get("VAL")
            poc = prof.get("POC")

            if start_ts is None or vah is None or val is None:
                continue

            start_ts = pd.to_datetime(start_ts, utc=True)
            # clamp to chart window
            if start_ts < chart_start:
                start_ts = chart_start
            if start_ts > chart_end:
                # starts after current chart window; nothing to draw
                continue

            # start_iso = _ts_iso(start_ts)
            start_str = fmt_time(start_ts) # either 'YYYY-MM-DD' or unix seconds
            
            if extend_boxes_to_chart_end:
                end_ts = chart_end
            else:
                end_ts = pd.to_datetime(
                    prof.get("end") or prof.get("end_date") or chart_end,
                    utc=True,
                )
                if end_ts > chart_end:
                    end_ts = chart_end
            if end_ts < start_ts:
                end_ts = start_ts

            out_boxes.append({
                "x1": _to_unix_s(start_ts),   # epoch seconds
                "x2": _to_unix_s(end_ts),     # epoch seconds
                "y1": float(val),             # VAL
                "y2": float(vah),             # VAH
                "color": "rgba(156,163,175,0.18)",   # neutral grey w/ alpha; UI can recolor later
            })

            logger.debug(
                "event=market_profile_lightweight_box start=%s end=%s x1=%d x2=%d y1=%.4f y2=%.4f",
                start_ts,
                end_ts,
                _to_unix_s(start_ts),
                _to_unix_s(end_ts),
                float(val),
                float(vah),
            )

            # ----- price lines (Lightweight "price line" settings) -----
            # # VAL (solid)
            # out_lines.append({
            #     "price": float(val),
            #     "title": "VAL",
            #     "time": start_str,          # when the line begins
            #     "lineStyle": 0,             # 0=Solid, 2=Dashed (Lightweight enum)
            #     "lineWidth": 0,
            #     "extend": "right",
            #     "axisLabelVisible": True,
            #     "color": "#6b7280",
            # })
            # # POC (dashed) — if available
            # if poc is not None:
            #     out_lines.append({
            #         "price": float(poc),
            #         "title": "POC",
            #         "time": _to_unix_s(start_ts),
            #         "lineStyle": 2,         # dashed to distinguish from VA band edges
            #         "lineWidth": 0,
            #         "extend": "right",
            #         "axisLabelVisible": False,
            #         "color": "#f59e0b",
            #     })
            # # VAH (solid)
            # out_lines.append({
            #     "price": float(vah),
            #     "title": "VAH",
            #     "time": start_str,
            #     "lineStyle": 0,
            #     "lineWidth": 0,
            #     "extend": "right",
            #     "axisLabelVisible": True,
            #     "color": "#6b7280",
            # })

            # ----- touchpoint markers (optional) -----
            if include_touches:
                out_markers.extend(_find_touch_markers(plot_df, float(val), start_ts, "VAL", fmt_time))
                out_markers.extend(_find_touch_markers(plot_df, float(vah), start_ts, "VAH", fmt_time))

        logger.info(
            "event=market_profile_lightweight_summary price_lines=%d markers=%d boxes=%d",
            len(out_lines),
            len(out_markers),
            len(out_boxes),
        )

        return {
            "price_lines": out_lines,
            "markers": out_markers,
            "boxes": out_boxes
        }
