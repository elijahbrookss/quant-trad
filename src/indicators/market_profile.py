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
    start_ts = pd.Timestamp(start_ts)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")

    if df.index.tz is None:
        df = df.tz_localize("UTC")
    else:
        df = df.tz_convert("UTC")

    window = df.loc[df.index >= start_ts]
    if window.empty:
        return []

    lows = pd.to_numeric(window.get("low"), errors="coerce")
    highs = pd.to_numeric(window.get("high"), errors="coerce")
    if lows is None or highs is None:
        return []

    touches = (lows <= level) & (highs >= level)
    if not touches.any():
        return []

    touch_index = window.index[touches]
    # Convert tz-aware index to epoch seconds in bulk to avoid per-row allocations
    times = (touch_index.view("int64") // 10**9).astype(int)

    return [
        {
            "time": int(ts),
            "shape": "circle",
            "color": "#6b7280",
            "subtype": "touch",
            "price": float(level),
        }
        for ts in times
    ]

DEFAULT_BREAKOUT_CONFIRMATION_BARS = 3


class MarketProfileIndicator(BaseIndicator):
    _DATAFRAME_CACHE: Dict[Tuple[str, str, str, int, str], Dict[str, Any]] = {}
    """
    Computes daily market profile (TPO) to identify Point of Control (POC),
    Value Area High (VAH), and Value Area Low (VAL), and provides plotting overlays.
    """
    NAME = "market_profile"
    DEFAULT_MIN_MERGE_SESSIONS = 3

    def __init__(
        self,
        df: pd.DataFrame,
        bin_size: Optional[float] = None,
        mode: str = "tpo",
        interval: str = "30m",
        extend_value_area_to_chart_end: bool = True,
        use_merged_value_areas: bool = True,
        merge_threshold: float = 0.6,
        min_merge_sessions: int = DEFAULT_MIN_MERGE_SESSIONS,
        market_profile_breakout_confirmation_bars: int = DEFAULT_BREAKOUT_CONFIRMATION_BARS,
        days_back: Optional[int] = None,
    ):
        super().__init__(df)
        self._bin_size_locked = False
        self.bin_size = self._select_bin_size(df, bin_size)
        self._bin_precision = self._infer_precision_from_step(self.bin_size)
        self.price_precision = max(2, self._bin_precision)
        self.mode = mode
        self.days_back = days_back
        # Compute raw daily profiles on initialization
        self.daily_profiles = self._compute_daily_profiles()
        self.merged_profiles = []
        self.interval = interval
        self.extend_value_area_to_chart_end = bool(extend_value_area_to_chart_end)
        self.use_merged_value_areas = bool(use_merged_value_areas)
        self.merge_threshold = float(merge_threshold) if merge_threshold is not None else 0.6
        self.min_merge_sessions = int(min_merge_sessions)
        self.market_profile_breakout_confirmation_bars = self._normalise_confirmation_bars(
            market_profile_breakout_confirmation_bars
        )

    @staticmethod
    def _normalise_ts(value: Any) -> pd.Timestamp:
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is None:
            stamp = stamp.tz_localize("UTC")
        else:
            stamp = stamp.tz_convert("UTC")
        return stamp

    @classmethod
    def _fetch_with_cache(
        cls,
        provider,
        ctx: DataContext,
        *,
        days_back: Optional[int],
    ) -> pd.DataFrame:
        lookback = max(int(days_back or 0), 0)
        start_ts = cls._normalise_ts(ctx.start)
        if lookback:
            start_ts = start_ts - pd.Timedelta(days=lookback)
        end_ts = cls._normalise_ts(ctx.end)
        cache_key = (
            getattr(provider, "CACHE_KEY", provider.__class__.__name__),
            getattr(provider, "exchange", None) or getattr(provider, "exchange_id", None) or "",
            ctx.symbol or "",
            ctx.interval or "",
            lookback,
        )
        entry = cls._DATAFRAME_CACHE.get(cache_key)
        if entry is not None:
            cached_df: pd.DataFrame = entry["df"]
            cached_start: pd.Timestamp = entry["start"]
            cached_end: pd.Timestamp = entry["end"]
            if start_ts >= cached_start and end_ts <= cached_end:
                window = cached_df.loc[(cached_df.index >= start_ts) & (cached_df.index <= end_ts)]
                return window.copy()

        request_ctx = DataContext(
            symbol=ctx.symbol,
            start=start_ts.isoformat(),
            end=end_ts.isoformat(),
            interval=ctx.interval,
        )
        df = provider.get_ohlcv(request_ctx)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.sort_index()
        cls._DATAFRAME_CACHE[cache_key] = {
            "df": df,
            "start": cls._normalise_ts(df.index.min()),
            "end": cls._normalise_ts(df.index.max()),
        }
        return df.copy()

    @staticmethod
    def _normalise_confirmation_bars(value: Any) -> int:
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            return DEFAULT_BREAKOUT_CONFIRMATION_BARS
        return numeric if numeric >= 1 else 1

    @staticmethod
    def _normalize_step(value: float) -> float:
        if not math.isfinite(value) or value <= 0:
            return 0.1
        exponent = math.floor(math.log10(value))
        mantissa = value / (10 ** exponent)
        if mantissa < 1.5:
            mantissa = 1
        elif mantissa < 3:
            mantissa = 2
        elif mantissa < 7:
            mantissa = 5
        else:
            mantissa = 10
        return mantissa * (10 ** exponent)

    def _select_bin_size(self, df: pd.DataFrame, provided: Optional[float]) -> float:
        """Return a sane bin size, coercing user supplied values before fallback."""

        candidate = provided
        if isinstance(candidate, str):
            candidate = candidate.strip()
            if not candidate:
                candidate = None
        if candidate is not None:
            try:
                numeric = float(candidate)
            except (TypeError, ValueError):
                numeric = None
            if numeric is not None and numeric > 0:
                self._bin_size_locked = True
                return numeric
        self._bin_size_locked = False
        return self._infer_bin_size(df)

    def _infer_bin_size(self, df: pd.DataFrame) -> float:
        highs = pd.to_numeric(df.get("high"), errors="coerce")
        lows = pd.to_numeric(df.get("low"), errors="coerce")
        closes = pd.to_numeric(df.get("close"), errors="coerce")

        highs = highs.dropna()
        lows = lows.dropna()
        closes = closes.dropna()

        if highs.empty or lows.empty:
            return 0.1

        span = float(highs.max() - lows.min())
        if not math.isfinite(span) or span <= 0:
            base_price = float(closes.median()) if not closes.empty else 1.0
            span = max(abs(base_price) * 0.05, 1e-6)

        spreads = (highs - lows).abs()
        spreads = spreads.replace(0, np.nan).dropna()
        characteristic = float(spreads.median()) if not spreads.empty else span / max(len(df), 1)
        characteristic = max(characteristic, span / 50, 1e-8)

        step = max(self._normalize_step(characteristic), 1e-8)

        max_bins = 2000
        if span / step > max_bins:
            step = max(self._normalize_step(span / max_bins), 1e-8)

        return step

    @staticmethod
    def _infer_precision_from_step(step: float) -> int:
        if not math.isfinite(step) or step <= 0:
            return 4
        exponent = math.floor(math.log10(step))
        if exponent >= 0:
            return 2
        return min(8, abs(exponent) + 2)

    def _format_price(self, value: Any) -> str:
        if value is None:
            return "n/a"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "n/a"
        if math.isnan(numeric) or math.isinf(numeric):
            return "n/a"
        return f"{numeric:.{self.price_precision}f}"

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

        def _format_price(value: Any, precision: int) -> str:
            numeric = None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return "n/a"

            if math.isnan(numeric) or math.isinf(numeric):
                return "n/a"
            return f"{numeric:.{precision}f}"

        start_ts = profile.get("start") or profile.get("start_date") or profile.get("date")
        end_ts = profile.get("end") or profile.get("end_date") or start_ts

        val = profile.get("VAL")
        vah = profile.get("VAH")
        poc = profile.get("POC")
        precision = int(profile.get("precision", 4))

        session_count = profile.get("session_count") or profile.get("sessions")
        if not session_count:
            session_count = profile.get("sessionCount")

        extra_bits = []
        if session_count:
            extra_bits.append(f"sessions={session_count}")

        return (
            f"start={_format_ts(start_ts)} | end={_format_ts(end_ts)} | "
            f"VAL={_format_price(val, precision)} | VAH={_format_price(vah, precision)} | "
            f"POC={_format_price(poc, precision)}" + (" | " + ", ".join(extra_bits) if extra_bits else "")
        )

    @classmethod
    def from_context(
        cls,
        provider,
        ctx: DataContext,
        bin_size: Optional[float] = None,
        mode: str = "tpo",
        interval: str = "30m",
        extend_value_area_to_chart_end: bool = True,
        use_merged_value_areas: bool = True,
        merge_threshold: float = 0.6,
        min_merge_sessions: int = DEFAULT_MIN_MERGE_SESSIONS,
        market_profile_breakout_confirmation_bars: int = DEFAULT_BREAKOUT_CONFIRMATION_BARS,
        days_back: Optional[int] = None,
    ):
        """
        Fetches OHLCV from provider and constructs the indicator.
        Raises ValueError if no data is available.
        """
        ctx = DataContext(symbol=ctx.symbol, start=ctx.start, end=ctx.end, interval=interval)
        ctx.validate()

        df = cls._fetch_with_cache(provider, ctx, days_back=days_back)
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
            min_merge_sessions=min_merge_sessions,
            market_profile_breakout_confirmation_bars=market_profile_breakout_confirmation_bars,
            days_back=days_back,
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
                "end_date": group.index.max(),
                "precision": self.price_precision,
            })

            profiles.append(value_area)
            logger.info(
                "Profile for %s: POC=%s, VAH=%s, VAL=%s",
                session_date,
                self._format_price(value_area["POC"]),
                self._format_price(value_area["VAH"]),
                self._format_price(value_area["VAL"]),
            )

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
            low, high = float(row["low"]), float(row["high"])
            if not math.isfinite(low) or not math.isfinite(high):
                continue
            if high < low:
                low, high = high, low

            step = self.bin_size
            if step <= 0:
                continue

            tolerance = abs(step) * 1e-9
            span = max(high - low, 0.0)
            steps = int(math.floor(span / step + 1e-9))

            for idx in range(steps + 1):
                price = low + idx * step
                if price > high + tolerance:
                    break

                scaled = round(price / step)
                bucket = round(scaled * step, self._bin_precision)
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

        poc_price = round(float(poc_price), self.price_precision)
        vah = round(float(max(va_prices)), self.price_precision)
        val = round(float(min(va_prices)), self.price_precision)

        logger.debug(
            "Extracted value area: POC=%s, VAH=%s, VAL=%s, total TPO=%d",
            self._format_price(poc_price),
            self._format_price(vah),
            self._format_price(val),
            total,
        )
        return {"POC": poc_price, "VAH": vah, "VAL": val, "precision": self.price_precision}

    def merge_value_areas(
        self,
        threshold: Optional[float] = None,
        min_merge: Optional[int] = None,
    ) -> List[Dict[str, float]]:
        """
        Combine consecutive daily profiles whose value areas overlap
        at least `threshold` fraction, requiring at least `min_merge` days.
        """
        if threshold is None:
            threshold = getattr(self, "merge_threshold", 0.6)
        threshold = float(threshold)

        if min_merge is None:
            min_merge = getattr(
                self,
                "min_merge_sessions",
                getattr(self, "DEFAULT_MIN_MERGE_SESSIONS", 3),
            )
        else:
            min_merge = int(min_merge)

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
                if avg_poc is not None:
                    avg_poc = round(float(avg_poc), self.price_precision)
                merged_payload = {
                    "start": start_ts,
                    "end": end_ts,
                    "VAL": round(float(merged_val), self.price_precision),
                    "VAH": round(float(merged_vah), self.price_precision),
                    "POC": avg_poc,
                    "session_count": count,
                    "precision": self.price_precision,
                }
                merged.append(merged_payload)
                logger.info(
                    "Merged %d profiles: [%s → %s], VAL=%s, VAH=%s, avg POC=%s",
                    count,
                    start_ts,
                    end_ts,
                    self._format_price(merged_payload["VAL"]),
                    self._format_price(merged_payload["VAH"]),
                    self._format_price(avg_poc),
                )
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
        merge_threshold: Optional[float] = None,
        min_merge: Optional[int] = None,
        extend_to_chart_end: Optional[bool] = None,
    ) -> Tuple[List, Set[Tuple[str, str]]]:
        """
        Emit two kinds of overlay specs:
        • kind="rect" → persistent VAH/VAL zones
        • kind="addplot" → POC horizontal line
        """
        if plot_df is None or plot_df.empty:
            return [], set()

        plot_df = plot_df.copy()
        plot_df.index = pd.to_datetime(plot_df.index, utc=True)

        if use_merged is None:
            use_merged = getattr(self, "use_merged_value_areas", True)

        if merge_threshold is None:
            merge_threshold = getattr(self, "merge_threshold", 0.6)

        if extend_to_chart_end is None:
            extend_to_chart_end = getattr(self, "extend_value_area_to_chart_end", True)
        else:
            extend_to_chart_end = bool(extend_to_chart_end)

        if use_merged:
            default_min_merge = getattr(
                self,
                "min_merge_sessions",
                getattr(self, "DEFAULT_MIN_MERGE_SESSIONS", 3),
            )
            effective_min_merge = default_min_merge if min_merge is None else int(min_merge)
            if not getattr(self, "merged_profiles", None):
                self.merge_value_areas(
                    threshold=merge_threshold,
                    min_merge=effective_min_merge,
                )
            profiles = self.merged_profiles or []
            if not profiles:
                profiles = self.daily_profiles or []
        else:
            profiles = self.daily_profiles or []

        if not profiles:
            logger.warning("No profiles to generate overlays.")
            return [], set()

        overlays: List[Dict[str, Any]] = []
        legend_entries: Set[Tuple[str, str]] = set()
        full_idx = plot_df.index
        chart_start = full_idx.min()
        chart_end = full_idx.max()

        logger.info(
            "event=market_profile_overlay_start profiles=%d use_merged=%s extend=%s",
            len(profiles),
            use_merged,
            extend_to_chart_end,
        )
        poc_series = pd.Series(np.nan, index=full_idx, dtype=float)
        has_poc_values = False

        for idx, prof in enumerate(profiles):
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

            start_ts = pd.to_datetime(start_ts, utc=True)
            end_ts_source = prof.get("end") or prof.get("end_date")
            end_ts = pd.to_datetime(end_ts_source, utc=True) if end_ts_source is not None else chart_end

            if extend_to_chart_end:
                end_ts = chart_end
            else:
                if end_ts > chart_end:
                    end_ts = chart_end
                if end_ts < start_ts:
                    end_ts = start_ts

            if start_ts < chart_start:
                logger.debug("Adjusting rect start from %s to %s (plot start)", start_ts, chart_start)
                start_ts = chart_start

            if start_ts > chart_end:
                logger.debug("Profile %d start %s after chart_end %s. Skipping.", idx, start_ts, chart_end)
                continue

            overlays.append({
                "kind": "rect",
                "start": start_ts,
                "end": end_ts,
                "val": float(val),
                "vah": float(vah),
                "color": "gray",
                "alpha": 0.2,
            })
            legend_entries.add(("Value Area", "gray"))
            logger.debug(
                "event=market_profile_rect start=%s end=%s val=%.4f vah=%.4f",
                start_ts,
                end_ts,
                float(val),
                float(vah),
            )

            if poc is not None:
                mask = full_idx >= start_ts
                if not extend_to_chart_end:
                    mask = mask & (full_idx <= end_ts)
                poc_series.loc[mask] = float(poc)
                has_poc_values = True
                logger.debug(
                    "event=market_profile_poc start=%s end=%s poc=%.4f",
                    start_ts,
                    end_ts,
                    float(poc),
                )

        if has_poc_values and poc_series.notna().any():
            ap = make_addplot(poc_series, color="orange", width=1.0, linestyle="--")
            overlays.append({"kind": "addplot", "plot": ap})
            legend_entries.add(("POC", "orange"))

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
        min_merge: Optional[int] = None,
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
                default_min_merge = getattr(
                    self,
                    "min_merge_sessions",
                    getattr(self, "DEFAULT_MIN_MERGE_SESSIONS", 3),
                )
                effective_min_merge = default_min_merge if min_merge is None else int(min_merge)
                self.merge_value_areas(
                    threshold=merge_threshold,
                    min_merge=effective_min_merge,
                )
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
                "precision": prof.get("precision", self.price_precision),
                "extend": bool(extend_boxes_to_chart_end),
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
