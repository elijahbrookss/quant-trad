"""
Market Profile Indicator - Pure Computation.

This module computes TPO-based market profiles and returns structured domain objects.
No dependencies on visualization libraries, signal decorators, or UI concerns.
"""

import logging
import os
import threading
import time
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd

from core.logger import logger
from indicators.base import ComputeIndicator
from indicators.config import DataContext
from utils.log_context import build_log_context, with_log_context
from utils.perf_log import get_obs_enabled, get_obs_step_sample_rate, should_sample

from .models import Profile, ValueArea
from ..params import DEFAULT_DAYS_BACK, DEFAULT_MIN_MERGE_SESSIONS, DEFAULT_PARAMS
from .internal.computation import build_tpo_histogram, extract_value_area
from .internal.bin_size import select_bin_size, infer_precision_from_step
from .internal.merging import merge_profiles

if TYPE_CHECKING:
    from indicators.runtime.incremental_cache import IncrementalCache


def _to_utc_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


class MarketProfileIndicator(ComputeIndicator):
    """
    Computes daily market profiles using Time Price Opportunity (TPO) methodology.

    Returns structured Profile objects containing value areas (VAH, VAL, POC).
    Pure computation - no knowledge of plotting, signals, or UI concerns.

    Example:
        >>> indicator = MarketProfileIndicator(df, bin_size=0.25)
        >>> profiles = indicator.get_profiles()
        >>> for profile in profiles:
        ...     print(f"VAH: {profile.vah}, VAL: {profile.val}, POC: {profile.poc}")
    """

    NAME = "market_profile"
    DEFAULT_MIN_MERGE_SESSIONS = DEFAULT_MIN_MERGE_SESSIONS
    DEFAULT_DAYS_BACK = DEFAULT_DAYS_BACK

    # Define required params with defaults (used during creation only)
    # These params MUST be present in stored indicator records
    REQUIRED_PARAMS = {
        "use_merged_value_areas": True,
        "merge_threshold": 0.6,
        "min_merge_sessions": DEFAULT_MIN_MERGE_SESSIONS,
        "extend_value_area_to_chart_end": True,
        "days_back": DEFAULT_DAYS_BACK,
    }
    DEFAULT_PARAMS = DEFAULT_PARAMS
    RUNTIME_INPUT_SPECS = [
        {
            "source_timeframe": "30m",
            "lookback_days_param": "days_back",
            "session_scope": "global",
            "alignment": "closed_bar_only",
            "normalization": "project_to_strategy_timeframe",
            "incremental_eval": True,
        }
    ]

    def __init__(
        self,
        df: pd.DataFrame,
        bin_size: Optional[float] = None,
        use_merged_value_areas: bool = True,
        merge_threshold: float = 0.6,
        min_merge_sessions: int = DEFAULT_MIN_MERGE_SESSIONS,
        extend_value_area_to_chart_end: bool = True,
        days_back: int = DEFAULT_DAYS_BACK,
        symbol: Optional[str] = None,
        bot_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
    ):
        """
        Initialize Market Profile indicator.

        Args:
            df: DataFrame with OHLC data indexed by timestamp
            bin_size: Price bucket size (auto-inferred if not provided)
            use_merged_value_areas: Whether to merge overlapping value areas (default: True)
            merge_threshold: Overlap threshold for merging (default: 0.6)
            min_merge_sessions: Minimum sessions required for merge (default: 3)
            extend_value_area_to_chart_end: Extend value area boxes to chart end (default: True)
            days_back: Number of days of historical data to use (default: 180)
            symbol: Optional symbol for logging context
            bot_id: Optional bot identifier for logging context
            strategy_id: Optional strategy identifier for logging context
        """
        super().__init__(df)

        # Determine bin size
        self.bin_size, self._bin_size_locked = select_bin_size(df, bin_size)
        self._bin_precision = infer_precision_from_step(self.bin_size)
        self.price_precision = max(2, self._bin_precision)

        # Store configuration parameters
        self.use_merged_value_areas = use_merged_value_areas
        self.merge_threshold = merge_threshold
        self.min_merge_sessions = min_merge_sessions
        self.extend_value_area_to_chart_end = extend_value_area_to_chart_end
        self.days_back = days_back

        # Store context for logging
        self.symbol = symbol
        self.bot_id = bot_id
        self.strategy_id = strategy_id

        # Compute profiles on initialization
        self._profiles = self._compute_daily_profiles()
        # Cache normalized profile timelines keyed by chart timeframe + merge policy.
        self._normalized_runtime_profiles_cache: Dict[tuple[str, bool, float, int], List[Profile]] = {}

    @classmethod
    def from_context(
        cls,
        provider,
        ctx: DataContext,
        bin_size: Optional[float] = None,
        use_merged_value_areas: bool = True,
        merge_threshold: float = 0.6,
        min_merge_sessions: int = DEFAULT_MIN_MERGE_SESSIONS,
        extend_value_area_to_chart_end: bool = True,
        days_back: int = DEFAULT_DAYS_BACK,
        **kwargs
    ):
        """
        Instantiate from a DataContext and data provider.

        Market Profile always fetches data on a 30-minute interval for the specified
        days_back period, regardless of the chart's timeframe or date range.

        Args:
            provider: Data provider with get_ohlcv method
            ctx: DataContext with symbol, start, end, interval (interval and start are overridden)
            bin_size: Price bucket size (auto-inferred if not provided)
            use_merged_value_areas: Whether to merge overlapping value areas (default: True)
            merge_threshold: Overlap threshold for merging (default: 0.6)
            min_merge_sessions: Minimum sessions required for merge (default: 3)
            extend_value_area_to_chart_end: Extend value area boxes to chart end (default: True)
            days_back: Number of days of historical data to fetch (default: 180)
            **kwargs: Additional parameters (bot_id, strategy_id)

        Returns:
            MarketProfileIndicator instance

        Raises:
            ValueError: If no OHLCV data is returned
        """
        from datetime import timedelta

        # Normalize the request window once at the indicator boundary so provider
        # data and computed profiles stay on a single UTC timeline.
        end_date = ctx.end_utc()
        start_date = end_date - timedelta(days=days_back)

        mp_ctx = DataContext(
            symbol=ctx.symbol,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            interval="30m",  # Always use 30min for Market Profile
            instrument_id=ctx.instrument_id,
        )

        logger.info(
            "Fetching Market Profile data: symbol=%s, days_back=%d, start=%s, end=%s, interval=30m",
            ctx.symbol,
            days_back,
            start_date.isoformat(),
            end_date.isoformat(),
        )

        df = provider.get_ohlcv(mp_ctx)
        if df is None or df.empty:
            raise ValueError(
                f"Missing OHLCV for {ctx.symbol} from {start_date.isoformat()} to {end_date.isoformat()}"
            )

        # Log the actual data range obtained
        actual_start = _to_utc_timestamp(df.index.min())
        actual_end = _to_utc_timestamp(df.index.max())

        logger.info(
            "✓ Market Profile instantiated | symbol=%s | requested_start=%s | actual_start=%s | end=%s | rows=%d | interval=30m",
            ctx.symbol,
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            actual_start.strftime("%Y-%m-%d %H:%M:%S"),
            actual_end.strftime("%Y-%m-%d %H:%M:%S"),
            len(df),
        )

        if actual_start > start_date:
            logger.warning(
                "⚠ Market Profile: Requested data from %s but oldest available is %s (symbol=%s). "
                "Historical data limited by provider.",
                start_date.strftime("%Y-%m-%d"),
                actual_start.strftime("%Y-%m-%d"),
                ctx.symbol,
            )

        return cls(
            df=df,
            bin_size=bin_size,
            use_merged_value_areas=use_merged_value_areas,
            merge_threshold=merge_threshold,
            min_merge_sessions=min_merge_sessions,
            extend_value_area_to_chart_end=extend_value_area_to_chart_end,
            days_back=days_back,
            symbol=ctx.symbol,
            bot_id=kwargs.get("bot_id"),
            strategy_id=kwargs.get("strategy_id"),
        )

    @classmethod
    def build_runtime_indicator(
        cls,
        *,
        indicator_id: str,
        meta: Mapping[str, Any],
        strategy_indicator_metas: Mapping[str, Mapping[str, Any]],
    ):
        from indicators.market_profile.runtime.typed_indicator import TypedMarketProfileIndicator

        raw_params = meta.get("params")
        resolved = dict(cls.DEFAULT_PARAMS)
        if isinstance(raw_params, Mapping):
            resolved.update(dict(raw_params))
        return TypedMarketProfileIndicator(
            indicator_id=indicator_id,
            version=str(meta.get("version") or "v1"),
            params=resolved,
        )

    @classmethod
    def from_context_with_incremental_cache(
        cls,
        provider,
        ctx: DataContext,
        cache: "IncrementalCache",
        inst_id: str,
        bin_size: Optional[float] = None,
        use_merged_value_areas: bool = True,
        merge_threshold: float = 0.6,
        min_merge_sessions: int = DEFAULT_MIN_MERGE_SESSIONS,
        extend_value_area_to_chart_end: bool = True,
        days_back: int = DEFAULT_DAYS_BACK,
        **kwargs
    ):
        """
        Instantiate from a DataContext with incremental caching.

        This method checks the cache for previously computed daily profiles
        and only fetches/computes data for missing days, enabling efficient
        incremental updates.

        Args:
            provider: Data provider with get_ohlcv method
            ctx: DataContext with symbol, start, end, interval
            cache: IncrementalCache instance for storing/retrieving daily profiles
            inst_id: Indicator instance ID for cache key
            bin_size: Price bucket size (auto-inferred if not provided)
            use_merged_value_areas: Whether to merge overlapping value areas
            merge_threshold: Overlap threshold for merging
            min_merge_sessions: Minimum sessions required for merge
            extend_value_area_to_chart_end: Extend value area boxes to chart end
            days_back: Number of days of historical data to use
            **kwargs: Additional parameters (bot_id, strategy_id, etc.)

        Returns:
            MarketProfileIndicator instance with profiles from cache + fresh computation
        """
        from datetime import timedelta

        end_date = ctx.end_utc()
        start_date = end_date - timedelta(days=days_back)

        # Build date keys for cache lookup
        date_keys = []
        current = start_date
        while current <= end_date:
            date_keys.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        # NOTE: Incremental cache is per-process; key=(inst_id, symbol, date).
        # NOTE: No eviction beyond max_entries; multiprocessing/container-per-bot will duplicate work.
        should_log = get_obs_enabled() and should_sample(get_obs_step_sample_rate())
        get_started = time.perf_counter() if should_log else 0.0
        # Check cache for existing daily profiles
        cached_profiles_dict = cache.get_range(inst_id, ctx.symbol, date_keys)
        if should_log:
            get_ms = (time.perf_counter() - get_started) * 1000.0
            cache_key_summary = f"{ctx.symbol}:{days_back}d"
            base_context = build_log_context(
                cache_name="incremental_profile_cache",
                cache_scope="process",
                cache_key_summary=cache_key_summary,
                time_taken_ms=get_ms,
                pid=os.getpid(),
                thread_name=threading.current_thread().name,
                symbol=ctx.symbol,
                timeframe=ctx.interval,
                indicator_id=inst_id,
            )
            logger.debug(
                with_log_context(
                    "cache.get",
                    build_log_context(event="cache.get", **base_context),
                )
            )
            if cached_profiles_dict:
                logger.debug(
                    with_log_context(
                        "cache.hit",
                        build_log_context(event="cache.hit", **base_context),
                    )
                )
            if len(cached_profiles_dict) < len(date_keys):
                logger.debug(
                    with_log_context(
                        "cache.miss",
                        build_log_context(event="cache.miss", **base_context),
                    )
                )

        logger.info(
            "Incremental cache check | inst_id=%s symbol=%s | cached_days=%d | total_days=%d",
            inst_id,
            ctx.symbol,
            len(cached_profiles_dict),
            len(date_keys),
        )

        # If we have all profiles cached, we can skip data fetching entirely
        if len(cached_profiles_dict) >= len(date_keys):
            logger.info(
                "Using fully cached profiles | inst_id=%s symbol=%s | profiles=%d",
                inst_id,
                ctx.symbol,
                len(cached_profiles_dict),
            )
            # Create a minimal instance with cached profiles
            empty_df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
            empty_df.index = pd.DatetimeIndex([])

            instance = cls.__new__(cls)
            instance.bin_size = bin_size or 0.01
            instance._bin_size_locked = bin_size is not None
            instance._bin_precision = 2
            instance.price_precision = 2
            instance.use_merged_value_areas = use_merged_value_areas
            instance.merge_threshold = merge_threshold
            instance.min_merge_sessions = min_merge_sessions
            instance.extend_value_area_to_chart_end = extend_value_area_to_chart_end
            instance.days_back = days_back
            instance.symbol = ctx.symbol
            instance.bot_id = kwargs.get("bot_id")
            instance.strategy_id = kwargs.get("strategy_id")
            instance.df = empty_df
            instance._profiles = sorted(cached_profiles_dict.values(), key=lambda p: p.start)
            instance._normalized_runtime_profiles_cache = {}

            return instance

        # Need to fetch data and compute profiles
        mp_ctx = DataContext(
            symbol=ctx.symbol,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            interval="30m",
            instrument_id=ctx.instrument_id,
        )

        logger.info(
            "Fetching Market Profile data with cache | symbol=%s days_back=%d cached_days=%d",
            ctx.symbol,
            days_back,
            len(cached_profiles_dict),
        )

        df = provider.get_ohlcv(mp_ctx)
        if df is None or df.empty:
            raise ValueError(
                f"Missing OHLCV for {ctx.symbol} from {start_date.isoformat()} to {end_date.isoformat()}"
            )

        # Create instance normally
        instance = cls(
            df=df,
            bin_size=bin_size,
            use_merged_value_areas=use_merged_value_areas,
            merge_threshold=merge_threshold,
            min_merge_sessions=min_merge_sessions,
            extend_value_area_to_chart_end=extend_value_area_to_chart_end,
            days_back=days_back,
            symbol=ctx.symbol,
            bot_id=kwargs.get("bot_id"),
            strategy_id=kwargs.get("strategy_id"),
        )

        # Cache the newly computed profiles by date
        set_started = time.perf_counter() if should_log else 0.0
        for profile in instance._profiles:
            date_key = profile.start.strftime("%Y-%m-%d")
            cache.set(inst_id, ctx.symbol, date_key, profile)
        if should_log:
            set_ms = (time.perf_counter() - set_started) * 1000.0
            cache_key_summary = f"{ctx.symbol}:{days_back}d"
            set_context = build_log_context(
                cache_name="incremental_profile_cache",
                cache_scope="process",
                cache_key_summary=cache_key_summary,
                time_taken_ms=set_ms,
                pid=os.getpid(),
                thread_name=threading.current_thread().name,
                symbol=ctx.symbol,
                timeframe=ctx.interval,
                indicator_id=inst_id,
            )
            logger.debug(
                with_log_context(
                    "cache.set",
                    build_log_context(event="cache.set", **set_context),
                )
            )

        logger.info(
            "Cached %d daily profiles | inst_id=%s symbol=%s",
            len(instance._profiles),
            inst_id,
            ctx.symbol,
        )

        return instance

    def get_profiles(self) -> List[Profile]:
        """
        Get all computed daily profiles.

        Returns:
            List of Profile objects, one per trading session
        """
        return self._profiles

    def get_merged_profiles(
        self,
        threshold: float = 0.6,
        min_sessions: int = DEFAULT_MIN_MERGE_SESSIONS
    ) -> List[Profile]:
        """
        Get merged profiles where consecutive sessions have overlapping value areas.

        Args:
            threshold: Minimum overlap ratio (0.0 to 1.0) to merge profiles
            min_sessions: Minimum number of sessions required for a merged profile

        Returns:
            List of merged Profile objects
        """
        return merge_profiles(
            self._profiles,
            threshold,
            min_sessions,
            bot_id=self.bot_id,
            symbol=self.symbol,
            strategy_id=self.strategy_id,
        )

    def clone_for_overlay(self, **override_params) -> "MarketProfileIndicator":
        """
        Create a shallow copy for overlay generation, preserving computed profiles.

        This method creates a new instance without recomputing profiles, which is
        critical for overlay generation. The profiles are computed from 30m data
        and should be reused regardless of the chart's timeframe.

        Args:
            **override_params: Optional parameters to override during cloning
                - bin_size: Override bin size
                - use_merged_value_areas: Override merge setting
                - merge_threshold: Override merge threshold
                - min_merge_sessions: Override minimum sessions
                - extend_value_area_to_chart_end: Override extension setting

        Returns:
            New MarketProfileIndicator instance sharing the same profiles
        """
        # Create new instance without calling __init__ (avoids recomputation)
        clone = MarketProfileIndicator.__new__(MarketProfileIndicator)

        # Copy all attributes, allowing overrides
        clone.bin_size = override_params.get("bin_size", self.bin_size)
        clone._bin_size_locked = self._bin_size_locked
        clone._bin_precision = self._bin_precision
        clone.price_precision = self.price_precision
        clone.use_merged_value_areas = override_params.get(
            "use_merged_value_areas", self.use_merged_value_areas
        )
        clone.merge_threshold = override_params.get("merge_threshold", self.merge_threshold)
        clone.min_merge_sessions = override_params.get(
            "min_merge_sessions", self.min_merge_sessions
        )
        clone.extend_value_area_to_chart_end = override_params.get(
            "extend_value_area_to_chart_end", self.extend_value_area_to_chart_end
        )
        clone.days_back = self.days_back

        # Copy context attributes
        clone.symbol = self.symbol
        clone.bot_id = self.bot_id
        clone.strategy_id = self.strategy_id

        # Reuse computed profiles (no recomputation!)
        clone._profiles = self._profiles
        clone.df = self.df  # Keep original 30m data

        logger.debug(
            "Cloned Market Profile for overlay: profiles=%d | use_merged=%s | extend_to_end=%s",
            len(self._profiles),
            clone.use_merged_value_areas,
            clone.extend_value_area_to_chart_end,
        )

        return clone

    def to_lightweight(
        self,
        plot_df: pd.DataFrame,
        **kwargs
    ) -> dict:
        """
        Generate TradingView Lightweight Charts overlay data for market profiles.

        Args:
            plot_df: DataFrame containing the chart data (for time alignment)
            **kwargs: Additional parameters (ignored for compatibility)

        Returns:
            Dictionary with price_lines, markers, and boxes for rendering
        """
        from typing import Any, Dict

        if plot_df is None or plot_df.empty:
            logger.warning("Market Profile to_lightweight: plot_df is None or empty")
            return {"price_lines": [], "markers": [], "boxes": []}

        # Select profiles to render based on instance configuration
        profiles = (
            self.get_merged_profiles(self.merge_threshold, self.min_merge_sessions)
            if self.use_merged_value_areas
            else self._profiles
        )
        raw_profiles = self._profiles

        logger.info(
            "Market Profile to_lightweight: total_profiles=%d | use_merged=%s | plot_df_rows=%d | plot_df_range=%s to %s",
            len(profiles),
            self.use_merged_value_areas,
            len(plot_df),
            plot_df.index.min(),
            plot_df.index.max(),
        )

        boxes: List[Dict[str, Any]] = []
        profiles_payload: List[Dict[str, Any]] = []

        # Get chart boundaries for visual clamping
        chart_start = _to_utc_timestamp(plot_df.index.min())
        chart_end = _to_utc_timestamp(plot_df.index.max())

        for idx, profile in enumerate(profiles):
            # Clamp box start to chart start (visual only - don't skip profiles)
            profile_start = _to_utc_timestamp(profile.start)
            box_start = max(profile_start, chart_start)

            # Extend to chart end if configured, otherwise use profile end
            if self.extend_value_area_to_chart_end:
                box_end = chart_end
            else:
                box_end = _to_utc_timestamp(profile.end)

            # Convert timestamps to Unix seconds
            x1 = int(pd.Timestamp(box_start).timestamp())
            x2 = int(pd.Timestamp(box_end).timestamp())

            # Value Area box
            boxes.append({
                "x1": x1,
                "x2": x2,
                "y1": float(profile.val),
                "y2": float(profile.vah),
                "fillColor": "rgba(59, 130, 246, 0.1)",  # Blue with transparency
                "borderColor": "#3b82f6",
                "borderWidth": 1,
                "borderStyle": 2,  # Dashed
            })

            if idx < 3:  # Log first 3 boxes for debugging
                logger.debug(
                    "Market Profile box[%d]: profile_range=%s to %s | box_range=%s to %s | x1=%d x2=%d | VAL=%.2f VAH=%.2f",
                    idx,
                    profile.start,
                    profile.end,
                    box_start,
                    box_end,
                    x1,
                    x2,
                    profile.val,
                    profile.vah,
                )

        for profile in raw_profiles:
            profiles_payload.append(
                {
                    "start": int(_to_utc_timestamp(profile.start).timestamp()),
                    "end": int(_to_utc_timestamp(profile.end).timestamp()),
                    "VAH": float(profile.vah),
                    "VAL": float(profile.val),
                    "POC": float(profile.poc),
                    "session_count": int(getattr(profile, "session_count", 1) or 1),
                    "precision": int(getattr(profile, "precision", 4) or 4),
                }
            )

        logger.info(
            "Market Profile overlays: %d boxes rendered | chart_range=%s to %s",
            len(boxes),
            pd.Timestamp(chart_start).strftime("%Y-%m-%d"),
            pd.Timestamp(chart_end).strftime("%Y-%m-%d"),
        )

        return {
            "type": "market-profile",
            "price_lines": [],
            "markers": [],
            "boxes": boxes,
            "profiles": profiles_payload,
            "profile_params": {
                "use_merged_value_areas": bool(self.use_merged_value_areas),
                "merge_threshold": float(self.merge_threshold),
                "min_merge_sessions": int(self.min_merge_sessions),
                "extend_value_area_to_chart_end": bool(self.extend_value_area_to_chart_end),
            },
            "bot_id": self.bot_id,
            "symbol": self.symbol,
            "strategy_id": self.strategy_id,
        }

    def build_runtime_signal_payload(
        self,
        *,
        indicator_id: Optional[str] = None,
        params: Optional[Mapping[str, Any]] = None,
        symbol: Optional[str] = None,
        color: Optional[str] = None,
        chart_timeframe: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return canonical runtime signal payload from already-computed profiles.

        This allows signal execution to reuse the indicator instance/cache state
        without replaying indicator computation per bar.
        """
        profile_params = dict(params or {})
        profile_params.setdefault("use_merged_value_areas", bool(self.use_merged_value_areas))
        profile_params.setdefault("merge_threshold", float(self.merge_threshold))
        profile_params.setdefault("min_merge_sessions", int(self.min_merge_sessions))
        profile_params.setdefault(
            "extend_value_area_to_chart_end", bool(self.extend_value_area_to_chart_end)
        )
        profile_params.setdefault("days_back", int(self.days_back))
        use_merged = bool(profile_params.get("use_merged_value_areas", self.use_merged_value_areas))
        merge_threshold = float(profile_params.get("merge_threshold", self.merge_threshold))
        min_merge_sessions = int(profile_params.get("min_merge_sessions", self.min_merge_sessions))
        resolved_chart_timeframe = str(chart_timeframe or "").strip().lower() or "30m"
        # Signals run on chart timeframe bars, but profile boundaries must stay in
        # source/session time semantics so overlays and runtime decisions share
        # the same merged profile identity.
        runtime_profiles = (
            self.get_merged_profiles(merge_threshold, min_merge_sessions)
            if use_merged
            else self.get_profiles()
        )
        profile_params["profiles_premerged"] = bool(use_merged)
        payload_profiles: List[Dict[str, Any]] = []
        for profile in runtime_profiles:
            start_ts = _to_utc_timestamp(profile.start)
            end_ts = _to_utc_timestamp(profile.end)
            payload_profiles.append(
                {
                    "start": int(start_ts.timestamp()),
                    "end": int(end_ts.timestamp()),
                    "VAH": float(profile.vah),
                    "VAL": float(profile.val),
                    "POC": float(profile.poc),
                    "session_count": int(getattr(profile, "session_count", 1) or 1),
                    "precision": int(getattr(profile, "precision", self.price_precision) or self.price_precision),
                    "formed_at": int(end_ts.timestamp()),
                    "known_at": int(end_ts.timestamp()),
                }
            )
        return {
            "_indicator_id": str(indicator_id or ""),
            "symbol": str(symbol or self.symbol or ""),
            "profiles": payload_profiles,
            "profile_params": profile_params,
            "profile_chart_timeframe": resolved_chart_timeframe,
            "profile_source_timeframe": "30m",
            "profile_boundary_semantics": "source_session",
            "overlay_color": str(color).strip() if isinstance(color, str) and color.strip() else None,
        }

    def _runtime_profiles_for_chart_timeframe(
        self,
        *,
        chart_timeframe: str,
        use_merged: bool,
        merge_threshold: float,
        min_merge_sessions: int,
    ) -> List[Profile]:
        key = (
            str(chart_timeframe or "").strip().lower() or "30m",
            bool(use_merged),
            float(merge_threshold),
            int(min_merge_sessions),
        )
        cached = self._normalized_runtime_profiles_cache.get(key)
        if cached is not None:
            logger.debug(
                "Market Profile runtime profile cache hit | symbol=%s chart_timeframe=%s use_merged=%s profiles=%d",
                self.symbol,
                key[0],
                key[1],
                len(cached),
            )
            return cached

        base_profiles = (
            self.get_merged_profiles(merge_threshold, min_merge_sessions)
            if use_merged
            else self.get_profiles()
        )
        normalized = self._normalize_profiles_to_chart_timeframe(base_profiles, key[0])
        self._normalized_runtime_profiles_cache[key] = normalized
        logger.info(
            "Market Profile runtime profile cache miss | symbol=%s chart_timeframe=%s use_merged=%s source_profiles=%d normalized_profiles=%d",
            self.symbol,
            key[0],
            key[1],
            len(base_profiles),
            len(normalized),
        )
        return normalized

    def _normalize_profiles_to_chart_timeframe(
        self,
        profiles: List[Profile],
        chart_timeframe: str,
    ) -> List[Profile]:
        tf = str(chart_timeframe or "").strip().lower() or "30m"
        if tf in {"30m", "30min"}:
            return list(profiles)
        normalized: List[Profile] = []
        for profile in profiles:
            start = _to_utc_timestamp(profile.start)
            end = _to_utc_timestamp(profile.end)
            try:
                norm_start = start.floor(tf)
            except Exception:
                norm_start = start
            try:
                norm_end = end.ceil(tf)
            except Exception:
                norm_end = end
            if norm_end < norm_start:
                norm_end = norm_start
            normalized.append(
                Profile(
                    start=norm_start,
                    end=norm_end,
                    value_area=ValueArea(
                        vah=float(profile.vah),
                        val=float(profile.val),
                        poc=float(profile.poc),
                    ),
                    session_count=int(getattr(profile, "session_count", 1) or 1),
                    tpo_histogram=getattr(profile, "tpo_histogram", None),
                    precision=int(getattr(profile, "precision", self.price_precision) or self.price_precision),
                )
            )
        return normalized

    def _compute_daily_profiles(self) -> List[Profile]:
        """
        Build daily TPO profiles: POC, VAH, VAL, plus session timestamps.

        Returns:
            List of Profile objects
        """
        df = self.df.copy()
        df.index = pd.to_datetime(df.index, utc=True)
        profiles = []

        logger.info("Starting daily profile computation for %d sessions", len(np.unique(df.index.date)))

        # Group rows by calendar date
        grouped = df.groupby(df.index.date)
        for session_date, group in grouped:
            logger.debug("Processing session: %s, bars: %d", session_date, len(group))

            # Build TPO histogram
            tpo_hist = build_tpo_histogram(group, self.bin_size, self._bin_precision)

            # Extract value area
            value_area = extract_value_area(tpo_hist, self.price_precision)

            if value_area is None:
                logger.warning("Skipping session %s: empty TPO histogram", session_date)
                continue

            # Create Profile object
            profile = Profile(
                start=group.index.min(),
                end=group.index.max(),
                value_area=value_area,
                session_count=1,
                tpo_histogram=tpo_hist,
                precision=self.price_precision,
            )

            profiles.append(profile)
            logger.info(
                "Profile for %s: POC=%.{prec}f, VAH=%.{prec}f, VAL=%.{prec}f".replace("{prec}", str(self.price_precision)),
                session_date,
                profile.poc,
                profile.vah,
                profile.val,
            )

        logger.info("Completed daily profile computation. Total profiles: %d", len(profiles))
        return profiles
