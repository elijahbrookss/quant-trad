"""
Market Profile Indicator - Pure Computation.

This module computes TPO-based market profiles and returns structured domain objects.
No dependencies on visualization libraries, signal decorators, or UI concerns.
"""

import logging
from typing import List, Optional

import numpy as np
import pandas as pd

from core.logger import logger
from indicators.base import ComputeIndicator
from indicators.config import DataContext
from indicators.registry import indicator
from indicators.runtime.overlay_cache_registry import overlay_cacheable

from .domain import Profile, ValueArea
from ._internal.computation import build_tpo_histogram, extract_value_area
from ._internal.bin_size import select_bin_size, infer_precision_from_step
from ._internal.merging import merge_profiles


@overlay_cacheable("market_profile")
@indicator(name="market_profile", inputs=["ohlc"], outputs=["profiles"])
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
    DEFAULT_MIN_MERGE_SESSIONS = 3
    DEFAULT_DAYS_BACK = 180

    # Define required params with defaults (used during creation only)
    # These params MUST be present in stored indicator records
    REQUIRED_PARAMS = {
        "use_merged_value_areas": True,
        "merge_threshold": 0.6,
        "min_merge_sessions": DEFAULT_MIN_MERGE_SESSIONS,
        "extend_value_area_to_chart_end": True,
        "days_back": DEFAULT_DAYS_BACK,
    }

    def __init__(
        self,
        df: pd.DataFrame,
        bin_size: Optional[float] = None,
        use_merged_value_areas: bool = True,
        merge_threshold: float = 0.6,
        min_merge_sessions: int = DEFAULT_MIN_MERGE_SESSIONS,
        extend_value_area_to_chart_end: bool = True,
        days_back: int = DEFAULT_DAYS_BACK,
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

        # Compute profiles on initialization
        self._profiles = self._compute_daily_profiles()

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
        **kwargs  # Accept and ignore extra kwargs like 'mode', 'interval' for compatibility
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
            **kwargs: Additional parameters (ignored for compatibility)

        Returns:
            MarketProfileIndicator instance

        Raises:
            ValueError: If no OHLCV data is returned
        """
        from datetime import timedelta

        # Override DataContext: use chart's end date, but calculate start from days_back
        # Always use 30m interval for Market Profile computation
        end_date = pd.Timestamp(ctx.end)
        start_date = end_date - timedelta(days=days_back)

        mp_ctx = DataContext(
            symbol=ctx.symbol,
            start=start_date.isoformat(),
            end=ctx.end,
            interval="30m",  # Always use 30min for Market Profile
        )

        logger.info(
            "Fetching Market Profile data: symbol=%s, days_back=%d, start=%s, end=%s, interval=30m",
            ctx.symbol,
            days_back,
            start_date.isoformat(),
            ctx.end,
        )

        df = provider.get_ohlcv(mp_ctx)
        if df is None or df.empty:
            raise ValueError(
                f"Missing OHLCV for {ctx.symbol} from {start_date.isoformat()} to {ctx.end}"
            )

        # Log the actual data range obtained
        actual_start = df.index.min()
        actual_end = df.index.max()

        logger.info(
            "✓ Market Profile instantiated | symbol=%s | requested_start=%s | actual_start=%s | end=%s | rows=%d | interval=30m",
            ctx.symbol,
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            pd.Timestamp(actual_start).strftime("%Y-%m-%d %H:%M:%S"),
            pd.Timestamp(actual_end).strftime("%Y-%m-%d %H:%M:%S"),
            len(df),
        )

        if pd.Timestamp(actual_start) > start_date:
            logger.warning(
                "⚠ Market Profile: Requested data from %s but oldest available is %s (symbol=%s). "
                "Historical data limited by provider.",
                start_date.strftime("%Y-%m-%d"),
                pd.Timestamp(actual_start).strftime("%Y-%m-%d"),
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
        )

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
        return merge_profiles(self._profiles, threshold, min_sessions)

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
        chart_start = pd.Timestamp(plot_df.index.min()).tz_localize(None) if plot_df.index.min().tz is None else pd.Timestamp(plot_df.index.min())
        chart_end = pd.Timestamp(plot_df.index.max()).tz_localize(None) if plot_df.index.max().tz is None else pd.Timestamp(plot_df.index.max())

        for idx, profile in enumerate(profiles):
            # Clamp box start to chart start (visual only - don't skip profiles)
            profile_start = pd.Timestamp(profile.start).tz_localize(None) if pd.Timestamp(profile.start).tz is None else pd.Timestamp(profile.start)
            box_start = max(profile_start, chart_start)

            # Extend to chart end if configured, otherwise use profile end
            if self.extend_value_area_to_chart_end:
                box_end = chart_end
            else:
                box_end = profile.end

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
                    "start": int(pd.Timestamp(profile.start).timestamp()),
                    "end": int(pd.Timestamp(profile.end).timestamp()),
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
        }

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

    # ========================================================================
    # LEGACY COMPATIBILITY LAYER
    # These methods provide backwards compatibility with old dict-based API
    # TODO: Remove once all consumers are updated to use Profile objects
    # ========================================================================

    @property
    def daily_profiles(self) -> List[dict]:
        """
        Legacy property returning profiles as dictionaries.

        DEPRECATED: Use get_profiles() instead which returns Profile objects.
        """
        return [p.to_dict() for p in self._profiles]

    @property
    def merged_profiles(self) -> List[dict]:
        """
        Legacy property returning merged profiles as dictionaries.

        DEPRECATED: Use get_merged_profiles() instead which returns Profile objects.
        """
        # Cache merged results
        if not hasattr(self, '_cached_merged'):
            self._cached_merged = self.get_merged_profiles()
        return [p.to_dict() for p in self._cached_merged]

    # -----------------------------------------------------------------------
    # Legacy compatibility
    # -----------------------------------------------------------------------
    def merge_value_areas(self, threshold: float = 0.6, min_merge: Optional[int] = None):
        """Backward-compatible wrapper that returns merged profiles as dicts."""

        min_sessions = self.min_merge_sessions if min_merge is None else min_merge
        merged_profiles = self.get_merged_profiles(threshold, min_sessions)
        return [profile.to_dict() for profile in merged_profiles]
