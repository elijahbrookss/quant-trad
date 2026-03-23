"""
Market Profile Indicator - Pure Computation.

This module computes TPO-based market profiles and returns structured domain objects.
No dependencies on visualization libraries, signal decorators, or UI concerns.
"""

import logging
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd

from core.logger import logger
from data_providers.utils.ohlcv import interval_to_timedelta
from indicators.base import ComputeIndicator

from .models import Profile, ValueArea
from ..manifest import (
    DEFAULT_DAYS_BACK,
    DEFAULT_MIN_MERGE_SESSIONS,
)
from .internal.computation import build_tpo_histogram, extract_value_area
from .internal.bin_size import select_bin_size, infer_precision_from_step
from .internal.merging import merge_profiles


def _to_utc_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _ensure_utc_index(index: Any) -> pd.DatetimeIndex:
    if not isinstance(index, pd.DatetimeIndex):
        return pd.to_datetime(index, utc=True)
    if index.tz is None:
        return index.tz_localize("UTC")
    if str(index.tz) != "UTC":
        return index.tz_convert("UTC")
    return index


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
            end_ts = _to_utc_timestamp(profile.end)
            profiles_payload.append(
                {
                    "start": int(_to_utc_timestamp(profile.start).timestamp()),
                    "end": int(end_ts.timestamp()),
                    "VAH": float(profile.vah),
                    "VAL": float(profile.val),
                    "POC": float(profile.poc),
                    "session_count": int(getattr(profile, "session_count", 1) or 1),
                    "precision": int(getattr(profile, "precision", 4) or 4),
                    "formed_at": int(end_ts.timestamp()),
                    "known_at": int(end_ts.timestamp()),
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

    def build_runtime_source_facts(
        self,
        *,
        params: Optional[Mapping[str, Any]] = None,
        symbol: Optional[str] = None,
        chart_timeframe: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return immutable runtime source facts for walk-forward execution."""
        profile_params = dict(params or {})
        profile_params.setdefault("use_merged_value_areas", bool(self.use_merged_value_areas))
        profile_params.setdefault("merge_threshold", float(self.merge_threshold))
        profile_params.setdefault("min_merge_sessions", int(self.min_merge_sessions))
        profile_params.setdefault(
            "extend_value_area_to_chart_end", bool(self.extend_value_area_to_chart_end)
        )
        profile_params.setdefault("days_back", int(self.days_back))
        resolved_chart_timeframe = str(chart_timeframe or "").strip().lower() or "30m"
        base_profiles = self.get_profiles()
        profile_params["profiles_premerged"] = False
        profile_params["strategy_timeframe"] = resolved_chart_timeframe
        payload_profiles: List[Dict[str, Any]] = []
        for profile in base_profiles:
            payload_profiles.append(
                self._serialize_runtime_profile_for_strategy_timeframe(
                    profile,
                    chart_timeframe=resolved_chart_timeframe,
                )
            )
        return {
            "symbol": str(symbol or self.symbol or ""),
            "profiles": payload_profiles,
            "profile_params": profile_params,
            "profile_chart_timeframe": resolved_chart_timeframe,
            "profile_source_timeframe": "30m",
            "profile_boundary_semantics": "strategy_timeframe_projection",
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
        """Return canonical runtime signal payload from already-computed profiles."""
        payload = self.build_runtime_source_facts(
            params=params,
            symbol=symbol,
            chart_timeframe=chart_timeframe,
        )
        payload["_indicator_id"] = str(indicator_id or "")
        payload["overlay_color"] = (
            str(color).strip() if isinstance(color, str) and color.strip() else None
        )
        return payload

    def _serialize_runtime_profile_for_strategy_timeframe(
        self,
        profile: Profile,
        *,
        chart_timeframe: str,
    ) -> Dict[str, Any]:
        source_start = _to_utc_timestamp(profile.start)
        source_end = _to_utc_timestamp(profile.end)
        projected_start = self._project_profile_start_to_strategy_timeframe(
            source_start,
            chart_timeframe=chart_timeframe,
        )
        projected_end = self._project_profile_end_to_strategy_timeframe(
            source_end,
            chart_timeframe=chart_timeframe,
        )
        if projected_end < projected_start:
            projected_end = projected_start
        return {
            "start": int(projected_start.timestamp()),
            "end": int(projected_end.timestamp()),
            "source_start": int(source_start.timestamp()),
            "source_end": int(source_end.timestamp()),
            "projected_start": int(projected_start.timestamp()),
            "projected_end": int(projected_end.timestamp()),
            "VAH": float(profile.vah),
            "VAL": float(profile.val),
            "POC": float(profile.poc),
            "session_count": int(getattr(profile, "session_count", 1) or 1),
            "precision": int(
                getattr(profile, "precision", self.price_precision)
                or self.price_precision
            ),
            "formed_at": int(source_end.timestamp()),
            "known_at": int(projected_end.timestamp()),
        }

    @staticmethod
    def _project_profile_start_to_strategy_timeframe(
        value: pd.Timestamp,
        *,
        chart_timeframe: str,
    ) -> pd.Timestamp:
        return MarketProfileIndicator._project_timestamp_to_timeframe(
            value,
            chart_timeframe=chart_timeframe,
            mode="floor",
        )

    @staticmethod
    def _project_profile_end_to_strategy_timeframe(
        value: pd.Timestamp,
        *,
        chart_timeframe: str,
    ) -> pd.Timestamp:
        return MarketProfileIndicator._project_timestamp_to_timeframe(
            value,
            chart_timeframe=chart_timeframe,
            mode="ceil",
        )

    @staticmethod
    def _project_timestamp_to_timeframe(
        value: pd.Timestamp,
        *,
        chart_timeframe: str,
        mode: str,
    ) -> pd.Timestamp:
        ts = _to_utc_timestamp(value)
        tf = str(chart_timeframe or "").strip().lower() or "30m"
        if tf in {"30m", "30min"}:
            return ts
        try:
            step = interval_to_timedelta(tf)
        except Exception:
            step = None
        if step is None:
            try:
                return ts.floor(tf) if mode == "floor" else ts.ceil(tf)
            except Exception:
                return ts
        step_seconds = int(step.total_seconds())
        if step_seconds <= 0:
            return ts
        epoch = int(ts.timestamp())
        remainder = epoch % step_seconds
        if mode == "floor":
            projected_epoch = epoch - remainder
        else:
            projected_epoch = epoch if remainder == 0 else epoch + (step_seconds - remainder)
        return pd.Timestamp(projected_epoch, unit="s", tz="UTC")

    def _compute_daily_profiles(self) -> List[Profile]:
        """
        Build daily TPO profiles: POC, VAH, VAL, plus session timestamps.

        Returns:
            List of Profile objects
        """
        if self.df is None or self.df.empty:
            logger.info("Starting daily profile computation for %d sessions", 0)
            logger.info("Completed daily profile computation. Total profiles: %d", 0)
            return []

        df = self.df
        index = _ensure_utc_index(df.index)
        if not index.is_monotonic_increasing:
            order = np.argsort(index.asi8, kind="stable")
            df = df.iloc[order]
            index = index.take(order)

        session_keys = index.normalize()
        session_values = session_keys.asi8
        boundaries = (
            np.flatnonzero(session_values[1:] != session_values[:-1]) + 1
            if len(session_values) > 1
            else np.array([], dtype=np.int64)
        )
        start_indices = np.concatenate((np.array([0], dtype=np.int64), boundaries))
        end_indices = np.concatenate((boundaries, np.array([len(df)], dtype=np.int64)))
        profiles: List[Profile] = []

        logger.info("Starting daily profile computation for %d sessions", len(start_indices))

        for start_idx, end_idx in zip(start_indices.tolist(), end_indices.tolist()):
            group = df.iloc[start_idx:end_idx]
            session_date = session_keys[start_idx].date()
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
                start=index[start_idx],
                end=index[end_idx - 1],
                value_area=value_area,
                session_count=1,
                tpo_histogram=tpo_hist,
                precision=self.price_precision,
            )

            profiles.append(profile)
            logger.debug(
                "Profile for %s: POC=%.{prec}f, VAH=%.{prec}f, VAL=%.{prec}f".replace("{prec}", str(self.price_precision)),
                session_date,
                profile.poc,
                profile.vah,
                profile.val,
            )

        logger.info("Completed daily profile computation. Total profiles: %d", len(profiles))
        return profiles
