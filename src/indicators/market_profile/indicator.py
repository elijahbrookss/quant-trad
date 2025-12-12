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
from indicators.base import BaseIndicator

from .domain import Profile, ValueArea
from ._internal.computation import build_tpo_histogram, extract_value_area
from ._internal.bin_size import select_bin_size, infer_precision_from_step
from ._internal.merging import merge_profiles


class MarketProfileIndicator(BaseIndicator):
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

    def __init__(
        self,
        df: pd.DataFrame,
        bin_size: Optional[float] = None,
    ):
        """
        Initialize Market Profile indicator.

        Args:
            df: DataFrame with OHLC data indexed by timestamp
            bin_size: Price bucket size (auto-inferred if not provided)
        """
        super().__init__(df)

        # Determine bin size
        self.bin_size, self._bin_size_locked = select_bin_size(df, bin_size)
        self._bin_precision = infer_precision_from_step(self.bin_size)
        self.price_precision = max(2, self._bin_precision)

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
        return merge_profiles(self._profiles, threshold, min_sessions)

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
