"""
Profile merging logic for Market Profile.

Combines consecutive profiles with overlapping value areas.
"""

import logging
from typing import List

from ..domain import Profile, ValueArea

logger = logging.getLogger(__name__)


def calculate_overlap(val1: float, vah1: float, val2: float, vah2: float) -> float:
    """
    Calculate overlap ratio between two value areas.

    Args:
        val1, vah1: First value area boundaries
        val2, vah2: Second value area boundaries

    Returns:
        Overlap fraction (0.0 to 1.0)
    """
    if vah1 < val2 or vah2 < val1:
        return 0.0

    overlap_low = max(val1, val2)
    overlap_high = min(vah1, vah2)
    overlap_range = overlap_high - overlap_low

    min_range = min(vah1 - val1, vah2 - val2)
    if min_range == 0:
        return 0.0

    return overlap_range / min_range


def merge_profiles(
    profiles: List[Profile],
    threshold: float = 0.6,
    min_sessions: int = 3,
) -> List[Profile]:
    """
    Merge consecutive profiles with overlapping value areas.

    Args:
        profiles: List of daily profiles to merge
        threshold: Minimum overlap ratio (0.0 to 1.0) to merge
        min_sessions: Minimum number of sessions required for a merged profile

    Returns:
        List of merged profiles
    """
    if not profiles:
        return []

    merged = []
    i, n = 0, len(profiles)

    logger.info(
        "event=market_profile_merge_start threshold=%.2f min_merge_sessions=%d profiles=%d",
        threshold,
        min_sessions,
        len(profiles),
    )

    while i < n:
        base = profiles[i]
        merged_val = base.val
        merged_vah = base.vah
        start_ts = base.start
        end_ts = base.end
        poc_list = [base.poc]
        count = 1
        j = i + 1

        logger.debug(
            "event=market_profile_merge_group_start index=%d start=%s",
            i,
            start_ts,
        )

        # Merge consecutive profiles that overlap
        while j < n:
            next_prof = profiles[j]
            overlap = calculate_overlap(merged_val, merged_vah, next_prof.val, next_prof.vah)
            logger.debug(
                "event=market_profile_merge_overlap_check index=%d overlap=%.2f threshold=%.2f",
                j,
                overlap,
                threshold,
            )

            if overlap < threshold:
                logger.debug(
                    "event=market_profile_merge_overlap_stop index=%d overlap=%.2f threshold=%.2f",
                    j,
                    overlap,
                    threshold,
                )
                break

            # Expand boundaries
            merged_val = min(merged_val, next_prof.val)
            merged_vah = max(merged_vah, next_prof.vah)
            end_ts = next_prof.end
            poc_list.append(next_prof.poc)
            count += 1
            j += 1

        # Only keep merged profiles that meet minimum session count
        if count >= min_sessions:
            avg_poc = sum(poc_list) / len(poc_list) if poc_list else merged_val
            merged_value_area = ValueArea(vah=merged_vah, val=merged_val, poc=avg_poc)

            merged_profile = Profile(
                start=start_ts,
                end=end_ts,
                value_area=merged_value_area,
                session_count=count,
                precision=base.precision,
            )
            merged.append(merged_profile)

            logger.info(
                "event=market_profile_merge_group_complete sessions=%d start=%s end=%s val=%.{prec}f vah=%.{prec}f poc=%.{prec}f".replace(
                    "{prec}",
                    str(base.precision),
                ),
                count,
                start_ts,
                end_ts,
                merged_val,
                merged_vah,
                avg_poc,
            )
        else:
            logger.debug(
                "event=market_profile_merge_group_skipped sessions=%d min_merge_sessions=%d",
                count,
                min_sessions,
            )

        i = j

    logger.info(
        "event=market_profile_merge_complete merged_profiles=%d",
        len(merged),
    )
    return merged
