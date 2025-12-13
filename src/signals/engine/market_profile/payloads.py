from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.rules.common.utils import format_duration


log = logging.getLogger("MarketProfilePayloads")


def _profiles_to_dicts(profiles: Optional[Sequence[Any]]) -> List[Dict[str, Any]]:
    """Normalize Profile objects or mappings into payload dictionaries."""

    normalized: List[Dict[str, Any]] = []
    for profile in profiles or []:
        if isinstance(profile, Mapping):
            normalized.append(dict(profile))
        elif hasattr(profile, "to_dict"):
            normalized.append(profile.to_dict())
    return normalized


def _clone_indicator_for_runtime(
    indicator: MarketProfileIndicator,
    df: pd.DataFrame,
    *,
    interval: Optional[str] = None,
) -> Optional[MarketProfileIndicator]:
    """Create a lightweight indicator instance for signal evaluation."""

    if df is None or df.empty:
        return None

    try:
        # MarketProfileIndicator.__init__ accepts: df, bin_size, use_merged_value_areas,
        # merge_threshold, min_merge_sessions, extend_value_area_to_chart_end, days_back
        # Note: 'interval' and 'mode' are NOT __init__ parameters
        runtime = MarketProfileIndicator(
            df=df.copy(),
            bin_size=getattr(indicator, "bin_size", None),  # None = auto-infer
            use_merged_value_areas=getattr(
                indicator,
                "use_merged_value_areas",
                True,
            ),
            merge_threshold=getattr(indicator, "merge_threshold", 0.6),
            min_merge_sessions=getattr(
                indicator,
                "min_merge_sessions",
                getattr(MarketProfileIndicator, "DEFAULT_MIN_MERGE_SESSIONS", 3),
            ),
            extend_value_area_to_chart_end=getattr(
                indicator,
                "extend_value_area_to_chart_end",
                True,
            ),
            days_back=getattr(
                indicator,
                "days_back",
                getattr(MarketProfileIndicator, "DEFAULT_DAYS_BACK", 180),
            ),
        )
    except Exception:
        log.exception("Failed to initialise MarketProfileIndicator for signal payloads")
        return None

    return runtime


def _value_area_reference(area: Mapping[str, Any], index: int) -> str:
    """Return a human-readable label for a value area payload."""

    for key in (
        "session_label",
        "session",
        "session_start",
        "profile_start",
        "date",
        "value_area_id",
    ):
        value = area.get(key)
        if value not in (None, ""):
            return str(value)

    return f"profile-{index + 1}"


def build_value_area_payloads(
    indicator: MarketProfileIndicator,
    df: pd.DataFrame,
    *,
    runtime_indicator: Optional[MarketProfileIndicator] = None,
    interval: Optional[str] = None,
    use_merged: Optional[bool] = None,
    merge_threshold: Optional[float] = None,
    min_merge_sessions: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Derive value area payloads for market profile signal rules."""

    symbol = getattr(indicator, "symbol", None)

    if df is None or df.empty:
        log.info(
            "Market profile payloads skipped | symbol=%s | reason=empty-data",
            symbol,
        )
        return []

    log.info(
        "Market profile payload generation | symbol=%s | df_shape=%s | df_start=%s | df_end=%s | runtime_provided=%s",
        symbol,
        df.shape if df is not None else None,
        df.index[0] if df is not None and not df.empty else None,
        df.index[-1] if df is not None and not df.empty else None,
        runtime_indicator is not None,
    )

    start_time = perf_counter()
    runtime = runtime_indicator or _clone_indicator_for_runtime(
        indicator, df, interval=interval
    )
    if runtime is None:
        log.info(
            "Market profile payloads skipped | symbol=%s | reason=indicator-init",
            symbol,
        )
        return []

    if use_merged is None:
        use_merged = getattr(runtime, "use_merged_value_areas", True)
    else:
        use_merged = bool(use_merged)

    # Get daily profiles count before merging
    daily_profiles = getattr(runtime, "daily_profiles", None) or getattr(runtime, "_profiles", None)
    daily_count = len(daily_profiles) if daily_profiles else 0

    if use_merged:
        threshold = (
            getattr(runtime, "merge_threshold", 0.6)
            if merge_threshold is None
            else float(merge_threshold)
        )
        default_min_merge = getattr(
            runtime,
            "min_merge_sessions",
            getattr(MarketProfileIndicator, "DEFAULT_MIN_MERGE_SESSIONS", 3),
        )
        min_merge = default_min_merge if min_merge_sessions is None else int(min_merge_sessions)

        log.info(
            "Market profile merge params | symbol=%s | use_merged=True | threshold=%s | min_merge_sessions=%s | daily_profiles=%d | param_source=[threshold_from_config=%s, min_sessions_from_config=%s]",
            symbol,
            threshold,
            min_merge,
            daily_count,
            merge_threshold is not None,
            min_merge_sessions is not None,
        )

        merged_profiles = None
        if hasattr(runtime, "get_merged_profiles"):
            merged_profiles = runtime.get_merged_profiles(
                threshold=threshold, min_sessions=min_merge
            )
        elif hasattr(runtime, "merged_profiles"):
            merged_profiles = runtime.merged_profiles
        value_areas = _profiles_to_dicts(merged_profiles)

        log.info(
            "Market profile merge result | symbol=%s | daily_profiles=%d | merged_profiles=%d",
            symbol,
            daily_count,
            len(value_areas),
        )
    else:
        value_areas = _profiles_to_dicts(getattr(runtime, "daily_profiles", None))
        log.info(
            "Market profile no merge | symbol=%s | use_merged=False | daily_profiles=%d",
            symbol,
            daily_count,
        )

    payloads: List[Dict[str, Any]] = []
    profile_labels: List[str] = []
    for idx, area in enumerate(value_areas or []):
        if isinstance(area, Mapping) and area.get("VAH") is not None and area.get("VAL") is not None:
            payload = dict(area)
            payloads.append(payload)
            label = _value_area_reference(payload, idx)
            profile_labels.append(label)

    elapsed = perf_counter() - start_time
    if profile_labels:
        preview = ", ".join(profile_labels[:5])
        if len(profile_labels) > 5:
            preview = f"{preview}, …"
        session_summary = f" | sessions={preview}"
    else:
        session_summary = ""

    # Log detailed payload info for debugging signal generation differences
    payload_summary = []
    for p in payloads[:3]:  # First 3 for diagnostics
        payload_summary.append({
            "value_area_id": p.get("value_area_id"),
            "VAH": p.get("VAH"),
            "VAL": p.get("VAL"),
            "POC": p.get("POC"),
        })

    log.info(
        "Market profile payloads ready | symbol=%s | profiles=%d | merged=%s | duration=%s%s | sample_payloads=%s",
        symbol,
        len(payloads),
        use_merged,
        format_duration(elapsed),
        session_summary,
        payload_summary if payloads else "[]",
    )

    return payloads


__all__ = [
    "build_value_area_payloads",
]
