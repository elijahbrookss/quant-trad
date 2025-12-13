from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.rules.common.utils import format_duration


log = logging.getLogger("MarketProfilePayloads")


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
        runtime = MarketProfileIndicator(
            df=df.copy(),
            bin_size=getattr(indicator, "bin_size", 0.1),
            mode=getattr(indicator, "mode", "tpo"),
            interval=interval or getattr(indicator, "interval", "30m"),
            extend_value_area_to_chart_end=getattr(
                indicator,
                "extend_value_area_to_chart_end",
                True,
            ),
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
        value_areas = runtime.merge_value_areas(threshold=threshold, min_merge=min_merge)
    else:
        value_areas = runtime.daily_profiles

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

    log.info(
        "Market profile payloads ready | symbol=%s | profiles=%d | merged=%s | duration=%s%s",
        symbol,
        len(payloads),
        use_merged,
        format_duration(elapsed),
        session_summary,
    )

    return payloads


__all__ = [
    "build_value_area_payloads",
]
