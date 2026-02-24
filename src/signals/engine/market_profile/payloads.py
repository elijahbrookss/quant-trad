from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.rules.common.utils import format_duration, value_area_identifier

from .params import MarketProfileParams, resolve_market_profile_params

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
    params: MarketProfileParams,
    *,
    interval: Optional[str] = None,
) -> Optional[MarketProfileIndicator]:
    """Clone indicator without recomputing profiles for signal evaluation."""

    if not isinstance(indicator, MarketProfileIndicator):
        return None

    runtime = indicator.clone_for_overlay(
        use_merged_value_areas=params.use_merged_value_areas,
        merge_threshold=params.merge_threshold,
        min_merge_sessions=params.min_merge_sessions,
    )

    if interval is not None:
        setattr(runtime, "interval", interval)

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
    symbol: Optional[str] = None,
    use_merged: Optional[bool] = None,
    merge_threshold: Optional[float] = None,
    min_merge_sessions: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Derive value area payloads for market profile signal rules."""

    runtime_symbol = symbol or getattr(runtime_indicator, "symbol", None) or getattr(indicator, "symbol", None)

    if df is None or df.empty:
        log.info(
            "Market profile payloads skipped | symbol=%s | reason=empty-data",
            runtime_symbol,
        )
        return []

    log.info(
        "Market profile payload generation | symbol=%s | df_shape=%s | df_start=%s | df_end=%s | runtime_provided=%s",
        runtime_symbol,
        df.shape if df is not None else None,
        df.index[0] if df is not None and not df.empty else None,
        df.index[-1] if df is not None and not df.empty else None,
        runtime_indicator is not None,
    )

    params = resolve_market_profile_params(
        runtime_indicator or indicator,
        use_merged_value_areas=use_merged,
        merge_threshold=merge_threshold,
        min_merge_sessions=min_merge_sessions,
    )

    start_time = perf_counter()
    va_source = "stored_indicator"
    runtime = None
    if runtime_indicator is not None and isinstance(runtime_indicator, MarketProfileIndicator):
        runtime_symbol_set = getattr(runtime_indicator, "symbol", None)
        if runtime_symbol is not None and runtime_symbol_set and runtime_symbol_set != runtime_symbol:
            # Extract all required params from runtime_indicator - MUST be present (no fallbacks/defaults)
            if not hasattr(runtime_indicator, "days_back"):
                raise ValueError(
                    "Market Profile runtime_indicator missing 'days_back' attribute - "
                    "indicator may not have been loaded with stored params from database"
                )
            if not hasattr(runtime_indicator, "extend_value_area_to_chart_end"):
                raise ValueError(
                    "Market Profile runtime_indicator missing 'extend_value_area_to_chart_end' attribute - "
                    "indicator may not have been loaded with stored params from database"
                )

            runtime = MarketProfileIndicator(
                df,
                bin_size=getattr(runtime_indicator, "bin_size", None),
                use_merged_value_areas=params.use_merged_value_areas,
                merge_threshold=params.merge_threshold,
                min_merge_sessions=params.min_merge_sessions,
                extend_value_area_to_chart_end=runtime_indicator.extend_value_area_to_chart_end,
                days_back=runtime_indicator.days_back,
            )
            setattr(runtime, "symbol", runtime_symbol)
            va_source = "runtime_df"
        elif hasattr(runtime_indicator, "clone_for_overlay"):
            runtime = runtime_indicator.clone_for_overlay(
                use_merged_value_areas=params.use_merged_value_areas,
                merge_threshold=params.merge_threshold,
                min_merge_sessions=params.min_merge_sessions,
            )
    if runtime is None:
        runtime = _clone_indicator_for_runtime(
            indicator,
            params,
            interval=interval,
        )
        va_source = "stored_indicator"
    if runtime is None:
        log.info(
            "Market profile payloads skipped | symbol=%s | reason=indicator-init",
            runtime_symbol,
        )
        return []

    if interval is not None:
        setattr(runtime, "interval", interval)
    if runtime_symbol is not None:
        setattr(runtime, "symbol", runtime_symbol)

    get_profiles = getattr(runtime, "get_profiles", None)
    if not callable(get_profiles):
        raise RuntimeError("market_profile_runtime_missing_get_profiles")
    daily_profiles = list(get_profiles() or [])
    daily_count = len(daily_profiles)

    signature = params.signature(va_source=va_source)

    if params.use_merged_value_areas:
        log.info(
            "Market profile merge params | symbol=%s | use_merged=True | threshold=%s | min_merge_sessions=%s | daily_profiles=%d | param_source=[threshold_from_config=%s, min_sessions_from_config=%s]",
            runtime_symbol,
            params.merge_threshold,
            params.min_merge_sessions,
            daily_count,
            merge_threshold is not None,
            min_merge_sessions is not None,
        )

        get_merged_profiles = getattr(runtime, "get_merged_profiles", None)
        if not callable(get_merged_profiles):
            raise RuntimeError("market_profile_runtime_missing_get_merged_profiles")
        merged_profiles = get_merged_profiles(
            threshold=params.merge_threshold, min_sessions=params.min_merge_sessions
        )
        value_areas = _profiles_to_dicts(merged_profiles)

        log.info(
            "Market profile merge result | symbol=%s | daily_profiles=%d | merged_profiles=%d",
            runtime_symbol,
            daily_count,
            len(value_areas),
        )
    else:
        value_areas = _profiles_to_dicts(daily_profiles)
        log.info(
            "Market profile no merge | symbol=%s | use_merged=False | daily_profiles=%d",
            runtime_symbol,
            daily_count,
        )

    payloads: List[Dict[str, Any]] = []
    profile_labels: List[str] = []
    for idx, area in enumerate(value_areas or []):
        if isinstance(area, Mapping) and area.get("VAH") is not None and area.get("VAL") is not None:
            payload = dict(area)
            # Enrich with formed_at and identifiers for walk-forward gating
            formed_at = payload.get("end") or payload.get("profile_end") or payload.get("session_end")
            if formed_at is not None:
                try:
                    payload["formed_at"] = pd.Timestamp(formed_at)
                except Exception:
                    payload["formed_at"] = formed_at
            payload.setdefault("session_count", payload.get("session_count", getattr(area, "session_count", 1)))
            payload.setdefault("min_merge_sessions", min_merge_sessions or params.min_merge_sessions)
            payload.setdefault("value_area_id", value_area_identifier(payload) or _value_area_reference(payload, idx))
            payload.setdefault("va_start", payload.get("start") or payload.get("session_start"))
            payload.setdefault("va_end", payload.get("end") or payload.get("session_end"))
            payload.update(signature)
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
        "Market profile payloads ready | symbol=%s | profiles=%d | merged=%s | duration=%s | source=%s%s | sample_payloads=%s",
        runtime_symbol,
        len(payloads),
        params.use_merged_value_areas,
        format_duration(elapsed),
        va_source,
        session_summary,
        payload_summary if payloads else "[]",
    )

    return payloads


__all__ = [
    "build_value_area_payloads",
]
