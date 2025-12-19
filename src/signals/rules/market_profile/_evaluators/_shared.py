"""Shared utilities for Market Profile evaluators."""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional, Tuple

import pandas as pd

from indicators.market_profile import MarketProfileIndicator

log = logging.getLogger("MarketProfileRules")

# Cache key for storing breakouts between evaluations
BREAKOUT_CACHE_KEY = "market_profile_breakouts"
VALUE_AREA_SIGNATURE_KEYS = (
    "va_source",
    "use_merged_value_areas",
    "merge_threshold",
    "min_merge_sessions",
)


def validate_context_and_dataframe(
    context: Mapping[str, Any],
    value_area: Mapping[str, Any],
    *,
    require_close_column: bool = False,
    min_bars: int = 0,
    log_prefix: str = "mp",
) -> Optional[Tuple[MarketProfileIndicator, pd.DataFrame]]:
    """
    Validate context, indicator, and dataframe.

    Returns:
        Tuple of (indicator, df) if valid, None otherwise.
    """
    indicator = context.get("indicator")
    if not isinstance(indicator, MarketProfileIndicator):
        log.debug(
            "%s | skip | reason=invalid_indicator | indicator=%s",
            log_prefix,
            type(indicator),
        )
        return None

    if not isinstance(value_area, Mapping):
        log.debug(
            "%s | skip | reason=invalid_value_area_payload | payload_type=%s",
            log_prefix,
            type(value_area),
        )
        return None

    df: Optional[pd.DataFrame] = context.get("df")  # type: ignore[assignment]
    if df is None or df.empty:
        log.debug(
            "%s | skip | reason=no_price_data | has_df=%s",
            log_prefix,
            df is not None,
        )
        return None

    if require_close_column and "close" not in df.columns:
        log.debug(
            "%s | skip | reason=missing_close_column | columns=%s",
            log_prefix,
            list(df.columns) if isinstance(df, pd.DataFrame) else None,
        )
        return None

    if min_bars > 0 and len(df) < min_bars:
        log.debug(
            "%s | skip | reason=insufficient_bars | bars=%d | required=%d",
            log_prefix,
            len(df),
            min_bars,
        )
        return None

    return indicator, df


def map_breakout_direction_to_trade(
    breakout_direction: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Map breakout direction to trade and pointer directions.

    Args:
        breakout_direction: "above", "below", "up", or "down"

    Returns:
        Tuple of (trade_direction, pointer_direction)
        trade_direction: "long" or "short" or None
        pointer_direction: "up" or "down" or None
    """
    direction = str(breakout_direction).lower()

    if direction in {"above", "up"}:
        return "long", "up"
    elif direction in {"below", "down"}:
        return "short", "down"
    else:
        return None, None


def enrich_with_value_area_fields(
    enriched: Dict[str, Any],
    breakout_meta: Mapping[str, Any],
) -> None:
    """
    Enrich a signal dict with value area fields from breakout metadata.

    Mutates enriched dict in-place.
    """
    enriched.update(
        {
            "source": breakout_meta.get("source"),
            "level_type": breakout_meta.get("level_type"),
            "value_area_id": breakout_meta.get("value_area_id"),
            "value_area_start": breakout_meta.get("value_area_start"),
            "value_area_end": breakout_meta.get("value_area_end"),
            "value_area_range": breakout_meta.get("value_area_range"),
            "value_area_mid": breakout_meta.get("value_area_mid"),
            "VAH": breakout_meta.get("VAH"),
            "VAL": breakout_meta.get("VAL"),
            "POC": breakout_meta.get("POC"),
        }
    )
    for key in VALUE_AREA_SIGNATURE_KEYS:
        if key in breakout_meta:
            enriched[key] = breakout_meta.get(key)


def set_direction_fields(
    enriched: Dict[str, Any],
    breakout_direction: Optional[str] = None,
    *,
    force_override: bool = False,
) -> None:
    """
    Set direction and pointer_direction fields based on breakout direction.

    Args:
        enriched: Signal dict to update
        breakout_direction: If provided, uses this. Otherwise uses enriched["breakout_direction"]
        force_override: If False, only sets direction if not already "long"/"short"
    """
    if breakout_direction is None:
        breakout_direction = enriched.get("breakout_direction")

    trade_direction, pointer_direction = map_breakout_direction_to_trade(
        breakout_direction or ""
    )

    if trade_direction:
        existing_direction = str(enriched.get("direction", "")).strip().lower()
        if force_override or existing_direction not in {"long", "short"}:
            enriched["direction"] = trade_direction

    if pointer_direction and not enriched.get("pointer_direction"):
        enriched["pointer_direction"] = pointer_direction
