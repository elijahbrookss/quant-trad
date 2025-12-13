"""Breakout and retest evaluators for Market Profile rules."""

from __future__ import annotations

import logging
import math
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Set

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.rules.common.utils import (
    as_timestamp,
    clean_numeric,
    normalise_meta_timestamp,
    resolve_index_position,
    value_area_identifier,
)
from signals.rules.market_profile._config import (
    MarketProfileBreakoutConfig,
    _DEFAULT_BREAKOUT_CONFIG,
    resolve_breakout_config,
)
from signals.rules.pivot import _detect_retest as _pivot_detect_retest, _evaluate_level as _pivot_evaluate_level
from signals.rules.patterns import SignalPattern, evaluate_signal_patterns, maybe_mutable_context

from ._shared import (
    BREAKOUT_CACHE_KEY,
    enrich_with_value_area_fields,
    set_direction_fields,
    validate_context_and_dataframe,
)

log = logging.getLogger("MarketProfileRules")


def _compute_confidence(distance_pct: float) -> float:
    scaled = abs(distance_pct) * 5.0
    return max(0.1, min(scaled, 1.0))


def _detect_value_area_retest(
    df: pd.DataFrame,
    breakout_meta: Mapping[str, Any],
    *,
    tolerance_pct: float,
    max_bars: int,
    min_bars: int,
    mode: str,
) -> Optional[Dict[str, Any]]:
    level_price = clean_numeric(breakout_meta.get("level_price"))
    direction = breakout_meta.get("breakout_direction")
    if level_price is None or direction not in {"above", "below"}:
        log.debug(
            "mp_retest | skip | reason=invalid_breakout_meta | level_price=%s | direction=%s",
            level_price,
            direction,
        )
        return None

    start_idx = _resolve_breakout_bar_index(breakout_meta, df)
    if start_idx is None:
        log.debug(
            "mp_retest | skip | reason=start_index_unresolved | breakout_time=%s | session=%s",
            breakout_meta.get("trigger_time"),
            breakout_meta.get("value_area_id"),
        )
        return None

    slice_start = 0
    value_area_start_index = breakout_meta.get("value_area_start_index")
    if isinstance(value_area_start_index, int):
        slice_start = max(0, value_area_start_index)
    else:
        value_area_start = breakout_meta.get("value_area_start")
        if value_area_start is not None:
            try:
                tz = getattr(df.index, "tz", None)
                va_start_ts = pd.Timestamp(value_area_start)
                if tz is not None:
                    if va_start_ts.tzinfo is None:
                        va_start_ts = va_start_ts.tz_localize(tz)  # type: ignore[arg-type]
                    else:
                        va_start_ts = va_start_ts.tz_convert(tz)  # type: ignore[arg-type]
                positions = df.index.get_indexer([va_start_ts], method="nearest")
                if positions.size and positions[0] >= 0:
                    slice_start = max(0, int(positions[0]))
            except Exception:
                log.debug(
                    "mp_retest | warn | reason=value_area_start_unresolved | session=%s | start=%s",
                    breakout_meta.get("value_area_id"),
                    value_area_start,
                )

    slice_end = len(df)
    value_area_end_index = breakout_meta.get("value_area_end_index")
    if isinstance(value_area_end_index, int):
        slice_end = min(len(df), max(0, value_area_end_index + 1))
    else:
        value_area_end = breakout_meta.get("value_area_end")
        if value_area_end is not None:
            try:
                tz = getattr(df.index, "tz", None)
                va_end_ts = pd.Timestamp(value_area_end)
                if tz is not None:
                    if va_end_ts.tzinfo is None:
                        va_end_ts = va_end_ts.tz_localize(tz)  # type: ignore[arg-type]
                    else:
                        va_end_ts = va_end_ts.tz_convert(tz)  # type: ignore[arg-type]
                positions = df.index.get_indexer([va_end_ts], method="nearest")
                if positions.size and positions[0] >= 0:
                    slice_end = min(len(df), int(positions[0]) + 1)
            except Exception:
                log.debug(
                    "mp_retest | warn | reason=value_area_end_unresolved | session=%s | end=%s",
                    breakout_meta.get("value_area_id"),
                    value_area_end,
                )

    if slice_end <= slice_start:
        log.debug(
            "mp_retest | skip | reason=invalid_scope | session=%s | slice_start=%s | slice_end=%s",
            breakout_meta.get("value_area_id"),
            slice_start,
            slice_end,
        )
        return None

    df_scope = df.iloc[slice_start:slice_end]
    breakout_scope = breakout_meta
    if slice_start > 0 and not df_scope.empty:
        breakout_scope = dict(breakout_meta)
        for key in ("trigger_bar_index", "breakout_start_bar_index"):
            idx_val = breakout_scope.get(key)
            if isinstance(idx_val, int):
                adjusted = idx_val - slice_start
                if adjusted < 0 or adjusted >= len(df_scope):
                    breakout_scope.pop(key, None)
                else:
                    breakout_scope[key] = adjusted
    elif slice_end < len(df) and not df_scope.empty:
        breakout_scope = dict(breakout_meta)

    if breakout_scope is not breakout_meta and not df_scope.empty:
        for key in ("trigger_bar_index", "breakout_start_bar_index"):
            idx_val = breakout_scope.get(key)
            if isinstance(idx_val, int) and idx_val >= len(df_scope):
                breakout_scope.pop(key, None)
    if df_scope.empty:
        log.debug(
            "mp_retest | skip | reason=scope_empty | session=%s | slice_start=%s",
            breakout_meta.get("value_area_id"),
            slice_start,
        )
        return None

    result = _pivot_detect_retest(
        df_scope,
        breakout_scope,
        tolerance_pct=tolerance_pct,
        max_bars=max_bars,
        min_bars=min_bars,
        mode=mode,
    )

    if result is None:
        return None

    level_price_value = clean_numeric(result.get("level_price"))
    close_value = clean_numeric(result.get("retest_close"))
    tolerance_abs = max(
        0.0, abs(level_price_value) * float(max(tolerance_pct, 0.0)) if level_price_value is not None else 0.0
    )
    if (
        level_price_value is not None
        and close_value is not None
        and tolerance_abs > 0.0
        and abs(close_value - level_price_value) > tolerance_abs * 2.0
    ):
        log.debug(
            "mp_retest | skip | reason=close_out_of_range | session=%s | level=%.5f | close=%.5f | tol=%.5f",
            breakout_meta.get("value_area_id"),
            level_price_value,
            close_value,
            tolerance_abs,
        )
        return None

    enriched = dict(result)
    enrich_with_value_area_fields(enriched, breakout_meta)

    direction = str(enriched.get("breakout_direction", "")).lower()
    set_direction_fields(enriched, direction)

    enriched.setdefault("retest_role", "support" if direction in {"above", "up"} else "resistance")
    enriched["confidence"] = max(
        0.15,
        min(1.0, 1.0 - float(enriched.get("bars_since_breakout", 0)) * 0.05),
    )

    canonical_rule = "market_profile_retest"
    canonical_pattern = "value_area_retest"

    alias_values: Set[str] = set()

    def _ingest_aliases(source: Mapping[str, Any]) -> None:
        for key in ("rule_id", "pattern_id"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                alias_values.add(value.strip())
        for alias_key in ("aliases", "rule_aliases", "pattern_aliases"):
            value = source.get(alias_key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                for item in value:
                    if isinstance(item, str) and item.strip():
                        alias_values.add(item.strip())

    _ingest_aliases(enriched)

    metadata = enriched.get("metadata")
    if isinstance(metadata, Mapping):
        metadata = dict(metadata)
        _ingest_aliases(metadata)
    else:
        metadata = {}

    alias_values.update({canonical_rule, canonical_pattern})

    enriched["rule_id"] = canonical_rule
    enriched["pattern_id"] = canonical_pattern
    enriched["rule_aliases"] = sorted(alias_values)
    enriched["pattern_aliases"] = sorted(alias_values)

    metadata["rule_id"] = canonical_rule
    metadata["pattern_id"] = canonical_pattern
    metadata["rule_aliases"] = sorted(alias_values)
    metadata["pattern_aliases"] = sorted(alias_values)
    enriched["metadata"] = metadata

    return enriched


def _value_area_retest_evaluator(context: Mapping[str, Any], value_area: Mapping[str, Any]) -> List[Dict[str, Any]]:
    validation_result = validate_context_and_dataframe(
        context,
        value_area,
        require_close_column=False,
        min_bars=0,
        log_prefix="mp_retest",
    )
    if validation_result is None:
        return []

    indicator, df = validation_result

    mutable_context = maybe_mutable_context(context)
    breakouts: Sequence[Mapping[str, Any]] = context.get(BREAKOUT_CACHE_KEY, [])  # type: ignore[assignment]
    if not isinstance(breakouts, Sequence):
        breakouts = []
        if mutable_context is not None:
            mutable_context[BREAKOUT_CACHE_KEY] = list(breakouts)

    if not breakouts:
        log.debug("mp_retest | skip | reason=empty_breakout_cache")
        return []

    mode = str(context.get("mode", "backtest")).lower()

    try:
        tolerance_pct = float(context.get("market_profile_retest_tolerance_pct", 0.0015))
    except (TypeError, ValueError):
        tolerance_pct = 0.0015

    try:
        max_bars = int(context.get("market_profile_retest_max_bars", 20))
    except (TypeError, ValueError):
        max_bars = 20

    try:
        min_bars = int(context.get("market_profile_retest_min_bars", 1))
    except (TypeError, ValueError):
        min_bars = 1

    target_session = value_area_identifier(value_area)
    results: List[Dict[str, Any]] = []
    for breakout_meta in breakouts:
        if not isinstance(breakout_meta, Mapping):
            continue
        if target_session and breakout_meta.get("value_area_id") != target_session:
            log.debug(
                "mp_retest | continue | reason=session_mismatch | target=%s | breakout_session=%s",
                target_session,
                breakout_meta.get("value_area_id"),
            )
            continue
        retest = _detect_value_area_retest(
            df,
            breakout_meta,
            tolerance_pct=tolerance_pct,
            max_bars=max_bars,
            min_bars=min_bars,
            mode=mode,
        )
        if retest is not None:
            set_direction_fields(retest, retest.get("breakout_direction"))
            retest.setdefault("VAH", breakout_meta.get("VAH"))
            retest.setdefault("VAL", breakout_meta.get("VAL"))
            retest.setdefault("POC", breakout_meta.get("POC"))
            results.append(retest)

    log.debug(
        "mp_retest | complete | session=%s | detected=%d",
        target_session,
        len(results),
    )
    return results


RETEST_PATTERN = SignalPattern(
    pattern_id="value_area_retest",
    label="Value Area Retest",
    description="Price revisits a recently broken value area boundary without invalidating the breakout.",
    signal_type="retest",
    evaluator=_value_area_retest_evaluator,
    rule_id="market_profile_retest",
)


