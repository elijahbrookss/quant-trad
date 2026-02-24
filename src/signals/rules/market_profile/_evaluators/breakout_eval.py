"""Breakout and retest evaluators for Market Profile rules."""

from __future__ import annotations

import logging
import time
import math
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
from signals.rules.market_profile.confirmation import enforce_full_bar_confirmation
from signals.rules.market_profile._evaluators._shared import VALUE_AREA_SIGNATURE_KEYS
from signals.rules.pivot import _detect_retest as _pivot_detect_retest, _evaluate_level as _pivot_evaluate_level
from signals.rules.patterns import SignalPattern, evaluate_signal_patterns

log = logging.getLogger("MarketProfileRules")
_WARNED_FALLBACKS: Set[str] = set()


def _compute_confidence(distance_pct: float) -> float:
    scaled = abs(distance_pct) * 5.0
    return max(0.1, min(scaled, 1.0))


def _value_area_breakout_evaluator(context: Mapping[str, Any], value_area: Mapping[str, Any]) -> List[Dict[str, Any]]:
    indicator = context.get("indicator")
    if not isinstance(indicator, MarketProfileIndicator):
        log.debug("mp_brk | skip | reason=invalid_indicator | indicator=%s", type(indicator))
        return []

    if not isinstance(value_area, Mapping):
        log.debug("mp_brk | skip | reason=invalid_value_area_payload | payload_type=%s", type(value_area))
        return []

    df: Optional[pd.DataFrame] = context.get("df")  # type: ignore[assignment]
    if df is None or df.empty or "close" not in df.columns:
        log.debug(
            "mp_brk | skip | reason=no_price_data | has_df=%s | columns=%s",
            df is not None,
            list(df.columns) if isinstance(df, pd.DataFrame) else None,
        )
        return []

    if len(df) < 2:
        log.debug("mp_brk | skip | reason=insufficient_bars | bars=%s", len(df))
        return []

    vah = clean_numeric(value_area.get("VAH"))
    val = clean_numeric(value_area.get("VAL"))
    if vah is None or val is None:
        log.debug("mp_brk | skip | reason=invalid_value_area_bounds | value_area=%s", value_area)
        return []
    session_count = int(value_area.get("session_count") or 1)
    min_merge_sessions = int(value_area.get("min_merge_sessions") or 1)
    if session_count < min_merge_sessions:
        log.debug(
            "mp_brk | skip | reason=insufficient_sessions | session=%s | sessions=%d | min_merge_sessions=%d",
            value_area.get("value_area_id") or value_area.get("session_id"),
            session_count,
            min_merge_sessions,
        )
        return []

    mode = str(context.get("mode", "backtest")).lower()

    tz = getattr(df.index, "tz", None)
    start_ts = as_timestamp(value_area.get("start"), tz) or as_timestamp(
        value_area.get("start_date"), tz
    )
    end_ts = as_timestamp(value_area.get("end"), tz) or as_timestamp(
        value_area.get("end_date"), tz
    )

    chart_end_ts: Optional[pd.Timestamp] = None
    if len(df.index):
        try:
            chart_end_ts = pd.Timestamp(df.index.max())
            if tz is not None:
                if chart_end_ts.tzinfo is None:
                    chart_end_ts = chart_end_ts.tz_localize(tz)  # type: ignore[arg-type]
                else:
                    chart_end_ts = chart_end_ts.tz_convert(tz)  # type: ignore[arg-type]
        except Exception:  # pragma: no cover - defensive
            chart_end_ts = None

    extend_override = context.get("market_profile_extend_value_area_to_chart_end")
    if extend_override is None:
        # MUST be present on indicator (no fallback/default)
        if not hasattr(indicator, "extend_value_area_to_chart_end"):
            raise ValueError(
                "Market Profile indicator missing 'extend_value_area_to_chart_end' attribute - "
                "indicator may not have been loaded with stored params from database"
            )
        extend_to_chart_end = bool(indicator.extend_value_area_to_chart_end)
    elif isinstance(extend_override, str):
        extend_to_chart_end = extend_override.strip().lower() not in {"false", "0", "no", "off"}
    else:
        extend_to_chart_end = bool(extend_override)

    if extend_to_chart_end and chart_end_ts is not None:
        end_ts = chart_end_ts
    elif end_ts is None and chart_end_ts is not None:
        end_ts = chart_end_ts

    if end_ts is not None and chart_end_ts is not None and end_ts > chart_end_ts:
        end_ts = chart_end_ts

    if end_ts is not None and start_ts is not None and end_ts < start_ts:
        end_ts = start_ts

    try:
        min_age_hours = float(context.get("market_profile_breakout_min_age_hours", 24.0))
    except (TypeError, ValueError):
        min_age_hours = 24.0
    min_age = pd.Timedelta(hours=max(min_age_hours, 0.0))
    min_allowed_ts = None
    if start_ts is not None:
        try:
            min_allowed_ts = start_ts + min_age
        except Exception:  # pragma: no cover - defensive
            min_allowed_ts = None

    session_id = value_area_identifier(value_area)
    config = resolve_breakout_config(context)
    symbol = context.get("symbol") or getattr(indicator, "symbol", None)

    profile_for_session: Optional[Mapping[str, Any]] = None
    if session_id:
        get_profiles = getattr(indicator, "get_profiles", None)
        if not callable(get_profiles):
            raise RuntimeError("market_profile_indicator_missing_get_profiles")
        profiles = list(get_profiles() or [])
        for profile in profiles:
            if isinstance(profile, Mapping):
                profile_mapping = profile
            elif hasattr(profile, "to_dict"):
                profile_mapping = profile.to_dict()
            else:
                continue
            profile_id = value_area_identifier(profile_mapping)
            if profile_id == session_id:
                profile_for_session = profile_mapping
                break

    log.debug(
        "mp_brk | evaluating | session=%s | mode=%s | bars=%d | vah=%.5f | val=%.5f | min_age=%s | confirmation=%d",
        session_id,
        mode,
        len(df),
        vah,
        val,
        min_age,
        config.confirmation_bars,
    )

    value_area_range = float(vah - val)
    value_area_mid = float((vah + val) / 2.0)
    poc = clean_numeric(value_area.get("POC"))

    start_index = resolve_index_position(df.index, start_ts)
    end_index = resolve_index_position(df.index, end_ts)

    boundaries = (
        ("VAH", float(vah), "resistance"),
        ("VAL", float(val), "support"),
    )

    breakouts: List[Dict[str, Any]] = []
    debug_enabled = log.isEnabledFor(logging.DEBUG)
    overall_start = time.perf_counter() if debug_enabled else None
    boundary_summaries: List[str] = []

    formed_at = end_ts
    eval_df = df
    if formed_at is not None:
        eval_df = df[df.index >= formed_at]
        if eval_df.empty:
            if debug_enabled:
                log.debug(
                    "mp_brk | no eligible bars after formed_at=%s | session=%s",
                    formed_at,
                    session_id,
                )
            return breakouts

    for level_type, level_price, level_kind in boundaries:
        candidate_prices = [float(level_price)]
        fallback_price: Optional[float] = None
        if profile_for_session is not None:
            fallback_price = clean_numeric(profile_for_session.get(level_type))
            if fallback_price is not None:
                fallback_price = float(fallback_price)
                if "mp_breakout_fallback_price" not in _WARNED_FALLBACKS:
                    log.warning("market_profile_breakout_fallback_price | level=%s", level_type)
                    _WARNED_FALLBACKS.add("mp_breakout_fallback_price")
                if not candidate_prices or not math.isclose(
                    candidate_prices[0],
                    fallback_price,
                    rel_tol=1e-9,
                    abs_tol=1e-9,
                ):
                    candidate_prices.append(fallback_price)

        metas: List[Dict[str, Any]] = []
        active_level_price = float(level_price)

        for candidate_price in candidate_prices:
            level = SimpleNamespace(
                price=candidate_price,
                kind=level_kind,
                lookback=value_area.get("lookback"),
                timeframe=value_area.get("timeframe"),
                first_touched=start_ts,
            )

            eval_start = time.perf_counter() if debug_enabled else None
            metas = _pivot_evaluate_level(
                eval_df,
                level,
                config.confirmation_bars,
                mode=mode,
                config=config,
            )
            if metas:
                active_level_price = float(candidate_price)
                break

        eval_duration_ms = 0.0
        if debug_enabled and eval_start is not None:
            eval_duration_ms = (time.perf_counter() - eval_start) * 1000.0

        metas_count = len(metas) if metas else 0

        if debug_enabled:
            log.debug(
                (
                    "mp_brk | pivot_summary | session=%s | boundary=%s | "
                    "level_price=%.5f | confirmation=%d | result_count=%d | eval_ms=%.3f"
                ),
                session_id,
                level_type,
                level_price,
                config.confirmation_bars,
                metas_count,
                eval_duration_ms,
            )

        passed_filters = 0
        filter_start = time.perf_counter() if debug_enabled else None

        if metas:
            expected_direction = "above" if level_kind == "resistance" else "below"
            directional_candidates = [
                meta
                for meta in metas
                if str(meta.get("breakout_direction", "")).lower() == expected_direction
            ]
            metas_to_process = directional_candidates if directional_candidates else metas

            for meta in metas_to_process:
                trigger_ts = normalise_meta_timestamp(meta.get("trigger_time"), tz)
                if trigger_ts is None:
                    continue

                if start_ts is not None and trigger_ts < start_ts:
                    continue

                if min_allowed_ts is not None and trigger_ts < min_allowed_ts:
                    continue

                enriched = dict(meta)

                direction = str(enriched.get("breakout_direction", "")).lower()
                trigger_close = float(enriched.get("trigger_close", active_level_price))

                if direction == "above":
                    clearance = trigger_close - active_level_price
                    bubble_direction = "up"
                elif direction == "below":
                    clearance = active_level_price - trigger_close
                    bubble_direction = "down"
                else:
                    clearance = 0.0
                    bubble_direction = "up"

                denominator = abs(active_level_price) if active_level_price else 1.0
                distance_pct = clearance / denominator if denominator else 0.0

                trade_direction = None
                pointer_direction = None
                if direction == "above":
                    trade_direction = "long"
                    pointer_direction = "up"
                elif direction == "below":
                    trade_direction = "short"
                    pointer_direction = "down"

                enriched.update(
                    {
                        "source": "MarketProfile",
                        "time": trigger_ts.to_pydatetime() if hasattr(trigger_ts, "to_pydatetime") else trigger_ts,
                        "breakout_time": enriched.get("trigger_time"),
                        "level_type": level_type,
                        "value_area_id": session_id,
                        "value_area_start": start_ts.to_pydatetime() if start_ts is not None else None,
                        "value_area_end": end_ts.to_pydatetime() if end_ts is not None else None,
                        "session_start": start_ts.to_pydatetime() if start_ts is not None else None,
                        "session_end": end_ts.to_pydatetime() if end_ts is not None else None,
                        "known_at": end_ts.to_pydatetime() if end_ts is not None else None,
                        "formed_at": end_ts.to_pydatetime() if end_ts is not None else None,
                        "value_area_start_index": start_index,
                        "value_area_end_index": end_index,
                        "value_area_range": value_area_range,
                        "value_area_mid": value_area_mid,
                        "VAH": float(vah),
                        "VAL": float(val),
                        "POC": poc,
                        "direction": trade_direction or enriched.get("direction"),
                        "pointer_direction": pointer_direction or bubble_direction,
                        "breakout_clearance": round(clearance, 5),
                        "distance_pct": round(distance_pct, 5),
                        "confidence": _compute_confidence(distance_pct),
                    }
                )

                for signature_key in VALUE_AREA_SIGNATURE_KEYS:
                    if signature_key in value_area:
                        enriched[signature_key] = value_area.get(signature_key)

                if symbol is not None:
                    enriched["symbol"] = symbol

                allow_beyond_end = False
                trigger_idx = _resolve_breakout_bar_index(enriched, df)
                if trigger_idx is not None:
                    enriched["trigger_bar_index"] = trigger_idx
                    if end_index is not None and trigger_idx > end_index:
                        window_start_index = (
                            start_index if isinstance(start_index, int) and start_index >= 0 else 0
                        )
                        window_size = max(0, end_index - window_start_index + 1)
                        allow_beyond_end = (
                            not extend_to_chart_end
                            and window_size >= max(1, int(config.confirmation_bars))
                        )
                        if not allow_beyond_end:
                            continue
                    if trigger_idx > 0:
                        try:
                            enriched["prev_close"] = float(df.iloc[trigger_idx - 1]["close"])
                        except Exception:  # pragma: no cover - defensive
                            pass
                if end_ts is not None and trigger_ts is not None and trigger_ts > end_ts:
                    if not allow_beyond_end:
                        continue

                if not enforce_full_bar_confirmation(
                    df,
                    start_index=trigger_idx,
                    boundary_price=active_level_price,
                    direction=direction,
                    required_bars=config.confirmation_bars,
                ):
                    continue

                breakout_start_ts = normalise_meta_timestamp(enriched.get("breakout_start"), tz)
                if breakout_start_ts is not None:
                    if start_ts is not None and breakout_start_ts < start_ts:
                        continue
                    if min_allowed_ts is not None and breakout_start_ts < min_allowed_ts:
                        continue
                    breakout_start_idx = resolve_index_position(df.index, breakout_start_ts)
                    if end_index is not None and breakout_start_idx is not None and breakout_start_idx > end_index:
                        if not allow_beyond_end:
                            continue
                    if breakout_start_idx is not None:
                        enriched.setdefault("breakout_start_bar_index", breakout_start_idx)
                        if 0 <= breakout_start_idx < len(df):
                            enriched.setdefault(
                                "breakout_start_index_label",
                                df.index[int(breakout_start_idx)],
                            )
                breakouts.append(enriched)
                passed_filters += 1

        filter_duration_ms = 0.0
        if debug_enabled and filter_start is not None:
            filter_duration_ms = (time.perf_counter() - filter_start) * 1000.0

        if debug_enabled:
            boundary_summaries.append(
                (
                    f"{level_type}:metas={metas_count}:passed={passed_filters}:"
                    f"eval_ms={eval_duration_ms:.3f}:filter_ms={filter_duration_ms:.3f}"
                )
            )

    if breakouts:
        log.debug(
            "mp_brk | complete | session=%s | detected=%d | mode=%s",
            session_id,
            len(breakouts),
            mode,
        )
    else:
        log.debug("mp_brk | complete | session=%s | detected=0 | mode=%s", session_id, mode)

    if debug_enabled:
        total_duration_ms = 0.0
        if overall_start is not None:
            total_duration_ms = (time.perf_counter() - overall_start) * 1000.0
        log.debug(
            "mp_brk | summary | session=%s | mode=%s | boundaries=[%s] | total_ms=%.3f",
            session_id,
            mode,
            "; ".join(boundary_summaries) if boundary_summaries else "",
            total_duration_ms,
        )

    return breakouts


def _resolve_breakout_bar_index(meta: Mapping[str, Any], df: pd.DataFrame) -> Optional[int]:
    explicit = meta.get("trigger_bar_index")
    if isinstance(explicit, int) and 0 <= explicit < len(df):
        return explicit

    label = meta.get("trigger_index_label") or meta.get("trigger_time")
    if label is None:
        return None

    try:
        ts = pd.Timestamp(label)
    except Exception:
        return None

    try:
        return int(df.index.get_loc(ts))
    except KeyError:
        try:
            idx = df.index.get_indexer([ts], method="nearest")
            return int(idx[0]) if idx.size else None
        except Exception:
            return None


BREAKOUT_PATTERN = SignalPattern(
    pattern_id="value_area_breakout",
    label="Value Area Breakout",
    description="Price leaves the active value area after closing inside it on the prior bar.",
    signal_type="breakout",
    evaluator=_value_area_breakout_evaluator,
    rule_id="market_profile_breakout",
)
