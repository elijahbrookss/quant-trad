"""Signal rules for Market Profile indicators."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.rules.pivot import _detect_retest as _pivot_detect_retest
from signals.rules.pivot import _evaluate_level as _pivot_evaluate_level
from signals.rules.patterns import (
    SignalPattern,
    assign_rule_metadata,
    evaluate_signal_patterns,
    maybe_mutable_context,
)

log = logging.getLogger("MarketProfileRules")

_BREAKOUT_CACHE_KEY = "market_profile_breakouts"
_BREAKOUT_CACHE_INITIALISED = "_market_profile_breakouts_initialised"
_BREAKOUT_READY_FLAG = "_market_profile_breakouts_ready"
@dataclass(frozen=True)
class MarketProfileBreakoutConfig:
    """Configuration for Market Profile breakout confirmations."""

    confirmation_bars: int = 1
    early_confirmation_window: int = 3
    early_confirmation_distance_pct: float = 0.01

    def __post_init__(self) -> None:  # pragma: no cover - dataclass guard
        if self.confirmation_bars < 1:
            raise ValueError("confirmation_bars must be >= 1")
        if self.early_confirmation_window < 1:
            raise ValueError("early_confirmation_window must be >= 1")
        if self.early_confirmation_distance_pct < 0:
            raise ValueError("early_confirmation_distance_pct must be >= 0")


_DEFAULT_BREAKOUT_CONFIG = MarketProfileBreakoutConfig()


def _as_timestamp(value: Any, tz: Optional[str]) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if ts.tzinfo is None and tz is not None:
        return ts.tz_localize(tz)
    if tz is not None:
        try:
            ts = ts.tz_convert(tz)
        except Exception:
            pass
    return ts


def _normalise_meta_timestamp(value: Any, tz: Optional[str]) -> Optional[pd.Timestamp]:
    """Convert metadata timestamps into timezone-aware pandas timestamps."""

    if value is None:
        return None

    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None

    if tz is not None:
        if ts.tzinfo is None:
            try:
                ts = ts.tz_localize(tz)  # type: ignore[arg-type]
            except Exception:
                return None
        else:
            try:
                ts = ts.tz_convert(tz)  # type: ignore[arg-type]
            except Exception:
                return None
    return ts


def _value_area_identifier(value_area: Mapping[str, Any]) -> Optional[str]:
    start = value_area.get("start") or value_area.get("start_date")
    if start is None:
        return None
    try:
        return pd.Timestamp(start).isoformat()
    except Exception:
        return None


def _resolve_index_position(index: pd.Index, ts: Optional[pd.Timestamp]) -> Optional[int]:
    """Return the integer position of ``ts`` within ``index`` when possible."""

    if ts is None:
        return None

    try:
        positions = index.get_indexer([ts], method="nearest")
    except Exception:
        return None

    if positions.size and positions[0] >= 0:
        return int(positions[0])
    return None


def _compute_confidence(distance_pct: float) -> float:
    scaled = abs(distance_pct) * 5.0
    return max(0.1, min(scaled, 1.0))


def _clean_numeric(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Return a float if the value is finite, otherwise ``default``."""

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default

    if math.isnan(numeric) or math.isinf(numeric):
        return default

    return numeric


def _resolve_breakout_config(context: Mapping[str, Any]) -> MarketProfileBreakoutConfig:
    confirmation = context.get(
        "market_profile_breakout_confirmation_bars",
        _DEFAULT_BREAKOUT_CONFIG.confirmation_bars,
    )
    try:
        confirmation = int(confirmation)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        confirmation = _DEFAULT_BREAKOUT_CONFIG.confirmation_bars
    if confirmation < 1:
        confirmation = _DEFAULT_BREAKOUT_CONFIG.confirmation_bars

    early_window = context.get(
        "market_profile_breakout_early_window",
        _DEFAULT_BREAKOUT_CONFIG.early_confirmation_window,
    )
    try:
        early_window = int(early_window)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        early_window = _DEFAULT_BREAKOUT_CONFIG.early_confirmation_window
    if early_window < 1:
        early_window = _DEFAULT_BREAKOUT_CONFIG.early_confirmation_window

    early_pct = context.get(
        "market_profile_breakout_early_distance_pct",
        _DEFAULT_BREAKOUT_CONFIG.early_confirmation_distance_pct,
    )
    try:
        early_pct = float(early_pct)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        early_pct = _DEFAULT_BREAKOUT_CONFIG.early_confirmation_distance_pct
    if early_pct < 0:
        early_pct = _DEFAULT_BREAKOUT_CONFIG.early_confirmation_distance_pct

    resolved = MarketProfileBreakoutConfig(
        confirmation_bars=confirmation,
        early_confirmation_window=early_window,
        early_confirmation_distance_pct=early_pct,
    )
    log.debug(
        (
            "mp_brk | config_resolved | confirmation_bars=%d | early_window=%d "
            "| early_pct=%.5f"
        ),
        resolved.confirmation_bars,
        resolved.early_confirmation_window,
        resolved.early_confirmation_distance_pct,
    )
    return resolved


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

    vah = _clean_numeric(value_area.get("VAH"))
    val = _clean_numeric(value_area.get("VAL"))
    if vah is None or val is None:
        log.debug("mp_brk | skip | reason=invalid_value_area_bounds | value_area=%s", value_area)
        return []

    mode = str(context.get("mode", "backtest")).lower()

    tz = getattr(df.index, "tz", None)
    start_ts = _as_timestamp(value_area.get("start"), tz) or _as_timestamp(
        value_area.get("start_date"), tz
    )
    end_ts = _as_timestamp(value_area.get("end"), tz) or _as_timestamp(
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
        extend_to_chart_end = bool(getattr(indicator, "extend_value_area_to_chart_end", True))
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

    session_id = _value_area_identifier(value_area)
    config = _resolve_breakout_config(context)
    symbol = context.get("symbol") or getattr(indicator, "symbol", None)

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
    poc = _clean_numeric(value_area.get("POC"))

    start_index = _resolve_index_position(df.index, start_ts)
    end_index = _resolve_index_position(df.index, end_ts)

    boundaries = (
        ("VAH", float(vah), "resistance"),
        ("VAL", float(val), "support"),
    )

    breakouts: List[Dict[str, Any]] = []

    for level_type, level_price, level_kind in boundaries:
        level = SimpleNamespace(
            price=level_price,
            kind=level_kind,
            lookback=value_area.get("lookback"),
            timeframe=value_area.get("timeframe"),
            first_touched=start_ts,
        )

        metas = _pivot_evaluate_level(
            df,
            level,
            config.confirmation_bars,
            mode=mode,
            config=config,
        )

        if not metas:
            continue

        for meta in metas:
            trigger_ts = _normalise_meta_timestamp(meta.get("trigger_time"), tz)
            if trigger_ts is None:
                continue

            if start_ts is not None and trigger_ts < start_ts:
                continue

            if end_ts is not None and trigger_ts > end_ts:
                continue

            if min_allowed_ts is not None and trigger_ts < min_allowed_ts:
                continue

            breakout_start_ts = _normalise_meta_timestamp(meta.get("breakout_start"), tz)
            if breakout_start_ts is not None:
                if start_ts is not None and breakout_start_ts < start_ts:
                    continue
                if end_ts is not None and breakout_start_ts > end_ts:
                    continue
                if min_allowed_ts is not None and breakout_start_ts < min_allowed_ts:
                    continue

            enriched = dict(meta)

            direction = str(enriched.get("breakout_direction", "")).lower()
            trigger_close = float(enriched.get("trigger_close", level_price))

            if direction == "above":
                clearance = trigger_close - level_price
                bubble_direction = "up"
            elif direction == "below":
                clearance = level_price - trigger_close
                bubble_direction = "down"
            else:
                clearance = 0.0
                bubble_direction = "up"

            denominator = abs(level_price) if level_price else 1.0
            distance_pct = clearance / denominator if denominator else 0.0

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
                    "value_area_start_index": start_index,
                    "value_area_end_index": end_index,
                    "value_area_range": value_area_range,
                    "value_area_mid": value_area_mid,
                    "VAH": float(vah),
                    "VAL": float(val),
                    "POC": poc,
                    "direction": bubble_direction,
                    "breakout_clearance": round(clearance, 5),
                    "distance_pct": round(distance_pct, 5),
                    "confidence": _compute_confidence(distance_pct),
                }
            )

            if symbol is not None:
                enriched["symbol"] = symbol

            trigger_idx = _resolve_breakout_bar_index(enriched, df)
            if trigger_idx is not None:
                if end_index is not None and trigger_idx > end_index:
                    continue
            if trigger_idx is not None and trigger_idx > 0:
                try:
                    enriched["prev_close"] = float(df.iloc[trigger_idx - 1]["close"])
                except Exception:  # pragma: no cover - defensive
                    pass

            breakouts.append(enriched)

    if breakouts:
        log.debug(
            "mp_brk | complete | session=%s | detected=%d | mode=%s",
            session_id,
            len(breakouts),
            mode,
        )
    else:
        log.debug("mp_brk | complete | session=%s | detected=0 | mode=%s", session_id, mode)

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


def _detect_value_area_retest(
    df: pd.DataFrame,
    breakout_meta: Mapping[str, Any],
    *,
    tolerance_pct: float,
    max_bars: int,
    min_bars: int,
    mode: str,
) -> Optional[Dict[str, Any]]:
    level_price = _clean_numeric(breakout_meta.get("level_price"))
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

    enriched = dict(result)
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
    direction = str(enriched.get("breakout_direction", "")).lower()
    enriched.setdefault("retest_role", "support" if direction == "above" else "resistance")
    enriched["confidence"] = max(
        0.15,
        min(1.0, 1.0 - float(enriched.get("bars_since_breakout", 0)) * 0.05),
    )
    return enriched


def _value_area_retest_evaluator(context: Mapping[str, Any], value_area: Mapping[str, Any]) -> List[Dict[str, Any]]:
    indicator = context.get("indicator")
    if not isinstance(indicator, MarketProfileIndicator):
        log.debug(
            "mp_retest | skip | reason=invalid_indicator | indicator=%s",
            type(indicator),
        )
        return []

    if not isinstance(value_area, Mapping):
        log.debug(
            "mp_retest | skip | reason=invalid_value_area_payload | payload_type=%s",
            type(value_area),
        )
        return []

    df: Optional[pd.DataFrame] = context.get("df")  # type: ignore[assignment]
    if df is None or df.empty:
        log.debug("mp_retest | skip | reason=no_price_data | has_df=%s", df is not None)
        return []

    mutable_context = maybe_mutable_context(context)
    breakouts: Sequence[Mapping[str, Any]] = context.get(_BREAKOUT_CACHE_KEY, [])  # type: ignore[assignment]
    if not isinstance(breakouts, Sequence):
        breakouts = []
        if mutable_context is not None:
            mutable_context[_BREAKOUT_CACHE_KEY] = list(breakouts)

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

    target_session = _value_area_identifier(value_area)
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
            retest.setdefault(
                "direction", "up" if retest.get("breakout_direction") == "above" else "down"
            )
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


_BREAKOUT_PATTERN = SignalPattern(
    pattern_id="value_area_breakout",
    label="Value Area Breakout",
    description="Price leaves the active value area after closing inside it on the prior bar.",
    signal_type="breakout",
    evaluator=_value_area_breakout_evaluator,
)

_RETEST_PATTERN = SignalPattern(
    pattern_id="value_area_retest",
    label="Value Area Retest",
    description="Price revisits a recently broken value area boundary without invalidating the breakout.",
    signal_type="retest",
    evaluator=_value_area_retest_evaluator,
)


def _initialise_breakout_cache(context: Mapping[str, Any]) -> MutableMapping[str, Any] | None:
    mutable = maybe_mutable_context(context)
    if mutable is None:
        return None

    if not mutable.get(_BREAKOUT_CACHE_INITIALISED):
        mutable[_BREAKOUT_CACHE_KEY] = []
        mutable[_BREAKOUT_CACHE_INITIALISED] = True
    elif not isinstance(mutable.get(_BREAKOUT_CACHE_KEY), list):
        mutable[_BREAKOUT_CACHE_KEY] = []
    return mutable


def market_profile_breakout_rule(context: Mapping[str, Any], payload: Any) -> List[Dict[str, Any]]:
    mutable = _initialise_breakout_cache(context)
    if mutable is not None:
        mutable[_BREAKOUT_READY_FLAG] = False

    results = evaluate_signal_patterns(context, payload, [_BREAKOUT_PATTERN])

    if mutable is not None and results:
        breakout_cache = mutable.get(_BREAKOUT_CACHE_KEY)
        if isinstance(breakout_cache, list):
            breakout_cache.extend(results)

    if mutable is not None:
        mutable[_BREAKOUT_READY_FLAG] = True

    cache_size = None
    if mutable and isinstance(mutable.get(_BREAKOUT_CACHE_KEY), list):
        cache_size = len(mutable[_BREAKOUT_CACHE_KEY])

    log.debug("mp_brk_rule | emitted=%d | cache_size=%s", len(results), cache_size)
    return results


def market_profile_retest_rule(context: Mapping[str, Any], payload: Any) -> List[Dict[str, Any]]:
    df = context.get("df")
    if df is None or getattr(df, "empty", True):
        return []

    if not context.get(_BREAKOUT_READY_FLAG):
        market_profile_breakout_rule(context, payload)

    results = evaluate_signal_patterns(context, payload, [_RETEST_PATTERN])

    mutable = maybe_mutable_context(context)
    if mutable is not None:
        mutable[_BREAKOUT_READY_FLAG] = True

    log.debug("mp_retest_rule | emitted=%d", len(results))
    return results


assign_rule_metadata(
    market_profile_breakout_rule,
    rule_id="market_profile_breakout",
    label="Value Area Breakout",
    description=(
        "Detects when price closes outside the current value area, flagging potential initiative order flow."
    ),
)

assign_rule_metadata(
    market_profile_retest_rule,
    rule_id="market_profile_retest",
    label="Value Area Retest",
    description=(
        "Highlights pullbacks to a recently broken value area boundary that hold, signalling continuation setups."
    ),
)


__all__ = [
    "market_profile_breakout_rule",
    "market_profile_retest_rule",
]
