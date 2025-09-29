"""Signal rules for Market Profile indicators."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
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


def _latest_bar(df: pd.DataFrame) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    return df.iloc[-1]


def _previous_bar(df: pd.DataFrame) -> Optional[pd.Series]:
    if df is None or len(df) < 2:
        return None
    return df.iloc[-2]


def _value_area_identifier(value_area: Mapping[str, Any]) -> Optional[str]:
    start = value_area.get("start") or value_area.get("start_date")
    if start is None:
        return None
    try:
        return pd.Timestamp(start).isoformat()
    except Exception:
        return None


def _compute_confidence(distance_pct: float) -> float:
    scaled = abs(distance_pct) * 5.0
    return max(0.1, min(scaled, 1.0))


def _value_area_breakout_evaluator(context: Mapping[str, Any], value_area: Mapping[str, Any]) -> List[Dict[str, Any]]:
    indicator = context.get("indicator")
    if not isinstance(indicator, MarketProfileIndicator):
        return []

    if not isinstance(value_area, Mapping):
        return []

    df: Optional[pd.DataFrame] = context.get("df")  # type: ignore[assignment]
    if df is None or df.empty or "close" not in df.columns:
        return []

    current_bar = _latest_bar(df)
    previous_bar = _previous_bar(df)
    if current_bar is None or previous_bar is None:
        return []

    vah = value_area.get("VAH")
    val = value_area.get("VAL")
    if vah is None or val is None:
        return []

    vah = float(vah)
    val = float(val)
    prev_close = float(previous_bar.get("close"))
    curr_close = float(current_bar.get("close"))

    inside_value_area = val <= prev_close <= vah
    if not inside_value_area:
        return []

    tz = getattr(df.index, "tz", None)
    current_ts = pd.Timestamp(df.index[-1])
    start_ts = _as_timestamp(value_area.get("start"), tz) or _as_timestamp(
        value_area.get("start_date"), tz
    )
    end_ts = _as_timestamp(value_area.get("end"), tz) or _as_timestamp(
        value_area.get("end_date"), tz
    )

    min_age_hours = float(context.get("market_profile_breakout_min_age_hours", 24.0))
    if start_ts is not None:
        min_age = pd.Timedelta(hours=max(min_age_hours, 0.0))
        if current_ts - start_ts < min_age:
            log.debug(
                "mp_brk | skip | reason=value_area_too_young | start=%s | age=%s | min_age=%s",
                start_ts,
                current_ts - start_ts,
                min_age,
            )
            return []

    value_area_range = float(vah - val) if vah is not None and val is not None else None
    session_id = _value_area_identifier(value_area)

    metas: List[Dict[str, Any]] = []

    def _base_meta(level_price: float, direction: str, level_type: str) -> Dict[str, Any]:
        distance_pct = (curr_close - level_price) / level_price
        clearance = curr_close - level_price
        if direction == "below":
            distance_pct = (level_price - curr_close) / level_price
            clearance = level_price - curr_close

        confidence = _compute_confidence(distance_pct)

        meta: Dict[str, Any] = {
            "source": "MarketProfile",
            "symbol": context.get("symbol"),
            "time": current_ts.to_pydatetime(),
            "level_type": level_type,
            "value_area_start": start_ts.to_pydatetime() if start_ts is not None else None,
            "value_area_end": end_ts.to_pydatetime() if end_ts is not None else None,
            "value_area_id": session_id,
            "value_area_range": value_area_range,
            "value_area_mid": float((vah + val) / 2.0),
            "level_price": float(level_price),
            "breakout_direction": "above" if direction == "above" else "below",
            "direction": "up" if direction == "above" else "down",
            "trigger_time": current_ts.to_pydatetime(),
            "trigger_close": curr_close,
            "trigger_open": float(current_bar.get("open", curr_close)),
            "trigger_high": float(current_bar.get("high", curr_close)),
            "trigger_low": float(current_bar.get("low", curr_close)),
            "trigger_volume": float(current_bar.get("volume", 0.0)),
            "prev_close": prev_close,
            "VAH": float(vah),
            "VAL": float(val),
            "POC": value_area.get("POC"),
            "session_start": start_ts.to_pydatetime() if start_ts is not None else None,
            "session_end": end_ts.to_pydatetime() if end_ts is not None else None,
            "distance_pct": round(distance_pct, 5),
            "breakout_clearance": round(clearance, 5),
            "trigger_bar_index": len(df) - 1,
            "trigger_index_label": current_ts,
            "confidence": confidence,
        }
        return meta

    if curr_close > vah:
        metas.append(_base_meta(vah, "above", "VAH"))

    if curr_close < val:
        metas.append(_base_meta(val, "below", "VAL"))

    return metas


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
    level_price = breakout_meta.get("level_price")
    direction = breakout_meta.get("breakout_direction")
    if level_price is None or direction not in {"above", "below"}:
        return None

    start_idx = _resolve_breakout_bar_index(breakout_meta, df)
    if start_idx is None:
        return None

    look_start = start_idx + max(min_bars, 1)
    if look_start >= len(df):
        return None

    look_end = min(len(df) - 1, look_start + max(max_bars, 1))
    if look_start > look_end:
        return None

    tolerance = abs(float(level_price)) * max(tolerance_pct, 0.0)
    simulate_current_only = mode in {"sim", "live"}

    for idx in range(look_start, look_end + 1):
        candle = df.iloc[idx]
        high = float(candle.get("high", candle.get("close")))
        low = float(candle.get("low", candle.get("close")))
        close = float(candle.get("close"))

        if direction == "above":
            touched = low <= float(level_price) + tolerance
            invalidated = close < float(level_price) - tolerance
        else:
            touched = high >= float(level_price) - tolerance
            invalidated = close > float(level_price) + tolerance

        if not touched:
            continue

        if simulate_current_only and idx != len(df) - 1:
            continue

        if invalidated:
            continue

        ts = df.index[idx]
        bars_since = idx - start_idx
        return {
            "type": "retest",
            "symbol": breakout_meta.get("symbol"),
            "time": ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
            "source": breakout_meta.get("source"),
            "level_price": float(level_price),
            "breakout_time": breakout_meta.get("trigger_time"),
            "breakout_direction": direction,
            "level_type": breakout_meta.get("level_type"),
            "value_area_id": breakout_meta.get("value_area_id"),
            "value_area_start": breakout_meta.get("value_area_start"),
            "value_area_end": breakout_meta.get("value_area_end"),
            "retest_role": "support" if direction == "above" else "resistance",
            "bars_since_breakout": bars_since,
            "retest_close": close,
            "retest_high": high,
            "retest_low": low,
            "confidence": max(0.15, min(1.0, 1.0 - bars_since * 0.05)),
        }

    return None


def _value_area_retest_evaluator(context: Mapping[str, Any], value_area: Mapping[str, Any]) -> List[Dict[str, Any]]:
    indicator = context.get("indicator")
    if not isinstance(indicator, MarketProfileIndicator):
        return []

    if not isinstance(value_area, Mapping):
        return []

    df: Optional[pd.DataFrame] = context.get("df")  # type: ignore[assignment]
    if df is None or df.empty:
        return []

    mutable_context = maybe_mutable_context(context)
    breakouts: Sequence[Mapping[str, Any]] = context.get(_BREAKOUT_CACHE_KEY, [])  # type: ignore[assignment]
    if not isinstance(breakouts, Sequence):
        breakouts = []
        if mutable_context is not None:
            mutable_context[_BREAKOUT_CACHE_KEY] = list(breakouts)

    if not breakouts:
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
