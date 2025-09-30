"""Signal rules for Market Profile indicators."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

import numpy as np
import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.rules.breakout import (
    BreakoutRunState,
    mark_breakout_emitted,
    reset_breakout_state,
    update_breakout_state,
)
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
_PRICE_ARRAY_CACHE_KEY = "_market_profile_price_arrays"


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


def _clean_numeric(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Return a float if the value is finite, otherwise ``default``."""

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default

    if math.isnan(numeric) or math.isinf(numeric):
        return default

    return numeric


def _safe_array_value(array: Optional[np.ndarray], index: int, default: float) -> float:
    """Fetch a value from ``array`` guarding against NaNs and missing data."""

    if array is None or index >= len(array):
        return default

    value = array[index]
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default

    if math.isnan(numeric) or math.isinf(numeric):
        return default

    return numeric


def _resolve_price_arrays(
    context: Mapping[str, Any], df: pd.DataFrame
) -> Optional[Dict[str, Any]]:
    """Extract numpy arrays for OHLCV data with simple caching."""

    if df is None or df.empty or "close" not in df.columns:
        return None

    mutable = maybe_mutable_context(context)
    cached: Optional[Dict[str, Any]] = None
    if mutable is not None:
        existing = mutable.get(_PRICE_ARRAY_CACHE_KEY)
        if isinstance(existing, dict) and existing.get("df_id") == id(df):
            cached = existing

    if cached is not None:
        return cached

    close_series = pd.to_numeric(df["close"], errors="coerce")
    closes = close_series.to_numpy(dtype=float, copy=False)
    if closes.size == 0:
        return None

    def _column_to_array(column: str, *, default: float = np.nan) -> np.ndarray:
        if column in df.columns:
            series = pd.to_numeric(df[column], errors="coerce")
            return series.to_numpy(dtype=float, copy=False)
        return np.full_like(closes, float(default))

    arrays: Dict[str, Any] = {
        "df_id": id(df),
        "index": df.index,
        "close": closes,
        "open": _column_to_array("open"),
        "high": _column_to_array("high"),
        "low": _column_to_array("low"),
        "volume": _column_to_array("volume", default=0.0),
    }

    if mutable is not None:
        mutable[_PRICE_ARRAY_CACHE_KEY] = arrays

    return arrays


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
    restrict_to_last = mode in {"live", "sim"}

    tz = getattr(df.index, "tz", None)
    start_ts = _as_timestamp(value_area.get("start"), tz) or _as_timestamp(
        value_area.get("start_date"), tz
    )
    end_ts = _as_timestamp(value_area.get("end"), tz) or _as_timestamp(
        value_area.get("end_date"), tz
    )

    try:
        min_age_hours = float(context.get("market_profile_breakout_min_age_hours", 24.0))
    except (TypeError, ValueError):
        min_age_hours = 24.0
    min_age = pd.Timedelta(hours=max(min_age_hours, 0.0))

    session_id = _value_area_identifier(value_area)
    config = _resolve_breakout_config(context)

    price_arrays = _resolve_price_arrays(context, df)
    if price_arrays is None:
        log.debug("mp_brk | skip | reason=no_price_cache | session=%s", session_id)
        return []

    closes: np.ndarray = price_arrays["close"]
    opens: np.ndarray = price_arrays["open"]
    highs: np.ndarray = price_arrays["high"]
    lows: np.ndarray = price_arrays["low"]
    volumes: np.ndarray = price_arrays["volume"]
    index = price_arrays["index"]

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
    breakouts: List[Dict[str, Any]] = []
    simulate_current_only = restrict_to_last
    vah_state = BreakoutRunState()
    val_state = BreakoutRunState()
    last_index = len(closes) - 1
    min_allowed_ts = None
    if start_ts is not None:
        try:
            min_allowed_ts = start_ts + min_age
        except Exception:  # pragma: no cover - defensive
            min_allowed_ts = None

    for idx in range(1, len(closes)):
        prev_close = closes[idx - 1]
        curr_close = closes[idx]

        if not (math.isfinite(prev_close) and math.isfinite(curr_close)):
            log.debug(
                "mp_brk | skip_bar | reason=nan_close | session=%s | idx=%d | prev_close=%s | curr_close=%s",
                session_id,
                idx,
                prev_close,
                curr_close,
            )
            continue

        ts = pd.Timestamp(index[idx])
        if min_allowed_ts is not None and ts < min_allowed_ts:
            log.debug(
                "mp_brk | skip_bar | reason=value_area_too_young | session=%s | idx=%d | start=%s | bar_time=%s | age=%s | min_age=%s",
                session_id,
                idx,
                start_ts,
                ts,
                ts - start_ts if start_ts is not None else None,
                min_age,
            )
            continue

        prev_inside = val <= prev_close <= vah

        def _build_meta(level_price: float, direction: str, level_type: str) -> Dict[str, Any]:
            clearance = curr_close - level_price
            level_denominator = abs(level_price) if level_price else 1.0
            distance_pct = clearance / level_denominator
            if direction == "below":
                clearance = level_price - curr_close
                level_denominator = abs(level_price) if level_price else 1.0
                distance_pct = clearance / level_denominator

            confidence = _compute_confidence(distance_pct)
            trigger_time = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts

            trigger_open = _safe_array_value(opens, idx, curr_close)
            trigger_high = _safe_array_value(highs, idx, max(curr_close, trigger_open))
            trigger_low = _safe_array_value(lows, idx, min(curr_close, trigger_open))
            trigger_volume = _safe_array_value(volumes, idx, 0.0)

            meta: Dict[str, Any] = {
                "source": "MarketProfile",
                "symbol": context.get("symbol"),
                "time": trigger_time,
                "level_type": level_type,
                "value_area_start": start_ts.to_pydatetime() if start_ts is not None else None,
                "value_area_end": end_ts.to_pydatetime() if end_ts is not None else None,
                "value_area_id": session_id,
                "value_area_range": value_area_range,
                "value_area_mid": value_area_mid,
                "level_price": float(level_price),
                "breakout_direction": "above" if direction == "above" else "below",
                "direction": "up" if direction == "above" else "down",
                "trigger_time": trigger_time,
                "trigger_close": curr_close,
                "trigger_open": trigger_open,
                "trigger_high": trigger_high,
                "trigger_low": trigger_low,
                "trigger_volume": trigger_volume,
                "prev_close": prev_close,
                "VAH": float(vah),
                "VAL": float(val),
                "POC": _clean_numeric(value_area.get("POC")),
                "session_start": start_ts.to_pydatetime() if start_ts is not None else None,
                "session_end": end_ts.to_pydatetime() if end_ts is not None else None,
                "distance_pct": round(distance_pct, 5),
                "breakout_clearance": round(clearance, 5),
                "trigger_bar_index": idx,
                "trigger_index_label": ts,
                "confidence": confidence,
            }
            return meta

        if curr_close > vah:
            if prev_inside or vah_state.active_side == "above":
                result = update_breakout_state(
                    vah_state,
                    side="above",
                    clearance=curr_close - vah,
                    position=idx,
                    level_price=vah,
                    config=config,
                )
                if result.ready:
                    if not (simulate_current_only and idx != last_index):
                        meta = _build_meta(vah, "above", "VAH")
                        meta.update(
                            {
                                "confirmation_bars_required": config.confirmation_bars,
                                "bars_closed_beyond_level": result.consecutive,
                                "accelerated_confirmation": result.accelerated,
                            }
                        )
                        if result.start_position is not None:
                            meta["breakout_start_bar_index"] = result.start_position
                            try:
                                meta["breakout_start_index_label"] = df.index[result.start_position]
                            except Exception:  # pragma: no cover - defensive
                                pass

                        breakouts.append(meta)
                        mark_breakout_emitted(vah_state)
                        log.debug(
                            (
                                "mp_brk | detected | direction=above | session=%s | idx=%d "
                                "| bars=%d | accelerated=%s"
                            ),
                            session_id,
                            idx,
                            result.consecutive,
                            result.accelerated,
                        )
                        if restrict_to_last:
                            break
            else:
                reset_breakout_state(vah_state)
        else:
            if vah_state.active_side is not None:
                reset_breakout_state(vah_state)

        if curr_close < val:
            if prev_inside or val_state.active_side == "below":
                result = update_breakout_state(
                    val_state,
                    side="below",
                    clearance=val - curr_close,
                    position=idx,
                    level_price=val,
                    config=config,
                )
                if result.ready and not (simulate_current_only and idx != last_index):
                    meta = _build_meta(val, "below", "VAL")
                    meta.update(
                        {
                            "confirmation_bars_required": config.confirmation_bars,
                            "bars_closed_beyond_level": result.consecutive,
                            "accelerated_confirmation": result.accelerated,
                        }
                    )
                    if result.start_position is not None:
                        meta["breakout_start_bar_index"] = result.start_position
                        try:
                            meta["breakout_start_index_label"] = df.index[result.start_position]
                        except Exception:  # pragma: no cover - defensive
                            pass

                    breakouts.append(meta)
                    mark_breakout_emitted(val_state)
                    log.debug(
                        (
                            "mp_brk | detected | direction=below | session=%s | idx=%d "
                            "| bars=%d | accelerated=%s"
                        ),
                        session_id,
                        idx,
                        result.consecutive,
                        result.accelerated,
                    )
                    if restrict_to_last:
                        break
            else:
                reset_breakout_state(val_state)
        else:
            if val_state.active_side is not None:
                reset_breakout_state(val_state)

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
    price_arrays: Mapping[str, Any],
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

    look_start = start_idx + max(min_bars, 1)
    if look_start >= len(df):
        log.debug(
            "mp_retest | skip | reason=retest_window_oob | session=%s | start_idx=%s | look_start=%s | bars=%s",
            breakout_meta.get("value_area_id"),
            start_idx,
            look_start,
            len(df),
        )
        return None

    look_end = min(len(df) - 1, look_start + max(max_bars, 1))
    if look_start > look_end:
        log.debug(
            "mp_retest | skip | reason=invalid_window | session=%s | look_start=%s | look_end=%s",
            breakout_meta.get("value_area_id"),
            look_start,
            look_end,
        )
        return None

    closes: np.ndarray = price_arrays["close"]
    highs: np.ndarray = price_arrays["high"]
    lows: np.ndarray = price_arrays["low"]
    index = price_arrays["index"]

    tolerance = abs(float(level_price)) * max(tolerance_pct, 0.0)
    simulate_current_only = mode in {"sim", "live"}

    log.debug(
        "mp_retest | evaluating | session=%s | direction=%s | level=%.5f | tolerance_pct=%.5f | tolerance=%.5f | window=[%s,%s] | mode=%s",
        breakout_meta.get("value_area_id"),
        direction,
        float(level_price),
        tolerance_pct,
        tolerance,
        look_start,
        look_end,
        mode,
    )

    for idx in range(look_start, look_end + 1):
        close = _safe_array_value(closes, idx, math.nan)
        if not math.isfinite(close):
            continue

        high = _safe_array_value(highs, idx, close)
        low = _safe_array_value(lows, idx, close)

        if direction == "above":
            touched = low <= float(level_price) + tolerance
            invalidated = close < float(level_price) - tolerance
        else:
            touched = high >= float(level_price) - tolerance
            invalidated = close > float(level_price) + tolerance

        if not touched:
            log.debug(
                "mp_retest | continue | reason=not_touched | session=%s | idx=%d | high=%.5f | low=%.5f | close=%.5f | level=%.5f | tolerance=%.5f",
                breakout_meta.get("value_area_id"),
                idx,
                high,
                low,
                close,
                float(level_price),
                tolerance,
            )
            continue

        if simulate_current_only and idx != len(df) - 1:
            log.debug(
                "mp_retest | continue | reason=mode_restrict | session=%s | idx=%d | mode=%s",
                breakout_meta.get("value_area_id"),
                idx,
                mode,
            )
            continue

        if invalidated:
            log.debug(
                "mp_retest | continue | reason=invalidated | session=%s | idx=%d | close=%.5f | level=%.5f | tolerance=%.5f",
                breakout_meta.get("value_area_id"),
                idx,
                close,
                float(level_price),
                tolerance,
            )
            continue

        ts = index[idx]
        bars_since = idx - start_idx
        log.debug(
            "mp_retest | detected | session=%s | idx=%d | breakout_idx=%d | bars_since=%d | close=%.5f | high=%.5f | low=%.5f",
            breakout_meta.get("value_area_id"),
            idx,
            start_idx,
            bars_since,
            close,
            high,
            low,
        )
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

    price_arrays = _resolve_price_arrays(context, df)
    if price_arrays is None:
        log.debug("mp_retest | skip | reason=no_price_cache")
        return []

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
            price_arrays=price_arrays,
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
