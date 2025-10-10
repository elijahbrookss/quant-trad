"""Pivot level based signal rules."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import pandas as pd

from indicators.pivot_level import Level, PivotLevelIndicator
from signals.base import BaseSignal
from signals.rules.breakout import (
    BreakoutRunState,
    mark_breakout_emitted,
    reset_breakout_state,
    update_breakout_state,
)
from signals.rules.patterns import assign_rule_metadata


@dataclass(frozen=True)
class PivotBreakoutConfig:
    """Configuration for validating pivot level breakouts."""

    confirmation_bars: int = 1
    early_confirmation_window: int = 3
    early_confirmation_distance_pct: float = 0.01
    require_full_candle_confirmation: bool = True
    accelerated_confirmation_min_bars: int = 1

    def __post_init__(self) -> None:  # pragma: no cover - dataclass guard
        if self.confirmation_bars < 1:
            raise ValueError("confirmation_bars must be >= 1")
        if self.early_confirmation_window < 1:
            raise ValueError("early_confirmation_window must be >= 1")
        if self.early_confirmation_distance_pct < 0:
            raise ValueError("early_confirmation_distance_pct must be >= 0")
        if self.accelerated_confirmation_min_bars < 1:
            raise ValueError("accelerated_confirmation_min_bars must be >= 1")
        object.__setattr__(
            self,
            "accelerated_confirmation_min_bars",
            min(self.confirmation_bars, self.accelerated_confirmation_min_bars),
        )


log = logging.getLogger("PivotBreakoutRule")

_DEFAULT_RETEST_CONFIRMATION_BARS = 1

_DEFAULT_CONFIG = PivotBreakoutConfig()
_PIVOT_BREAKOUT_READY_FLAG = "_pivot_breakouts_ready"


def _maybe_mutable_context(context: Mapping[str, Any]) -> Optional[MutableMapping[str, Any]]:
    if isinstance(context, MutableMapping):
        return context
    return None


def _summarise_level(level: Level) -> str:
    return "|".join(
        [
            f"px={getattr(level, 'price', 'na'):.5f}" if hasattr(level, "price") else "px=na",
            f"kind={getattr(level, 'kind', 'na')}",
            f"lb={getattr(level, 'lookback', 'na')}",
            f"tf={getattr(level, 'timeframe', 'na')}",
        ]
    )


def _build_run_id(indicator: PivotLevelIndicator, df: pd.DataFrame, symbol: str) -> str:
    last_index = df.index[-1] if len(df.index) else "na"
    return "|".join(
        [
            "pivotbrk",
            f"symbol={symbol}",
            f"trace={getattr(indicator, 'trace_id', 'na')}",
            f"end={last_index}",
        ]
    )


def _to_datetime(value: Any) -> Any:
    """Return a native datetime for pandas timestamps."""

    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    return value


def _resolve_config(context: Mapping[str, Any]) -> PivotBreakoutConfig:
    config = context.get("pivot_breakout_config")
    if isinstance(config, PivotBreakoutConfig):
        return config

    confirmation_bars = context.get(
        "pivot_breakout_confirmation_bars",
        _DEFAULT_CONFIG.confirmation_bars,
    )
    try:
        confirmation_bars = int(confirmation_bars)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        confirmation_bars = _DEFAULT_CONFIG.confirmation_bars

    if confirmation_bars < 1:
        confirmation_bars = _DEFAULT_CONFIG.confirmation_bars

    early_window = context.get(
        "pivot_breakout_early_window",
        _DEFAULT_CONFIG.early_confirmation_window,
    )
    try:
        early_window = int(early_window)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        early_window = _DEFAULT_CONFIG.early_confirmation_window
    if early_window < 1:
        early_window = _DEFAULT_CONFIG.early_confirmation_window

    early_pct = context.get(
        "pivot_breakout_early_distance_pct",
        _DEFAULT_CONFIG.early_confirmation_distance_pct,
    )
    try:
        early_pct = float(early_pct)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        early_pct = _DEFAULT_CONFIG.early_confirmation_distance_pct
    if early_pct < 0:
        early_pct = _DEFAULT_CONFIG.early_confirmation_distance_pct

    require_full_candle = context.get("pivot_breakout_require_full_candle")
    if require_full_candle is None:
        require_full_candle = _DEFAULT_CONFIG.require_full_candle_confirmation
    elif isinstance(require_full_candle, str):
        require_full_candle = require_full_candle.strip().lower() in {"1", "true", "yes", "on"}
    else:
        require_full_candle = bool(require_full_candle)

    accel_min = context.get(
        "pivot_breakout_acceleration_min_bars",
        _DEFAULT_CONFIG.accelerated_confirmation_min_bars,
    )
    try:
        accel_min = int(accel_min)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        accel_min = _DEFAULT_CONFIG.accelerated_confirmation_min_bars
    if accel_min < 1:
        accel_min = _DEFAULT_CONFIG.accelerated_confirmation_min_bars
    accel_min = min(accel_min, confirmation_bars)

    resolved = PivotBreakoutConfig(
        confirmation_bars=confirmation_bars,
        early_confirmation_window=early_window,
        early_confirmation_distance_pct=early_pct,
        require_full_candle_confirmation=bool(require_full_candle),
        accelerated_confirmation_min_bars=accel_min,
    )
    log.debug(
        (
            "pivotbrk | config_resolved | confirmation_bars=%d | early_window=%d "
            "| early_pct=%.4f | require_full_candle=%s | accel_min=%d | context_keys=%s"
        ),
        resolved.confirmation_bars,
        resolved.early_confirmation_window,
        resolved.early_confirmation_distance_pct,
        resolved.require_full_candle_confirmation,
        resolved.accelerated_confirmation_min_bars,
        sorted(context.keys()),
    )
    return resolved


def _ensure_indicator_levels(indicator: Any) -> Iterable[Level]:
    levels = getattr(indicator, "levels", None)
    if not levels:
        return []
    return levels


def _select_symbol(context: Mapping[str, Any], indicator: Any) -> Optional[str]:
    symbol = context.get("symbol")
    if symbol:
        return symbol
    return getattr(indicator, "symbol", None)


def _evaluate_level(
    df: pd.DataFrame,
    level: Level,
    confirmation_bars: int,
    *,
    mode: str = "backtest",
    config: PivotBreakoutConfig,
) -> List[Dict[str, Any]]:
    if "close" not in df.columns:
        raise KeyError("DataFrame must contain a 'close' column for pivot breakout rule")

    simulate_current_only = mode in {"sim", "live"}
    required_bars = max(1, confirmation_bars)
    if not simulate_current_only:
        required_bars = confirmation_bars + 1

    if len(df) < required_bars:
        log.debug(
            "pivotbrk | level_skip | reason=insufficient_bars | required=%d | available=%d",
            required_bars,
            len(df),
        )
        return []

    closes = df["close"]
    highs = df["high"] if "high" in df.columns else None
    lows = df["low"] if "low" in df.columns else None
    level_price = float(level.price)
    require_full_candle = bool(getattr(config, "require_full_candle_confirmation", True))

    def _get_series_value(series: Optional[pd.Series], position: int, fallback: float) -> float:
        if series is None:
            return fallback
        return float(series.iloc[position])

    def _classify_position(position: int) -> Tuple[str, float]:
        """Return the side of the level for a candle and the clearance distance."""

        close_value = float(closes.iloc[position])
        high_value = _get_series_value(highs, position, close_value)
        low_value = _get_series_value(lows, position, close_value)

        clearance = 0.0

        if not require_full_candle:
            if close_value > level_price:
                clearance = close_value - level_price
                return "above", clearance
            if close_value < level_price:
                clearance = level_price - close_value
                return "below", clearance
            if close_value == level_price:
                return "at", 0.0
            return "straddle", 0.0

        # Require the full candle to be beyond the level before classifying as above/below.
        above = False
        below = False

        if lows is not None:
            above = low_value > level_price
            clearance = max(clearance, low_value - level_price)
        else:
            above = close_value > level_price
            clearance = max(clearance, close_value - level_price)

        if highs is not None:
            below = high_value < level_price
            clearance = max(clearance, level_price - high_value)
        else:
            below = close_value < level_price
            clearance = max(clearance, level_price - close_value)

        if above:
            return "above", clearance
        if below:
            return "below", clearance

        is_at_level = (
            close_value == level_price
            and (highs is None or high_value == level_price)
            and (lows is None or low_value == level_price)
        )

        if is_at_level:
            return "at", 0.0

        return "straddle", 0.0
    level_id = _summarise_level(level)
    last_idx_position = len(closes) - 1

    state = BreakoutRunState()
    confirmed_side: Optional[str] = None
    current_run_prior_confirmed_side: Optional[str] = None
    results: List[Dict[str, Any]] = []
    sides: List[str] = []
    for position, (index, _close_value_obj) in enumerate(closes.items()):
        side, clearance = _classify_position(position)
        sides.append(side)

        if side in {"at", "straddle"}:
            if state.active_side is not None and state.run_confirmed and state.run_emitted:
                confirmed_side = state.active_side
            reset_breakout_state(state)
            current_run_prior_confirmed_side = None
            continue

        if (
            state.active_side is not None
            and state.active_side != side
            and state.run_confirmed
            and state.run_emitted
        ):
            confirmed_side = state.active_side

        if state.active_side != side and confirmed_side is not None and state.run_emitted:
            current_run_prior_confirmed_side = confirmed_side

        allow_accelerated = True
        if current_run_prior_confirmed_side is not None and current_run_prior_confirmed_side != side:
            allow_accelerated = False

        result = update_breakout_state(
            state,
            side=side if side in {"above", "below"} else None,
            clearance=clearance,
            position=position,
            level_price=level_price,
            config=config,
            allow_accelerated=allow_accelerated,
        )

        prior_confirmed_side = current_run_prior_confirmed_side
        if result.just_confirmed and result.active_side is not None:
            confirmed_side = result.active_side
            if state.run_emitted:
                current_run_prior_confirmed_side = result.active_side

        breakout_start_pos = result.start_position

        if breakout_start_pos is None or not result.ready:
            continue

        if simulate_current_only and position != last_idx_position:
            # In live/sim modes only emit for the latest candle.
            continue

        breakout_start_idx = closes.index[breakout_start_pos]
        breakout_end_idx = index
        last_bar = df.loc[breakout_end_idx]

        active_side = result.active_side
        detected_level_kind: Optional[str] = None
        source_kind = getattr(level, "kind", None)
        source_kind_key = str(source_kind).lower() if source_kind is not None else None

        opposite_side = "below" if active_side == "above" else "above"
        prior_sides = [
            historical_side
            for historical_side in sides[:breakout_start_pos]
            if historical_side in {"above", "below"}
        ]
        has_opposite_history = opposite_side in prior_sides if opposite_side else False

        if (
            not has_opposite_history
            and breakout_start_pos is not None
            and breakout_start_pos > 0
        ):
            history_slice = df.iloc[:breakout_start_pos]
            if not history_slice.empty:
                if opposite_side == "below":
                    reference = (
                        history_slice["low"]
                        if "low" in history_slice.columns
                        else history_slice["close"]
                    )
                    has_opposite_history = bool((reference <= level_price).any())
                elif opposite_side == "above":
                    reference = (
                        history_slice["high"]
                        if "high" in history_slice.columns
                        else history_slice["close"]
                    )
                    has_opposite_history = bool((reference >= level_price).any())

        if prior_confirmed_side == "above" and active_side == "below":
            detected_level_kind = "support"
        elif prior_confirmed_side == "below" and active_side == "above":
            detected_level_kind = "resistance"
        elif prior_confirmed_side is None:
            if not has_opposite_history:
                log.debug(
                    "pivotbrk | level_skip | level=%s | reason=no_prior_flip | "
                    "active_side=%s",
                    level_id,
                    active_side,
                )
                continue
            if active_side == "above":
                if source_kind_key in {"resistance", "pivot", "na", "none", ""}:
                    detected_level_kind = "resistance"
                elif source_kind_key is None:
                    detected_level_kind = "resistance"
            elif active_side == "below":
                if source_kind_key in {"support", "pivot", "na", "none", ""}:
                    detected_level_kind = "support"
                elif source_kind_key is None:
                    detected_level_kind = "support"

        if detected_level_kind is None:
            log.debug(
                "pivotbrk | level_skip | level=%s | reason=unconfirmed_prior_state | "
                "prior_side=%s | active_side=%s",
                level_id,
                prior_confirmed_side,
                active_side,
            )
            continue

        meta: Dict[str, Any] = {
            "level_kind": detected_level_kind,
            "source_level_kind": getattr(level, "kind", None),
            "level_price": level_price,
            "breakout_direction": active_side,
            "confirmation_bars_required": confirmation_bars,
            "bars_closed_beyond_level": result.consecutive,
            "breakout_start": _to_datetime(breakout_start_idx),
            "level_lookback": getattr(level, "lookback", None),
            "level_timeframe": getattr(level, "timeframe", None),
            "level_first_touched": _to_datetime(getattr(level, "first_touched", None)),
            "trigger_close": float(last_bar["close"]),
            "trigger_time": _to_datetime(breakout_end_idx),
            "accelerated_confirmation": result.accelerated,
            "prior_confirmed_side": prior_confirmed_side,
            "trigger_bar_index": position,
            "trigger_index_label": breakout_end_idx,
        }

        for column in ("open", "high", "low", "volume"):
            if column in df.columns:
                meta[f"trigger_{column}"] = float(last_bar[column])

        log.debug(
            "pivotbrk | level_breakout | level=%s | direction=%s | trigger_close=%.5f",
            level_id,
            active_side,
            last_bar["close"],
        )

        results.append(meta)
        mark_breakout_emitted(state)
        current_run_prior_confirmed_side = result.active_side

    if not results:
        log.debug(
            "pivotbrk | level_skip | level=%s | reason=no_breakout | confirmation_bars=%d",
            level_id,
            confirmation_bars,
        )

    return results


def pivot_breakout_rule(
    context: Mapping[str, Any],
    _: Any = None,
) -> List[Dict[str, Any]]:
    """Detect breakouts through pivot levels using closing price confirmation."""

    indicator = context.get("indicator")
    if indicator is None or not hasattr(indicator, "levels"):
        log.debug(
            "pivotbrk | guard_fail | reason=indicator_type | indicator=%r",
            type(indicator) if indicator is not None else None,
        )
        return []

    df = context.get("df")
    if df is None or df.empty:
        log.debug(
            "pivotbrk | guard_fail | reason=missing_df | indicator=%s",
            getattr(indicator, "NAME", type(indicator)),
        )
        return []

    levels = _ensure_indicator_levels(indicator)
    if not levels:
        log.debug(
            "pivotbrk | guard_fail | reason=no_levels | indicator_trace=%s",
            getattr(indicator, "trace_id", "unknown"),
        )
        return []

    symbol = _select_symbol(context, indicator)
    if symbol is None:
        log.debug(
            "pivotbrk | guard_fail | reason=no_symbol | indicator_trace=%s",
            getattr(indicator, "trace_id", "unknown"),
        )
        return []

    config = _resolve_config(context)
    confirmation_bars = config.confirmation_bars

    run_id = _build_run_id(indicator, df, symbol)
    log.debug(
        "%s | run_start | levels=%d | confirmation_bars=%d",
        run_id,
        len(levels),
        confirmation_bars,
    )

    mode = str(context.get("mode", "backtest")).lower()

    results: List[Dict[str, Any]] = []
    mutable_context = _maybe_mutable_context(context)
    breakout_cache: Optional[List[Dict[str, Any]]] = None
    if mutable_context is not None:
        cached = mutable_context.get("pivot_breakouts")
        if isinstance(cached, list):
            cached.clear()
            breakout_cache = cached
        else:
            breakout_cache = []
            mutable_context["pivot_breakouts"] = breakout_cache
        mutable_context[_PIVOT_BREAKOUT_READY_FLAG] = False
    else:
        breakout_cache = []

    for level in levels:
        level_id = _summarise_level(level)
        log.debug("%s | level_eval | level=%s", run_id, level_id)
        metas = _evaluate_level(
            df,
            level,
            confirmation_bars,
            mode=mode,
            config=config,
        )
        if not metas:
            log.debug("%s | level_eval_complete | level=%s | breakout=False", run_id, level_id)
            continue

        for meta in metas:
            breakout_time = meta.get("trigger_time", df.index[-1])
            detected_direction = meta.get("level_kind", getattr(level, "kind", None))
            results.append(
                {
                    "type": "breakout",
                    "symbol": symbol,
                    "time": _to_datetime(breakout_time),
                    "source": getattr(indicator, "NAME", indicator.__class__.__name__),
                    "direction": detected_direction,
                    **meta,
                }
            )
            log.debug("%s | level_eval_complete | level=%s | breakout=True", run_id, level_id)

    if breakout_cache is not None and results:
        breakout_cache.extend(results)

    log.debug("%s | run_complete | signals=%d", run_id, len(results))
    if mutable_context is not None:
        mutable_context[_PIVOT_BREAKOUT_READY_FLAG] = True
    return results


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


def _detect_retest(
    df: pd.DataFrame,
    breakout_meta: Mapping[str, Any],
    *,
    tolerance_pct: float,
    max_bars: int,
    min_bars: int,
    mode: str,
) -> Optional[Dict[str, Any]]:
    level_price = breakout_meta.get("level_price")
    breakout_direction = breakout_meta.get("breakout_direction")
    if level_price is None or breakout_direction not in {"above", "below"}:
        return None

    start_idx = _resolve_breakout_bar_index(breakout_meta, df)
    if start_idx is None:
        return None

    raw_confirmation = breakout_meta.get("confirmation_bars_required")
    try:
        confirmation_bars_required = int(raw_confirmation)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        confirmation_bars_required = _DEFAULT_RETEST_CONFIRMATION_BARS

    if confirmation_bars_required < 0:
        confirmation_bars_required = _DEFAULT_RETEST_CONFIRMATION_BARS

    effective_min_bars = max(min_bars, confirmation_bars_required, 1)

    look_start = start_idx + effective_min_bars
    if look_start >= len(df):
        return None

    look_end = min(len(df) - 1, look_start + max_bars)
    if look_start > look_end:
        return None

    tolerance = max(abs(float(level_price)) * float(max(tolerance_pct, 0.0)), 0.0)
    simulate_current_only = mode in {"sim", "live"}

    for idx in range(look_start, look_end + 1):
        candle = df.iloc[idx]
        high = float(candle.get("high", candle.get("close")))
        low = float(candle.get("low", candle.get("close")))
        close = float(candle.get("close"))

        if breakout_direction == "above":
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
            "time": _to_datetime(ts),
            "source": breakout_meta.get("source"),
            "level_price": float(level_price),
            "breakout_time": breakout_meta.get("trigger_time"),
            "breakout_direction": breakout_direction,
            "level_kind": breakout_meta.get("level_kind"),
            "retest_role": "support" if breakout_direction == "above" else "resistance",
            "bars_since_breakout": bars_since,
            "confirmation_bars_required": confirmation_bars_required,
            "retest_close": close,
            "retest_high": high,
            "retest_low": low,
            "level_timeframe": breakout_meta.get("level_timeframe"),
            "level_lookback": breakout_meta.get("level_lookback"),
        }

    return None


def pivot_retest_rule(context: Mapping[str, Any], payload: Any = None) -> List[Dict[str, Any]]:
    indicator = context.get("indicator")
    if not isinstance(indicator, PivotLevelIndicator):
        return []

    df = context.get("df")
    if df is None or df.empty:
        return []

    mutable_context = _maybe_mutable_context(context)
    breakouts = context.get("pivot_breakouts")

    if not context.get(_PIVOT_BREAKOUT_READY_FLAG):
        if not isinstance(breakouts, list):
            breakouts = []
            if mutable_context is not None:
                mutable_context["pivot_breakouts"] = breakouts
        pivot_breakout_rule(context, payload)
        breakouts = context.get("pivot_breakouts")

    if not isinstance(breakouts, list) or not breakouts:
        if mutable_context is not None:
            mutable_context[_PIVOT_BREAKOUT_READY_FLAG] = True
        return []

    mode = str(context.get("mode", "backtest")).lower()
    try:
        tolerance_pct = float(context.get("pivot_retest_tolerance_pct", 0.0015))
    except (TypeError, ValueError):
        tolerance_pct = 0.0015
    try:
        max_bars = int(context.get("pivot_retest_max_bars", 20))
    except (TypeError, ValueError):
        max_bars = 20
    try:
        min_bars = int(context.get("pivot_retest_min_bars", 1))
    except (TypeError, ValueError):
        min_bars = 1

    max_bars = max(max_bars, 1)
    min_bars = max(min_bars, 1)

    results: List[Dict[str, Any]] = []
    for breakout_meta in breakouts:
        if not isinstance(breakout_meta, Mapping):
            continue
        retest = _detect_retest(
            df,
            breakout_meta,
            tolerance_pct=tolerance_pct,
            max_bars=max_bars,
            min_bars=min_bars,
            mode=mode,
        )
        if retest is not None:
            results.append(retest)

    if mutable_context is not None:
        mutable_context[_PIVOT_BREAKOUT_READY_FLAG] = True
    return results


assign_rule_metadata(
    pivot_breakout_rule,
    rule_id="pivot_breakout",
    label="Breakout",
    description=(
        "Detects when price closes beyond a pivot-derived level with confirmation."
    ),
)

assign_rule_metadata(
    pivot_retest_rule,
    rule_id="pivot_retest",
    label="Retest",
    description=(
        "Labels the first pullback to a freshly broken pivot level while it holds."
    ),
)


def _to_unix_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        ts = pd.Timestamp(value)
    except Exception:  # pragma: no cover - defensive guard
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.timestamp())


_BREAKOUT_COLORS = {
    "above": "#16a34a",  # green
    "below": "#dc2626",  # red
}

_LEVEL_ROLE_COLORS = {
    "resistance": "#ef4444",  # match indicator resistance color
    "support": "#22c55e",  # match indicator support color
}

_RETEST_COLORS = {
    "support": "#0ea5e9",  # sky blue
    "resistance": "#f97316",  # amber
}


def _hex_to_rgb(color: str) -> Optional[Tuple[int, int, int]]:
    """Return RGB tuple for a hex color string."""

    if not isinstance(color, str):
        return None

    value = color.strip().lstrip("#")
    if len(value) != 6:
        return None

    try:
        r = int(value[0:2], 16)
        g = int(value[2:4], 16)
        b = int(value[4:6], 16)
    except ValueError:  # pragma: no cover - defensive guard
        return None

    return r, g, b


def _rgba_from_hex(color: str, alpha: float) -> Optional[str]:
    """Convert a hex color to an rgba() string with the provided alpha."""

    rgb = _hex_to_rgb(color)
    if rgb is None:
        return None

    r, g, b = rgb
    a = min(max(alpha, 0.0), 1.0)
    return f"rgba({r},{g},{b},{a:.2f})"


def _readable_text_color(color: str) -> str:
    """Pick a contrasting text color for the provided background color."""

    rgb = _hex_to_rgb(color)
    if rgb is None:
        return "#0f172a"

    r, g, b = rgb
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return "#0f172a" if luminance > 0.55 else "#f8fafc"


def pivot_signals_to_overlays(
    signals: Sequence[BaseSignal],
    plot_df: "pd.DataFrame",
    **ignored: Any,
) -> List[Dict[str, Any]]:
    """Convert pivot breakout signals into Lightweight Charts overlay payloads."""

    if not signals:
        return []

    bubbles: List[Dict[str, Any]] = []
    markers: List[Dict[str, Any]] = []
    price_lines: List[Dict[str, Any]] = []

    for signal in signals:
        metadata = signal.metadata or {}
        level_price = metadata.get("level_price")
        if level_price is None:
            continue

        marker_time = _to_unix_seconds(signal.time)
        if signal.type == "retest":
            retest_role = str(metadata.get("retest_role", "retest")).lower()
            color = _RETEST_COLORS.get(retest_role, "#38bdf8")
            anchor_price = float(metadata.get("retest_close", level_price))
            bars_since = metadata.get("bars_since_breakout")
            if bars_since is not None:
                detail = f"Retest after {int(bars_since)} bars at {float(level_price):.2f}"
            else:
                detail = f"Retest near {float(level_price):.2f}"

            tf = metadata.get("level_timeframe")
            lookback = metadata.get("level_lookback")
            meta_label = None
            if tf and lookback:
                meta_label = f"TF {tf} · LB {lookback}"
            elif tf:
                meta_label = f"TF {tf}"
            elif lookback:
                meta_label = f"LB {lookback}"
            bubble_payload = {
                "time": marker_time,
                "price": anchor_price,
                "label": "Retest",
                "detail": detail,
                "meta": meta_label,
                "accentColor": color,
                "backgroundColor": _rgba_from_hex(color, 0.18) or "rgba(14,165,233,0.25)",
                "textColor": "#ffffff",
                "direction": metadata.get("breakout_direction"),
                "subtype": "bubble",
            }
            bubbles.append(bubble_payload)
            continue

        breakout_direction = metadata.get("breakout_direction")

        raw_level_kind = str(metadata.get("level_kind", "pivot"))
        level_kind_key = raw_level_kind.lower()
        color = _LEVEL_ROLE_COLORS.get(level_kind_key)
        if color is None:
            color = _BREAKOUT_COLORS.get(breakout_direction, "#6b7280")  # gray fallback

        level_kind = raw_level_kind.capitalize()
        if level_kind == "Resistance":
            marker_label = "Resistance breakout"
        elif level_kind == "Support":
            marker_label = "Support breakdown"
        else:
            marker_label = f"{level_kind} breakout"

        trigger_close = metadata.get("trigger_close")
        trigger_high = metadata.get("trigger_high")
        trigger_low = metadata.get("trigger_low")
        level_tf = metadata.get("level_timeframe")
        detail_prefix = "Closed above" if breakout_direction == "above" else "Closed below"
        detail = f"{detail_prefix} {level_price:.2f}"

        meta_bits = []
        if trigger_close is not None:
            meta_bits.append(f"Close {float(trigger_close):.2f}")
        if level_tf:
            meta_bits.append(f"TF {level_tf}")
        timeframe_badge = " · ".join(meta_bits) if meta_bits else None

        anchor_price = float(trigger_close) if trigger_close is not None else float(level_price)
        level_gap = abs(anchor_price - float(level_price))
        wick_gap_above = 0.0
        wick_gap_below = 0.0
        if trigger_high is not None:
            wick_gap_above = max(0.0, float(trigger_high) - anchor_price)
        if trigger_low is not None:
            wick_gap_below = max(0.0, anchor_price - float(trigger_low))

        base_offset = max(anchor_price * 0.001, 0.1)
        if breakout_direction == "above":
            offset = max(level_gap * 0.25, wick_gap_above * 0.5, base_offset)
            bubble_price = anchor_price + offset
        elif breakout_direction == "below":
            offset = max(level_gap * 0.25, wick_gap_below * 0.5, base_offset)
            bubble_price = anchor_price - offset
        else:
            bubble_price = anchor_price + base_offset

        bubble_payload = {
            "time": marker_time,
            "price": bubble_price,
            "label": marker_label,
            "detail": detail,
            "meta": timeframe_badge,
            "accentColor": color,
            "backgroundColor": _rgba_from_hex(color, 0.2) or "rgba(30,41,59,0.75)",
            "textColor": _readable_text_color(color),
            "direction": breakout_direction,
            "subtype": "bubble",
        }
        bubbles.append(bubble_payload)
        if breakout_direction in {"above", "below"}:
            shape = "triangleUp" if breakout_direction == "above" else "triangleDown"
            marker_color = _BREAKOUT_COLORS.get(breakout_direction, color)
            marker_entry = {
                "time": marker_time,
                "price": anchor_price,
                "shape": shape,
                "color": marker_color,
                "text": marker_label,
            }
            markers.append(marker_entry)

            origin_time = metadata.get("breakout_start")
            price_lines.append(
                {
                    "price": float(level_price),
                    "color": marker_color,
                    "extend": "none",
                    "lineWidth": 1,
                    "lineStyle": 0,
                    "axisLabelVisible": True,
                    "title": marker_label,
                    "originTime": _to_unix_seconds(origin_time),
                    "endTime": marker_time,
                }
            )

    if not bubbles:
        return []

    payload = {
        "price_lines": price_lines,
        "markers": markers,
        "bubbles": bubbles,
    }

    return [
        {
            "type": PivotLevelIndicator.NAME,
            "payload": payload,
        }
    ]


__all__ = [
    "PivotBreakoutConfig",
    "pivot_breakout_rule",
    "pivot_retest_rule",
    "pivot_signals_to_overlays",
]
