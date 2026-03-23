"""Native candle stats runtime indicator."""

from __future__ import annotations

import math
import statistics
from datetime import datetime
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import (
    Indicator,
    RuntimeOverlay,
    RuntimeOutput,
)
from indicators.manifest import build_runtime_spec
from overlays.builders import build_line_overlay

from . import overlays as _overlay_registrations
from .manifest import MANIFEST

HISTORY_LIMIT = 600


def _as_positive_int(name: str, value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"candle_stats_config_invalid: {name} must be int") from exc
    if parsed <= 0:
        raise RuntimeError(f"candle_stats_config_invalid: {name} must be > 0")
    return parsed


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0 or not math.isfinite(numerator) or not math.isfinite(denominator):
        return 0.0
    return float(numerator) / float(denominator)


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    try:
        return float(statistics.median(values))
    except statistics.StatisticsError:
        return 0.0


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    try:
        return float(statistics.stdev(values))
    except statistics.StatisticsError:
        return 0.0


def _zscore(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = _mean(values)
    std_value = _std(values)
    if std_value <= 0:
        return 0.0
    return (float(values[-1]) - mean_value) / std_value


def _trim(history: list[Any], limit: int = HISTORY_LIMIT) -> None:
    overflow = len(history) - limit
    if overflow > 0:
        del history[:overflow]


def _finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


class TypedCandleStatsIndicator(Indicator):
    def __init__(self, *, indicator_id: str, version: str, params: Mapping[str, Any]) -> None:
        _ = _overlay_registrations
        self.runtime_spec = build_runtime_spec(
            MANIFEST,
            instance_id=indicator_id,
            version=version,
        )
        self._atr_short_window = _as_positive_int("atr_short_window", params.get("atr_short_window"))
        self._atr_long_window = _as_positive_int("atr_long_window", params.get("atr_long_window"))
        self._atr_z_window = _as_positive_int("atr_z_window", params.get("atr_z_window"))
        self._directional_efficiency_window = _as_positive_int(
            "directional_efficiency_window",
            params.get("directional_efficiency_window"),
        )
        self._slope_window = _as_positive_int("slope_window", params.get("slope_window"))
        self._range_window = _as_positive_int("range_window", params.get("range_window"))
        self._expansion_window = _as_positive_int("expansion_window", params.get("expansion_window"))
        self._volume_window = _as_positive_int("volume_window", params.get("volume_window"))
        self._overlap_window = _as_positive_int("overlap_window", params.get("overlap_window"))
        self._slope_stability_lookback = _as_positive_int(
            "slope_stability_lookback",
            params.get("slope_stability_lookback"),
        )
        self._warmup_bars = _as_positive_int("warmup_bars", params.get("warmup_bars"))
        self._bars: list[Candle] = []
        self._true_ranges: list[float] = []
        self._atr_short_history: list[float] = []
        self._atr_long_history: list[float] = []
        self._atr_zscore_history: list[float] = []
        self._slope_history: list[float] = []
        self._slope_std_history: list[float] = []
        self._range_width_history: list[float] = []
        self._body_overlap_history: list[float] = []
        self._atr_short_points: list[dict[str, float | int]] = []
        self._atr_long_points: list[dict[str, float | int]] = []
        self._atr_zscore_points: list[dict[str, float | int]] = []
        self._overlay_history_limit_bars = HISTORY_LIMIT
        self._current_bar_time = datetime.min
        self._output = RuntimeOutput(bar_time=datetime.min, ready=False, value={})

    def configure_replay_window(self, *, history_bars: int | None = None) -> None:
        if history_bars is None:
            return
        try:
            parsed = int(history_bars)
        except (TypeError, ValueError):
            return
        if parsed <= 0:
            return
        self._overlay_history_limit_bars = max(parsed, 1)

    def apply_bar(self, bar: Any, inputs: Mapping[Any, RuntimeOutput]) -> None:
        if not isinstance(bar, Candle):
            raise RuntimeError("candle_stats_apply_failed: Candle input required")
        if inputs:
            raise RuntimeError("candle_stats_apply_failed: candle_stats has no dependencies")

        self._current_bar_time = bar.time
        self._bars.append(bar)
        _trim(self._bars)

        previous_close = float(self._bars[-2].close) if len(self._bars) > 1 else float(bar.close)
        true_range = max(
            float(bar.high) - float(bar.low),
            abs(float(bar.high) - previous_close),
            abs(float(bar.low) - previous_close),
        )
        self._true_ranges.append(true_range)
        _trim(self._true_ranges)

        atr_short = self._next_ema(
            self._atr_short_history[-1] if self._atr_short_history else None,
            true_range,
            self._atr_short_window,
        )
        atr_long = self._next_ema(
            self._atr_long_history[-1] if self._atr_long_history else None,
            true_range,
            self._atr_long_window,
        )
        self._atr_short_history.append(atr_short)
        self._atr_long_history.append(atr_long)
        _trim(self._atr_short_history)
        _trim(self._atr_long_history)
        atr_zscore = _zscore(list(self._atr_short_history[-self._atr_z_window:]))
        self._atr_zscore_history.append(atr_zscore)
        _trim(self._atr_zscore_history)
        self._append_point(
            self._atr_short_points,
            bar=bar,
            value=atr_short,
            limit=self._overlay_history_limit_bars,
        )
        self._append_point(
            self._atr_long_points,
            bar=bar,
            value=atr_long,
            limit=self._overlay_history_limit_bars,
        )
        self._append_point(
            self._atr_zscore_points,
            bar=bar,
            value=atr_zscore,
            limit=self._overlay_history_limit_bars,
        )

        slope = self._current_slope()
        if slope is not None:
            self._slope_history.append(slope)
            _trim(self._slope_history)
        slope_std = self._current_slope_std()
        if slope_std is not None:
            self._slope_std_history.append(slope_std)
            _trim(self._slope_std_history)
        range_width = self._current_range_width()
        if range_width is not None:
            self._range_width_history.append(range_width)
            _trim(self._range_width_history)
        overlap = self._current_body_overlap()
        if overlap is not None:
            self._body_overlap_history.append(overlap)
            _trim(self._body_overlap_history)

        if not self._is_ready():
            self._output = RuntimeOutput(bar_time=bar.time, ready=False, value={})
            return

        metrics = self._build_metrics(bar)
        self._output = RuntimeOutput(bar_time=bar.time, ready=True, value=metrics)

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {"candle_stats": self._output}

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        return {
            "atr_short": RuntimeOverlay(
                bar_time=self._current_bar_time,
                ready=bool(self._atr_short_points),
                value=self._build_line_overlay(
                    overlay_type="candle_stats_atr_short",
                    role="atr_short",
                    points=self._atr_short_points,
                    color="#22c55e",
                ) if self._atr_short_points else {},
            ),
            "atr_long": RuntimeOverlay(
                bar_time=self._current_bar_time,
                ready=bool(self._atr_long_points),
                value=self._build_line_overlay(
                    overlay_type="candle_stats_atr_long",
                    role="atr_long",
                    points=self._atr_long_points,
                    color="#f59e0b",
                ) if self._atr_long_points else {},
            ),
            "atr_zscore": RuntimeOverlay(
                bar_time=self._current_bar_time,
                ready=len(self._atr_short_history) >= self._atr_z_window and bool(self._atr_zscore_points),
                value=self._build_line_overlay(
                    overlay_type="candle_stats_atr_zscore",
                    role="atr_zscore",
                    points=self._atr_zscore_points,
                    color="#38bdf8",
                ) if len(self._atr_short_history) >= self._atr_z_window and self._atr_zscore_points else {},
            ),
        }

    @staticmethod
    def _next_ema(previous: float | None, value: float, window: int) -> float:
        if previous is None:
            return float(value)
        alpha = 1.0 / float(window)
        return alpha * float(value) + (1.0 - alpha) * float(previous)

    def _is_ready(self) -> bool:
        if len(self._bars) < self._warmup_bars:
            return False
        if len(self._atr_short_history) < self._atr_z_window:
            return False
        if len(self._slope_std_history) < self._slope_stability_lookback:
            return False
        volumes = [bar.volume for bar in self._bars[-self._volume_window:]]
        return all(_finite_float(volume) is not None for volume in volumes)

    def _current_slope(self) -> float | None:
        if len(self._bars) <= self._slope_window:
            return None
        return (
            float(self._bars[-1].close) - float(self._bars[-(self._slope_window + 1)].close)
        ) / float(self._slope_window)

    def _current_slope_std(self) -> float | None:
        if len(self._slope_history) < self._slope_window:
            return None
        return _std(list(self._slope_history[-self._slope_window:]))

    def _current_range_width(self) -> float | None:
        if len(self._bars) < self._range_window:
            return None
        highs = [float(item.high) for item in self._bars[-self._range_window:]]
        lows = [float(item.low) for item in self._bars[-self._range_window:]]
        return max(highs) - min(lows)

    def _current_body_overlap(self) -> float | None:
        if len(self._bars) < 2:
            return None
        current = self._bars[-1]
        previous = self._bars[-2]
        current_high = max(float(current.open), float(current.close))
        current_low = min(float(current.open), float(current.close))
        previous_high = max(float(previous.open), float(previous.close))
        previous_low = min(float(previous.open), float(previous.close))
        overlap = max(min(current_high, previous_high) - max(current_low, previous_low), 0.0)
        body_range = max(current_high, previous_high) - min(current_low, previous_low)
        if body_range == 0:
            return 1.0 if current_high == previous_high and current_low == previous_low else 0.0
        return max(0.0, min(1.0, overlap / body_range))

    def _build_metrics(self, bar: Candle) -> dict[str, float]:
        bar_range = max(float(bar.high) - float(bar.low), 0.0)
        body = abs(float(bar.close) - float(bar.open))
        upper_wick = max(float(bar.high) - max(float(bar.open), float(bar.close)), 0.0)
        lower_wick = max(min(float(bar.open), float(bar.close)) - float(bar.low), 0.0)
        recent_atr_short = list(self._atr_short_history[-self._atr_z_window:])
        recent_closes = [
            float(item.close)
            for item in self._bars[-(self._directional_efficiency_window + 1):]
        ]
        close_diffs = [
            abs(recent_closes[index] - recent_closes[index - 1])
            for index in range(1, len(recent_closes))
        ]
        recent_highs = [float(item.high) for item in self._bars[-self._range_window:]]
        recent_lows = [float(item.low) for item in self._bars[-self._range_window:]]
        range_high = max(recent_highs)
        range_low = min(recent_lows)
        range_width = max(range_high - range_low, 0.0)
        recent_volumes = [_finite_float(item.volume) or 0.0 for item in self._bars[-self._volume_window:]]
        atr_short = float(self._atr_short_history[-1])
        atr_long = float(self._atr_long_history[-1])
        return {
            "body_pct": _safe_div(body, bar_range),
            "upper_wick_pct": _safe_div(upper_wick, bar_range),
            "lower_wick_pct": _safe_div(lower_wick, bar_range),
            "range_pct": _safe_div(bar_range, abs(float(bar.close)) or 1.0),
            "tr": float(self._true_ranges[-1]),
            "tr_pct": _safe_div(float(self._true_ranges[-1]), float(self._bars[-2].close)),
            "atr_short": atr_short,
            "atr_long": atr_long,
            "atr_ratio": _safe_div(atr_short, atr_long),
            "atr_zscore": float(self._atr_zscore_history[-1]),
            "directional_efficiency": _safe_div(
                abs(recent_closes[-1] - recent_closes[0]),
                sum(close_diffs),
            ),
            "slope": float(self._slope_history[-1]),
            "slope_stability": _zscore(list(self._slope_std_history[-self._slope_stability_lookback:])),
            "range_width": range_width,
            "range_position": _safe_div(float(bar.close) - range_low, range_width),
            "atr_slope": atr_short - float(self._atr_short_history[-self._expansion_window]),
            "range_contraction": _safe_div(
                range_width,
                float(self._range_width_history[-self._expansion_window]),
            ),
            "overlap_pct": _mean(list(self._body_overlap_history[-self._overlap_window:])),
            "volume_zscore": _zscore(recent_volumes),
            "volume_vs_median": _safe_div(recent_volumes[-1], _median(recent_volumes)),
        }

    @staticmethod
    def _append_point(
        points: list[dict[str, float | int]],
        *,
        bar: Candle,
        value: float,
        limit: int,
    ) -> None:
        points.append(
            {
                "time": int(bar.time.timestamp()),
                "price": float(value),
            }
        )
        _trim(points, limit=max(int(limit), 1))

    def _build_line_overlay(
        self,
        *,
        overlay_type: str,
        role: str,
        points: list[dict[str, float | int]],
        color: str,
    ) -> dict[str, Any]:
        return build_line_overlay(
            overlay_type,
            points=points,
            line_style=0,
            line_width=1.5,
            color=color,
            role=role,
        )


__all__ = ["TypedCandleStatsIndicator"]
