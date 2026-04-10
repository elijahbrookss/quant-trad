"""Native regime runtime indicator."""

from __future__ import annotations

from collections import OrderedDict, deque
from datetime import datetime
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import (
    Indicator,
    OutputRef,
    RuntimeOverlay,
    RuntimeOutput,
)
from indicators.manifest import build_runtime_spec
from overlays.schema import build_overlay

from .config import RegimeStabilizerConfig
from .engine import RegimeEngine
from .manifest import MANIFEST
from .overlays import build_regime_overlays
from .stabilizer import RegimeStabilizer

HISTORY_LIMIT = 320


def resolve_regime_dependency(
    *,
    indicator_id: str,
    meta: Mapping[str, Any],
    strategy_indicator_metas: Mapping[str, Mapping[str, Any]],
) -> str:
    _ = strategy_indicator_metas
    raw_dependencies = meta.get("dependencies")
    if not isinstance(raw_dependencies, (list, tuple)):
        raise RuntimeError(
            "regime_dependency_missing: explicit candle_stats dependency binding required "
            f"indicator_id={indicator_id}"
        )
    refs = [
        item
        for item in raw_dependencies
        if isinstance(item, Mapping)
        and str(item.get("output_name") or "").strip() == "candle_stats"
        and str(item.get("indicator_id") or "").strip()
    ]
    if len(refs) != 1:
        raise RuntimeError(
            "regime_dependency_invalid: exactly one candle_stats dependency binding required "
            f"indicator_id={indicator_id}"
        )
    return str(refs[0].get("indicator_id") or "").strip()


def _as_positive_int(name: str, value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"regime_config_invalid: {name} must be int") from exc
    if parsed <= 0:
        raise RuntimeError(f"regime_config_invalid: {name} must be > 0")
    return parsed


def _as_float(name: str, value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"regime_config_invalid: {name} must be numeric") from exc


def _float_or_none(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(*values: Any, default: float = 0.0) -> float:
    for value in values:
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return float(default)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


class TypedRegimeIndicator(Indicator):
    def __init__(
        self,
        *,
        indicator_id: str,
        version: str,
        params: Mapping[str, Any],
        candle_stats_indicator_id: str,
    ) -> None:
        dependency_ref = OutputRef(
            indicator_id=candle_stats_indicator_id,
            output_name="candle_stats",
        )
        self.runtime_spec = build_runtime_spec(
            MANIFEST,
            instance_id=indicator_id,
            version=version,
            dependencies=(dependency_ref,),
        )
        self._dependency_ref = dependency_ref
        config = RegimeStabilizerConfig(
            min_confidence=_as_float("min_confidence", params.get("min_confidence")),
            structure_min_confidence=_as_float(
                "structure_min_confidence",
                params.get("structure_min_confidence"),
            ),
            confirm_bars={
                "structure": _as_positive_int(
                    "structure_confirm_bars",
                    params.get("structure_confirm_bars"),
                ),
                "volatility": _as_positive_int(
                    "volatility_confirm_bars",
                    params.get("volatility_confirm_bars"),
                ),
                "liquidity": _as_positive_int(
                    "liquidity_confirm_bars",
                    params.get("liquidity_confirm_bars"),
                ),
                "expansion": _as_positive_int(
                    "expansion_confirm_bars",
                    params.get("expansion_confirm_bars"),
                ),
            },
            smoothing_alpha=_as_float("smoothing_alpha", params.get("smoothing_alpha")),
        )
        self._engine = RegimeEngine()
        self._stabilizer = RegimeStabilizer(config)
        self._context_output = RuntimeOutput(bar_time=datetime.min, ready=False, value={})
        self._metric_output = RuntimeOutput(bar_time=datetime.min, ready=False, value={})
        self._overlay_history_limit_bars = HISTORY_LIMIT
        self._candles = deque(maxlen=self._overlay_history_limit_bars)
        self._regime_rows: OrderedDict[datetime, dict[str, Any]] = OrderedDict()
        self._timeframe_seconds = 60
        self._current_bar_time = datetime.min
        self._overlay_ready = False
        self._overlay_cache_bar_time: datetime | None = None
        self._overlay_cache: dict[str, RuntimeOverlay] | None = None

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
        self._candles = deque(
            list(self._candles)[-self._overlay_history_limit_bars :],
            maxlen=self._overlay_history_limit_bars,
        )
        self._prune_regime_rows()
        self._overlay_cache = None
        self._overlay_cache_bar_time = None

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        if not isinstance(bar, Candle):
            raise RuntimeError("regime_apply_failed: Candle input required")
        self._current_bar_time = bar.time
        dependency_output = inputs.get(self._dependency_ref)
        if dependency_output is None:
            raise RuntimeError(
                "regime_apply_failed: dependency output missing "
                f"dependency={self._dependency_ref.indicator_id}.{self._dependency_ref.output_name}"
            )
        if not dependency_output.ready:
            self._overlay_ready = False
            self._overlay_cache = None
            self._overlay_cache_bar_time = None
            self._context_output = RuntimeOutput(bar_time=bar.time, ready=False, value={})
            self._metric_output = RuntimeOutput(bar_time=bar.time, ready=False, value={})
            return

        if self._candles:
            delta_seconds = int((bar.time - self._candles[-1].time).total_seconds())
            if delta_seconds > 0:
                self._timeframe_seconds = delta_seconds
        self._candles.append(bar)
        classified = self._engine.classify(
            candle={
                "time": bar.time,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            },
            stats=dict(dependency_output.value),
        ).as_dict()
        stabilized = self._stabilizer.stabilize(
            classified,
            bar_time=bar.time,
            timeframe_seconds=self._timeframe_seconds,
        )
        self._regime_rows[self._normalized_bar_time(bar.time)] = dict(stabilized)
        self._prune_regime_rows()
        structure = stabilized.get("structure") or {}
        volatility = stabilized.get("volatility") or {}
        liquidity = stabilized.get("liquidity") or {}
        expansion = stabilized.get("expansion") or {}
        metrics = stabilized.get("metrics") or {}
        metric_output_value = {
            key: float(value)
            for key, value in metrics.items()
            if not isinstance(value, bool) and isinstance(value, (int, float))
        }

        regime_confidence = _clamp01(
            _first_float(
                metric_output_value.get("context_trust_score"),
                structure.get("context_trust_score"),
            )
        )
        regime_conviction = _clamp01(
            _first_float(
                metric_output_value.get("score_margin"),
                structure.get("score_margin"),
            )
        )
        trend_strength = _clamp01(
            _first_float(
                metric_output_value.get("trend_score"),
                structure.get("trend_score"),
            )
        )
        trend_direction_value = _first_float(
            metric_output_value.get("trend_direction_value"),
            structure.get("trend_direction_value"),
        )
        directional_conviction = max(-1.0, min(1.0, trend_direction_value * trend_strength))
        volatility_intensity = _clamp01(
            abs(
                _first_float(
                    metric_output_value.get("atr_zscore"),
                    volatility.get("atr_zscore"),
                )
            )
            / 2.0
        )
        context_age_since_known_bars = _first_float(
            metric_output_value.get("context_age_since_known_bars"),
            structure.get("context_age_since_known_bars"),
        )
        mature_floor = float(
            getattr(self._stabilizer.config, "context_mature_after_known_bars", 2.0)
        )
        # 0.0 = newly known; 1.0 = well-established several bars beyond the maturity floor.
        regime_maturity = _clamp01((context_age_since_known_bars - mature_floor) / 4.0)

        derived_output_fields = {
            "regime_confidence": regime_confidence,
            "regime_conviction": regime_conviction,
            "trend_strength": trend_strength,
            "directional_conviction": directional_conviction,
            "volatility_intensity": volatility_intensity,
            "regime_maturity": regime_maturity,
        }

        self._context_output = RuntimeOutput(
            bar_time=bar.time,
            ready=True,
            value={
                "state_key": str(structure.get("actionable_state") or structure.get("context_regime_state") or ""),
                "fields": {
                    "regime_key": str(stabilized.get("regime_key") or ""),
                    "committed_state": str(structure.get("state") or ""),
                    "context_regime_state": str(structure.get("context_regime_state") or ""),
                    "context_regime_direction": str(structure.get("context_regime_direction") or "neutral"),
                    "actionable_state": str(
                        structure.get("actionable_state") or structure.get("context_regime_state") or ""
                    ),
                    "trend_direction": str(structure.get("trend_direction") or "neutral"),
                    "structure_confidence": float(structure.get("confidence") or 0.0),
                    "score_margin": float(structure.get("score_margin") or 0.0),
                    "volatility_state": str(volatility.get("state") or ""),
                    "liquidity_state": str(liquidity.get("state") or ""),
                    "expansion_state": str(expansion.get("state") or ""),
                    "bars_in_regime": int(structure.get("bars_in_regime") or 0),
                    "age_since_known_bars": int(structure.get("age_since_known_bars") or 0),
                    "recent_switch_count": int(structure.get("recent_switch_count") or 0),
                    "known_at_epoch": (
                        int(structure.get("known_at_epoch"))
                        if isinstance(structure.get("known_at_epoch"), (int, float))
                        else None
                    ),
                    "is_known": bool(structure.get("is_known")),
                    "is_mature": bool(structure.get("is_mature")),
                    "is_trustworthy": bool(structure.get("is_trustworthy")),
                    "trust_score": float(structure.get("trust_score") or 0.0),
                    "context_bars_in_regime": int(structure.get("context_bars_in_regime") or 0),
                    "context_age_since_known_bars": int(structure.get("context_age_since_known_bars") or 0),
                    "context_recent_switch_count": int(structure.get("context_recent_switch_count") or 0),
                    "context_known_at_epoch": (
                        int(structure.get("context_known_at_epoch"))
                        if isinstance(structure.get("context_known_at_epoch"), (int, float))
                        else None
                    ),
                    "context_is_known": bool(structure.get("context_is_known")),
                    "context_is_mature": bool(structure.get("context_is_mature")),
                    "context_is_trustworthy": bool(structure.get("context_is_trustworthy")),
                    "context_trust_score": float(structure.get("context_trust_score") or 0.0),
                    "candidate_state": (
                        str(structure.get("candidate_state"))
                        if structure.get("candidate_state") is not None
                        else None
                    ),
                    "candidate_count": int(structure.get("candidate_count") or 0),
                    "current_confirm_required": int(structure.get("current_confirm_required") or 0),
                    **derived_output_fields,
                },
            },
        )
        metric_output_value.update(derived_output_fields)
        self._metric_output = RuntimeOutput(
            bar_time=bar.time,
            ready=True,
            value=metric_output_value,
        )
        self._overlay_ready = True
        self._overlay_cache = None
        self._overlay_cache_bar_time = None

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {
            "market_regime": self._context_output,
            "regime_metrics": self._metric_output,
        }

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        if not self._overlay_ready:
            return {
                "regime": RuntimeOverlay(bar_time=self._current_bar_time, ready=False, value={}),
                "regime_markers": RuntimeOverlay(bar_time=self._current_bar_time, ready=False, value={}),
            }
        if self._overlay_cache is None or self._overlay_cache_bar_time != self._current_bar_time:
            built = build_regime_overlays(
                candles=list(self._candles),
                regime_rows=self._regime_rows,
                timeframe_seconds=max(int(self._timeframe_seconds), 1),
                regime_version=self.runtime_spec.version,
                include_change_markers=True,
                include_marker_overlay=True,
            )
            regime_overlay = next(
                (
                    dict(overlay)
                    for overlay in built
                    if isinstance(overlay, Mapping) and str(overlay.get("type") or "") == "regime_overlay"
                ),
                build_overlay(
                    "regime_overlay",
                    {
                        "boxes": [],
                        "segments": [],
                        "regime_blocks": [],
                        "regime_points": [],
                        "summary": {},
                    },
                ),
            )
            marker_overlay = next(
                (
                    dict(overlay)
                    for overlay in built
                    if isinstance(overlay, Mapping) and str(overlay.get("type") or "") == "regime_markers"
                ),
                build_overlay("regime_markers", {"markers": []}),
            )
            self._overlay_cache = {
                "regime": RuntimeOverlay(
                    bar_time=self._current_bar_time,
                    ready=True,
                    value=dict(regime_overlay),
                ),
                "regime_markers": RuntimeOverlay(
                    bar_time=self._current_bar_time,
                    ready=True,
                    value=dict(marker_overlay),
                ),
            }
            self._overlay_cache_bar_time = self._current_bar_time
        return dict(self._overlay_cache)

    @staticmethod
    def _normalized_bar_time(bar_time: datetime) -> datetime:
        return bar_time.replace(tzinfo=None) if bar_time.tzinfo is not None else bar_time

    def _prune_regime_rows(self) -> None:
        if not self._candles:
            self._regime_rows.clear()
            return
        cutoff = self._normalized_bar_time(self._candles[0].time)
        while self._regime_rows:
            first_key = next(iter(self._regime_rows))
            if first_key >= cutoff:
                break
            self._regime_rows.popitem(last=False)


__all__ = ["TypedRegimeIndicator", "resolve_regime_dependency"]
