from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Mapping, Optional

import logging

from .regime_engine import _classify_expansion, _classify_liquidity, _classify_structure, _classify_volatility
from .regime_config import RegimeStabilizerConfig

logger = logging.getLogger(__name__)


@dataclass
class _AxisState:
    current_state: Optional[str] = None
    candidate_state: Optional[str] = None
    candidate_count: int = 0


class _FeatureSmoother:
    def __init__(self, alpha: float) -> None:
        if not 0 < alpha <= 1:
            raise ValueError("EMA alpha must be in (0, 1].")
        self._alpha = alpha
        self._values: Dict[str, float] = {}

    def update(self, key: str, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        if key not in self._values:
            self._values[key] = float(value)
        else:
            prior = self._values[key]
            self._values[key] = self._alpha * float(value) + (1 - self._alpha) * prior
        return self._values[key]


class RegimeStabilizer:
    def __init__(self, config: Optional[RegimeStabilizerConfig] = None) -> None:
        self._config = config or RegimeStabilizerConfig()
        self._states: Dict[str, _AxisState] = {
            "structure": _AxisState(),
            "volatility": _AxisState(),
            "liquidity": _AxisState(),
            "expansion": _AxisState(),
        }
        self._smoother = _FeatureSmoother(self._config.smoothing_alpha)

    @property
    def config(self) -> RegimeStabilizerConfig:
        return self._config

    def stabilize(
        self,
        raw_regime: Mapping[str, Any],
        *,
        bar_time: Optional[datetime] = None,
        instrument_id: Optional[str] = None,
        timeframe_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        structure = raw_regime.get("structure") or {}
        volatility = raw_regime.get("volatility") or {}
        expansion = raw_regime.get("expansion") or {}
        liquidity = raw_regime.get("liquidity") or {}
        confidence = raw_regime.get("confidence")
        confidence_value = float(confidence) if isinstance(confidence, (int, float)) else 0.0

        raw_features: Dict[str, Optional[float]] = {
            "directional_efficiency": _as_float(structure.get("directional_efficiency")),
            "slope_stability": _as_float(structure.get("slope_stability")),
            "range_position": _as_float(structure.get("range_position")),
            "atr_ratio": _as_float(volatility.get("atr_ratio")),
            "atr_zscore": _as_float(volatility.get("atr_zscore")),
            "tr_pct": _as_float(volatility.get("tr_pct")),
            "atr_slope": _as_float(expansion.get("atr_slope")),
            "range_contraction": _as_float(expansion.get("range_contraction")),
            "overlap_pct": _as_float(expansion.get("overlap_pct")),
            "volume_zscore": _as_float(liquidity.get("volume_zscore")),
            "volume_vs_median": _as_float(liquidity.get("volume_vs_median")),
        }

        smoothed_features: Dict[str, Optional[float]] = {}
        for key, value in raw_features.items():
            if key in self._config.smoothing_features:
                smoothed_features[key] = self._smoother.update(key, value)
            else:
                smoothed_features[key] = value

        structure_features = self._select_features(
            raw_features,
            smoothed_features,
            axis="structure",
            keys=("directional_efficiency", "slope_stability", "range_position"),
        )
        volatility_features = self._select_features(
            raw_features,
            smoothed_features,
            axis="volatility",
            keys=("atr_zscore", "tr_pct", "atr_ratio"),
        )
        expansion_features = self._select_features(
            raw_features,
            smoothed_features,
            axis="expansion",
            keys=("atr_slope", "range_contraction", "overlap_pct"),
        )
        liquidity_features = self._select_features(
            raw_features,
            smoothed_features,
            axis="liquidity",
            keys=("volume_zscore", "volume_vs_median"),
        )

        desired_structure = _classify_structure(
            structure_features["directional_efficiency"],
            structure_features["slope_stability"],
            structure_features["range_position"],
        )
        desired_structure = self._apply_structure_hysteresis(
            self._states["structure"].current_state,
            desired_structure,
            structure_features["directional_efficiency"],
        )

        desired_volatility = _classify_volatility(
            volatility_features["atr_zscore"],
            volatility_features["tr_pct"],
            volatility_features["atr_ratio"],
        )
        desired_volatility = self._apply_volatility_hysteresis(
            self._states["volatility"].current_state,
            desired_volatility,
            volatility_features["atr_ratio"],
        )

        desired_expansion = _classify_expansion(
            expansion_features["atr_slope"],
            expansion_features["range_contraction"],
            expansion_features["overlap_pct"],
        )
        desired_liquidity = _classify_liquidity(
            liquidity_features["volume_zscore"],
            liquidity_features["volume_vs_median"],
        )

        stabilized_structure, structure_meta = self._confirm_state(
            axis="structure",
            desired_state=desired_structure,
            confidence=confidence_value,
            allow_override=False,
            bar_time=bar_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )
        stabilized_volatility, volatility_meta = self._confirm_state(
            axis="volatility",
            desired_state=desired_volatility,
            confidence=confidence_value,
            allow_override=self._hard_volatility_override(volatility_features),
            bar_time=bar_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )
        stabilized_expansion, expansion_meta = self._confirm_state(
            axis="expansion",
            desired_state=desired_expansion,
            confidence=confidence_value,
            allow_override=False,
            bar_time=bar_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )
        stabilized_liquidity, liquidity_meta = self._confirm_state(
            axis="liquidity",
            desired_state=desired_liquidity,
            confidence=confidence_value,
            allow_override=False,
            bar_time=bar_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )

        regime_key = _regime_key(
            stabilized_structure,
            stabilized_volatility,
            stabilized_liquidity,
            stabilized_expansion,
        )

        return {
            "structure": {
                **structure,
                "state": stabilized_structure,
                "raw_state": structure.get("state"),
                "directional_efficiency_smooth": smoothed_features.get("directional_efficiency"),
            },
            "volatility": {
                **volatility,
                "state": stabilized_volatility,
                "raw_state": volatility.get("state"),
                "atr_ratio_smooth": smoothed_features.get("atr_ratio"),
                "atr_zscore_smooth": smoothed_features.get("atr_zscore"),
            },
            "expansion": {
                **expansion,
                "state": stabilized_expansion,
                "raw_state": expansion.get("state"),
                "atr_slope_smooth": smoothed_features.get("atr_slope"),
            },
            "liquidity": {
                **liquidity,
                "state": stabilized_liquidity,
                "raw_state": liquidity.get("state"),
                "volume_zscore_smooth": smoothed_features.get("volume_zscore"),
            },
            "confidence": confidence_value,
            "regime_key": regime_key,
            "stabilization": {
                "min_confidence": self._config.min_confidence,
                "structure": structure_meta,
                "volatility": volatility_meta,
                "expansion": expansion_meta,
                "liquidity": liquidity_meta,
            },
        }

    def _select_features(
        self,
        raw_features: Mapping[str, Optional[float]],
        smoothed_features: Mapping[str, Optional[float]],
        *,
        axis: str,
        keys: Sequence[str],
    ) -> Dict[str, Optional[float]]:
        use_smoothed = axis in {name.lower() for name in self._config.smoothing_axes}
        source = smoothed_features if use_smoothed else raw_features
        return {key: source.get(key) for key in keys}

    def _apply_structure_hysteresis(
        self,
        current_state: Optional[str],
        desired_state: str,
        directional_efficiency: Optional[float],
    ) -> str:
        if directional_efficiency is None:
            return desired_state
        if current_state == "trend" and desired_state != "trend":
            if directional_efficiency > self._config.structure_exit_trend:
                return "trend"
        if current_state != "trend" and desired_state == "trend":
            if directional_efficiency < self._config.structure_enter_trend:
                return current_state or desired_state
        return desired_state

    def _apply_volatility_hysteresis(
        self,
        current_state: Optional[str],
        desired_state: str,
        atr_ratio: Optional[float],
    ) -> str:
        if atr_ratio is None:
            return desired_state
        if current_state == "high" and desired_state != "high":
            if atr_ratio > self._config.volatility_exit_high:
                return "high"
        if current_state != "high" and desired_state == "high":
            if atr_ratio < self._config.volatility_enter_high:
                return current_state or desired_state
        if current_state == "low" and desired_state != "low":
            if atr_ratio < self._config.volatility_exit_low:
                return "low"
        if current_state != "low" and desired_state == "low":
            if atr_ratio > self._config.volatility_enter_low:
                return current_state or desired_state
        return desired_state

    def _confirm_state(
        self,
        *,
        axis: str,
        desired_state: str,
        confidence: float,
        allow_override: bool,
        bar_time: Optional[datetime],
        instrument_id: Optional[str],
        timeframe_seconds: Optional[int],
    ) -> tuple[str, Dict[str, Any]]:
        axis_state = self._states[axis]
        confirm_required = int(self._config.confirm_bars.get(axis, 1))
        current_state = axis_state.current_state
        previous_state = current_state

        if current_state is None:
            axis_state.current_state = desired_state
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
            return desired_state, self._axis_meta(axis_state, confirm_required, seeded=True)

        if desired_state == current_state:
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
            return current_state, self._axis_meta(axis_state, confirm_required)

        if confidence < self._config.min_confidence and not allow_override:
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
            if previous_state != desired_state:
                logger.debug(
                    "regime_axis_switch_blocked | axis=%s from=%s desired=%s confidence=%s min_conf=%s bar_time=%s instrument_id=%s timeframe_seconds=%s",
                    axis,
                    previous_state,
                    desired_state,
                    round(confidence, 4),
                    self._config.min_confidence,
                    bar_time.isoformat() if bar_time else None,
                    instrument_id,
                    timeframe_seconds,
                )
            return current_state, self._axis_meta(axis_state, confirm_required)

        if axis_state.candidate_state != desired_state:
            axis_state.candidate_state = desired_state
            axis_state.candidate_count = 1
        else:
            axis_state.candidate_count += 1

        if axis_state.candidate_count >= max(confirm_required, 1):
            axis_state.current_state = desired_state
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
            if previous_state != axis_state.current_state:
                logger.info(
                    "regime_axis_switch_confirmed | axis=%s from=%s to=%s confidence=%s min_conf=%s confirm_required=%s bar_time=%s instrument_id=%s timeframe_seconds=%s override=%s",
                    axis,
                    previous_state,
                    axis_state.current_state,
                    round(confidence, 4),
                    self._config.min_confidence,
                    confirm_required,
                    bar_time.isoformat() if bar_time else None,
                    instrument_id,
                    timeframe_seconds,
                    allow_override,
                )

        return axis_state.current_state, self._axis_meta(axis_state, confirm_required)

    def _axis_meta(self, axis_state: _AxisState, confirm_required: int, seeded: bool = False) -> Dict[str, Any]:
        return {
            "current_state": axis_state.current_state,
            "candidate_state": axis_state.candidate_state,
            "candidate_count": axis_state.candidate_count,
            "confirm_required": confirm_required,
            "seeded": seeded,
        }

    def _hard_volatility_override(self, volatility_features: Mapping[str, Optional[float]]) -> bool:
        atr_ratio = volatility_features.get("atr_ratio")
        atr_zscore = volatility_features.get("atr_zscore")
        if atr_ratio is None and atr_zscore is None:
            return False
        if atr_ratio is not None and atr_ratio >= self._config.hard_volatility_high_atr_ratio:
            return True
        if atr_zscore is not None and atr_zscore >= self._config.hard_volatility_high_atr_zscore:
            return True
        if atr_ratio is not None and atr_ratio <= self._config.hard_volatility_low_atr_ratio:
            return True
        if atr_zscore is not None and atr_zscore <= self._config.hard_volatility_low_atr_zscore:
            return True
        return False


def _regime_key(structure: Optional[str], volatility: Optional[str], liquidity: Optional[str], expansion: Optional[str]) -> str:
    return "|".join(
        [
            (structure or "unknown").lower(),
            (volatility or "unknown").lower(),
            (liquidity or "unknown").lower(),
            (expansion or "unknown").lower(),
        ]
    )


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "RegimeStabilizer",
    "RegimeStabilizerConfig",
]
