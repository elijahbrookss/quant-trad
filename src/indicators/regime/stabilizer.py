from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Mapping, Optional, Sequence

from .config import RegimeStabilizerConfig
from .engine import (
    _classify_expansion,
    _classify_liquidity,
    _classify_volatility,
    build_structure_snapshot,
)


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


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
        self._context_state = _AxisState()
        self._smoother = _FeatureSmoother(self._config.smoothing_alpha)
        self._bar_index = 0
        self._active_structure_state: Optional[str] = None
        self._structure_start_index = 0
        self._structure_start_time: Optional[datetime] = None
        self._structure_known_at_index: Optional[int] = None
        self._structure_known_at_time: Optional[datetime] = None
        self._structure_switch_indices: list[int] = []
        self._active_context_state: Optional[str] = None
        self._context_start_index = 0
        self._context_start_time: Optional[datetime] = None
        self._context_known_at_index: Optional[int] = None
        self._context_known_at_time: Optional[datetime] = None
        self._context_switch_indices: list[int] = []

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
        self._bar_index += 1
        structure = raw_regime.get("structure") or {}
        volatility = raw_regime.get("volatility") or {}
        expansion = raw_regime.get("expansion") or {}
        liquidity = raw_regime.get("liquidity") or {}
        metrics = raw_regime.get("metrics") or {}
        confidence = raw_regime.get("confidence")
        confidence_value = float(confidence) if isinstance(confidence, (int, float)) else 0.0
        structure_confidence = _as_float(structure.get("confidence"))
        volatility_confidence = _as_float(volatility.get("confidence"))
        expansion_confidence = _as_float(expansion.get("confidence"))
        liquidity_confidence = _as_float(liquidity.get("confidence"))

        raw_features: Dict[str, Optional[float]] = {
            "directional_efficiency": _as_float(structure.get("directional_efficiency")),
            "slope": _as_float(structure.get("slope")),
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
            keys=(
                "directional_efficiency",
                "slope",
                "slope_stability",
                "range_position",
                "range_contraction",
                "overlap_pct",
            ),
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

        structure_snapshot = build_structure_snapshot(
            directional_efficiency=float(structure_features["directional_efficiency"] or 0.0),
            slope=float(structure_features["slope"] or 0.0),
            slope_stability=float(structure_features["slope_stability"] or 0.0),
            range_position=float(structure_features["range_position"] or 0.5),
            range_contraction=float(structure_features["range_contraction"] or 1.0),
            overlap_pct=float(structure_features["overlap_pct"] or 0.5),
        )
        desired_structure = self._apply_structure_hysteresis(
            current_state=self._states["structure"].current_state,
            structure_snapshot=structure_snapshot,
        )

        desired_volatility = _classify_volatility(
            float(volatility_features["atr_zscore"] or 0.0),
            float(volatility_features["tr_pct"] or 0.0),
            float(volatility_features["atr_ratio"] or 1.0),
        )
        desired_volatility = self._apply_volatility_hysteresis(
            self._states["volatility"].current_state,
            desired_volatility,
            volatility_features["atr_ratio"],
            volatility_features["tr_pct"],
            volatility_features["atr_zscore"],
        )

        desired_expansion = _classify_expansion(
            float(expansion_features["atr_slope"] or 0.0),
            float(expansion_features["range_contraction"] or 1.0),
            float(expansion_features["overlap_pct"] or 0.5),
        )
        desired_liquidity = _classify_liquidity(
            float(liquidity_features["volume_zscore"] or 0.0),
            float(liquidity_features["volume_vs_median"] or 1.0),
        )

        stabilized_structure, structure_meta = self._confirm_state(
            axis="structure",
            desired_state=desired_structure,
            confidence=structure_confidence if structure_confidence is not None else confidence_value,
            min_confidence=self._config.structure_min_confidence,
            allow_override=False,
            bar_time=bar_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )
        stabilized_volatility, volatility_meta = self._confirm_state(
            axis="volatility",
            desired_state=desired_volatility,
            confidence=volatility_confidence if volatility_confidence is not None else confidence_value,
            min_confidence=self._config.min_confidence,
            allow_override=self._hard_volatility_override(volatility_features),
            bar_time=bar_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )
        stabilized_expansion, expansion_meta = self._confirm_state(
            axis="expansion",
            desired_state=desired_expansion,
            confidence=expansion_confidence if expansion_confidence is not None else confidence_value,
            min_confidence=self._config.min_confidence,
            allow_override=False,
            bar_time=bar_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )
        stabilized_liquidity, liquidity_meta = self._confirm_state(
            axis="liquidity",
            desired_state=desired_liquidity,
            confidence=liquidity_confidence if liquidity_confidence is not None else confidence_value,
            min_confidence=self._config.min_confidence,
            allow_override=False,
            bar_time=bar_time,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
        )

        structure_context = self._update_structure_context(
            stabilized_structure=stabilized_structure,
            trend_direction=str(structure_snapshot.get("trend_direction") or "neutral"),
            bar_time=bar_time,
            timeframe_seconds=timeframe_seconds,
        )
        structure_trust = self._build_structure_trust(
            stabilized_structure=stabilized_structure,
            structure_snapshot=structure_snapshot,
            structure_context=structure_context,
            structure_confidence=structure_confidence if structure_confidence is not None else confidence_value,
        )
        structure_trust["candidate_state"] = structure_meta.get("candidate_state")
        structure_trust["candidate_count"] = int(structure_meta.get("candidate_count") or 0)
        structure_trust["current_confirm_required"] = int(structure_meta.get("confirm_required") or 1)
        desired_context_state = self._derive_context_state(
            stabilized_structure=stabilized_structure,
            structure_snapshot=structure_snapshot,
            structure_context=structure_context,
            structure_trust=structure_trust,
        )
        desired_context_state = self._apply_context_hysteresis(
            current_state=self._context_state.current_state,
            desired_state=desired_context_state,
            structure_snapshot=structure_snapshot,
            structure_context=structure_context,
        )
        stabilized_context_state, context_meta = self._confirm_context_state(desired_context_state)
        context_direction = _context_direction(stabilized_context_state)
        context_regime_state = stabilized_context_state
        context_context = self._update_context_regime_context(
            stabilized_context_state=stabilized_context_state,
            bar_time=bar_time,
            timeframe_seconds=timeframe_seconds,
        )
        context_trust = self._build_context_regime_trust(
            stabilized_context_state=stabilized_context_state,
            context_regime_state=context_regime_state,
            context_context=context_context,
        )
        context_trust["context_candidate_state"] = context_meta.get("candidate_state")
        context_trust["context_candidate_count"] = int(context_meta.get("candidate_count") or 0)
        context_trust["context_confirm_required"] = int(context_meta.get("confirm_required") or 1)

        regime_key = _regime_key(
            context_regime_state,
            stabilized_volatility,
            stabilized_liquidity,
            stabilized_expansion,
        )

        metrics_payload = {
            **{
                key: float(value)
                for key, value in metrics.items()
                if not isinstance(value, bool) and isinstance(value, (int, float))
            },
            "bars_in_regime": float(structure_context["bars_in_regime"]),
            "age_since_known_bars": float(structure_context["age_since_known_bars"]),
            "recent_switch_count": float(structure_context["recent_switch_count"]),
            "structure_switch_count": float(structure_context["structure_switch_count"]),
            "trust_score": float(structure_trust["trust_score"]),
            "is_known": 1.0 if structure_trust["is_known"] else 0.0,
            "is_mature": 1.0 if structure_trust["is_mature"] else 0.0,
            "is_trustworthy": 1.0 if structure_trust["is_trustworthy"] else 0.0,
            "context_bars_in_regime": float(context_context["bars_in_regime"]),
            "context_age_since_known_bars": float(context_context["age_since_known_bars"]),
            "context_recent_switch_count": float(context_context["recent_switch_count"]),
            "context_switch_count": float(context_context["context_switch_count"]),
            "context_trust_score": float(context_trust["context_trust_score"]),
            "context_is_known": 1.0 if context_trust["context_is_known"] else 0.0,
            "context_is_mature": 1.0 if context_trust["context_is_mature"] else 0.0,
            "context_is_trustworthy": 1.0 if context_trust["context_is_trustworthy"] else 0.0,
        }

        return {
            "structure": {
                **structure,
                "state": stabilized_structure,
                "raw_state": structure.get("state"),
                "directional_efficiency_smooth": smoothed_features.get("directional_efficiency"),
                "slope_smooth": smoothed_features.get("slope"),
                "slope_stability_smooth": smoothed_features.get("slope_stability"),
                "overlap_pct_smooth": smoothed_features.get("overlap_pct"),
                "range_contraction_smooth": smoothed_features.get("range_contraction"),
                "trend_direction": structure_snapshot.get("trend_direction"),
                "trend_direction_value": structure_snapshot.get("trend_direction_value"),
                "trend_score": structure_snapshot.get("trend_score"),
                "range_score": structure_snapshot.get("range_score"),
                "transition_score": structure_snapshot.get("transition_score"),
                "winner_state": structure_snapshot.get("winner_state"),
                "winner_score": structure_snapshot.get("winner_score"),
                "runner_up_state": structure_snapshot.get("runner_up_state"),
                "score_margin": structure_snapshot.get("score_margin"),
                **{
                    "structure_bars_in_regime": structure_context.get("bars_in_regime"),
                    "structure_age_since_known_bars": structure_context.get("age_since_known_bars"),
                    "structure_known_at_epoch": structure_context.get("known_at_epoch"),
                    "structure_regime_start_epoch": structure_context.get("regime_start_epoch"),
                    "structure_recent_switch_count": structure_context.get("recent_switch_count"),
                    "structure_switch_count": structure_context.get("structure_switch_count"),
                },
                **{
                    "structure_is_known": structure_trust.get("is_known"),
                    "structure_is_mature": structure_trust.get("is_mature"),
                    "structure_is_trustworthy": structure_trust.get("is_trustworthy"),
                    "structure_trust_score": structure_trust.get("trust_score"),
                    "structure_candidate_state": structure_trust.get("candidate_state"),
                    "structure_candidate_count": structure_trust.get("candidate_count"),
                    "structure_current_confirm_required": structure_trust.get("current_confirm_required"),
                },
                "context_regime_state": context_regime_state,
                "context_regime_direction": context_direction,
                "context_regime_internal_state": stabilized_context_state,
                "context_is_known": context_trust.get("context_is_known"),
                "context_is_mature": context_trust.get("context_is_mature"),
                "context_is_trustworthy": context_trust.get("context_is_trustworthy"),
                "context_trust_score": context_trust.get("context_trust_score"),
                "context_known_at_epoch": context_context.get("known_at_epoch"),
                "context_regime_start_epoch": context_context.get("regime_start_epoch"),
                "context_bars_in_regime": context_context.get("bars_in_regime"),
                "context_age_since_known_bars": context_context.get("age_since_known_bars"),
                "context_recent_switch_count": context_context.get("recent_switch_count"),
                "context_switch_count": context_context.get("context_switch_count"),
                "context_candidate_state": context_trust.get("context_candidate_state"),
                "context_candidate_count": context_trust.get("context_candidate_count"),
                "context_confirm_required": context_trust.get("context_confirm_required"),
                "is_known": context_trust.get("context_is_known"),
                "is_mature": context_trust.get("context_is_mature"),
                "is_trustworthy": context_trust.get("context_is_trustworthy"),
                "trust_score": context_trust.get("context_trust_score"),
                "bars_in_regime": context_context.get("bars_in_regime"),
                "age_since_known_bars": context_context.get("age_since_known_bars"),
                "known_at_epoch": context_context.get("known_at_epoch"),
                "regime_start_epoch": context_context.get("regime_start_epoch"),
                "recent_switch_count": context_context.get("recent_switch_count"),
                "actionable_state": context_trust.get("actionable_state"),
                "candidate_state": context_trust.get("context_candidate_state"),
                "candidate_count": context_trust.get("context_candidate_count"),
                "current_confirm_required": context_trust.get("context_confirm_required"),
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
                "range_contraction_smooth": smoothed_features.get("range_contraction"),
            },
            "liquidity": {
                **liquidity,
                "state": stabilized_liquidity,
                "raw_state": liquidity.get("state"),
                "volume_zscore_smooth": smoothed_features.get("volume_zscore"),
            },
            "metrics": metrics_payload,
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
        *,
        current_state: Optional[str],
        structure_snapshot: Mapping[str, Any],
    ) -> str:
        desired_state = str(structure_snapshot.get("state") or "transition")
        winner_score = float(structure_snapshot.get("winner_score") or 0.0)
        score_margin = float(structure_snapshot.get("score_margin") or 0.0)
        trend_score = float(structure_snapshot.get("trend_score") or 0.0)
        range_score = float(structure_snapshot.get("range_score") or 0.0)
        transition_score = float(structure_snapshot.get("transition_score") or 0.0)
        if current_state is None:
            return desired_state
        if desired_state == current_state:
            return desired_state
        current_age_bars = self._current_structure_age_bars(current_state)
        current_hold_bars = max(int(self._config.structure_hold_bars.get(current_state, 1)), 1)
        current_score = self._structure_state_score(
            current_state=current_state,
            trend_score=trend_score,
            range_score=range_score,
            transition_score=transition_score,
        )
        if current_state in {"trend", "range"}:
            if current_age_bars < current_hold_bars and current_score >= self._config.structure_exit_score - 0.03:
                return current_state
            if desired_state == "transition":
                if current_score >= self._config.structure_exit_score:
                    return current_state
                if transition_score < self._config.structure_transition_enter_score:
                    return current_state
                if score_margin < self._config.structure_transition_min_margin:
                    return current_state
                return "transition"
        if desired_state in {"trend", "range"}:
            required_score = self._config.structure_enter_score
            required_margin = self._config.structure_min_margin
            if current_state in {"trend", "range"} and desired_state != current_state:
                required_score += self._config.structure_reversal_extra_score
                required_margin += self._config.structure_reversal_extra_margin
            if winner_score < required_score:
                return current_state
            if score_margin < required_margin:
                return current_state
            return desired_state
        if current_state == "trend" and trend_score >= self._config.structure_exit_score:
            if score_margin <= self._config.structure_transition_margin:
                return "trend"
        if current_state == "range" and range_score >= self._config.structure_exit_score:
            if score_margin <= self._config.structure_transition_margin:
                return "range"
        return desired_state

    def _apply_volatility_hysteresis(
        self,
        current_state: Optional[str],
        desired_state: str,
        atr_ratio: Optional[float],
        tr_pct: Optional[float],
        atr_zscore: Optional[float],
    ) -> str:
        if atr_ratio is None or tr_pct is None or atr_zscore is None:
            return desired_state
        high_enter = (
            atr_ratio >= self._config.volatility_enter_high
            or tr_pct >= self._config.volatility_enter_high_tr_pct
            or atr_zscore >= 0.75
        )
        high_exit = (
            atr_ratio <= self._config.volatility_exit_high
            and tr_pct <= self._config.volatility_exit_high_tr_pct
            and atr_zscore < 0.75
        )
        low_enter = (
            atr_ratio <= self._config.volatility_enter_low
            and tr_pct <= self._config.volatility_enter_low_tr_pct
            and atr_zscore <= -0.75
        )
        low_exit = (
            atr_ratio >= self._config.volatility_exit_low
            or tr_pct >= self._config.volatility_exit_low_tr_pct
            or atr_zscore > -0.75
        )

        if current_state == "high":
            return "high" if not high_exit else desired_state
        if current_state == "low":
            return "low" if not low_exit else desired_state
        if desired_state == "high" and not high_enter:
            return current_state or desired_state
        if desired_state == "low" and not low_enter:
            return current_state or desired_state
        return desired_state

    def _confirm_state(
        self,
        *,
        axis: str,
        desired_state: str,
        confidence: float,
        min_confidence: float,
        allow_override: bool,
        bar_time: Optional[datetime],
        instrument_id: Optional[str],
        timeframe_seconds: Optional[int],
    ) -> tuple[str, Dict[str, Any]]:
        axis_state = self._states[axis]
        confirm_required = int(self._config.confirm_bars.get(axis, 1))
        current_state = axis_state.current_state
        previous_state = current_state
        if (
            axis == "structure"
            and desired_state == "transition"
            and current_state in {"trend", "range"}
        ):
            confirm_required = max(
                confirm_required,
                int(self._config.structure_transition_confirm_bars),
            )
        if (
            axis == "structure"
            and previous_state == "trend"
            and desired_state == "range"
        ):
            confirm_required = max(
                confirm_required,
                int(self._config.structure_range_after_trend_hold_bars),
            )
        if (
            axis == "structure"
            and previous_state in {"trend", "range"}
            and desired_state in {"trend", "range"}
            and previous_state != desired_state
        ):
            confirm_required = max(
                confirm_required,
                int(self._config.structure_reversal_confirm_bars),
            )

        if current_state is None:
            axis_state.current_state = desired_state
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
            return desired_state, self._axis_meta(axis_state, confirm_required, seeded=True)

        if desired_state == current_state:
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
            return current_state, self._axis_meta(axis_state, confirm_required)

        if confidence < min_confidence and not allow_override:
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
            if previous_state != desired_state and self._config.log_axis_switch_blocked:
                logger.debug(
                    "regime_axis_switch_blocked | axis=%s from=%s desired=%s confidence=%s min_conf=%s bar_time=%s instrument_id=%s timeframe_seconds=%s",
                    axis,
                    previous_state,
                    desired_state,
                    round(confidence, 4),
                    min_confidence,
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
            if previous_state != axis_state.current_state and self._config.log_axis_switch_confirmed:
                logger.info(
                    "regime_axis_switch_confirmed | axis=%s from=%s to=%s confidence=%s min_conf=%s confirm_required=%s bar_time=%s instrument_id=%s timeframe_seconds=%s override=%s",
                    axis,
                    previous_state,
                    axis_state.current_state,
                    round(confidence, 4),
                    min_confidence,
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

    def _update_structure_context(
        self,
        *,
        stabilized_structure: str,
        trend_direction: str,
        bar_time: Optional[datetime],
        timeframe_seconds: Optional[int],
    ) -> Dict[str, Any]:
        if self._active_structure_state != stabilized_structure:
            if self._active_structure_state is not None:
                self._structure_switch_indices.append(self._bar_index)
            self._active_structure_state = stabilized_structure
            self._structure_start_index = self._bar_index
            self._structure_start_time = bar_time
            self._structure_known_at_index = None
            self._structure_known_at_time = None

        bars_in_regime = max(self._bar_index - self._structure_start_index + 1, 1)
        min_block_bars = max(int(self._config.structure_block_min_bars), 1)
        if self._structure_known_at_index is None and bars_in_regime >= min_block_bars:
            self._structure_known_at_index = self._structure_start_index + min_block_bars - 1
            if self._structure_start_time is not None and timeframe_seconds and timeframe_seconds > 0:
                self._structure_known_at_time = self._structure_start_time + timedelta(
                    seconds=int(timeframe_seconds) * (min_block_bars - 1)
                )
            else:
                self._structure_known_at_time = self._structure_start_time

        recent_window = max(int(self._config.structure_recent_switch_window_bars), 1)
        recent_switch_count = sum(
            1 for idx in self._structure_switch_indices if idx > self._bar_index - recent_window
        )
        age_since_known_bars = (
            max(self._bar_index - self._structure_known_at_index + 1, 0)
            if self._structure_known_at_index is not None
            else 0
        )
        known_at_epoch = (
            int(self._structure_known_at_time.timestamp())
            if isinstance(self._structure_known_at_time, datetime)
            else None
        )
        start_epoch = (
            int(self._structure_start_time.timestamp())
            if isinstance(self._structure_start_time, datetime)
            else None
        )
        return {
            "bars_in_regime": bars_in_regime,
            "age_since_known_bars": age_since_known_bars,
            "known_at_epoch": known_at_epoch,
            "regime_start_epoch": start_epoch,
            "recent_switch_count": recent_switch_count,
            "structure_switch_count": len(self._structure_switch_indices),
            "trend_direction": trend_direction if stabilized_structure == "trend" else "neutral",
        }

    def _build_structure_trust(
        self,
        *,
        stabilized_structure: str,
        structure_snapshot: Mapping[str, Any],
        structure_context: Mapping[str, Any],
        structure_confidence: float,
    ) -> Dict[str, Any]:
        score_margin = float(structure_snapshot.get("score_margin") or 0.0)
        winner_score = float(structure_snapshot.get("winner_score") or 0.0)
        bars_in_regime = int(structure_context.get("bars_in_regime") or 0)
        age_since_known_bars = int(structure_context.get("age_since_known_bars") or 0)
        recent_switch_count = int(structure_context.get("recent_switch_count") or 0)
        known_at_epoch = structure_context.get("known_at_epoch")
        is_known = isinstance(known_at_epoch, int)
        is_mature = is_known and age_since_known_bars >= int(self._config.structure_mature_after_known_bars)
        confidence_component = _bounded_ratio(
            structure_confidence,
            self._config.structure_trust_min_confidence,
            0.24,
        )
        margin_component = _bounded_ratio(
            score_margin,
            self._config.structure_trust_min_margin,
            0.16,
        )
        maturity_component = _bounded_ratio(
            float(age_since_known_bars),
            float(self._config.structure_mature_after_known_bars),
            4.0,
        )
        stability_penalty = _clamp01(
            recent_switch_count / max(float(self._config.structure_trust_max_recent_switches), 1.0)
        )
        winner_component = _bounded_ratio(winner_score, 0.56, 0.22)
        trust_score = _clamp01(
            (confidence_component * 0.34)
            + (margin_component * 0.28)
            + (maturity_component * 0.20)
            + (winner_component * 0.18)
            - (stability_penalty * 0.18)
        )
        is_trustworthy = (
            stabilized_structure in {"trend", "range"}
            and is_mature
            and structure_confidence >= self._config.structure_trust_min_confidence
            and score_margin >= self._config.structure_trust_min_margin
            and recent_switch_count <= self._config.structure_trust_max_recent_switches
        )
        return {
            "is_known": is_known,
            "is_mature": is_mature,
            "is_trustworthy": is_trustworthy,
            "trust_score": round(float(trust_score), 4),
            "actionable_state": stabilized_structure if is_trustworthy else "transition",
            "candidate_state": self._states["structure"].candidate_state,
            "candidate_count": self._states["structure"].candidate_count,
            "current_confirm_required": int(self._config.confirm_bars.get("structure", 1)),
            "bars_since_known": age_since_known_bars,
            "bars_in_regime": bars_in_regime,
        }

    def _current_structure_age_bars(self, current_state: Optional[str]) -> int:
        if not current_state or self._active_structure_state != current_state:
            return 0
        return max(self._bar_index - self._structure_start_index + 1, 1)

    def _derive_context_state(
        self,
        *,
        stabilized_structure: str,
        structure_snapshot: Mapping[str, Any],
        structure_context: Mapping[str, Any],
        structure_trust: Mapping[str, Any],
    ) -> str:
        trend_direction = _normalized_context_direction(structure_snapshot.get("trend_direction"))
        age_since_known_bars = int(structure_context.get("age_since_known_bars") or 0)
        score_margin = float(structure_snapshot.get("score_margin") or 0.0)
        structure_is_known = bool(structure_trust.get("is_known"))
        if (
            stabilized_structure == "trend"
            and structure_is_known
            and trend_direction in {"up", "down"}
            and age_since_known_bars >= int(self._config.context_trend_promote_after_known_bars)
            and score_margin >= max(float(self._config.structure_min_margin), 0.08)
        ):
            return f"trend_{trend_direction}"
        if (
            stabilized_structure == "range"
            and structure_is_known
            and age_since_known_bars >= int(self._config.context_range_promote_after_known_bars)
        ):
            return "range"
        if stabilized_structure == "trend" and trend_direction in {"up", "down"}:
            return f"transition_{trend_direction}"
        if stabilized_structure == "transition" and trend_direction in {"up", "down"}:
            return f"transition_{trend_direction}"
        return "transition_neutral"

    def _apply_context_hysteresis(
        self,
        *,
        current_state: Optional[str],
        desired_state: str,
        structure_snapshot: Mapping[str, Any],
        structure_context: Mapping[str, Any],
    ) -> str:
        if current_state is None or desired_state == current_state:
            return desired_state
        current_age_bars = self._current_context_age_bars(current_state)
        current_hold_bars = max(int(self._config.context_hold_bars.get(current_state, 1)), 1)
        score_margin = float(structure_snapshot.get("score_margin") or 0.0)
        age_since_known_bars = int(structure_context.get("age_since_known_bars") or 0)

        if current_state in {"trend_up", "trend_down", "range"} and desired_state.startswith("transition_"):
            if current_age_bars < current_hold_bars:
                return current_state
            if current_state == "range" and desired_state == "transition_neutral":
                return current_state
            return desired_state

        if current_state == "range" and desired_state in {"trend_up", "trend_down"}:
            if age_since_known_bars < int(self._config.context_trend_promote_after_known_bars):
                return "range"
            if score_margin < max(float(self._config.structure_trust_min_margin), 0.12):
                return "range"
            return f"transition_{desired_state.split('_', 1)[1]}"

        if current_state == "range" and desired_state in {"transition_up", "transition_down"}:
            if age_since_known_bars < int(self._config.context_trend_promote_after_known_bars):
                return "range"
            if score_margin < max(float(self._config.structure_min_margin), 0.08):
                return "range"
            return desired_state

        if current_state in {"trend_up", "trend_down"} and desired_state == "range":
            if current_age_bars < current_hold_bars:
                return current_state
            if age_since_known_bars < int(self._config.context_range_promote_after_known_bars):
                return "transition_neutral"
            return "range"

        if (
            current_state in {"trend_up", "trend_down"}
            and desired_state in {"trend_up", "trend_down"}
            and desired_state != current_state
        ):
            return f"transition_{desired_state.split('_', 1)[1]}"

        if current_state in {"transition_up", "transition_down"} and desired_state == "range":
            if current_age_bars < current_hold_bars:
                return current_state
            return "transition_neutral"

        if current_state in {"transition_up", "transition_down"} and desired_state in {"trend_up", "trend_down"}:
            current_direction = current_state.split("_", 1)[1]
            desired_direction = desired_state.split("_", 1)[1]
            if current_direction != desired_direction and current_age_bars < current_hold_bars:
                return current_state
            return desired_state

        if current_state in {"transition_up", "transition_down"} and desired_state in {"transition_up", "transition_down"}:
            current_direction = current_state.split("_", 1)[1]
            desired_direction = desired_state.split("_", 1)[1]
            if current_direction != desired_direction and current_age_bars < current_hold_bars:
                return "transition_neutral"
            return desired_state

        if current_state == "transition_neutral" and desired_state in {"trend_up", "trend_down"}:
            if age_since_known_bars < int(self._config.context_trend_promote_after_known_bars):
                return "transition_neutral"
            if score_margin < max(float(self._config.structure_trust_min_margin), 0.12):
                return "transition_neutral"
            return desired_state

        return desired_state

    def _confirm_context_state(self, desired_state: str) -> tuple[str, Dict[str, Any]]:
        axis_state = self._context_state
        current_state = axis_state.current_state
        confirm_required = int(self._config.context_confirm_bars)
        if desired_state.startswith("transition_") and current_state in {"trend_up", "trend_down", "range"}:
            confirm_required = max(confirm_required, int(self._config.context_transition_confirm_bars))
        if (
            current_state == "range"
            and desired_state in {"trend_up", "trend_down"}
        ):
            confirm_required = max(confirm_required, int(self._config.context_reversal_confirm_bars))
        if (
            current_state in {"trend_up", "trend_down"}
            and desired_state == "range"
        ):
            confirm_required = max(confirm_required, int(self._config.context_reversal_confirm_bars))
        if (
            current_state in {"trend_up", "trend_down"}
            and desired_state in {"trend_up", "trend_down"}
            and desired_state != current_state
        ):
            confirm_required = max(confirm_required, int(self._config.context_reversal_confirm_bars))
        if (
            current_state in {"transition_up", "transition_down"}
            and desired_state in {"transition_up", "transition_down"}
            and desired_state != current_state
        ):
            confirm_required = max(confirm_required, int(self._config.context_reversal_confirm_bars))
        if (
            current_state == "transition_neutral"
            and desired_state in {"transition_up", "transition_down"}
        ):
            confirm_required = max(confirm_required, int(self._config.context_reversal_confirm_bars))

        if current_state is None:
            axis_state.current_state = desired_state
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
            return desired_state, self._axis_meta(axis_state, confirm_required, seeded=True)

        if desired_state == current_state:
            axis_state.candidate_state = None
            axis_state.candidate_count = 0
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

        return axis_state.current_state or desired_state, self._axis_meta(axis_state, confirm_required)

    def _update_context_regime_context(
        self,
        *,
        stabilized_context_state: str,
        bar_time: Optional[datetime],
        timeframe_seconds: Optional[int],
    ) -> Dict[str, Any]:
        if self._active_context_state != stabilized_context_state:
            if self._active_context_state is not None:
                self._context_switch_indices.append(self._bar_index)
            self._active_context_state = stabilized_context_state
            self._context_start_index = self._bar_index
            self._context_start_time = bar_time
            self._context_known_at_index = None
            self._context_known_at_time = None

        bars_in_regime = max(self._bar_index - self._context_start_index + 1, 1)
        min_block_bars = max(int(self._config.context_block_min_bars), 1)
        if self._context_known_at_index is None and bars_in_regime >= min_block_bars:
            self._context_known_at_index = self._context_start_index + min_block_bars - 1
            if self._context_start_time is not None and timeframe_seconds and timeframe_seconds > 0:
                self._context_known_at_time = self._context_start_time + timedelta(
                    seconds=int(timeframe_seconds) * (min_block_bars - 1)
                )
            else:
                self._context_known_at_time = self._context_start_time

        recent_window = max(int(self._config.context_recent_switch_window_bars), 1)
        recent_switch_count = sum(
            1 for idx in self._context_switch_indices if idx > self._bar_index - recent_window
        )
        age_since_known_bars = (
            max(self._bar_index - self._context_known_at_index + 1, 0)
            if self._context_known_at_index is not None
            else 0
        )
        known_at_epoch = (
            int(self._context_known_at_time.timestamp())
            if isinstance(self._context_known_at_time, datetime)
            else None
        )
        start_epoch = (
            int(self._context_start_time.timestamp())
            if isinstance(self._context_start_time, datetime)
            else None
        )
        return {
            "bars_in_regime": bars_in_regime,
            "age_since_known_bars": age_since_known_bars,
            "known_at_epoch": known_at_epoch,
            "regime_start_epoch": start_epoch,
            "recent_switch_count": recent_switch_count,
            "context_switch_count": len(self._context_switch_indices),
        }

    def _build_context_regime_trust(
        self,
        *,
        stabilized_context_state: str,
        context_regime_state: str,
        context_context: Mapping[str, Any],
    ) -> Dict[str, Any]:
        age_since_known_bars = int(context_context.get("age_since_known_bars") or 0)
        recent_switch_count = int(context_context.get("recent_switch_count") or 0)
        known_at_epoch = context_context.get("known_at_epoch")
        is_known = isinstance(known_at_epoch, int)
        is_mature = is_known and age_since_known_bars >= int(self._config.context_mature_after_known_bars)
        maturity_component = _bounded_ratio(
            float(age_since_known_bars),
            float(self._config.context_mature_after_known_bars),
            4.0,
        )
        stability_penalty = _clamp01(
            recent_switch_count / max(float(self._config.context_trust_max_recent_switches), 1.0)
        )
        if context_regime_state in {"trend_up", "trend_down", "range"}:
            state_component = 1.0
        elif context_regime_state in {"transition_up", "transition_down"}:
            state_component = 0.45
        else:
            state_component = 0.2
        trust_score = _clamp01(
            (state_component * 0.50)
            + (maturity_component * 0.28)
            + ((1.0 - stability_penalty) * 0.22)
        )
        is_trustworthy = (
            context_regime_state in {"trend_up", "trend_down", "range"}
            and is_mature
            and recent_switch_count <= self._config.context_trust_max_recent_switches
        )
        return {
            "context_regime_state": context_regime_state,
            "context_regime_direction": _context_direction(stabilized_context_state),
            "context_is_known": is_known,
            "context_is_mature": is_mature,
            "context_is_trustworthy": is_trustworthy,
            "context_trust_score": round(float(trust_score), 4),
            "actionable_state": context_regime_state,
        }

    def _current_context_age_bars(self, current_state: Optional[str]) -> int:
        if not current_state or self._active_context_state != current_state:
            return 0
        return max(self._bar_index - self._context_start_index + 1, 1)

    @staticmethod
    def _structure_state_score(
        *,
        current_state: Optional[str],
        trend_score: float,
        range_score: float,
        transition_score: float,
    ) -> float:
        if current_state == "trend":
            return trend_score
        if current_state == "range":
            return range_score
        return transition_score


def _regime_key(
    structure: Optional[str],
    volatility: Optional[str],
    liquidity: Optional[str],
    expansion: Optional[str],
) -> str:
    return "|".join(
        [
            (structure or "unknown").lower(),
            (volatility or "unknown").lower(),
            (liquidity or "unknown").lower(),
            (expansion or "unknown").lower(),
        ]
    )


def _as_float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _bounded_ratio(value: float, floor: float, span: float) -> float:
    if span <= 0:
        return 0.0
    return _clamp01((float(value) - float(floor)) / float(span))


def _normalized_context_direction(value: Any) -> str:
    direction = str(value or "neutral").strip().lower()
    if direction in {"up", "down"}:
        return direction
    return "neutral"


def _context_direction(context_state: str) -> str:
    normalized = str(context_state or "").strip().lower()
    if normalized.endswith("_up"):
        return "up"
    if normalized.endswith("_down"):
        return "down"
    return "neutral"


__all__ = ["RegimeStabilizer"]
