from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class RegimeOutput:
    volatility: Dict[str, Any]
    structure: Dict[str, Any]
    expansion: Dict[str, Any]
    liquidity: Dict[str, Any]
    confidence: float

    def as_dict(self) -> Dict[str, Any]:
        return {
            "volatility": self.volatility,
            "structure": self.structure,
            "expansion": self.expansion,
            "liquidity": self.liquidity,
            "confidence": self.confidence,
        }


class RegimeEngineV1:
    """Deterministic, indicator-agnostic regime classifier."""

    version = "v1"

    def classify(self, candle: Dict[str, Any], stats: Dict[str, Any]) -> RegimeOutput:
        atr_z = _require(stats, "atr_zscore")
        tr_pct = _require(stats, "tr_pct")
        atr_ratio = _require(stats, "atr_ratio")
        directional_efficiency = _require(stats, "directional_efficiency")
        slope_stability = _require(stats, "slope_stability")
        range_position = _require(stats, "range_position")
        atr_short = _require(stats, "atr_short")
        atr_slope = _require(stats, "atr_slope")
        range_contraction = _require(stats, "range_contraction")
        overlap_pct = _require(stats, "overlap_pct")
        volume_z = _require(stats, "volume_zscore")
        volume_vs_median = _require(stats, "volume_vs_median")

        volatility_state = _classify_volatility(atr_z, tr_pct, atr_ratio)
        structure_state = _classify_structure(
            directional_efficiency,
            slope_stability,
            range_position,
            overlap_pct,
        )
        expansion_state = _classify_expansion(atr_slope, range_contraction, overlap_pct)
        liquidity_state = _classify_liquidity(volume_z, volume_vs_median)

        volatility_conf = _mean_confidence(
            _scale_abs(atr_z, 2.0),
            _scale_abs(atr_ratio - 1.0, 0.5),
            _scale_abs(tr_pct, 0.05),
        )
        structure_conf = _mean_confidence(
            _scale_abs(directional_efficiency - 0.5, 0.5),
            _scale_abs(slope_stability, 0.5),
            _scale_abs(range_position - 0.5, 0.5),
        )
        expansion_conf = _mean_confidence(
            _scale_abs(atr_slope / max(atr_short, 1e-6), 0.5),
            _scale_abs(range_contraction - 1.0, 0.5),
            _scale_abs(overlap_pct - 0.5, 0.5),
        )
        liquidity_conf = _mean_confidence(
            _scale_abs(volume_z, 2.0),
            _scale_abs(volume_vs_median - 1.0, 0.5),
        )
        confidence = round(
            _mean_confidence(volatility_conf, structure_conf, expansion_conf, liquidity_conf), 4
        )

        return RegimeOutput(
            volatility={
                "state": volatility_state,
                "atr_zscore": atr_z,
                "tr_pct": tr_pct,
                "atr_ratio": atr_ratio,
                "confidence": volatility_conf,
            },
            structure={
                "state": structure_state,
                "directional_efficiency": directional_efficiency,
                "slope_stability": slope_stability,
                "range_position": range_position,
                "confidence": structure_conf,
            },
            expansion={
                "state": expansion_state,
                "atr_slope": atr_slope,
                "atr_short": atr_short,
                "range_contraction": range_contraction,
                "overlap_pct": overlap_pct,
                "confidence": expansion_conf,
            },
            liquidity={
                "state": liquidity_state,
                "volume_zscore": volume_z,
                "volume_vs_median": volume_vs_median,
                "confidence": liquidity_conf,
            },
            confidence=confidence,
        )


def _classify_volatility(atr_z: float, tr_pct: float, atr_ratio: float) -> str:
    if atr_z <= -0.75 and atr_ratio <= 0.85 and tr_pct <= 0.008:
        return "low"
    if atr_z >= 0.75 or atr_ratio >= 1.15 or tr_pct >= 0.02:
        return "high"
    return "normal"


def _classify_structure(
    directional_efficiency: float,
    slope_stability: float,
    range_position: float,
    overlap_pct: float,
) -> str:
    if directional_efficiency >= 0.55 and slope_stability <= 0.7:
        return "trend"
    if directional_efficiency <= 0.45:
        return "range"
    return "transition"


def _classify_expansion(atr_slope: float, range_contraction: float, overlap_pct: float) -> str:
    if atr_slope >= 0 and range_contraction >= 1.0 and overlap_pct <= 0.5:
        return "expanding"
    return "compressing"


def _classify_liquidity(volume_z: float, volume_vs_median: float) -> str:
    if volume_z <= -0.75 or volume_vs_median <= 0.8:
        return "thin"
    if volume_z >= 0.75 or volume_vs_median >= 1.2:
        return "heavy"
    return "normal"


def _mean_confidence(*values: Optional[float]) -> float:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return 0.0
    return sum(filtered) / len(filtered)


def _scale_abs(value: Optional[float], scale: float) -> Optional[float]:
    if value is None:
        return None
    if scale == 0:
        return 0.0
    return min(1.0, abs(value) / scale)


def _require(stats: Dict[str, Any], key: str) -> float:
    value = stats.get(key)
    if value is None:
        raise ValueError(f"Missing required stat: {key}")
    return float(value)
