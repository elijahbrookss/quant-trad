from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Mapping, Optional


@dataclass(frozen=True)
class RegimeOutput:
    volatility: Dict[str, Any]
    structure: Dict[str, Any]
    expansion: Dict[str, Any]
    liquidity: Dict[str, Any]
    metrics: Dict[str, float]
    confidence: float

    def as_dict(self) -> Dict[str, Any]:
        return {
            "volatility": self.volatility,
            "structure": self.structure,
            "expansion": self.expansion,
            "liquidity": self.liquidity,
            "metrics": self.metrics,
            "confidence": self.confidence,
        }


@dataclass(frozen=True)
class StructureEvidence:
    state: str
    winner_state: str
    winner_score: float
    runner_up_state: str
    runner_up_score: float
    score_margin: float
    trend_score: float
    range_score: float
    transition_score: float
    trend_direction: str
    trend_direction_value: int

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RegimeEngine:
    """Deterministic, indicator-agnostic regime classifier."""

    version = "v1"

    def classify(self, candle: Mapping[str, Any], stats: Mapping[str, Any]) -> RegimeOutput:
        _ = candle
        atr_z = _require(stats, "atr_zscore")
        tr_pct = _require(stats, "tr_pct")
        atr_ratio = _require(stats, "atr_ratio")
        directional_efficiency = _require(stats, "directional_efficiency")
        slope_stability = _require(stats, "slope_stability")
        slope = _require(stats, "slope")
        range_position = _require(stats, "range_position")
        atr_short = _require(stats, "atr_short")
        atr_slope = _require(stats, "atr_slope")
        range_contraction = _require(stats, "range_contraction")
        overlap_pct = _require(stats, "overlap_pct")
        volume_z = _require(stats, "volume_zscore")
        volume_vs_median = _require(stats, "volume_vs_median")

        structure_evidence = build_structure_evidence(
            directional_efficiency=directional_efficiency,
            slope_stability=slope_stability,
            slope=slope,
            range_position=range_position,
            range_contraction=range_contraction,
            overlap_pct=overlap_pct,
        )
        structure_snapshot = structure_evidence.as_dict()
        volatility_state = _classify_volatility(atr_z, tr_pct, atr_ratio)
        expansion_state = _classify_expansion(atr_slope, range_contraction, overlap_pct)
        liquidity_state = _classify_liquidity(volume_z, volume_vs_median)

        volatility_conf = _mean_confidence(
            _scale_abs(atr_z, 2.0),
            _scale_abs(atr_ratio - 1.0, 0.45),
            _scale_abs(tr_pct, 0.03),
        )
        structure_conf = _clamp01(
            0.7 * float(structure_snapshot["winner_score"])
            + 0.3 * _clamp01(float(structure_snapshot["score_margin"]) / 0.25)
        )
        expansion_conf = _mean_confidence(
            _scale_abs(_safe_div(atr_slope, max(atr_short, 1e-6)), 0.35),
            _scale_abs(range_contraction - 1.0, 0.25),
            _scale_abs(overlap_pct - 0.5, 0.35),
        )
        liquidity_conf = _mean_confidence(
            _scale_abs(volume_z, 2.0),
            _scale_abs(volume_vs_median - 1.0, 0.4),
        )
        confidence = round(
            _mean_confidence(volatility_conf, structure_conf, expansion_conf, liquidity_conf), 4
        )

        metrics = {
            "trend_score": float(structure_snapshot["trend_score"]),
            "range_score": float(structure_snapshot["range_score"]),
            "transition_score": float(structure_snapshot["transition_score"]),
            "structure_confidence": float(structure_conf),
            "score_margin": float(structure_snapshot["score_margin"]),
            "winner_score": float(structure_snapshot["winner_score"]),
            "directional_efficiency": directional_efficiency,
            "slope": slope,
            "slope_stability": slope_stability,
            "range_position": range_position,
            "range_contraction": range_contraction,
            "overlap_pct": overlap_pct,
            "atr_ratio": atr_ratio,
            "atr_zscore": atr_z,
            "tr_pct": tr_pct,
            "atr_slope": atr_slope,
            "volume_zscore": volume_z,
            "volume_vs_median": volume_vs_median,
            "trend_direction_value": float(structure_snapshot["trend_direction_value"]),
        }

        return RegimeOutput(
            volatility={
                "state": volatility_state,
                "atr_zscore": atr_z,
                "tr_pct": tr_pct,
                "atr_ratio": atr_ratio,
                "confidence": volatility_conf,
            },
            structure={
                "state": str(structure_snapshot["state"]),
                "directional_efficiency": directional_efficiency,
                "slope": slope,
                "slope_stability": slope_stability,
                "range_position": range_position,
                "range_contraction": range_contraction,
                "overlap_pct": overlap_pct,
                "trend_score": float(structure_snapshot["trend_score"]),
                "range_score": float(structure_snapshot["range_score"]),
                "transition_score": float(structure_snapshot["transition_score"]),
                "score_margin": float(structure_snapshot["score_margin"]),
                "winner_score": float(structure_snapshot["winner_score"]),
                "winner_state": str(structure_snapshot["winner_state"]),
                "runner_up_state": str(structure_snapshot["runner_up_state"]),
                "trend_direction": str(structure_snapshot["trend_direction"]),
                "trend_direction_value": int(structure_snapshot["trend_direction_value"]),
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
            metrics=metrics,
            confidence=confidence,
        )


def build_structure_snapshot(
    *,
    directional_efficiency: float,
    slope_stability: float,
    slope: float,
    range_position: float,
    range_contraction: float,
    overlap_pct: float,
) -> Dict[str, Any]:
    return build_structure_evidence(
        directional_efficiency=directional_efficiency,
        slope_stability=slope_stability,
        slope=slope,
        range_position=range_position,
        range_contraction=range_contraction,
        overlap_pct=overlap_pct,
    ).as_dict()


def build_structure_evidence(
    *,
    directional_efficiency: float,
    slope_stability: float,
    slope: float,
    range_position: float,
    range_contraction: float,
    overlap_pct: float,
) -> StructureEvidence:
    trend_efficiency = _clamp01((directional_efficiency - 0.48) / 0.28)
    range_efficiency = _clamp01((0.56 - directional_efficiency) / 0.28)
    slope_support = _clamp01((0.85 - slope_stability) / 1.35)
    low_overlap = 1.0 - _clamp01((overlap_pct - 0.32) / 0.38)
    high_overlap = _clamp01((overlap_pct - 0.45) / 0.3)
    centered_position = 1.0 - _clamp01(abs(range_position - 0.5) / 0.5)
    edge_position = _clamp01(abs(range_position - 0.5) / 0.5)
    compression_support = _clamp01((1.04 - range_contraction) / 0.18)
    expansion_support = _clamp01((range_contraction - 0.98) / 0.22)

    trend_score = _weighted_mean(
        (trend_efficiency, 0.42),
        (slope_support, 0.20),
        (low_overlap, 0.23),
        (edge_position, 0.05),
        (expansion_support, 0.10),
    )
    range_score = _weighted_mean(
        (range_efficiency, 0.34),
        (high_overlap, 0.30),
        (centered_position, 0.22),
        (compression_support, 0.14),
    )
    middle_efficiency = 1.0 - _clamp01(abs(directional_efficiency - 0.5) / 0.18)
    overlap_mixed = 1.0 - _clamp01(abs(overlap_pct - 0.5) / 0.24)
    score_contest = 1.0 - _clamp01(abs(trend_score - range_score) / 0.30)
    transition_score = _weighted_mean(
        (middle_efficiency, 0.42),
        (score_contest, 0.34),
        (overlap_mixed, 0.14),
        (1.0 - max(trend_score, range_score), 0.10),
    )

    ranked = sorted(
        {
            "trend": trend_score,
            "range": range_score,
            "transition": transition_score,
        }.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    winner_state, winner_score = ranked[0]
    runner_up_state, runner_up_score = ranked[1]
    score_margin = max(float(winner_score) - float(runner_up_score), 0.0)
    state = _classify_structure_from_scores(winner_state, winner_score, score_margin)
    trend_direction_value = 1 if slope > 0 else -1 if slope < 0 else 0
    trend_direction = "up" if trend_direction_value > 0 else "down" if trend_direction_value < 0 else "neutral"
    return StructureEvidence(
        state=state,
        winner_state=winner_state,
        winner_score=round(float(winner_score), 4),
        runner_up_state=runner_up_state,
        runner_up_score=round(float(runner_up_score), 4),
        score_margin=round(float(score_margin), 4),
        trend_score=round(float(trend_score), 4),
        range_score=round(float(range_score), 4),
        transition_score=round(float(transition_score), 4),
        trend_direction=trend_direction,
        trend_direction_value=trend_direction_value,
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
    *,
    slope: float = 0.0,
    range_contraction: float = 1.0,
) -> str:
    return str(
        build_structure_snapshot(
            directional_efficiency=directional_efficiency,
            slope_stability=slope_stability,
            slope=slope,
            range_position=range_position,
            range_contraction=range_contraction,
            overlap_pct=overlap_pct,
        )["state"]
    )


def _classify_structure_from_scores(winner_state: str, winner_score: float, score_margin: float) -> str:
    if winner_state == "trend" and winner_score >= 0.56 and score_margin >= 0.08:
        return "trend"
    if winner_state == "range" and winner_score >= 0.54 and score_margin >= 0.06:
        return "range"
    return "transition"


def _classify_expansion(atr_slope: float, range_contraction: float, overlap_pct: float) -> str:
    normalized_slope = _safe_div(atr_slope, max(abs(atr_slope), 1.0))
    if normalized_slope >= 0 and range_contraction >= 1.03 and overlap_pct <= 0.45:
        return "expanding"
    if normalized_slope <= 0 and range_contraction <= 0.97 and overlap_pct >= 0.55:
        return "compressing"
    return "stable"


def _classify_liquidity(volume_z: float, volume_vs_median: float) -> str:
    if volume_z <= -0.75 or volume_vs_median <= 0.8:
        return "thin"
    if volume_z >= 0.75 or volume_vs_median >= 1.2:
        return "heavy"
    return "normal"


def _mean_confidence(*values: Optional[float]) -> float:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return 0.0
    return sum(filtered) / len(filtered)


def _weighted_mean(*pairs: tuple[float, float]) -> float:
    numerator = 0.0
    denominator = 0.0
    for value, weight in pairs:
        numerator += float(value) * float(weight)
        denominator += float(weight)
    if denominator <= 0:
        return 0.0
    return _clamp01(numerator / denominator)


def _scale_abs(value: Optional[float], scale: float) -> Optional[float]:
    if value is None:
        return None
    if scale == 0:
        return 0.0
    return min(1.0, abs(value) / scale)


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _require(stats: Mapping[str, Any], key: str) -> float:
    value = stats.get(key)
    if value is None:
        raise ValueError(f"Missing required stat: {key}")
    return float(value)


RegimeEngineV1 = RegimeEngine


__all__ = [
    "RegimeEngine",
    "RegimeEngineV1",
    "RegimeOutput",
    "StructureEvidence",
    "build_structure_evidence",
    "build_structure_snapshot",
    "_classify_expansion",
    "_classify_liquidity",
    "_classify_structure",
    "_classify_volatility",
]
