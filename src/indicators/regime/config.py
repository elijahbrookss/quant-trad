from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence


@dataclass(frozen=True)
class RegimeStabilizerConfig:
    min_confidence: float = 0.60
    structure_min_confidence: float = 0.45
    confirm_bars: Mapping[str, int] = field(
        default_factory=lambda: {
            "structure": 3,
            "volatility": 4,
            "liquidity": 3,
            "expansion": 3,
        }
    )
    structure_enter_score: float = 0.56
    structure_exit_score: float = 0.47
    structure_min_margin: float = 0.08
    structure_transition_margin: float = 0.05
    structure_transition_enter_score: float = 0.40
    structure_transition_min_margin: float = 0.10
    structure_reversal_extra_score: float = 0.04
    structure_reversal_extra_margin: float = 0.03
    structure_transition_confirm_bars: int = 3
    structure_hold_bars: Mapping[str, int] = field(
        default_factory=lambda: {
            "trend": 4,
            "range": 5,
            "transition": 2,
        }
    )
    structure_range_after_trend_hold_bars: int = 6
    structure_reversal_confirm_bars: int = 5
    context_confirm_bars: int = 3
    context_transition_confirm_bars: int = 3
    context_reversal_confirm_bars: int = 6
    context_hold_bars: Mapping[str, int] = field(
        default_factory=lambda: {
            "trend_up": 8,
            "trend_down": 8,
            "range": 10,
            "transition_up": 4,
            "transition_down": 4,
            "transition_neutral": 4,
        }
    )
    context_trend_promote_after_known_bars: int = 1
    context_range_promote_after_known_bars: int = 1
    context_block_min_bars: int = 6
    context_mature_after_known_bars: int = 2
    context_trust_max_recent_switches: int = 2
    context_recent_switch_window_bars: int = 80
    volatility_enter_high: float = 1.15
    volatility_exit_high: float = 1.10
    volatility_enter_low: float = 0.85
    volatility_exit_low: float = 0.90
    volatility_enter_high_tr_pct: float = 0.02
    volatility_exit_high_tr_pct: float = 0.015
    volatility_enter_low_tr_pct: float = 0.008
    volatility_exit_low_tr_pct: float = 0.010
    smoothing_alpha: float = 0.25
    smoothing_features: Sequence[str] = (
        "directional_efficiency",
        "slope",
        "slope_stability",
        "atr_ratio",
        "atr_zscore",
        "volume_zscore",
        "atr_slope",
        "range_contraction",
        "overlap_pct",
    )
    smoothing_axes: Sequence[str] = ("structure", "volatility")
    hard_volatility_high_atr_ratio: float = 1.6
    hard_volatility_high_atr_zscore: float = 1.6
    hard_volatility_low_atr_ratio: float = 0.6
    hard_volatility_low_atr_zscore: float = -1.6
    structure_block_min_bars: int = 6
    structure_mature_after_known_bars: int = 3
    structure_trust_min_confidence: float = 0.58
    structure_trust_min_margin: float = 0.12
    structure_trust_max_recent_switches: int = 3
    structure_recent_switch_window_bars: int = 50
    log_axis_switch_blocked: bool = False
    log_axis_switch_confirmed: bool = True


@dataclass(frozen=True)
class RegimeBlockConfig:
    min_block_bars: int = 6
    transition_band_height_ratio: float = 0.18
    label_compact_bars: int = 6
    label_full_bars: int = 10
    label_min_trust: float = 0.64
    label_min_margin: float = 0.12


@dataclass(frozen=True)
class RegimeRuntimeConfig:
    stabilizer: RegimeStabilizerConfig = field(default_factory=RegimeStabilizerConfig)
    blocks: RegimeBlockConfig = field(default_factory=RegimeBlockConfig)


def default_regime_runtime_config() -> RegimeRuntimeConfig:
    return RegimeRuntimeConfig()


__all__ = [
    "RegimeBlockConfig",
    "RegimeRuntimeConfig",
    "RegimeStabilizerConfig",
    "default_regime_runtime_config",
]
