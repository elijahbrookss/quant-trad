from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence


@dataclass(frozen=True)
class RegimeStabilizerConfig:
    min_confidence: float = 0.60
    confirm_bars: Mapping[str, int] = field(
        default_factory=lambda: {
            "structure": 3,
            "volatility": 4,
            "liquidity": 3,
            "expansion": 3,
        }
    )
    structure_enter_trend: float = 0.62
    structure_exit_trend: float = 0.52
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
        "atr_ratio",
        "atr_zscore",
        "volume_zscore",
        "atr_slope",
    )
    smoothing_axes: Sequence[str] = ("structure", "volatility")
    hard_volatility_high_atr_ratio: float = 1.6
    hard_volatility_high_atr_zscore: float = 1.6
    hard_volatility_low_atr_ratio: float = 0.6
    hard_volatility_low_atr_zscore: float = -1.6


@dataclass(frozen=True)
class RegimeBlockConfig:
    min_block_bars: int = 10


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
