from datetime import datetime, timedelta

from portal.backend.service.market.regime_blocks import RegimeBlockConfig, build_regime_blocks
from portal.backend.service.market.regime_engine import _classify_volatility
from portal.backend.service.market.regime_stabilizer import RegimeStabilizer, RegimeStabilizerConfig


def _regime(
    *,
    directional_efficiency: float,
    confidence: float,
    atr_ratio: float = 1.0,
    atr_zscore: float = 0.0,
    tr_pct: float = 0.01,
    slope_stability: float = 0.2,
    range_position: float = 0.5,
    atr_slope: float = 0.0,
    range_contraction: float = 1.0,
    overlap_pct: float = 0.4,
    volume_zscore: float = 0.0,
    volume_vs_median: float = 1.0,
) -> dict:
    return {
        "structure": {
            "state": "transition",
            "directional_efficiency": directional_efficiency,
            "slope_stability": slope_stability,
            "range_position": range_position,
        },
        "volatility": {"state": "normal", "atr_zscore": atr_zscore, "tr_pct": tr_pct, "atr_ratio": atr_ratio},
        "expansion": {
            "state": "compressing",
            "atr_slope": atr_slope,
            "range_contraction": range_contraction,
            "overlap_pct": overlap_pct,
        },
        "liquidity": {"state": "normal", "volume_zscore": volume_zscore, "volume_vs_median": volume_vs_median},
        "confidence": confidence,
    }


def test_structure_hysteresis_holds_trend_until_exit_threshold():
    config = RegimeStabilizerConfig(
        min_confidence=0.0,
        confirm_bars={"structure": 1, "volatility": 1, "liquidity": 1, "expansion": 1},
        smoothing_axes=(),
        smoothing_features=(),
    )
    stabilizer = RegimeStabilizer(config)

    first = stabilizer.stabilize(_regime(directional_efficiency=0.7, confidence=0.9))
    assert first["structure"]["state"] == "trend"

    second = stabilizer.stabilize(_regime(directional_efficiency=0.55, confidence=0.9))
    assert second["structure"]["state"] == "trend"

    third = stabilizer.stabilize(_regime(directional_efficiency=0.45, confidence=0.9))
    assert third["structure"]["state"] == "transition"


def test_confirmation_bars_prevent_one_bar_flip():
    config = RegimeStabilizerConfig(
        min_confidence=0.0,
        confirm_bars={"structure": 2, "volatility": 1, "liquidity": 1, "expansion": 1},
        smoothing_axes=(),
        smoothing_features=(),
    )
    stabilizer = RegimeStabilizer(config)

    start = stabilizer.stabilize(_regime(directional_efficiency=0.7, confidence=0.9))
    assert start["structure"]["state"] == "trend"

    first_attempt = stabilizer.stabilize(
        _regime(directional_efficiency=0.2, range_position=0.5, confidence=0.9)
    )
    assert first_attempt["structure"]["state"] == "trend"

    second_attempt = stabilizer.stabilize(
        _regime(directional_efficiency=0.2, range_position=0.5, confidence=0.9)
    )
    assert second_attempt["structure"]["state"] == "range"


def test_confidence_gate_blocks_low_confidence_switches():
    config = RegimeStabilizerConfig(
        min_confidence=0.8,
        confirm_bars={"structure": 1, "volatility": 1, "liquidity": 1, "expansion": 1},
        smoothing_axes=(),
        smoothing_features=(),
    )
    stabilizer = RegimeStabilizer(config)

    start = stabilizer.stabilize(_regime(directional_efficiency=0.7, confidence=0.95))
    assert start["structure"]["state"] == "trend"

    blocked = stabilizer.stabilize(
        _regime(directional_efficiency=0.2, range_position=0.5, confidence=0.5)
    )
    assert blocked["structure"]["state"] == "trend"


def test_short_blocks_merge_into_previous_block():
    start = datetime(2024, 1, 1)
    points = []
    states = ["trend"] * 3 + ["range"] + ["trend"] * 3
    for idx, state in enumerate(states):
        points.append(
            {
                "time": start + timedelta(minutes=idx),
                "structure_state": state,
                "volatility_state": "normal",
                "liquidity_state": "normal",
                "expansion_state": "compressing",
                "confidence": 0.8,
            }
        )

    blocks, block_ids = build_regime_blocks(
        points,
        timeframe_seconds=60,
        config=RegimeBlockConfig(min_block_bars=3),
    )

    assert len(blocks) == 1
    assert blocks[0]["regime_key"] == "trend|normal|normal|compressing"
    assert len(set(block_ids.values())) == 1


def test_raw_volatility_thresholds_use_updated_atr_ratio():
    assert _classify_volatility(atr_z=-1.0, tr_pct=0.006, atr_ratio=0.85) == "low"
    assert _classify_volatility(atr_z=0.0, tr_pct=0.01, atr_ratio=0.86) == "normal"
    assert _classify_volatility(atr_z=0.0, tr_pct=0.01, atr_ratio=1.15) == "high"


def test_volatility_hysteresis_respects_tr_pct_and_atr_zscore():
    config = RegimeStabilizerConfig(
        min_confidence=0.0,
        confirm_bars={"structure": 1, "volatility": 1, "liquidity": 1, "expansion": 1},
        smoothing_axes=(),
        smoothing_features=(),
    )
    stabilizer = RegimeStabilizer(config)

    first = stabilizer.stabilize(_regime(directional_efficiency=0.7, confidence=0.9, atr_ratio=1.0, tr_pct=0.012, atr_zscore=0.0))
    assert first["volatility"]["state"] == "normal"

    high = stabilizer.stabilize(
        _regime(directional_efficiency=0.7, confidence=0.9, atr_ratio=1.02, tr_pct=0.021, atr_zscore=0.2)
    )
    assert high["volatility"]["state"] == "high"

    still_high = stabilizer.stabilize(
        _regime(directional_efficiency=0.7, confidence=0.9, atr_ratio=1.08, tr_pct=0.017, atr_zscore=0.2)
    )
    assert still_high["volatility"]["state"] == "high"

    exit_high = stabilizer.stabilize(
        _regime(directional_efficiency=0.7, confidence=0.9, atr_ratio=1.05, tr_pct=0.014, atr_zscore=0.2)
    )
    assert exit_high["volatility"]["state"] == "normal"
