from __future__ import annotations

from indicators.candle_stats import CandleStatsIndicator
from indicators.param_contract import (
    indicator_default_params,
    indicator_field_types,
    indicator_required_params,
    resolve_indicator_params,
)


class _DeclaredIndicator:
    NAME = "declared"
    REQUIRED_PARAMS = ("threshold",)
    DEFAULT_PARAMS = {"window": 20}

    def __init__(self, threshold: float, window: int = 20, internal_flag: bool = False) -> None:
        self.threshold = threshold
        self.window = window
        self.internal_flag = internal_flag


class _LegacyIndicator:
    NAME = "legacy"

    def __init__(self, length: int, multiplier: float = 2.0) -> None:
        self.length = length
        self.multiplier = multiplier


def test_declared_indicator_params_stay_separate_from_internal_resolution() -> None:
    assert indicator_required_params(_DeclaredIndicator) == ["threshold"]
    assert indicator_default_params(_DeclaredIndicator) == {"window": 20}
    assert indicator_field_types(_DeclaredIndicator) == {
        "threshold": "float",
        "window": "int",
    }

    resolved = resolve_indicator_params(_DeclaredIndicator, {"threshold": 1.5})

    assert resolved == {"threshold": 1.5, "window": 20}


def test_candle_stats_uses_declared_defaults_without_required_inputs() -> None:
    assert indicator_required_params(CandleStatsIndicator) == []

    defaults = indicator_default_params(CandleStatsIndicator)

    assert defaults["atr_short_window"] == 14
    assert defaults["warmup_bars"] == 200
    assert "atr_short_window" in indicator_field_types(CandleStatsIndicator)


def test_legacy_indicator_contract_falls_back_to_signature() -> None:
    assert indicator_required_params(_LegacyIndicator) == ["length"]
    assert indicator_default_params(_LegacyIndicator) == {"multiplier": 2.0}
    assert indicator_field_types(_LegacyIndicator) == {
        "length": "int",
        "multiplier": "float",
    }

    resolved = resolve_indicator_params(_LegacyIndicator, {"length": 50})

    assert resolved == {"length": 50, "multiplier": 2.0}
