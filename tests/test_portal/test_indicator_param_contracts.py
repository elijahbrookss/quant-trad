from __future__ import annotations

import pytest

from indicators.candle_stats import CandleStatsIndicator
from indicators.manifest import IndicatorManifest, IndicatorOption, IndicatorParam, serialize_indicator_manifest
from indicators.param_contract import (
    indicator_default_params,
    indicator_field_types,
    indicator_required_params,
    resolve_indicator_params,
)


class _DeclaredIndicator:
    NAME = "declared"
    MANIFEST = IndicatorManifest(
        type="declared",
        version="v1",
        label="Declared",
        description="Test indicator.",
        params=(
            IndicatorParam(
                key="threshold",
                type="float",
                label="Threshold",
                required=True,
            ),
            IndicatorParam(
                key="window",
                type="int",
                label="Window",
                default=20,
            ),
        ),
    )


class _LegacyIndicator:
    NAME = "legacy"

    def __init__(self, length: int, multiplier: float = 2.0) -> None:
        self.length = length
        self.multiplier = multiplier


def test_declared_indicator_params_are_resolved_from_manifest() -> None:
    assert indicator_required_params(_DeclaredIndicator) == ["threshold"]
    assert indicator_default_params(_DeclaredIndicator) == {"window": 20}
    assert indicator_field_types(_DeclaredIndicator) == {
        "threshold": "float",
        "window": "int",
    }

    resolved = resolve_indicator_params(_DeclaredIndicator, {"threshold": 1.5})

    assert resolved == {"threshold": 1.5, "window": 20}


def test_candle_stats_uses_manifest_defaults_without_signature_inference() -> None:
    assert indicator_required_params(CandleStatsIndicator) == []

    defaults = indicator_default_params(CandleStatsIndicator)

    assert defaults["atr_short_window"] == 14
    assert defaults["warmup_bars"] == 200
    assert "atr_short_window" in indicator_field_types(CandleStatsIndicator)


def test_indicator_config_resolution_rejects_unknown_runtime_context_fields_on_save() -> None:
    with pytest.raises(ValueError, match="unknown params"):
        CandleStatsIndicator.resolve_config(
            {"atr_short_window": 10, "provider_id": "abc"},
            strict_unknown=True,
        )


def test_signature_based_param_contracts_are_rejected() -> None:
    with pytest.raises(RuntimeError, match="must declare MANIFEST"):
        indicator_required_params(_LegacyIndicator)


def test_manifest_serialization_preserves_rich_option_metadata() -> None:
    manifest = IndicatorManifest(
        type="optioned",
        version="v1",
        label="Optioned",
        description="Test options.",
        params=(
            IndicatorParam(
                key="timeframe",
                type="string",
                label="Timeframe",
                default="1h",
                options=(
                    IndicatorOption("1h", "1 Hour", "Intraday hourly bars.", badge="Featured"),
                    IndicatorOption("1d", "1 Day", "Daily bars."),
                ),
            ),
        ),
    )

    payload = serialize_indicator_manifest(manifest)
    options = payload["params"][0]["options"]

    assert options == [
        {
            "value": "1h",
            "label": "1 Hour",
            "description": "Intraday hourly bars.",
            "badge": "Featured",
            "disabled": False,
        },
        {
            "value": "1d",
            "label": "1 Day",
            "description": "Daily bars.",
            "badge": None,
            "disabled": False,
        },
    ]
