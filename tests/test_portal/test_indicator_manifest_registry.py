from __future__ import annotations

from pathlib import Path

from indicators.definition_contract import (
    definition_supports_compute,
    definition_supports_runtime,
)
from indicators.registry import INDICATOR_MAP, get_indicator_manifest


def test_registered_indicators_use_manifest_and_definition_modules() -> None:
    indicators_root = Path(__file__).resolve().parents[2] / "src" / "indicators"

    for indicator_type, definition in INDICATOR_MAP.items():
        manifest = get_indicator_manifest(indicator_type)
        assert manifest.type == indicator_type
        assert str(definition.__module__).endswith(".definition")
        assert (indicators_root / indicator_type / "definition.py").exists()
        assert (indicators_root / indicator_type / "manifest.py").exists()


def test_all_manifests_expose_stable_editor_params() -> None:
    for indicator_type in INDICATOR_MAP:
        manifest = get_indicator_manifest(indicator_type)
        for param in manifest.params:
            assert param.key
            assert param.type
            assert param.label


def test_manifests_expose_indicator_config_only() -> None:
    manifest = get_indicator_manifest("market_profile")
    by_key = {param.key: param for param in manifest.params}

    assert "symbol" not in by_key
    assert "start" not in by_key
    assert "end" not in by_key
    assert "interval" not in by_key
    assert "datasource" not in by_key
    assert "provider_id" not in by_key


def test_indicator_definitions_expose_explicit_compute_or_runtime_support() -> None:
    assert definition_supports_runtime(INDICATOR_MAP["market_profile"]) is True
    assert definition_supports_runtime(INDICATOR_MAP["candle_stats"]) is True
    assert definition_supports_compute(INDICATOR_MAP["vwap"]) is True
    assert definition_supports_compute(INDICATOR_MAP["trendline"]) is True
