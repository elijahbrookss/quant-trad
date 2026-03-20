from __future__ import annotations

import pytest

pytest.importorskip("pandas")

from portal.backend.service.indicators.indicator_service.api import get_type_details


def test_type_details_returns_serialized_manifest_shape() -> None:
    details = get_type_details("market_profile")

    assert details["type"] == "market_profile"
    assert details["label"] == "Market Profile"
    assert isinstance(details["params"], list)
    assert any(param["key"] == "use_merged_value_areas" for param in details["params"])
    assert isinstance(details["outputs"], list)
    assert isinstance(details["overlays"], list)
    assert details["runtime_supported"] is True
    assert details["compute_supported"] is False


def test_type_details_expose_compute_only_indicator_support() -> None:
    details = get_type_details("vwap")

    assert details["type"] == "vwap"
    assert details["runtime_supported"] is False
    assert details["compute_supported"] is True


def test_type_details_expose_manifest_dependency_contract() -> None:
    details = get_type_details("regime")

    assert details["type"] == "regime"
    assert details["dependencies"] == [
        {
            "indicator_type": "candle_stats",
            "output_name": "candle_stats",
            "label": "Candle Stats",
            "description": "Regime classification requires candle stats runtime metrics.",
        }
    ]
