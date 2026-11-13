from __future__ import annotations

import pytest

pytest.importorskip("pandas")

from portal.backend.service.indicators.indicator_service.api import get_type_details


def test_type_details_returns_serialized_manifest_shape() -> None:
    details = get_type_details("market_profile")

    assert details["type"] == "market_profile"
    assert details["label"] == "Market Profile"
    assert details["color_mode"] == "single"
    assert isinstance(details["params"], list)
    assert any(param["key"] == "use_merged_value_areas" for param in details["params"])
    assert isinstance(details["outputs"], list)
    assert isinstance(details["overlays"], list)
    assert details["runtime_supported"] is True
    assert details["compute_supported"] is False


@pytest.mark.parametrize("kind", ["vwap", "pivot_level", "trendline"])
def test_type_details_reject_unsupported_compute_only_entries(kind):
    with pytest.raises(KeyError): get_type_details(kind)


def test_reversal_type_exposes_supported_causal_runtime():
    details=get_type_details("reversal")
    assert details["runtime_supported"] is True
    assert {output["type"] for output in details["outputs"]} == {"signal","context","lifecycle"}


def test_type_details_expose_color_palettes_when_declared() -> None:
    details = get_type_details("candle_stats")

    assert details["type"] == "candle_stats"
    assert details["color_mode"] == "palette"
    assert [palette["key"] for palette in details["color_palettes"]] == [
        "bull_bear",
        "ocean",
        "slate",
    ]


def test_type_details_expose_manifest_dependency_contract() -> None:
    details = get_type_details("regime")

    assert details["type"] == "regime"
    assert details["color_mode"] == "palette"
    assert details["dependencies"] == [
        {
            "indicator_type": "candle_stats",
            "output_name": "candle_stats",
            "label": "Candle Stats",
            "description": "Regime classification requires candle stats runtime metrics.",
        }
    ]
