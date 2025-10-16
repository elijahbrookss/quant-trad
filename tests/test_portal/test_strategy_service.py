"""Tests for portal strategy rule matching helpers."""

import pytest

pytest.importorskip("numpy")

from datetime import datetime, timezone

from portal.backend.service.strategy_service import (
    RuleCondition,
    _build_markers_for_results,
    _evaluate_condition,
)


def test_market_profile_rule_aliases_match_strategy_conditions():
    """Signals exposing rule aliases should satisfy strategy rule filters."""

    condition = RuleCondition(
        indicator_id="indicator-1",
        signal_type="retest",
        rule_id="market_profile_retest",
        direction="long",
    )

    signal_payload = {
        "type": "retest",
        "rule_id": "market_profile_retest",
        "pattern_id": "value_area_retest",
        "direction": "long",
        "metadata": {
            "pattern_id": "value_area_retest",
            "rule_id": "market_profile_retest",
        },
    }

    payloads = {"indicator-1": {"signals": [signal_payload]}}

    result = _evaluate_condition(condition, payloads)

    assert result["matched"] is True
    assert result["signal"] is signal_payload
    assert result.get("direction_detected") == "long"
    assert result.get("signals") == [signal_payload]


def test_condition_collects_all_matching_signals():
    condition = RuleCondition(
        indicator_id="indicator-1",
        signal_type="retest",
        rule_id="market_profile_retest",
        direction="long",
    )

    first = {
        "type": "retest",
        "rule_id": "market_profile_retest",
        "pattern_id": "value_area_retest",
        "direction": "long",
        "time": "2025-01-01T00:00:00Z",
        "metadata": {"pattern_id": "value_area_retest", "rule_id": "market_profile_retest"},
    }
    second = {
        "type": "retest",
        "rule_id": "market_profile_retest",
        "pattern_id": "value_area_retest",
        "direction": "long",
        "time": "2025-01-02T00:00:00Z",
        "metadata": {"pattern_id": "value_area_retest", "rule_id": "market_profile_retest"},
    }

    payloads = {"indicator-1": {"signals": [first, second]}}

    result = _evaluate_condition(condition, payloads)

    assert result["matched"] is True
    assert result["signal"] is second
    assert result.get("signals") == [first, second]


def test_build_markers_uses_metadata_time():
    rule_result = {
        "rule_id": "market_profile_retest",
        "rule_name": "Retest Buy",
        "signals": [
            {
                "type": "retest",
                "metadata": {
                    "bar_time": "2025-01-03T12:00:00Z",
                    "price": 80.5,
                },
            }
        ],
    }

    markers = _build_markers_for_results([rule_result], action="buy")

    assert len(markers) == 1
    marker = markers[0]
    expected_epoch = int(datetime(2025, 1, 3, 12, tzinfo=timezone.utc).timestamp())
    assert marker["time"] == expected_epoch
    assert marker["price"] == 80.5
