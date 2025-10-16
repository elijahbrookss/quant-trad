"""Tests for portal strategy rule matching helpers."""

import pytest

pytest.importorskip("numpy")

from portal.backend.service.strategy_service import (
    RuleCondition,
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
