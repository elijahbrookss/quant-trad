"""Tests for portal strategy rule matching helpers."""

import pytest

pytest.importorskip("numpy")

from datetime import datetime, timezone

from portal.backend.service.indicator_service.signals import (
    BreakoutCacheContext,
    IndicatorSignalExecutor,
)
from portal.backend.service.strategy_service import RuleCondition
from portal.backend.service.strategy_service import evaluator, markers
from signals.base import BaseSignal


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

    result = evaluator._evaluate_condition(condition, payloads)

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

    result = evaluator._evaluate_condition(condition, payloads)

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

    chart_markers = markers._build_markers_for_results([rule_result], action="buy")

    assert len(chart_markers) == 1
    marker = chart_markers[0]
    expected_epoch = int(datetime(2025, 1, 3, 12, tzinfo=timezone.utc).timestamp())
    assert marker["time"] == expected_epoch
    assert marker["price"] == 80.5


def test_pivot_retest_signals_match_strategy_conditions():
    condition = RuleCondition(
        indicator_id="indicator-1",
        signal_type="retest",
        rule_id="pivot_retest",
        direction="long",
    )

    signal_payload = {
        "type": "retest",
        "rule_id": "pivot_retest",
        "pattern_id": "pivot_retest",
        "rule_aliases": ["pivot_level_retest"],
        "direction": "long",
        "metadata": {
            "rule_id": "pivot_retest",
            "pattern_id": "pivot_retest",
            "rule_aliases": ["pivot_level_retest"],
            "breakout_direction": "above",
            "direction": "long",
        },
    }

    payloads = {"indicator-1": {"signals": [signal_payload]}}

    result = evaluator._evaluate_condition(condition, payloads)

    assert result["matched"] is True
    assert result.get("direction_detected") == "long"
    assert result.get("stats", {}).get("final_matches") == 1
    assert "pivot_retest" in (result.get("observed_rules") or [])
    assert "pivot_level_retest" in (result.get("observed_rules") or [])


def test_direction_mismatch_reason_is_descriptive():
    condition = RuleCondition(
        indicator_id="indicator-1",
        signal_type="retest",
        rule_id="pivot_retest",
        direction="short",
    )

    signal_payload = {
        "type": "retest",
        "rule_id": "pivot_retest",
        "pattern_id": "pivot_retest",
        "direction": "long",
        "metadata": {
            "rule_id": "pivot_retest",
            "pattern_id": "pivot_retest",
            "breakout_direction": "above",
            "direction": "long",
        },
    }

    payloads = {"indicator-1": {"signals": [signal_payload]}}

    result = evaluator._evaluate_condition(condition, payloads)

    assert result["matched"] is False
    assert result.get("reason") == "No matching signals (direction mismatch)"
    stats = result.get("stats") or {}
    assert stats.get("rule_matches") == 1
    assert stats.get("direction_matches") == 0


def test_build_chart_markers_splits_actions():
    buy_rule = {
        "rule_id": "buy-rule",
        "rule_name": "Buy Rule",
        "signals": [
            {
                "type": "retest",
                "time": "2025-01-04T15:00:00Z",
                "price": 101.5,
            }
        ],
    }
    sell_rule = {
        "rule_id": "sell-rule",
        "rule_name": "Sell Rule",
        "signals": [
            {
                "type": "breakout",
                "metadata": {
                    "bar_time": "2025-01-05T10:30:00Z",
                    "price": 88.2,
                },
            }
        ],
    }

    chart_payload = markers.build_chart_markers([buy_rule], [sell_rule])

    assert chart_payload["buy"][0]["shape"] == "arrowUp"
    assert chart_payload["buy"][0]["position"] == "belowBar"
    assert chart_payload["sell"][0]["shape"] == "arrowDown"
    assert chart_payload["sell"][0]["position"] == "aboveBar"


def test_strategy_preview_enabled_rules_keep_alias_signals():
    executor = IndicatorSignalExecutor()
    signal = BaseSignal(
        type="breakout",
        symbol="CL",
        time=datetime(2025, 1, 6, tzinfo=timezone.utc),
        confidence=0.8,
        metadata={
            "rule_id": "market_profile_breakout",
            "pattern_id": "value_area_breakout",
            "aliases": ["mp_breakout"],
        },
    )

    cache_ctx = BreakoutCacheContext(
        cache_spec=None,
        cache_key=("strategy-preview",),
        requested_rule_ids={"market_profile_breakout"},
    )

    filtered = executor._filter_signals([signal], cache_ctx)

    assert filtered == [signal]


def test_strategy_preview_enabled_rules_accept_rule_suffix():
    executor = IndicatorSignalExecutor()
    signal = BaseSignal(
        type="breakout",
        symbol="GC",
        time=datetime(2025, 1, 7, tzinfo=timezone.utc),
        confidence=0.7,
        metadata={
            "rule_id": "market_profile_breakout",
            "pattern_id": "value_area_breakout",
        },
    )

    cache_ctx = BreakoutCacheContext(
        cache_spec=None,
        cache_key=("strategy-preview",),
        requested_rule_ids={"market_profile_breakout_rule"},
    )

    filtered = executor._filter_signals([signal], cache_ctx)

    assert filtered == [signal]
