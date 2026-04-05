from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from engines.indicator_engine.contracts import RuntimeOutput
from strategies.compiler import IndicatorMetaGetter, compile_strategy
from strategies.evaluator import DecisionEvaluationState, evaluate_strategy_bar


_SIGNAL_OUTPUT = {"name": "sig", "type": "signal", "event_keys": ["breakout_long"]}


def _make_meta_getter(outputs: list[dict[str, Any]]) -> IndicatorMetaGetter:
    def getter(indicator_id: str) -> dict[str, Any]:
        _ = indicator_id
        return {"typed_outputs": outputs}

    return getter


def _trigger() -> dict[str, Any]:
    return {
        "type": "signal_match",
        "indicator_id": "ind-1",
        "output_name": "sig",
        "event_key": "breakout_long",
    }


def test_evaluator_selects_highest_priority_matching_rule() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-b",
                "name": "Lower priority",
                "intent": "enter_long",
                "priority": 10,
                "trigger": _trigger(),
                "guards": [],
            },
            {
                "id": "rule-a",
                "name": "Higher priority",
                "intent": "enter_long",
                "priority": 50,
                "trigger": _trigger(),
                "guards": [],
            },
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT]),
    )

    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    result = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=DecisionEvaluationState(),
        outputs={
            "ind-1.sig": RuntimeOutput(
                bar_time=bar_time,
                ready=True,
                value={"events": [{"key": "breakout_long"}]},
            )
        },
        output_types={"ind-1.sig": "signal"},
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=bar_time,
    )

    assert result.selected_artifact is not None
    assert result.selected_artifact["rule_id"] == "rule-a"
    assert result.selected_artifact["strategy_hash"] == compiled.strategy_hash
    suppressed = next(artifact for artifact in result.artifacts if artifact["rule_id"] == "rule-b")
    assert suppressed["evaluation_result"] == "matched_suppressed"
    assert suppressed["suppression_reason"] == "higher_priority_rule_selected"


def test_evaluator_does_not_select_disabled_rule_even_when_it_matches() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-a",
                "name": "Disabled",
                "intent": "enter_long",
                "priority": 100,
                "enabled": False,
                "trigger": _trigger(),
                "guards": [],
            },
            {
                "id": "rule-b",
                "name": "Enabled",
                "intent": "enter_long",
                "priority": 0,
                "trigger": _trigger(),
                "guards": [],
            },
        ],
        attached_indicator_ids=["ind-1"],
        indicator_meta_getter=_make_meta_getter([_SIGNAL_OUTPUT]),
    )

    bar_time = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
    result = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=DecisionEvaluationState(),
        outputs={
            "ind-1.sig": RuntimeOutput(
                bar_time=bar_time,
                ready=True,
                value={"events": [{"key": "breakout_long"}]},
            )
        },
        output_types={"ind-1.sig": "signal"},
        instrument_id="instrument-1",
        symbol="BTCUSD",
        timeframe="1m",
        bar_time=bar_time,
    )

    assert result.selected_artifact is not None
    assert result.selected_artifact["rule_id"] == "rule-b"
    assert result.selected_artifact["strategy_hash"] == compiled.strategy_hash
    disabled = next(artifact for artifact in result.artifacts if artifact["rule_id"] == "rule-a")
    assert disabled["enabled"] is False
    assert disabled["evaluation_result"] == "not_matched"
