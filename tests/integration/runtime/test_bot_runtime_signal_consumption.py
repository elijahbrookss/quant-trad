from collections import deque

import pytest

from engines.bot_runtime.core.domain import StrategySignal
from engines.bot_runtime.runtime.mixins.execution_loop import RuntimeExecutionLoopMixin
from engines.bot_runtime.runtime.components.signal_consumption import SignalConsumption, consume_signals


def test_strategy_signal_builds_from_selected_decision_artifact():
    artifact = {
        "decision_id": "d-1",
        "rule_id": "rule-1",
        "strategy_hash": "hash-a",
        "bar_epoch": 95,
        "evaluation_result": "matched_selected",
        "emitted_intent": "enter_short",
        "trigger": {"event_key": "signal_b"},
    }

    signal = StrategySignal.from_decision_artifact(
        artifact,
        source_type="runtime",
        source_id="run-1",
    )
    expected_signal_id = StrategySignal.build_signal_id(
        decision_id="d-1",
        source_type="runtime",
        source_id="run-1",
    )

    assert signal.to_dict() == {
        "epoch": 95,
        "direction": "short",
        "signal_id": expected_signal_id,
        "source_type": "runtime",
        "source_id": "run-1",
        "strategy_hash": "hash-a",
        "decision_id": "d-1",
        "rule_id": "rule-1",
        "intent": "enter_short",
        "event_key": "signal_b",
    }


def test_strategy_signal_builds_from_minimal_decision_context():
    artifact = {
        "decision_id": "d-1",
        "rule_id": "rule-1",
        "strategy_hash": "hash-a",
        "bar_epoch": 95,
        "evaluation_result": "matched_selected",
        "emitted_intent": "enter_long",
        "decision_context": {
            "trigger_output_ref": "ind-1.sig",
            "event_key": "signal_a",
            "intent": "enter_long",
            "direction": "long",
        },
        "artifact_summary": {
            "trigger_ready": True,
            "trigger_matched": True,
            "guard_count": 0,
            "guards_ready": 0,
            "guards_matched": 0,
            "matched": True,
        },
    }

    signal = StrategySignal.from_decision_artifact(
        artifact,
        source_type="runtime",
        source_id="run-1",
    )

    assert signal.direction == "long"
    assert signal.intent == "enter_long"
    assert signal.event_key == "signal_a"


def test_runtime_decision_artifact_copy_is_compact_and_mutation_isolated():
    artifact = {
        "decision_id": "d-1",
        "strategy_id": "strategy-1",
        "strategy_hash": "hash-a",
        "instrument_id": "instrument-1",
        "symbol": "BTCUSD",
        "timeframe": "1m",
        "bar_epoch": 95,
        "bar_time": "2026-04-04T12:00:00Z",
        "decision_time": "2026-04-04T12:00:00Z",
        "rule_id": "rule-1",
        "rule_name": "Breakout",
        "priority": 5,
        "enabled": True,
        "evaluation_result": "matched_selected",
        "emitted_intent": "enter_long",
        "suppression_reason": None,
        "decision_context": {
            "trigger_output_ref": "ind-1.sig",
            "event_key": "signal_a",
            "intent": "enter_long",
            "direction": "long",
            "debug": {"large": True},
        },
        "referenced_outputs": {
            "ind-1.market_state": {
                "output_ref": "ind-1.market_state",
                "type": "context",
                "output_type": "context",
                "ready": True,
                "bar_time": "2026-04-04T12:00:00Z",
                "fields": {"bias": "long"},
                "debug": {"large": True},
            }
        },
        "observed_outputs": {
            "ind-1.market_state": {
                "output_ref": "ind-1.market_state",
                "type": "context",
                "output_type": "context",
                "ready": True,
                "bar_time": "2026-04-04T12:00:00Z",
                "fields": {"bias": "long"},
                "debug": {"large": True},
            }
        },
        "artifact_summary": {
            "trigger_ready": True,
            "trigger_matched": True,
            "guard_count": 0,
            "guards_ready": 0,
            "guards_matched": 0,
            "matched": True,
        },
        "output_filter_trace": {
            "schema_version": "strategy_output_filter_trace.v1",
            "filter_count": 1,
            "ready_count": 1,
            "matched_count": 1,
            "all_matched": True,
            "items": [
                {
                    "filter_index": 0,
                    "filter_hash": "filter-hash-1",
                    "scope": {"intent": ["enter_long"]},
                    "guard_type": "context_match",
                    "output_ref": "ind-1.market_state",
                    "field": "bias",
                    "operator": "equals",
                    "expected": ["long"],
                    "actual": "long",
                    "ready": True,
                    "matched": True,
                    "debug": {"large": True},
                }
            ],
        },
        "trigger": {"event_key": "signal_a", "debug": {"large": True}},
        "guard_results": [{"debug": {"large": True}}],
    }

    copied = RuntimeExecutionLoopMixin._copy_decision_artifact_for_runtime(artifact)

    assert "trigger" not in copied
    assert "guard_results" not in copied
    assert copied["decision_context"] == {
        "trigger_output_ref": "ind-1.sig",
        "event_key": "signal_a",
        "intent": "enter_long",
        "direction": "long",
    }
    assert copied["referenced_outputs"] == {
        "ind-1.market_state": {
            "output_ref": "ind-1.market_state",
            "type": "context",
            "output_type": "context",
            "ready": True,
            "bar_time": "2026-04-04T12:00:00Z",
            "fields": {"bias": "long"},
        }
    }
    assert copied["observed_outputs"] == {
        "ind-1.market_state": {
            "output_ref": "ind-1.market_state",
            "type": "context",
            "output_type": "context",
            "ready": True,
            "bar_time": "2026-04-04T12:00:00Z",
            "fields": {"bias": "long"},
        }
    }
    assert copied["artifact_summary"] == artifact["artifact_summary"]
    assert copied["output_filter_trace"] == {
        "schema_version": "strategy_output_filter_trace.v1",
        "filter_count": 1,
        "ready_count": 1,
        "matched_count": 1,
        "all_matched": True,
        "items": [
            {
                "filter_index": 0,
                "filter_hash": "filter-hash-1",
                "scope": {"intent": ["enter_long"]},
                "guard_type": "context_match",
                "output_ref": "ind-1.market_state",
                "field": "bias",
                "operator": "equals",
                "expected": ["long"],
                "actual": "long",
                "ready": True,
                "matched": True,
            }
        ],
    }
    assert copied["decision_context"] is not artifact["decision_context"]
    artifact["decision_context"]["event_key"] = "mutated"
    artifact["output_filter_trace"]["items"][0]["actual"] = "mutated"
    assert copied["decision_context"]["event_key"] == "signal_a"
    assert copied["output_filter_trace"]["items"][0]["actual"] == "long"


def test_signal_consumption_preserves_history():
    signals = deque(
        [
            StrategySignal(epoch=90, direction="long", strategy_hash="hash-a", decision_id="d-1", rule_id="r-1", intent="enter_long", event_key="signal_a"),
            StrategySignal(epoch=95, direction="short", strategy_hash="hash-a", decision_id="d-2", rule_id="r-2", intent="enter_short", event_key="signal_b"),
            StrategySignal(epoch=110, direction="long", strategy_hash="hash-a", decision_id="d-3", rule_id="r-3", intent="enter_long", event_key="signal_c"),
        ]
    )
    consumed, chosen, last_consumed = consume_signals(signals, epoch=100, last_consumed_epoch=0)

    assert [signal.to_dict() for signal in consumed] == [
        {"epoch": 90, "direction": "long", "signal_id": None, "source_type": None, "source_id": None, "strategy_hash": "hash-a", "decision_id": "d-1", "rule_id": "r-1", "intent": "enter_long", "event_key": "signal_a"},
        {"epoch": 95, "direction": "short", "signal_id": None, "source_type": None, "source_id": None, "strategy_hash": "hash-a", "decision_id": "d-2", "rule_id": "r-2", "intent": "enter_short", "event_key": "signal_b"},
    ]
    assert chosen is not None
    assert chosen.to_dict() == {
        "epoch": 95,
        "direction": "short",
        "signal_id": None,
        "source_type": None,
        "source_id": None,
        "strategy_hash": "hash-a",
        "decision_id": "d-2",
        "rule_id": "r-2",
        "intent": "enter_short",
        "event_key": "signal_b",
    }
    assert last_consumed == 95
    assert signals[0].epoch == 110


def test_signal_consumption_log_bounded():
    signal_log = deque(maxlen=500)
    for idx in range(505):
        signal_log.append(
            SignalConsumption(
                epoch=idx,
                consumed_signals=[{"epoch": idx, "direction": "long"}],
                chosen_signal={"epoch": idx, "direction": "long"},
            )
        )
    assert len(signal_log) == 500


def test_signal_consumption_skips_already_consumed_epochs():
    signals = deque(
        [
            StrategySignal(epoch=95, direction="long", decision_id="d-1"),
            StrategySignal(epoch=100, direction="short", decision_id="d-2"),
            StrategySignal(epoch=105, direction="long", decision_id="d-3"),
        ]
    )

    consumed, chosen, last_consumed = consume_signals(signals, epoch=110, last_consumed_epoch=100)

    assert [signal.to_dict() for signal in consumed] == [
        {
            "epoch": 105,
            "direction": "long",
            "signal_id": None,
            "source_type": None,
            "source_id": None,
            "strategy_hash": None,
            "decision_id": "d-3",
            "rule_id": None,
            "intent": None,
            "event_key": None,
        }
    ]
    assert chosen is not None
    assert chosen.to_dict() == {
        "epoch": 105,
        "direction": "long",
        "signal_id": None,
        "source_type": None,
        "source_id": None,
        "strategy_hash": None,
        "decision_id": "d-3",
        "rule_id": None,
        "intent": None,
        "event_key": None,
    }
    assert last_consumed == 105


def test_strategy_signal_rejects_signal_id_aliasing_decision_id() -> None:
    with pytest.raises(RuntimeError, match="signal_id must not equal decision_id"):
        StrategySignal(
            epoch=95,
            direction="long",
            signal_id="decision-1",
            decision_id="decision-1",
        )
