from collections import deque

from engines.bot_runtime.core.domain import StrategySignal
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

    assert signal.to_dict() == {
        "epoch": 95,
        "direction": "short",
        "signal_id": "d-1",
        "source_type": "runtime",
        "source_id": "run-1",
        "strategy_hash": "hash-a",
        "decision_id": "d-1",
        "rule_id": "rule-1",
        "intent": "enter_short",
        "event_key": "signal_b",
    }


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
