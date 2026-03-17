from datetime import datetime, timezone

from engines.indicator_engine.contracts import IndicatorStateSnapshot, SignalEvaluationInput
from engines.indicator_engine.signal_evaluator import evaluate_rules_from_state_snapshots
from engines.bot_runtime.strategy.series_builder_parts.live_updates import (
    SeriesBuilderLiveUpdatesMixin,
)


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def test_snapshot_runtime_rule_evaluator_emits_terminal_signal():
    epoch = int(datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc).timestamp())
    prior_epoch = epoch - 60
    payload_signals = [
        {"type": "breakout", "time": _iso(prior_epoch), "direction": "long"},
        {"type": "breakout", "time": _iso(epoch), "direction": "long"},
    ]
    rule_payload = {
        "id": "rule-1",
        "name": "Breakout Buy",
        "action": "buy",
        "conditions": [
            {
                "indicator_id": "ind-1",
                "signal_type": "breakout",
                "direction": "long",
            }
        ],
    }

    outcome = SeriesBuilderLiveUpdatesMixin._evaluate_rule_payload(
        rule_payload,
        {"ind-1": {"signals": payload_signals}},
    )

    assert outcome is not None
    assert outcome["matched"] is True
    assert isinstance(outcome.get("signal"), dict)
    assert outcome["signal"]["time"] == _iso(epoch)

    snapshot = IndicatorStateSnapshot(
        revision=1,
        known_at=datetime.fromtimestamp(epoch, tz=timezone.utc),
        formed_at=datetime.fromtimestamp(epoch, tz=timezone.utc),
        source_timeframe="1m",
        payload={"signals": payload_signals},
    )

    emitted = evaluate_rules_from_state_snapshots(
        signal_input=SignalEvaluationInput(snapshots={"ind-1": snapshot}),
        rules={"rule-1": rule_payload},
        current_epoch=epoch,
        rule_evaluator=SeriesBuilderLiveUpdatesMixin._evaluate_rule_payload,
    )

    assert len(emitted) == 1
    assert emitted[0].epoch == epoch
    assert emitted[0].direction == "long"
