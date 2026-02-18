from __future__ import annotations

from datetime import datetime, timezone

from engines.bot_runtime.core.indicator_state.contracts import IndicatorStateSnapshot, SignalEvaluationInput
from engines.bot_runtime.core.indicator_state.signal_evaluator import evaluate_rules_from_state_snapshots


def test_signal_evaluator_consumes_snapshots_only() -> None:
    observed = {}

    def _fake_eval(rule, payloads):
        observed["payloads"] = payloads
        return {
            "matched": True,
            "action": "buy",
            "rule_id": "r1",
            "signal": {"time": "2024-01-01T00:00:00+00:00"},
        }

    snapshot = IndicatorStateSnapshot(
        revision=2,
        known_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        formed_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        source_timeframe="30m",
        payload={"profiles": [{"session": "2024-01-01"}]},
    )

    signals = evaluate_rules_from_state_snapshots(
        signal_input=SignalEvaluationInput(snapshots={"ind-1": snapshot}),
        rules={"r1": {"id": "r1"}},
        current_epoch=int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()),
        rule_evaluator=_fake_eval,
    )

    assert len(signals) == 1
    payload = observed["payloads"]["ind-1"]
    assert payload["profiles"][0]["session"] == "2024-01-01"
    assert payload["source_timeframe"] == "30m"
