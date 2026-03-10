"""Signal evaluation from indicator state snapshots."""

from __future__ import annotations

from collections import deque
from typing import Any, Callable, Deque, Dict, Mapping

from engines.bot_runtime.core.domain import StrategySignal

from .contracts import SignalEvaluationInput

RuleEvaluator = Callable[[Any, Mapping[str, Dict[str, Any]]], Mapping[str, Any] | None]


def evaluate_rules_from_state_snapshots(
    *,
    signal_input: SignalEvaluationInput,
    rules: Mapping[str, Any],
    current_epoch: int,
    rule_evaluator: RuleEvaluator,
) -> Deque[StrategySignal]:
    payloads: Dict[str, Dict[str, Any]] = {}
    for indicator_id, snapshot in signal_input.snapshots.items():
        payload = dict(snapshot.payload)
        payload["snapshot_schema_version"] = int(getattr(snapshot, "schema_version", 1) or 1)
        payload["snapshot_revision"] = int(snapshot.revision)
        payload["known_at"] = snapshot.known_at
        payload["formed_at"] = snapshot.formed_at
        payload["source_timeframe"] = snapshot.source_timeframe
        payloads[indicator_id] = payload

    signals: Deque[StrategySignal] = deque()
    for rule in rules.values():
        outcome = rule_evaluator(rule, payloads)
        if not isinstance(outcome, Mapping) or not outcome.get("matched"):
            continue
        signal = outcome.get("signal")
        if not isinstance(signal, Mapping):
            continue
        from strategies import evaluator

        epoch = evaluator._extract_signal_epoch(signal)
        if epoch != current_epoch:
            continue
        action = str(outcome.get("action") or "").lower()
        if action == "buy":
            signals.append(StrategySignal(epoch=epoch, direction="long"))
        elif action == "sell":
            signals.append(StrategySignal(epoch=epoch, direction="short"))
    return signals
