"""Compiled strategy decision contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple


Intent = Literal["enter_long", "enter_short"]
RuleNodeType = Literal[
    "signal_match",
    "context_match",
    "metric_match",
    "holds_for_bars",
    "signal_seen_within_bars",
    "signal_absent_within_bars",
]


@dataclass(frozen=True)
class SignalMatchSpec:
    type: Literal["signal_match"]
    indicator_id: str
    output_name: str
    output_key: str
    event_key: str


@dataclass(frozen=True)
class ContextMatchSpec:
    type: Literal["context_match"]
    indicator_id: str
    output_name: str
    output_key: str
    field: str
    value: str


@dataclass(frozen=True)
class MetricMatchSpec:
    type: Literal["metric_match"]
    indicator_id: str
    output_name: str
    output_key: str
    field: str
    operator: str
    value: float


LeafGuardSpec = ContextMatchSpec | MetricMatchSpec


@dataclass(frozen=True)
class HoldsForBarsSpec:
    type: Literal["holds_for_bars"]
    bars: int
    guard: LeafGuardSpec


@dataclass(frozen=True)
class SignalWindowSpec:
    type: Literal["signal_seen_within_bars", "signal_absent_within_bars"]
    indicator_id: str
    output_name: str
    output_key: str
    event_key: str
    lookback_bars: int


GuardSpec = LeafGuardSpec | HoldsForBarsSpec | SignalWindowSpec


@dataclass(frozen=True)
class DecisionRuleSpec:
    id: str
    name: str
    intent: Intent
    priority: int
    enabled: bool
    trigger: SignalMatchSpec
    guards: Tuple[GuardSpec, ...]
    description: str | None = None


@dataclass(frozen=True)
class CompiledStrategySpec:
    strategy_id: str
    timeframe: str
    rules: Tuple[DecisionRuleSpec, ...]
    max_history_bars: int = 0


__all__ = [
    "CompiledStrategySpec",
    "ContextMatchSpec",
    "DecisionRuleSpec",
    "GuardSpec",
    "HoldsForBarsSpec",
    "Intent",
    "LeafGuardSpec",
    "MetricMatchSpec",
    "SignalMatchSpec",
    "SignalWindowSpec",
]
