"""Signal builders for market profile runtime outputs."""

from __future__ import annotations

from typing import Callable, Mapping

from engines.indicator_engine.contracts import RuntimeOutput

from .models import MarketProfileBarState

SignalEventBuilder = Callable[[MarketProfileBarState], list[dict[str, str]]]


def _balance_breakout_events(state: MarketProfileBarState) -> list[dict[str, str]]:
    if state.previous_location == "inside_value" and state.location == "above_value":
        return [{"key": "balance_breakout_long"}]
    if state.previous_location == "inside_value" and state.location == "below_value":
        return [{"key": "balance_breakout_short"}]
    return []


SIGNAL_EVENT_BUILDERS: Mapping[str, SignalEventBuilder] = {
    "balance_breakout": _balance_breakout_events,
}


def build_signal_outputs(state: MarketProfileBarState) -> dict[str, RuntimeOutput]:
    outputs: dict[str, RuntimeOutput] = {}
    for output_name, build_events in SIGNAL_EVENT_BUILDERS.items():
        outputs[output_name] = RuntimeOutput(
            bar_time=state.bar_time,
            ready=True,
            value={"events": build_events(state)},
        )
    return outputs


__all__ = ["SIGNAL_EVENT_BUILDERS", "build_signal_outputs"]
