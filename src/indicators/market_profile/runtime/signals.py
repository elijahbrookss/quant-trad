"""Signal builders for market profile runtime outputs."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from engines.indicator_engine.contracts import RuntimeOutput

from .models import MarketProfileBarState

SignalEventBuilder = Callable[[MarketProfileBarState], list[dict[str, Any]]]


def build_value_area_reference(
    state: MarketProfileBarState,
    *,
    level_name: str,
    price: float,
) -> dict[str, Any]:
    return {
        "kind": "price_level",
        "family": "value_area",
        "name": level_name,
        "label": level_name,
        "price": float(price),
        "precision": int(state.precision),
        "source": "market_profile",
        "key": state.active_profile_key,
        "context": {
            "profile_key": state.active_profile_key,
            "active_value_area": {
                "vah": float(state.vah),
                "val": float(state.val),
                "poc": float(state.poc),
            },
        },
    }


def _balance_breakout_events(state: MarketProfileBarState) -> list[dict[str, Any]]:
    if state.previous_location == "inside_value" and state.location == "above_value":
        return [{
            "key": "balance_breakout_long",
            "direction": "long",
            "metadata": {
                "trigger_price": float(state.close),
                "reference": build_value_area_reference(state, level_name="VAH", price=state.vah),
            },
        }]
    if state.previous_location == "inside_value" and state.location == "below_value":
        return [{
            "key": "balance_breakout_short",
            "direction": "short",
            "metadata": {
                "trigger_price": float(state.close),
                "reference": build_value_area_reference(state, level_name="VAL", price=state.val),
            },
        }]
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


__all__ = ["SIGNAL_EVENT_BUILDERS", "build_signal_outputs", "build_value_area_reference"]
