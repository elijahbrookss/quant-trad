"""Output builders for market profile runtime."""

from __future__ import annotations

from datetime import datetime

from engines.indicator_engine.contracts import RuntimeOutput

from .models import MarketProfileBarState
from .signals import build_signal_outputs


def build_not_ready_outputs(bar_time: datetime) -> dict[str, RuntimeOutput]:
    return {
        "value_area_metrics": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "value_location": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "balance_state": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "balance_breakout": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "confirmed_balance_breakout": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "balance_reclaim": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "balance_retest": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
    }


def build_market_profile_outputs(
    state: MarketProfileBarState,
    *,
    additional_signal_events: dict[str, list[dict[str, object]]] | None = None,
) -> dict[str, RuntimeOutput]:
    outputs = {
        "value_area_metrics": RuntimeOutput(
            bar_time=state.bar_time,
            ready=True,
            value={
                "poc": state.poc,
                "vah": state.vah,
                "val": state.val,
                "value_area_width": max(state.vah - state.val, 0.0),
            },
        ),
        "value_location": RuntimeOutput(
            bar_time=state.bar_time,
            ready=True,
            value={"state_key": state.location},
        ),
        "balance_state": RuntimeOutput(
            bar_time=state.bar_time,
            ready=True,
            value={"state_key": state.balance_state},
        ),
    }
    outputs.update(build_signal_outputs(state))
    for output_name in (
        "confirmed_balance_breakout",
        "balance_reclaim",
        "balance_retest",
    ):
        outputs[output_name] = RuntimeOutput(
            bar_time=state.bar_time,
            ready=True,
            value={"events": list((additional_signal_events or {}).get(output_name) or [])},
        )
    return outputs


__all__ = ["build_market_profile_outputs", "build_not_ready_outputs"]
