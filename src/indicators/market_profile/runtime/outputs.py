"""Output builders for market profile runtime."""

from __future__ import annotations

from datetime import datetime

from engines.indicator_engine.contracts import RuntimeOutput

from .models import MarketProfileBarState
from .signals import build_signal_outputs


def _confirmed_breakout_metrics_output(
    *,
    bar_time: datetime,
    events: list[dict[str, object]],
) -> RuntimeOutput:
    if not events:
        return RuntimeOutput(bar_time=bar_time, ready=False, value={})
    if len(events) > 1:
        raise RuntimeError(
            "market_profile_confirmed_breakout_metrics_invalid: "
            f"expected at most one confirmed event, got {len(events)}"
        )

    event = events[0]
    metadata = event.get("metadata")
    if not isinstance(metadata, dict):
        raise RuntimeError(
            "market_profile_confirmed_breakout_metrics_invalid: metadata missing"
        )
    reference = metadata.get("reference")
    if not isinstance(reference, dict):
        raise RuntimeError(
            "market_profile_confirmed_breakout_metrics_invalid: reference missing"
        )

    required_fields = (
        "distance_from_reference",
        "distance_from_reference_abs",
        "distance_from_reference_pct",
        "trigger_price",
        "outside_bars_observed",
        "confirmation_bars_required",
    )
    missing = [field for field in required_fields if field not in metadata]
    if "price" not in reference:
        missing.append("reference.price")
    if missing:
        raise RuntimeError(
            "market_profile_confirmed_breakout_metrics_invalid: "
            f"missing fields={','.join(missing)}"
        )

    return RuntimeOutput(
        bar_time=bar_time,
        ready=True,
        value={
            "distance_from_reference": float(metadata["distance_from_reference"]),
            "distance_from_reference_abs": float(metadata["distance_from_reference_abs"]),
            "distance_from_reference_pct": float(metadata["distance_from_reference_pct"]),
            "trigger_price": float(metadata["trigger_price"]),
            "reference_price": float(reference["price"]),
            "outside_bars_observed": float(metadata["outside_bars_observed"]),
            "confirmation_bars_required": float(metadata["confirmation_bars_required"]),
        },
    )


def build_not_ready_outputs(bar_time: datetime) -> dict[str, RuntimeOutput]:
    return {
        "value_area_metrics": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "confirmed_breakout_metrics": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "value_location": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "balance_state": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "balance_breakout": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "confirmed_balance_breakout": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "balance_reclaim": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "balance_retest": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
        "candidate_lifecycle": RuntimeOutput(bar_time=bar_time, ready=False, value={}),
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
            value={
                "state_key": state.location,
                "fields": {
                    "active_profile_key": state.active_profile_key,
                    "previous_location": state.previous_location,
                },
            },
        ),
        "balance_state": RuntimeOutput(
            bar_time=state.bar_time,
            ready=True,
            value={"state_key": state.balance_state},
        ),
    }
    outputs.update(build_signal_outputs(state))
    additional_events = additional_signal_events or {}
    for output_name in (
        "confirmed_balance_breakout",
        "balance_reclaim",
        "balance_retest",
        "candidate_lifecycle",
    ):
        outputs[output_name] = RuntimeOutput(
            bar_time=state.bar_time,
            ready=True,
            value={"events": list(additional_events.get(output_name) or [])},
        )
    outputs["confirmed_breakout_metrics"] = _confirmed_breakout_metrics_output(
        bar_time=state.bar_time,
        events=list(additional_events.get("confirmed_balance_breakout") or []),
    )
    return outputs


__all__ = ["build_market_profile_outputs", "build_not_ready_outputs"]
