"""Explicit historical clocks for the opt-in causal cash-spot bar model."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any


def bar_availability(candle: Any) -> datetime:
    end = candle.end_time
    known = candle.known_at or end
    if end <= candle.time or known != end:
        raise ValueError("causal_bar_requires_close_time_availability")
    return known


def execution_clock(candle: Any, assumptions: Any) -> datetime:
    return bar_availability(candle) if getattr(assumptions, "uses_causal_bar_clock", False) else candle.time


def execution_candle(candle: Any, assumptions: Any) -> Any:
    if not assumptions.uses_causal_bar_clock:
        return candle
    clock = bar_availability(candle)
    return replace(candle, time=clock, end=clock, known_at=clock)


def validate_causal_run_mode(assumptions: Any, run_type: str, execution_mode: str = "fast") -> None:
    if assumptions.uses_causal_bar_clock and assumptions.economic_claim_intent not in {"exploration", "economic"}:
        raise ValueError("causal_spot_resting_bar.v1 is development-only")
    if assumptions.uses_causal_bar_clock and str(run_type).lower() != "backtest":
        raise ValueError("causal_spot_resting_bar.v1 is backtest-only")
    if assumptions.uses_causal_bar_clock and str(execution_mode).lower() != "fast":
        raise ValueError("causal_spot_resting_bar.v1 requires fast bar execution")


def validate_output_availability(outputs: Any, clock: datetime) -> None:
    """Fail closed on future-owned clocks; do not truncate canonical output closure."""
    def visit(value: Any) -> None:
        if isinstance(value, dict) or hasattr(value, "items"):
            for key, item in value.items():
                if key == "known_at" and item is not None:
                    if isinstance(item, bool):
                        raise ValueError("causal_output_known_at_invalid")
                    if isinstance(item, datetime):
                        known = item
                    elif isinstance(item, (int, float)):
                        from datetime import timezone
                        known = datetime.fromtimestamp(item, timezone.utc)
                    else:
                        known = datetime.fromisoformat(str(item).replace("Z", "+00:00"))
                    if known.tzinfo is None or known > clock:
                        raise ValueError("causal_output_not_yet_available")
                else:
                    visit(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                visit(item)
    for output in outputs.values():
        if output.ready:
            for event in output.value.get("events", []):
                if "known_at" not in event or event["known_at"] is None:
                    raise ValueError("causal_signal_known_at_required")
            visit(output.value)


def validate_causal_execution_policy(assumptions: Any, profile: Any, plan: Any, input_capability: str) -> None:
    if not assumptions.uses_causal_bar_clock:
        return
    if profile.instrument.execution_semantics != "spot":
        raise ValueError("causal_spot_resting_bar requires cash spot")
    if plan.entry.order_type != "limit_maker" or plan.entry.limit_maker.fallback != "cancel":
        raise ValueError("causal_spot_resting_bar requires limit_maker with cancel fallback")
    if input_capability != "bars":
        raise ValueError("causal_spot_resting_bar requires bar inputs")
