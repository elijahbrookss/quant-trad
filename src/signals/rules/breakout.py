"""Shared helpers for breakout confirmation tracking across rules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol


class BreakoutConfig(Protocol):
    """Protocol describing breakout confirmation configuration."""

    confirmation_bars: int
    early_confirmation_window: int
    early_confirmation_distance_pct: float
    accelerated_confirmation_min_bars: int


@dataclass
class BreakoutRunState:
    """Mutable tracking state for breakout confirmation windows."""

    active_side: Optional[str] = None
    candidate_start_pos: Optional[int] = None
    consecutive: int = 0
    max_distance: float = 0.0
    run_confirmed: bool = False
    run_emitted: bool = False


@dataclass(frozen=True)
class BreakoutEvaluationResult:
    """Outcome of processing a single candle for breakout confirmation."""

    ready: bool
    just_confirmed: bool
    accelerated: bool
    start_position: Optional[int]
    consecutive: int
    active_side: Optional[str]


def reset_breakout_state(state: BreakoutRunState) -> None:
    """Reset a breakout state to its initial values."""

    state.active_side = None
    state.candidate_start_pos = None
    state.consecutive = 0
    state.max_distance = 0.0
    state.run_confirmed = False
    state.run_emitted = False


def mark_breakout_emitted(state: BreakoutRunState) -> None:
    """Mark the current breakout run as emitted to avoid duplicates."""

    state.run_emitted = True


def update_breakout_state(
    state: BreakoutRunState,
    *,
    side: Optional[str],
    clearance: float,
    position: int,
    level_price: float,
    config: BreakoutConfig,
    allow_accelerated: bool = True,
) -> BreakoutEvaluationResult:
    """Update a breakout state with the latest candle classification.

    Parameters
    ----------
    state:
        Mutable tracking state shared across candles.
    side:
        "above" or "below" when the candle is fully outside the level, otherwise
        any other value is treated as neutral and resets the run.
    clearance:
        The maximum distance between the candle and the level in the breakout
        direction.
    position:
        Zero-based index of the processed candle within the evaluated window.
    level_price:
        Price of the level being evaluated.
    config:
        Breakout confirmation configuration providing confirmation and early
        acceleration thresholds.
    """

    if side not in {"above", "below"}:
        reset_breakout_state(state)
        return BreakoutEvaluationResult(
            ready=False,
            just_confirmed=False,
            accelerated=False,
            start_position=None,
            consecutive=0,
            active_side=None,
        )

    if state.active_side != side:
        state.active_side = side
        state.candidate_start_pos = position
        state.consecutive = 1
        state.max_distance = max(0.0, float(clearance))
        state.run_confirmed = False
        state.run_emitted = False
    else:
        state.consecutive += 1
        state.max_distance = max(state.max_distance, float(clearance))

    ready = state.consecutive >= max(1, int(config.confirmation_bars))
    accelerated = False

    if allow_accelerated and not ready and state.candidate_start_pos is not None:
        threshold = abs(float(level_price)) * float(config.early_confirmation_distance_pct)
        if threshold > 0:
            bars_since_start = position - state.candidate_start_pos + 1
            if bars_since_start <= max(1, int(config.early_confirmation_window)):
                try:
                    configured_min = int(getattr(config, "accelerated_confirmation_min_bars", 0))
                except (TypeError, ValueError):
                    configured_min = 0
                if configured_min <= 0:
                    configured_min = max(1, int(config.confirmation_bars) - 1)
                required_accelerated_bars = max(
                    1, min(int(config.confirmation_bars), configured_min)
                )
                if state.max_distance >= threshold and state.consecutive >= required_accelerated_bars:
                    ready = True
                    accelerated = True

    just_confirmed = False
    if ready and not state.run_confirmed:
        state.run_confirmed = True
        just_confirmed = True

    return BreakoutEvaluationResult(
        ready=ready and not state.run_emitted,
        just_confirmed=just_confirmed,
        accelerated=accelerated,
        start_position=state.candidate_start_pos,
        consecutive=state.consecutive,
        active_side=state.active_side,
    )
