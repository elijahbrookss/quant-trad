"""Stateful breakout, reclaim, and retest tracking for Market Profile."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import MarketProfileBarState
from .signals import build_value_area_reference


def _as_positive_int(value: Any, *, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"market_profile_config_invalid: {field} must be int") from exc
    if parsed <= 0:
        raise RuntimeError(f"market_profile_config_invalid: {field} must be > 0")
    return parsed


def _as_non_negative_float(value: Any, *, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"market_profile_config_invalid: {field} must be numeric") from exc
    if parsed < 0.0:
        raise RuntimeError(f"market_profile_config_invalid: {field} must be >= 0")
    return parsed


def _epoch(value: datetime) -> int:
    return int(value.timestamp())


def _outside_location(direction: str) -> str:
    return "above_value" if direction == "long" else "below_value"


def _level_name(direction: str) -> str:
    return "VAH" if direction == "long" else "VAL"


def _level_price(state: MarketProfileBarState, direction: str) -> float:
    return float(state.vah if direction == "long" else state.val)


def _breakout_direction(state: MarketProfileBarState) -> str | None:
    if state.previous_location == "inside_value" and state.location == "above_value":
        return "long"
    if state.previous_location == "inside_value" and state.location == "below_value":
        return "short"
    return None


@dataclass
class AtrState:
    period: int
    previous_close: float | None = None
    seeded_count: int = 0
    seeded_sum: float = 0.0
    current_value: float | None = None

    def step(self, state: MarketProfileBarState) -> float:
        true_range = float(state.high - state.low)
        if self.previous_close is not None:
            true_range = max(
                true_range,
                abs(float(state.high) - self.previous_close),
                abs(float(state.low) - self.previous_close),
            )

        if self.current_value is None or self.seeded_count < self.period:
            self.seeded_count += 1
            self.seeded_sum += true_range
            self.current_value = self.seeded_sum / float(self.seeded_count)
        else:
            self.current_value = (
                ((self.current_value * float(self.period - 1)) + true_range)
                / float(self.period)
            )

        self.previous_close = float(state.close)
        return float(self.current_value)


@dataclass
class BreakoutSequence:
    pattern_id: str
    profile_key: str
    direction: str
    breakout_time: datetime
    breakout_trigger_price: float
    reference_price: float
    outside_bars: int = 1
    confirmed_time: datetime | None = None
    confirmed_trigger_price: float | None = None
    bars_since_confirmation: int = 0
    reclaim_touch_time: datetime | None = None
    reclaim_touch_price: float | None = None
    outside_bars_since_confirmation: int = 0
    acceptance_time: datetime | None = None
    acceptance_trigger_price: float | None = None
    acceptance_atr: float | None = None
    max_excursion_from_reference: float = 0.0
    bars_since_acceptance: int = 0
    retest_touch_time: datetime | None = None
    retest_touch_price: float | None = None
    retest_touch_penetration: float | None = None
    hold_confirm_bars_observed: int = 0
    hold_confirm_time: datetime | None = None
    hold_confirm_price: float | None = None

    @property
    def level_name(self) -> str:
        return _level_name(self.direction)

    @property
    def phase(self) -> str:
        if self.retest_touch_time is not None:
            return "retest_touched"
        if self.acceptance_time is not None:
            return "accepted"
        if self.reclaim_touch_time is not None:
            return "reclaim_touched"
        if self.confirmed_time is not None:
            return "confirmed"
        return "candidate"


class BreakoutRetestStateMachine:
    def __init__(
        self,
        *,
        breakout_confirm_bars: Any,
        reclaim_max_bars: Any,
        retest_min_acceptance_bars: Any,
        retest_min_excursion_atr: Any,
        retest_max_bars: Any,
        retest_atr_period: Any,
        retest_touch_tolerance_atr: Any,
        retest_max_penetration_atr: Any,
        retest_hold_confirm_bars: Any,
    ) -> None:
        self._breakout_confirm_bars = _as_positive_int(
            breakout_confirm_bars,
            field="breakout_confirm_bars",
        )
        self._reclaim_max_bars = _as_positive_int(
            reclaim_max_bars,
            field="reclaim_max_bars",
        )
        self._retest_min_acceptance_bars = _as_positive_int(
            retest_min_acceptance_bars,
            field="retest_min_acceptance_bars",
        )
        self._retest_min_excursion_atr = _as_non_negative_float(
            retest_min_excursion_atr,
            field="retest_min_excursion_atr",
        )
        self._retest_max_bars = _as_positive_int(
            retest_max_bars,
            field="retest_max_bars",
        )
        self._retest_touch_tolerance_atr = _as_non_negative_float(
            retest_touch_tolerance_atr,
            field="retest_touch_tolerance_atr",
        )
        self._retest_max_penetration_atr = _as_non_negative_float(
            retest_max_penetration_atr,
            field="retest_max_penetration_atr",
        )
        self._retest_hold_confirm_bars = _as_positive_int(
            retest_hold_confirm_bars,
            field="retest_hold_confirm_bars",
        )
        self._atr = AtrState(
            period=_as_positive_int(retest_atr_period, field="retest_atr_period"),
        )
        self._sequence: BreakoutSequence | None = None

    def step(self, state: MarketProfileBarState) -> dict[str, list[dict[str, Any]]]:
        events = {
            "confirmed_balance_breakout": [],
            "balance_reclaim": [],
            "balance_retest": [],
        }
        current_atr = self._atr.step(state)
        raw_breakout_direction = _breakout_direction(state)
        consumed_raw_breakout = False

        if self._sequence is not None and self._sequence.profile_key != state.active_profile_key:
            self._sequence = None

        if self._sequence is not None:
            sequence = self._sequence
            if sequence.phase == "candidate":
                if raw_breakout_direction and raw_breakout_direction != sequence.direction:
                    self._sequence = None
                elif state.location == _outside_location(sequence.direction):
                    sequence.outside_bars += 1
                    if sequence.outside_bars >= self._breakout_confirm_bars:
                        sequence.confirmed_time = state.bar_time
                        sequence.confirmed_trigger_price = float(state.close)
                        events["confirmed_balance_breakout"].append(
                            self._confirmed_event(state, sequence)
                        )
                else:
                    self._sequence = None
            else:
                if sequence.confirmed_time is not None and state.bar_time != sequence.confirmed_time:
                    sequence.bars_since_confirmation += 1
                if raw_breakout_direction and raw_breakout_direction != sequence.direction:
                    self._sequence = None
                elif sequence.phase == "reclaim_touched":
                    if sequence.bars_since_confirmation > self._reclaim_max_bars:
                        self._sequence = None
                    elif raw_breakout_direction == sequence.direction:
                        events["balance_reclaim"].append(self._reclaim_event(state, sequence))
                        self._sequence = None
                        consumed_raw_breakout = True
                    elif state.location == _outside_location(sequence.direction):
                        self._sequence = None
                    elif state.location != "inside_value":
                        self._sequence = None
                else:
                    self._step_post_confirmation(
                        state=state,
                        sequence=sequence,
                        current_atr=current_atr,
                        events=events,
                    )
                    if self._sequence is None and raw_breakout_direction == sequence.direction:
                        consumed_raw_breakout = bool(events["balance_retest"])

        if self._sequence is None and raw_breakout_direction and not consumed_raw_breakout:
            self._sequence = self._start_sequence(state, raw_breakout_direction)
            if self._breakout_confirm_bars == 1:
                self._sequence.confirmed_time = state.bar_time
                self._sequence.confirmed_trigger_price = float(state.close)
                events["confirmed_balance_breakout"].append(
                    self._confirmed_event(state, self._sequence)
                )

        return events

    def _step_post_confirmation(
        self,
        *,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
        current_atr: float,
        events: dict[str, list[dict[str, Any]]],
    ) -> None:
        if sequence.phase == "confirmed":
            self._update_acceptance_tracking(state=state, sequence=sequence)
            if (
                sequence.acceptance_time is None
                and state.location == "inside_value"
                and sequence.bars_since_confirmation <= self._reclaim_max_bars
            ):
                sequence.reclaim_touch_time = state.bar_time
                sequence.reclaim_touch_price = float(state.close)
                return
            if sequence.acceptance_time is None and self._acceptance_established(
                sequence=sequence,
                current_atr=current_atr,
            ):
                sequence.acceptance_time = state.bar_time
                sequence.acceptance_trigger_price = float(state.close)
                sequence.acceptance_atr = float(current_atr)
                return
            if state.location != _outside_location(sequence.direction):
                self._sequence = None
                return

        if sequence.acceptance_time is None:
            return

        if state.bar_time != sequence.acceptance_time:
            sequence.bars_since_acceptance += 1
        if sequence.bars_since_acceptance > self._retest_max_bars:
            self._sequence = None
            return

        if self._retest_penetration_exceeded(
            state=state,
            sequence=sequence,
            current_atr=current_atr,
        ):
            self._sequence = None
            return

        touched = self._touches_retest_band(
            state=state,
            sequence=sequence,
            current_atr=current_atr,
        )
        if touched and sequence.retest_touch_time is None:
            sequence.retest_touch_time = state.bar_time
            sequence.retest_touch_price = self._touch_price(state, sequence.direction)
            sequence.retest_touch_penetration = self._penetration_from_reference(
                state=state,
                sequence=sequence,
            )

        if sequence.retest_touch_time is None:
            return

        if self._holds_reference(state=state, sequence=sequence):
            sequence.hold_confirm_bars_observed += 1
            sequence.hold_confirm_time = state.bar_time
            sequence.hold_confirm_price = float(state.close)
            if sequence.hold_confirm_bars_observed >= self._retest_hold_confirm_bars:
                events["balance_retest"].append(
                    self._retest_event(
                        state=state,
                        sequence=sequence,
                        current_atr=current_atr,
                    )
                )
                self._sequence = None
        else:
            sequence.hold_confirm_bars_observed = 0
            sequence.hold_confirm_time = None
            sequence.hold_confirm_price = None

    def _update_acceptance_tracking(
        self,
        *,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
    ) -> None:
        if state.location != _outside_location(sequence.direction):
            return
        sequence.outside_bars_since_confirmation += 1
        sequence.max_excursion_from_reference = max(
            float(sequence.max_excursion_from_reference),
            self._excursion_from_reference(state=state, sequence=sequence),
        )

    def _acceptance_established(
        self,
        *,
        sequence: BreakoutSequence,
        current_atr: float,
    ) -> bool:
        required_excursion = float(current_atr) * self._retest_min_excursion_atr
        return (
            sequence.outside_bars_since_confirmation >= self._retest_min_acceptance_bars
            and sequence.max_excursion_from_reference >= required_excursion
        )

    def _excursion_from_reference(
        self,
        *,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
    ) -> float:
        if sequence.direction == "long":
            return max(float(state.high) - sequence.reference_price, 0.0)
        return max(sequence.reference_price - float(state.low), 0.0)

    def _penetration_from_reference(
        self,
        *,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
    ) -> float:
        if sequence.direction == "long":
            return max(sequence.reference_price - float(state.low), 0.0)
        return max(float(state.high) - sequence.reference_price, 0.0)

    def _touch_price(self, state: MarketProfileBarState, direction: str) -> float:
        return float(state.low if direction == "long" else state.high)

    def _touches_retest_band(
        self,
        *,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
        current_atr: float,
    ) -> bool:
        tolerance = float(current_atr) * self._retest_touch_tolerance_atr
        penetration = self._penetration_from_reference(state=state, sequence=sequence)
        if penetration > float(current_atr) * self._retest_max_penetration_atr:
            return False
        if sequence.direction == "long":
            return float(state.low) <= (sequence.reference_price + tolerance)
        return float(state.high) >= (sequence.reference_price - tolerance)

    def _retest_penetration_exceeded(
        self,
        *,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
        current_atr: float,
    ) -> bool:
        max_penetration = float(current_atr) * self._retest_max_penetration_atr
        return self._penetration_from_reference(state=state, sequence=sequence) > max_penetration

    def _holds_reference(
        self,
        *,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
    ) -> bool:
        if sequence.direction == "long":
            return float(state.close) > sequence.reference_price
        return float(state.close) < sequence.reference_price

    def _start_sequence(
        self,
        state: MarketProfileBarState,
        direction: str,
    ) -> BreakoutSequence:
        breakout_epoch = _epoch(state.bar_time)
        return BreakoutSequence(
            pattern_id=(
                f"market_profile_breakout_sequence:"
                f"{state.active_profile_key}:{direction}:{breakout_epoch}"
            ),
            profile_key=state.active_profile_key,
            direction=direction,
            breakout_time=state.bar_time,
            breakout_trigger_price=float(state.close),
            reference_price=_level_price(state, direction),
        )

    def _confirmed_event(
        self,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
    ) -> dict[str, Any]:
        return {
            "key": f"confirmed_balance_breakout_{sequence.direction}",
            "direction": sequence.direction,
            "pattern_id": sequence.pattern_id,
            "known_at": _epoch(state.bar_time),
            "metadata": {
                "trigger_price": float(state.close),
                "reference": build_value_area_reference(
                    state,
                    level_name=sequence.level_name,
                    price=sequence.reference_price,
                ),
                "sequence_state": "confirmed_breakout",
                "breakout_event_key": f"balance_breakout_{sequence.direction}",
                "breakout_time": _epoch(sequence.breakout_time),
                "breakout_trigger_price": float(sequence.breakout_trigger_price),
                "confirmation_bars_required": int(self._breakout_confirm_bars),
                "outside_bars_observed": int(sequence.outside_bars),
                "reclaim_max_bars": int(self._reclaim_max_bars),
                "retest_max_bars": int(self._retest_max_bars),
            },
        }

    def _reclaim_event(
        self,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
    ) -> dict[str, Any]:
        return {
            "key": f"balance_reclaim_{sequence.direction}",
            "direction": sequence.direction,
            "pattern_id": sequence.pattern_id,
            "known_at": _epoch(state.bar_time),
            "metadata": {
                "trigger_price": float(state.close),
                "reference": build_value_area_reference(
                    state,
                    level_name=sequence.level_name,
                    price=sequence.reference_price,
                ),
                "sequence_state": "reclaim_confirmed",
                "breakout_time": _epoch(sequence.breakout_time),
                "breakout_trigger_price": float(sequence.breakout_trigger_price),
                "confirmed_time": _epoch(sequence.confirmed_time or sequence.breakout_time),
                "confirmed_trigger_price": float(
                    sequence.confirmed_trigger_price
                    if sequence.confirmed_trigger_price is not None
                    else sequence.breakout_trigger_price
                ),
                "reclaim_touch_time": _epoch(sequence.reclaim_touch_time or state.bar_time),
                "reclaim_touch_price": float(
                    sequence.reclaim_touch_price
                    if sequence.reclaim_touch_price is not None
                    else state.close
                ),
                "bars_since_confirmation": int(sequence.bars_since_confirmation),
                "reclaim_max_bars": int(self._reclaim_max_bars),
            },
        }

    def _retest_event(
        self,
        *,
        state: MarketProfileBarState,
        sequence: BreakoutSequence,
        current_atr: float,
    ) -> dict[str, Any]:
        return {
            "key": f"balance_retest_{sequence.direction}",
            "direction": sequence.direction,
            "pattern_id": sequence.pattern_id,
            "known_at": _epoch(state.bar_time),
            "metadata": {
                "trigger_price": float(state.close),
                "reference": build_value_area_reference(
                    state,
                    level_name=sequence.level_name,
                    price=sequence.reference_price,
                ),
                "sequence_state": "retest_confirmed",
                "breakout_time": _epoch(sequence.breakout_time),
                "breakout_trigger_price": float(sequence.breakout_trigger_price),
                "confirmed_time": _epoch(sequence.confirmed_time or sequence.breakout_time),
                "confirmed_trigger_price": float(
                    sequence.confirmed_trigger_price
                    if sequence.confirmed_trigger_price is not None
                    else sequence.breakout_trigger_price
                ),
                "acceptance_time": _epoch(sequence.acceptance_time or state.bar_time),
                "acceptance_trigger_price": float(
                    sequence.acceptance_trigger_price
                    if sequence.acceptance_trigger_price is not None
                    else state.close
                ),
                "acceptance_atr": float(
                    sequence.acceptance_atr
                    if sequence.acceptance_atr is not None
                    else current_atr
                ),
                "outside_bars_since_confirmation": int(
                    sequence.outside_bars_since_confirmation
                ),
                "max_excursion_from_reference": float(
                    sequence.max_excursion_from_reference
                ),
                "retest_touch_time": _epoch(sequence.retest_touch_time or state.bar_time),
                "retest_touch_price": float(
                    sequence.retest_touch_price
                    if sequence.retest_touch_price is not None
                    else self._touch_price(state, sequence.direction)
                ),
                "retest_touch_penetration": float(
                    sequence.retest_touch_penetration
                    if sequence.retest_touch_penetration is not None
                    else self._penetration_from_reference(state=state, sequence=sequence)
                ),
                "hold_confirm_time": _epoch(sequence.hold_confirm_time or state.bar_time),
                "hold_confirm_price": float(
                    sequence.hold_confirm_price
                    if sequence.hold_confirm_price is not None
                    else state.close
                ),
                "hold_confirm_bars_observed": int(sequence.hold_confirm_bars_observed),
                "retest_min_acceptance_bars": int(self._retest_min_acceptance_bars),
                "retest_min_excursion_atr": float(self._retest_min_excursion_atr),
                "retest_max_bars": int(self._retest_max_bars),
                "retest_atr_period": int(self._atr.period),
                "retest_touch_tolerance_atr": float(self._retest_touch_tolerance_atr),
                "retest_max_penetration_atr": float(self._retest_max_penetration_atr),
                "retest_hold_confirm_bars": int(self._retest_hold_confirm_bars),
            },
        }


__all__ = ["BreakoutRetestStateMachine"]
