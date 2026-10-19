"""Provisional decline/rebound/lower-low/reclaim candidate recognition.

One sequential state implementation serves research methods and visual tools.
Candidate chains are not a settled independent-event population. No outcome,
filter fitting, source-event arbitration, trading signal or implicit reset.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Callable, Mapping, Any

from .recognition import (RecognitionCandle, RecognitionContext, RecognitionEvent,
                          RecognitionLevel, RecognitionStage, link_source_event)
from .hashing import stable_hash

DEFINITION_ID = "completed_reversal_candidates"
DEFINITION_VERSION = "provisional.1"


@dataclass(frozen=True)
class ReversalSettings:
    reference_basis: str
    ownership: str
    max_bars: int
    max_candidates: int

    def __post_init__(self) -> None:
        if self.reference_basis not in {"rising_leg_only", "rising_leg_plus_retreat"}:
            raise ValueError("reversal_recognition_invalid: unsupported reference basis")
        if self.ownership != "retain_all_provisional":
            raise ValueError("reversal_recognition_invalid: Study event ownership remains unresolved")
        for name, maximum in (("max_bars", 100000), ("max_candidates", 10000)):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"reversal_recognition_invalid: {name} out of bounds")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ReversalSettings:
        if set(value) != {"reference_basis", "ownership", "max_bars", "max_candidates"}:
            raise ValueError("reversal_recognition_invalid: all supported settings must be explicit")
        return cls(**dict(value))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PendingReversal:
    reference: RecognitionEvent
    stages: tuple[RecognitionStage, ...]
    levels: tuple[RecognitionLevel, ...]

    def to_dict(self) -> dict[str, Any]:
        from .recognition import plain
        return {"reference": self.reference.to_dict(),
                "stages": plain([asdict(x) for x in self.stages]),
                "levels": plain([asdict(x) for x in self.levels])}


@dataclass(frozen=True)
class ReversalSnapshot:
    references: tuple[RecognitionEvent, ...]
    candidates: tuple[RecognitionEvent, ...]
    pending: tuple[PendingReversal, ...]
    processed_bars: int
    last_known_at: datetime | None


@dataclass
class _Candidate:
    reference: RecognitionEvent
    anchor_low: float
    push: RecognitionCandle | None = None
    lowest: RecognitionCandle | None = None
    recovery: RecognitionCandle | None = None
    completed: bool = False


class ReversalRecognizer:
    """initialize -> apply_bar -> snapshot. No chart scope enters this state.

    Decline needs one lower-close step; flat closes can extend an existing leg.
    The first higher close starts a rebound; the first lower close confirms it.
    The two reference policies are explicit competing semantic proposals.
    There is no candidate expiry, minimum move, deduplication or episode reset.
    Each reference records its own first candidate completion once; deciding
    which overlapping chains become Study events remains outside this pilot.
    """

    def __init__(self, context: RecognitionContext, settings: ReversalSettings):
        self.context = context
        self.settings = settings
        self.configuration_hash = stable_hash(settings.to_dict())
        self._bars: list[RecognitionCandle] = []
        self._down: int | None = None
        self._up: int | None = None
        self._leg: tuple[int, int, float] | None = None
        self._refs: list[_Candidate] = []
        self._events: list[RecognitionEvent] = []

    @staticmethod
    def _stage(name: str, bar: RecognitionCandle, *, known=None) -> RecognitionStage:
        return RecognitionStage(name, bar.close_time, known or bar.known_at, bar.candle_id)

    def _progress(self, ref: _Candidate) -> tuple[tuple[RecognitionStage, ...], tuple[RecognitionLevel, ...]]:
        stages = list(ref.reference.stages)
        levels = list(ref.reference.levels)
        if ref.push is not None:
            stages.append(self._stage("lower_low_push", ref.push))
        if ref.lowest is not None:
            stages.append(self._stage("lowest_push", ref.lowest))
            levels.append(RecognitionLevel("push_low", ref.lowest.low, ref.lowest.close_time,
                                            ref.lowest.known_at, ref.lowest.known_at, ref.lowest.candle_id))
        if ref.recovery is not None:
            stages.append(self._stage("recovery", ref.recovery))
        return tuple(stages), tuple(levels)

    def apply_bar(self, bar: RecognitionCandle) -> None:
        if not isinstance(bar, RecognitionCandle):
            raise ValueError("reversal_recognition_invalid: typed candle required")
        step = timedelta(seconds=self.context.timeframe_seconds)
        expected = self._bars[-1].close_time if self._bars else self.context.computation_origin
        if (bar.open_time != expected or bar.close_time-bar.open_time != step
                or bar.source_identity_key != self.context.source_identity_key):
            raise ValueError("reversal_recognition_invalid: origin, cadence or source mismatch; no gap compression/reset")
        if bar.known_at != bar.close_time:
            raise ValueError("reversal_recognition_invalid: this pilot requires close-available candles; no delayed-close reinterpretation")
        if len(self._bars) >= self.settings.max_bars:
            raise ValueError("reversal_recognition_limit: maximum bars exceeded")
        self._bars.append(bar)
        i = len(self._bars)-1
        if i == 0:
            return
        previous = self._bars[i-1]
        for ref in self._refs:
            if ref.completed:
                continue
            if ref.push is None and bar.low < ref.anchor_low:
                ref.push = ref.lowest = bar
            elif ref.push is not None and bar.low < ref.lowest.low:
                ref.lowest = bar
            if ref.push is not None and bar.open_time > ref.push.open_time and bar.close > previous.close:
                if ref.recovery is None:
                    ref.recovery = bar
            if ref.recovery is not None and bar.close > ref.reference.levels[0].price:
                stages, levels = self._progress(ref)
                event = RecognitionEvent(DEFINITION_ID, DEFINITION_VERSION, self.configuration_hash,
                    self.context, "provisional_reversal_candidate", ref.reference.formation_start,
                    bar.close_time, bar.known_at, (*stages, self._stage("completion", bar)), levels)
                event = link_source_event(event, ref.reference, consumed_at=ref.push.known_at,
                    max_age_seconds=self.settings.max_bars*self.context.timeframe_seconds,
                    relation="rebound_reference")
                self._events.append(event)
                ref.completed = True
        change = bar.close-previous.close
        if self._up is not None:
            if change < 0:
                if len(self._refs) >= self.settings.max_candidates:
                    raise ValueError("reversal_recognition_limit: maximum references exceeded; no truncation")
                stop = i if self.settings.reference_basis == "rising_leg_only" else i+1
                peak = max(self._bars[self._up:stop], key=lambda x: x.high)
                down, trough, low = self._leg
                stages = (self._stage("decline_start", self._bars[down], known=self._bars[down+1].known_at),
                          self._stage("preceding_trough", self._bars[trough]),
                          self._stage("rebound_start", self._bars[self._up]),
                          self._stage("rebound_end", previous), self._stage("reference_confirmed", bar))
                level = RecognitionLevel("rebound_high", peak.high, peak.close_time,
                                         peak.known_at, bar.known_at, peak.candle_id)
                low_bar = min(self._bars[down:trough+1], key=lambda x: x.low)
                anchor = RecognitionLevel("preceding_trough_low", low, low_bar.close_time,
                                          low_bar.known_at, self._bars[self._up].known_at, low_bar.candle_id)
                reference = RecognitionEvent(DEFINITION_ID, DEFINITION_VERSION, self.configuration_hash,
                    self.context, "provisional_rebound_reference", self._bars[down].open_time,
                    bar.close_time, bar.known_at, stages, (level, anchor))
                self._refs.append(_Candidate(reference, low))
                self._up = None
                self._leg = None
                self._down = i-1
        elif change < 0:
            if self._down is None:
                self._down = i-1
        elif change > 0 and self._down is not None:
            self._leg = (self._down, i-1, min(x.low for x in self._bars[self._down:i]))
            self._up = i
            self._down = None

    def snapshot(self) -> ReversalSnapshot:
        pending = tuple(PendingReversal(x.reference, *self._progress(x)) for x in self._refs if not x.completed)
        return ReversalSnapshot(tuple(x.reference for x in self._refs), tuple(self._events), pending,
                                len(self._bars), self._bars[-1].known_at if self._bars else None)


def recognize_reversals(candles: tuple[RecognitionCandle, ...], *, context: RecognitionContext,
                       settings: ReversalSettings, checkpoint: Callable[[], None] | None = None) -> ReversalSnapshot:
    """Convenience caller over the same incremental state; never a second detector."""
    state = ReversalRecognizer(context, settings)
    for candle in candles:
        if checkpoint is not None:
            checkpoint()
        state.apply_bar(candle)
    return state.snapshot()
