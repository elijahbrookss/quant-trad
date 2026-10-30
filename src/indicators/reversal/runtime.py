"""One incremental recognizer; current-bar typed deltas without future labels."""
from dataclasses import asdict
from datetime import timedelta

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import Indicator, IndicatorGapRejectedError, RuntimeOutput
from indicators.manifest import build_runtime_spec
from research_science.recognition import RecognitionCandle, RecognitionContext, plain, utc
from research_science.reversal_recognition import ReversalRecognizer, ReversalSettings
from research_science.completed_event_context import CompletedEventContext
from research_science.reversal_completed_recovery import recovery_candles
from research_science.reversal_selection import stage_availability
from research_science.hashing import stable_hash

from .manifest import MANIFEST


class TypedReversalIndicator(Indicator):
    def __init__(self, *, indicator_id, version, params, context, candles):
        if not isinstance(context, RecognitionContext) or context.timeframe_seconds != 900:
            raise ValueError("reversal_runtime_invalid: native 15m recognition context required")
        if not isinstance(candles, tuple) or not candles or any(not isinstance(x, RecognitionCandle) for x in candles):
            raise ValueError("reversal_runtime_invalid: immutable typed source projection required")
        if len(candles) > params["max_bars"]:
            raise ValueError("reversal_runtime_limit: source frame exceeds maximum bars")
        self.runtime_spec = build_runtime_spec(MANIFEST, instance_id=indicator_id, version=version)
        self.context = context
        self.settings = ReversalSettings(params["reference_basis"], params["ownership"],
                                         params["max_bars"], params["max_candidates"])
        if self.settings.reference_basis != "rising_leg_plus_retreat" or self.settings.max_bars > 19104:
            raise ValueError("reversal_runtime_invalid: unsupported settings")
        self._source = candles
        self.initialize()

    def initialize(self):
        """Explicit fresh computation from the declared origin; never gap reset."""
        self._recognizer = ReversalRecognizer(self.context, self.settings)
        self._candles = []
        self._reference_count = self._completion_count = 0
        self._pending = {}
        self._failed = False
        self._outputs = {
            "provisional_reversals": RuntimeOutput(self.context.computation_origin, False, {"events": []}),
            "candidate_lifecycle": RuntimeOutput(self.context.computation_origin, False, {"events": []}),
            "reversal_context": RuntimeOutput(self.context.computation_origin, False,
                                               {"state_key": "provisional", "fields": {}}),
        }

    def handle_gap(self, *, policy, gap, next_bar_time, rewarm_bars):
        # Even a caller's continue/reset request cannot redefine the recognizer.
        self._failed = True
        raise IndicatorGapRejectedError(indicator_id=self.runtime_spec.instance_id, gap=gap)

    @staticmethod
    def _reference(event):
        level = event.levels[0]
        return {"kind": "provisional_rebound_reference", "key": event.event_id,
                "price": level.price, "source": event.context.source_identity_key,
                "formed_at": plain(level.observed_at), "known_at": plain(level.role_known_at),
                "context": event.to_dict()}

    def _lifecycle(self, reference, *, stage, known_at, metrics, completion=None):
        return {"candidate_id": reference.event_id, "family": "provisional_reversal",
                "side": "long", "stage": stage, "status": "provisional",
                "group_key": reference.event_id, "known_at": plain(known_at),
                "reason": "retain_all_provisional_no_independence_or_decision_claim",
                "reference": self._reference(reference), "metrics": metrics,
                **({"source_event_id": completion.event_id} if completion else {})}

    def apply_bar(self, bar, inputs):
        if self._failed:
            raise RuntimeError("reversal_runtime_failed: initialize a new explicit computation")
        try:
            self._apply_bar(bar, inputs)
        except Exception:
            self._failed = True
            raise

    def _apply_bar(self, bar, inputs):
        if not isinstance(bar, Candle) or inputs:
            raise ValueError("reversal_runtime_invalid: Candle and no dependency inputs required")
        index = len(self._candles)
        if index >= self.settings.max_bars or index >= len(self._source):
            raise ValueError("reversal_runtime_limit: maximum bars/source projection exhausted")
        source = self._source[index]
        if (utc(bar.time) != source.open_time or bar.end is None or utc(bar.end) != source.close_time
                or bar.known_at is None or utc(bar.known_at) != source.known_at
                or (bar.open, bar.high, bar.low, bar.close) !=
                   (source.open, source.high, source.low, source.close)):
            raise ValueError("reversal_runtime_invalid: current Candle disagrees with exact source projection")
        self._recognizer.apply_bar(source)
        self._candles.append(source)
        snapshot = self._recognizer.snapshot()
        references = snapshot.references[self._reference_count:]
        completions = snapshot.candidates[self._completion_count:]
        self._reference_count, self._completion_count = len(snapshot.references), len(snapshot.candidates)
        known_at = source.known_at
        signals, lifecycle = [], []
        for reference in references:
            lifecycle.append(self._lifecycle(reference, stage="reference_confirmed", known_at=known_at,
                metrics={"stages": plain([asdict(x) for x in reference.stages]),
                         "levels": plain([asdict(x) for x in reference.levels])}))
        current_pending = {}
        for pending in snapshot.pending:
            material = pending.to_dict()
            identity = pending.reference.event_id
            fingerprint = stable_hash(material)
            current_pending[identity] = fingerprint
            if identity in self._pending and self._pending[identity] != fingerprint:
                lifecycle.append(self._lifecycle(pending.reference, stage="progress", known_at=known_at,
                    metrics={"stages": material["stages"], "levels": material["levels"],
                             "lowest_role": "lowest_so_far_not_final"}))
        self._pending = current_pending
        if completions:
            context = CompletedEventContext(tuple(self._candles), snapshot.candidates,
                evaluation_start=self.context.computation_origin,
                evaluation_end=known_at+timedelta(seconds=900))
            references_by_id = {x.event_id: x for x in snapshot.references}
            for event in completions:
                if event.known_at != known_at:
                    raise RuntimeError("reversal_runtime_invalid: old completion delta")
                n = recovery_candles(event)
                i = context.by_close[event.known_at]
                formation = i-context.by_open[event.formation_start]+1
                features = context.geometry(i, formation)
                metadata = {"recognition": event.to_dict(), "stages": stage_availability(event),
                    "reference": self._reference(references_by_id[event.source_events[0].event_id]),
                    "recovery_candles": n, "context": features,
                    "context_missing_reason": None if features is not None else "strict_prior_context_unavailable",
                    "ownership": self.settings.ownership, "independent_event_claim": False,
                    "strategy_decision_claim": False}
                signals.append({"key": "provisional_reversal_completed", "direction": "long",
                                "pattern_id": event.event_id, "known_at": plain(known_at), "metadata": metadata})
                lifecycle.append(self._lifecycle(references_by_id[event.source_events[0].event_id],
                    stage="completion", known_at=known_at, metrics=metadata, completion=event))
        fields = {"instrument_id": self.context.instrument_id,
            "source_identity_key": self.context.source_identity_key,
            "computation_origin": plain(self.context.computation_origin),
            "processed_bars": snapshot.processed_bars, "reference_count": self._reference_count,
            "completion_count": self._completion_count, "ownership": self.settings.ownership,
            "reference_basis": self.settings.reference_basis}
        self._outputs = {
            "provisional_reversals": RuntimeOutput(bar.time, True, {"events": signals}),
            "candidate_lifecycle": RuntimeOutput(bar.time, True, {"events": lifecycle}),
            "reversal_context": RuntimeOutput(bar.time, True, {"state_key": "provisional", "fields": fields}),
        }

    def snapshot(self):
        return {name: output.copy() for name, output in self._outputs.items()}
