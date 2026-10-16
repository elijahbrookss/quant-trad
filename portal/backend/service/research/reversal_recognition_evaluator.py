"""Existing Check adapter for provisional recognition only; no forward outcomes."""
from __future__ import annotations

from dataclasses import dataclass
from collections import Counter
from datetime import timedelta
from typing import Mapping

from core.execution_control import execution_checkpoint
from data_providers.utils.ohlcv import interval_to_timedelta
from research_science.recognition import RecognitionCandle, RecognitionContext, plain, utc
from research_science.reversal_recognition import ReversalSettings, recognize_reversals
from .raw_candle_evaluator import _prepare

SCHEMA = "reversal_recognition.v1"
RESULT_SCHEMA = "reversal_recognition_result.v1"
RULES = {
    "recognition_status": "provisional", "population_status": "ownership_unresolved",
    "event_ownership": "check_candidate_records_not_independent_study_events",
    "timeline": "initialize at explicit computation_origin;apply closed bars;immutable snapshot",
    "reference_choices": ["rising_leg_only", "rising_leg_plus_retreat"],
    "ownership_choices": ["retain_all_provisional"],
    "definition": "decline;rebound confirmed at first lower close;later lower-low push;recovery;close above fixed reference",
    "minimum_size_filter": None, "candidate_expiry": None, "outcomes": "not_calculated",
    "gap_policy": "reject", "availability": "close_available_only",
    "evaluation_membership": "candidate completion close in [start,end)",
    "limits": {"evaluation_days": 32, "candles": 100000, "references": 10000},
    "inference": "none;visual_definition_preparation_only",
}


def choices(detector):
    if not isinstance(detector, Mapping) or set(detector) != {
        "type", "computation_origin", "reference_basis", "ownership", "max_bars", "max_candidates"
    } or detector["type"] != "completed_reversal_candidates":
        raise ValueError("reversal_recognition_invalid: explicit detector and all supported settings required")
    return ReversalSettings.from_dict({k: v for k, v in detector.items()
                                      if k not in {"type", "computation_origin"}})


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    settings = choices(detector)
    if outcomes != {"schema_version": SCHEMA} or statistics or inputs or scope.get("indicator_id") or scope.get("indicator_param_overrides"):
        raise ValueError("reversal_recognition_invalid: recognition-only schema;no outcomes,statistics,Indicator or extra inputs")
    if gap_policy != "reject":
        raise ValueError("reversal_recognition_invalid: reject gaps;no implicit state reset")
    start, end, origin = utc(scope.get("start")), utc(scope.get("end")), utc(detector["computation_origin"])
    step = interval_to_timedelta(str(scope.get("timeframe") or ""))
    seconds = int(step.total_seconds())
    if seconds <= 0 or not origin <= start < end or end-start > timedelta(days=32):
        raise ValueError("reversal_recognition_invalid: explicit origin<=start<end,maximum32evaluation days")
    if any(x.timestamp() % seconds for x in (origin, start, end)):
        raise ValueError("reversal_recognition_invalid: UTC interval-aligned boundaries required")
    warmup = int((start-origin).total_seconds())//seconds
    if "warmup_bars" in scope and (type(scope["warmup_bars"]) is not int or scope["warmup_bars"] != warmup):
        raise ValueError("reversal_recognition_invalid: warmup must equal explicit origin distance")
    if (end-origin).total_seconds()/seconds > settings.max_bars:
        raise ValueError("reversal_recognition_limit: requested origin-to-end exceeds max_bars")
    return {"schema_version": SCHEMA}


def prepare_recognition(*, plan, inputs, schema=SCHEMA):
    """Shared canonical preparation and one sequential recognizer for both Checks."""
    settings = choices(inputs["detector"])
    if inputs["outcomes"] != {"schema_version": schema} or plan.gap_policy != "reject":
        raise ValueError("reversal_recognition_invalid: pinned inputs required")
    primary = next(x for x in plan.market_data_requirements if x["alias"] == "primary_bars")
    seconds = int(primary["timeframe_seconds"])
    origin = utc(inputs["detector"]["computation_origin"])
    start, end = utc(plan.evaluation_range["start"]), utc(plan.evaluation_range["end_exclusive"])
    if utc(plan.materialization_range["start"]) != origin:
        raise ValueError("reversal_recognition_invalid: planned origin differs from pinned detector")
    _, rows = _prepare(inputs["candles"], step=timedelta(seconds=seconds), volume_required=False)
    if not rows or utc(rows[0]["open_time"]) != origin or utc(rows[-1]["close_time"]) != end:
        raise ValueError("reversal_recognition_invalid: incomplete origin-to-end candle path")
    candles = tuple(RecognitionCandle.from_mapping(r) for r in rows)
    context = RecognitionContext(str(primary["instrument_id"]), seconds, origin, candles[0].source_identity_key)
    snapshot = recognize_reversals(candles, context=context, settings=settings, checkpoint=execution_checkpoint)
    return settings, snapshot, rows, start, end, candles


@dataclass(frozen=True)
class ReversalRecognitionEvaluator:
    evaluator_id: str = "raw_forward_outcome"
    version: str = "9"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition, request):
        del definition
        normalize_configuration(detector=request.parameters["detector"], outcomes=request.parameters["outcomes"],
            statistics=request.parameters["statistics"], inputs=request.parameters["inputs"],
            scope=request.scope, gap_policy=request.parameters["gap_policy"])
        step = interval_to_timedelta(request.scope["timeframe"])
        n = int((utc(request.scope["start"])-utc(request.parameters["detector"]["computation_origin"]))/step)
        return {"input_kind": "market_data", "indicator_ids": [], "warmup_floor_bars": n,
            "feature_lookback_bars": n, "feature_windows_seconds_by_alias": {},
            "outcome_horizons": [], "required_outcome_horizons": [], "horizon_kind": "bars",
            "entry_lag_bars": 0, "invalidation_max_bars": 0,
            "outcome_boundary": "evaluation_end_exclusive",
            "primary_candle_required_fields": ["open", "high", "low", "close", "known_at"]}

    def evaluate(self, *, plan, inputs):
        settings, snapshot, rows, start, end, _ = prepare_recognition(plan=plan, inputs=inputs)
        origin = utc(inputs["detector"]["computation_origin"])
        events = [x.to_dict() for x in snapshot.candidates if start <= x.occurred_at < end]
        groups = {}
        for event in events:
            groups.setdefault(event["occurred_at"], []).append(event["event_id"])
        return {"schema_version": RESULT_SCHEMA, "check_family": self.evaluator_id, "status": "completed",
            "recognition_status": "provisional", "population_status": "ownership_unresolved",
            "recognition_choices": settings.to_dict(), "computation_origin": plain(origin),
            "context_left_censored": True, "processed_bars": snapshot.processed_bars,
            "snapshot_known_at": plain(snapshot.last_known_at), "candidate_count": len(events),
            "sample_count": 0, "study_events_admitted": 0, "outcomes_calculated": False,
            "events": events, "reference_events": [x.to_dict() for x in snapshot.references],
            "pending_candidates": [x.to_dict() for x in snapshot.pending],
            "simultaneous_candidate_groups": [{"occurred_at": t, "candidate_ids": ids}
                                               for t, ids in groups.items() if len(ids) > 1],
            "data_quality": dict(inputs.get("data_quality") or {}),
            "known_at_methods": dict(Counter(r["known_at_method"] for r in rows)),
            "recommendation": "settle reference and ownership semantics before outcome science",
            "caveats": ["No independent event population,statistical,causal,profitability or trading claim.",
                         "One completion per reference is candidate bookkeeping,not an episode ownership policy.",
                         "No candidate expiry or minimum move threshold;old references remain visible.",
                         "Frozen historical close availability is not measured live publication."]}
