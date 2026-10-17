"""Admitted selection-only Check; no forward-outcome import or dispatch."""
from dataclasses import dataclass
from datetime import timedelta
import json
from time import monotonic

from core.execution_control import execution_checkpoint
from research_science.recognition import plain, utc
from research_science.reversal_selection import SCHEMA, RULES, select_comparisons
from .reversal_recognition_evaluator import choices, prepare_recognition

RESULT_SCHEMA = "reversal_selection_only_result.v1"


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    settings = choices(detector)
    start, end, origin = utc(scope.get("start")), utc(scope.get("end")), utc(detector["computation_origin"])
    if (outcomes != {"schema_version": SCHEMA} or statistics or inputs or gap_policy != "reject"
            or scope.get("indicator_id") or scope.get("indicator_param_overrides")
            or scope.get("timeframe") != "15m" or settings.reference_basis != "rising_leg_plus_retreat"
            or not origin <= start < end or end-start != timedelta(days=84)
            or settings.max_bars > 12000 or settings.max_candidates > 10000
            or any(t.timestamp() % 900 for t in (start, end, origin))
            or (end-origin).total_seconds()/900 > settings.max_bars):
        raise ValueError("reversal_selection_invalid: fixed smaller-design84day15m selection-only protocol required")
    warmup = int((start-origin).total_seconds())//900
    if "warmup_bars" in scope and (type(scope["warmup_bars"]) is not int or scope["warmup_bars"] != warmup):
        raise ValueError("reversal_selection_invalid: warmup must equal declared origin distance")
    return {"schema_version": SCHEMA}


@dataclass(frozen=True)
class ReversalSelectionEvaluator:
    evaluator_id: str = "raw_forward_outcome"
    version: str = "11"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition, request):
        normalize_configuration(detector=request.parameters["detector"], outcomes=request.parameters["outcomes"],
            statistics=request.parameters["statistics"], inputs=request.parameters["inputs"],
            scope=request.scope, gap_policy=request.parameters["gap_policy"])
        n = int((utc(request.scope["start"])-utc(request.parameters["detector"]["computation_origin"])).total_seconds())//900
        return {"input_kind": "market_data", "indicator_ids": [], "warmup_floor_bars": n,
            "feature_lookback_bars": n, "feature_windows_seconds_by_alias": {},
            "outcome_horizons": [], "required_outcome_horizons": [], "horizon_kind": "bars",
            "entry_lag_bars": 0, "invalidation_max_bars": 0, "outcome_boundary": "evaluation_end_exclusive",
            "primary_candle_required_fields": ["open", "high", "low", "close", "known_at"]}

    def evaluate(self, *, plan, inputs):
        started = monotonic()
        def checkpoint():
            execution_checkpoint()
            if monotonic()-started >= 120:
                raise ValueError("reversal_selection_limit:120second method wall budget exceeded")
        # Normal admission also accounts delivered inputs; retain a method ceiling.
        frame = inputs["candles"]
        if int(frame.memory_usage(index=True, deep=True).sum()) > RULES["limits"]["input_bytes"]:
            raise ValueError("reversal_selection_limit: input exceeds256MiB")
        normalize_configuration(detector=inputs["detector"], outcomes=inputs["outcomes"],
            statistics=inputs["statistics"], inputs=inputs["inputs"], gap_policy=plan.gap_policy,
            scope={"start": plan.evaluation_range["start"], "end": plan.evaluation_range["end_exclusive"], "timeframe": "15m"})
        settings, snapshot, rows, start, end, candles = prepare_recognition(plan=plan, inputs=inputs, schema=SCHEMA, checkpoint=checkpoint)
        checkpoint()
        result = select_comparisons(candles, snapshot.candidates, evaluation_start=start, evaluation_end=end,
            checkpoint=checkpoint)
        result.update(schema_version=RESULT_SCHEMA, check_family=self.evaluator_id, status="completed",
            analysis_status="selection_feasibility_only", computation_origin=plain(candles[0].open_time),
            recognition_choices=settings.to_dict(), processed_bars=snapshot.processed_bars,
            evaluation_range=dict(plan.evaluation_range), source_identity_key=candles[0].source_identity_key,
            snapshot_known_at=plain(snapshot.last_known_at), context_left_censored=True,
            candidate_count=len(result["cases"]), sample_count=result["selection_summary"]["matched"]["completion_clocks"],
            sample_count_meaning="matched completion clocks;not independent observations",
            data_quality=dict(inputs.get("data_quality") or {}),
            caveats=["No forward returns,barrier touches,outcome classes,significance,power or trading claim.",
                "Fixed matching tolerances have no demonstrated feasibility/power justification;no coverage pass gate.",
                "Trailing-n control low is a comparison convention,not a formation low.",
                "Within-pair direct histories are disjoint;cross-pair and shared ancestry dependence remains.",
                "Every chain remains explicit;partial moments and matched-subset shifts must be assessed."])
        checkpoint()
        if len(json.dumps(result, allow_nan=False, separators=(",", ":")).encode()) > RULES["limits"]["result_bytes"]:
            raise ValueError("reversal_selection_limit: scientific result exceeds16MiB;no truncation")
        return result
