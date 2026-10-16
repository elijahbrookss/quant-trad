"""Versioned admitted-Check consumer of the shared completed-formation recognizer."""
from collections import Counter
from dataclasses import dataclass

from core.execution_control import execution_checkpoint
from research_science.recognition import plain
from research_science.reversal_outcomes import HORIZON_BARS, RULES, first_passage, overlap_components, summarize
from .reversal_recognition_evaluator import (SCHEMA as RECOGNITION_SCHEMA, choices,
    normalize_configuration as normalize_recognition, prepare_recognition, ReversalRecognitionEvaluator)

SCHEMA = "reversal_first_passage.v1"
RESULT_SCHEMA = "reversal_first_passage_result.v1"


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    settings = choices(detector)
    if (outcomes != {"schema_version": SCHEMA} or settings.reference_basis != "rising_leg_plus_retreat"
            or scope.get("timeframe") != "15m" or settings.max_bars > 4096 or settings.max_candidates > 4096):
        raise ValueError("reversal_first_passage_invalid: fixed retreat-inclusive15m protocol and <=4096 resource bounds required")
    normalize_recognition(detector=detector, outcomes={"schema_version": RECOGNITION_SCHEMA},
        statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
    return {"schema_version": SCHEMA}


@dataclass(frozen=True)
class ReversalFirstPassageEvaluator:
    evaluator_id: str = "raw_forward_outcome"
    version: str = "10"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition, request):
        from dataclasses import replace
        normalize_configuration(detector=request.parameters["detector"], outcomes=request.parameters["outcomes"],
            statistics=request.parameters["statistics"], inputs=request.parameters["inputs"],
            scope=request.scope, gap_policy=request.parameters["gap_policy"])
        recognition = replace(request, request_hash="", parameters={**request.parameters,
            "outcomes": {"schema_version": RECOGNITION_SCHEMA}})
        declaration = ReversalRecognitionEvaluator().declare_requirements(definition=definition, request=recognition)
        return {**declaration, "outcome_horizons": [HORIZON_BARS], "required_outcome_horizons": [],
                "outcome_boundary": "evaluation_end_exclusive"}

    def evaluate(self, *, plan, inputs):
        settings = choices(inputs["detector"])
        if settings.reference_basis != "rising_leg_plus_retreat" or settings.max_bars > 4096 or settings.max_candidates > 4096:
            raise ValueError("reversal_first_passage_invalid: fixed approved reference and resource limits required")
        settings, snapshot, rows, start, end, candles = prepare_recognition(plan=plan, inputs=inputs, schema=SCHEMA)
        events = tuple(e for e in snapshot.candidates if start <= e.occurred_at < end)
        # Freeze support/episode mapping before any outcome procedure sees the path.
        components = overlap_components(events)
        by_open = {c.open_time: c for c in candles}
        outcomes = {}
        for event in events:
            execution_checkpoint()
            outcomes[event.event_id] = first_passage(event, candles, by_open=by_open,
                evaluation_end=end, checkpoint=execution_checkpoint)
        summary = summarize(events, outcomes, components)
        return {"schema_version": RESULT_SCHEMA, "check_family": self.evaluator_id, "status": "completed",
            "analysis_status": "descriptive_only", "reference_status": "user_selected_retreat_inclusive",
            "population_status": "all_chains_retained_overlap_grouped", "rules": dict(RULES),
            "recognition_choices": settings.to_dict(), "computation_origin": plain(candles[0].open_time),
            "context_left_censored": True, "snapshot_known_at": plain(snapshot.last_known_at),
            "processed_bars": snapshot.processed_bars, "candidate_count": len(events),
            "sample_count": len(components), "sample_count_meaning": "overlap components,not independent observations",
            "outcomes_calculated": True, "events": [{**e.to_dict(), "first_passage": outcomes[e.event_id]} for e in events],
            "reference_events": [e.to_dict() for e in snapshot.references],
            "pending_candidates": [p.to_dict() for p in snapshot.pending], "first_passage_summary": summary,
            "known_at_methods": dict(Counter(r["known_at_method"] for r in rows)),
            "data_quality": dict(inputs.get("data_quality") or {}),
            "caveats": ["Retrospective exposed development;no independent-trial,causal,profitability or live-fill claim.",
                        "Every chain is retained;transitive overlap grouping may connect most or all of the month.",
                        "No outcome-driven filtering,tuning,expiry,minimum move or winner selection.",
                        "Neither and unresolved remain in all-class denominators;late-month first touches need no later data.",
                        "OHLC cannot order two barriers first touched within the same candle."]}
