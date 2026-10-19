"""Registered, provider-neutral event-and-fact analytical Check evaluator."""

from __future__ import annotations


from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any


from market_data.frozen import semantic_hash
from research_science.check import (
    GAP_POLICY_CONTINUE_DEGRADED,
    GAP_POLICY_REJECT,
    CheckDefinition,
    CheckRequest,
    ResolvedCheckPlan,
)


from .event_features import baseline_features, _path
from .event_preparation import EventCandlePreparation, select_public_events, _candle_rows
from .event_facts import (
    _record_known_at,
    _record_effective_at,
    _record_freshness_time,
    _record_event_key,
    _record_payload_document,
    _record_matches_where,
    _record_is_active,
    _causal_fact_records,
    _record_revision_key,
    _snapshot_revision_order_key,
    _causal_snapshot_series,
    _gap_bounds,
    _fact_gap_intersects,
    _fact_material,
    _alignment_key,
    _latest_record_key,
    _selected_record_key,
    _structured_snapshot_key,
    _fact_features_for_event,
    _required_fact_decisions,
    _fact_snapshot_outputs,
)
from .event_time import _utc, _iso, _known_at
from .event_observations import evaluate_event_observations
from .event_statistics import calculate_event_statistics, _distribution, _utc_day_cluster_ci, _descriptive_outcomes
from .event_configuration import (
    EVENT_FACT_ANALYSIS,
    LEGACY_EVENT_FACT_EVALUATOR_VERSION,
    LEGACY_EVENT_FACT_RESULT_VERSION,
    EVENT_FACT_EVALUATOR_VERSION,
    EVENT_FACT_RESULT_VERSION,
    _INDICATOR_EVENT_DETECTOR,
    _FACT_SNAPSHOT_DETECTOR,
    _REQUIRED_FACTS_AVAILABLE,
    _BASELINE_OPERATORS,
    _FACT_OPERATORS,
    _STRUCTURED_FACT_OPERATORS,
    _mapping,
    _list,
    _positive_int,
    _finite,
    _payload_path,
    _normalize_payload_where,
    normalize_event_fact_configuration,
)
from .directional_model import fit_paired_fold, _auc, _fit_logistic, _log_loss


@dataclass(frozen=True)
class EventFactEvaluator:
    evaluator_id: str = EVENT_FACT_ANALYSIS
    version: str = EVENT_FACT_EVALUATOR_VERSION
    result_schema_version: str = EVENT_FACT_RESULT_VERSION
    fact_snapshot_enabled: bool = True
    availability_trigger_enabled: bool = False
    descriptive_outcomes_enabled: bool = False
    retrospective_enabled: bool = False
    barrier_profiles_enabled: bool = False

    def _validate_trigger(self, detector: Mapping[str, Any]) -> None:
        if (
            detector.get("evaluation_trigger") == _REQUIRED_FACTS_AVAILABLE
            and not self.availability_trigger_enabled
        ):
            raise ValueError(
                "event_fact_check_invalid: required_facts_available requires definition version 5"
            )

    def declare_requirements(
        self,
        *,
        definition: CheckDefinition,
        request: CheckRequest,
    ) -> Mapping[str, Any]:
        del definition
        if self.barrier_profiles_enabled:
            from .event_barrier_check import requirements
            return requirements(request)
        if self.retrospective_enabled:
            from .retrospective_numeric import normalize_retrospective
            normalize_retrospective(request.parameters.get("outcomes", {}).get("retrospective"),
                detector=request.parameters.get("detector", {}), outcomes=request.parameters.get("outcomes", {}),
                statistics=request.parameters.get("statistics", {}), scope=request.scope,
                inputs=request.parameters.get("inputs", []))
        statistics = dict(request.parameters.get("statistics") or {})
        detector = dict(request.parameters.get("detector") or {})
        self._validate_trigger(detector)
        detector_type = str(detector.get("type") or "").strip()
        if detector_type == _FACT_SNAPSHOT_DETECTOR and not self.fact_snapshot_enabled:
            raise ValueError(
                "event_fact_check_invalid: fact_snapshot requires evaluator version 3"
            )
        features = dict(statistics.get("features") or {})
        baseline = [dict(row) for row in features.get("baseline") or []]
        enriched = [dict(row) for row in features.get("enriched") or []]
        feature_lookback = max(
            (
                int(row.get("lookback_bars") or row.get("period") or 0)
                for row in baseline
            ),
            default=0,
        )
        windows: dict[str, int] = {}
        predecessor_aliases: set[str] = set()
        for row in enriched:
            alias = str(row.get("input_alias") or "")
            windows[alias] = max(
                int(windows.get(alias) or 0),
                int(row.get("window_seconds") or 0),
            )
            if str(row.get("operator") or "") in {
                "latest_update_direction",
                "latest_update_magnitude_bps",
                "update_agreement",
                "aggregate_abs_change_bps_window",
            }:
                predecessor_aliases.add(alias)
        indicator_ids: list[str] = []
        if detector_type == _INDICATOR_EVENT_DETECTOR:
            indicator_id = str(request.scope.get("indicator_id") or "").strip()
            if not indicator_id:
                raise ValueError(
                    "check_requirement_plan_invalid: indicator_id is required"
                )
            indicator_ids.append(indicator_id)
        outcomes = dict(request.parameters.get("outcomes") or {})
        invalidation = dict(outcomes.get("invalidation") or {})
        return {
            **(
                {"decision_price_tail_bars": 1}
                if detector.get("evaluation_trigger") == _REQUIRED_FACTS_AVAILABLE
                else {}
            ),
            **(
                {"outcome_boundary": "evaluation_end_exclusive",
                 "input_coverage_policy": "complete_paired_rows"}
                if self.retrospective_enabled and outcomes.get("retrospective", {}).get(
                    "input_coverage_policy") == "complete_paired_rows"
                else {}
            ),
            "input_kind": "market_data",
            "indicator_ids": indicator_ids,
            "warmup_floor_bars": 0,
            "feature_lookback_bars": feature_lookback,
            "feature_windows_seconds_by_alias": windows,
            "feature_predecessor_aliases": sorted(predecessor_aliases),
            "outcome_horizons": list(outcomes.get("horizons") or []),
            "required_outcome_horizons": list(
                outcomes.get("required_horizons") or outcomes.get("horizons") or []
            ),
            "horizon_kind": str(outcomes.get("horizon_kind") or "bars"),
            "entry_lag_bars": int(outcomes.get("entry_lag_bars") or 0),
            "invalidation_max_bars": int(invalidation.get("max_bars") or 0),
            "event_source": (
                "indicator"
                if detector_type == _INDICATOR_EVENT_DETECTOR
                else "check_fact_snapshot"
            ),
            "fact_history_required": bool(enriched)
            or detector_type == _FACT_SNAPSHOT_DETECTOR,
        }

    def evaluate(
        self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if self.barrier_profiles_enabled:
            from .event_barrier_check import evaluate
            return evaluate(plan=plan,inputs=inputs)
        result = self._evaluate(plan=plan, inputs=inputs, preparation=EventCandlePreparation())
        if self.retrospective_enabled:
            from .retrospective_numeric import qualify_result
            return qualify_result(dict(result), inputs["outcomes"]["retrospective"])
        return result

    def _evaluate(
        self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any],
        preparation: EventCandlePreparation,
    ) -> Mapping[str, Any]:
        detector = dict(inputs.get("detector") or {})
        self._validate_trigger(detector)
        detector_type = str(detector.get("type") or "").strip()
        if detector_type == _FACT_SNAPSHOT_DETECTOR and not self.fact_snapshot_enabled:
            raise ValueError(
                "event_fact_check_invalid: fact_snapshot requires evaluator version 3"
            )
        outcomes = dict(inputs.get("outcomes") or {})
        retrospective = outcomes.get("retrospective")
        if bool(retrospective) != self.retrospective_enabled:
            raise ValueError("retrospective_invalid: explicit contract requires evaluator version 13")
        statistics = dict(inputs.get("statistics") or {})
        indicator_evidence = dict(inputs.get("indicator_evidence") or {})
        fact_records = {
            str(alias): tuple(rows)
            for alias, rows in dict(inputs.get("fact_records_by_alias") or {}).items()
        }
        fact_requirements = {
            str(alias): dict(raw)
            for alias, raw in dict(inputs.get("fact_requirements_by_alias") or {}).items()
        }
        gap_transitions = [
            dict(row) for row in indicator_evidence.get("gap_transitions") or []
        ]
        data_quality = dict(inputs.get("data_quality") or {})
        relevant_aliases = {
            "primary_bars",
            str(detector.get("input_alias") or ""),
            *(
                str(row.get("input_alias") or "")
                for row in (statistics.get("features") or {}).get("enriched") or []
            ),
        }
        fact_gap_evidence = [
            dict(row)
            for row in data_quality.get("recorded_gaps") or []
            if detector_type == _FACT_SNAPSHOT_DETECTOR
            and (
                not str(row.get("alias") or "").strip()
                or str(row.get("alias") or "") in relevant_aliases
            )
        ]
        gap_rejection = inputs.get("indicator_gap_rejection")
        if (
            detector_type == _FACT_SNAPSHOT_DETECTOR
            and plan.gap_policy == GAP_POLICY_REJECT
            and fact_gap_evidence
        ):
            gap_rejection = {
                "policy": GAP_POLICY_REJECT,
                "reason": "recorded_fact_gap",
                "recorded_gap_count": len(fact_gap_evidence),
                "gaps": fact_gap_evidence[:10],
            }
        gap_count = (
            len(fact_gap_evidence)
            if detector_type == _FACT_SNAPSHOT_DETECTOR
            else len(gap_transitions) + (1 if gap_rejection else 0)
        )
        if gap_rejection:
            return {
                "schema_version": self.result_schema_version,
                "check_family": EVENT_FACT_ANALYSIS,
                "status": "blocked",
                "analysis_status": "gap_policy_rejected",
                **(
                    {"event_ownership": "check"}
                    if detector_type == _FACT_SNAPSHOT_DETECTOR
                    else {}
                ),
                "gap_decision": {
                    "policy": GAP_POLICY_REJECT,
                    "recorded_gap_count": gap_count,
                    **(
                        {"check_action": "rejected_before_sample_emission"}
                        if detector_type == _FACT_SNAPSHOT_DETECTOR
                        else {"indicator_action": "rejected_before_event_emission"}
                    ),
                    "rejection": dict(gap_rejection),
                },
                "sample_count": 0,
                "events": [],
                "outcome_resolution": {},
                "statistics": {},
                "caveats": ["Recorded Dataset gaps intersect the bound evidence."],
            }

        candle_field = (
            "indicator_evidence.candles"
            if detector_type == _INDICATOR_EVENT_DETECTOR
            else "candles"
        )
        candles, candle_by_open = preparation.candles(
            indicator_evidence.get("candles")
            if detector_type == _INDICATOR_EVENT_DETECTOR else inputs.get("candles"),
            field=candle_field,
        )
        interval_seconds = int(plan.warmup.get("timeframe_seconds") or 0)
        if interval_seconds <= 0:
            raise ValueError(
                "event_fact_check_invalid: planned timeframe is missing"
            )
        evaluation_start = _utc(
            plan.evaluation_range["start"], field="evaluation_start"
        )
        evaluation_end = _utc(
            plan.evaluation_range["end_exclusive"], field="evaluation_end"
        )
        baseline_specs = list(
            (statistics.get("features") or {}).get("baseline") or []
        )
        enriched_specs = list(
            (statistics.get("features") or {}).get("enriched") or []
        )
        if self.retrospective_enabled:
            from .retrospective_numeric import retrospective_events
            ordered_revisions_by_alias, direction_by_key, output_name = {}, {}, "fact_snapshot"
            event_rows = retrospective_events(candles, start=evaluation_start, end=evaluation_end)
            detector_snapshots, trigger_readiness = {}, {}
        else:
            (ordered_revisions_by_alias, direction_by_key, output_name,
             event_rows, detector_snapshots, trigger_readiness) = select_public_events(
                detector=detector, detector_type=detector_type,
                indicator_evidence=indicator_evidence, candles=candles,
                baseline_specs=baseline_specs, enriched_specs=enriched_specs,
                fact_records=fact_records, fact_requirements=fact_requirements,
                fact_gap_evidence=fact_gap_evidence,
                evaluation_start=evaluation_start, evaluation_end=evaluation_end,
            )

        atr_by_period = preparation.atr(candles, baseline_specs)

        (event_results, selected_fact_material, exclusion_counts,
         horizon_resolution, invalidation_resolution, step) = evaluate_event_observations(
            detector=detector,
            detector_type=detector_type,
            direction_by_key=direction_by_key,
            output_name=output_name,
            event_rows=event_rows,
            outcomes=outcomes,
            interval_seconds=interval_seconds,
            evaluation_start=evaluation_start,
            evaluation_end=evaluation_end,
            candles=candles,
            candle_by_open=candle_by_open,
            detector_snapshots=detector_snapshots,
            baseline_specs=baseline_specs,
            atr_by_period=atr_by_period,
        )

        population_rows = [
            row for row in event_results if row["population_eligible"]
        ]
        primary_key = str(outcomes["primary_horizon"])
        positives = sum(
            1
            for row in population_rows
            if bool(row["outcomes"][primary_key]["positive"])
        )
        negatives = len(population_rows) - positives
        distinct_days = len(
            {str(row["decision_time"])[:10] for row in population_rows}
        )

        eligibility = dict(statistics.get("eligibility") or {})
        folds = list(statistics.get("folds") or [])
        eligibility_reasons: list[str] = []
        if len(population_rows) < int(eligibility.get("min_samples") or 0):
            eligibility_reasons.append("minimum_sample_count_not_met")
        if min(positives, negatives) < int(
            eligibility.get("min_class_count") or 0
        ):
            eligibility_reasons.append("minimum_class_count_not_met")
        if distinct_days < int(eligibility.get("min_distinct_utc_days") or 0):
            eligibility_reasons.append("minimum_distinct_utc_days_not_met")

        minimum_validation = int(
            eligibility.get("min_validation_samples_per_fold") or 0
        )
        population_fold_counts = [
            {
                "id": fold["id"],
                "validation_count": sum(
                    1
                    for row in population_rows
                    if _utc(
                        fold["validation"]["start"],
                        field="fold.validation.start",
                    )
                    <= _utc(row["decision_time"], field="decision_time")
                    < _utc(
                        fold["validation"]["end"],
                        field="fold.validation.end",
                    )
                ),
            }
            for fold in folds
        ]
        population_valid_fold_count = sum(
            1
            for row in population_fold_counts
            if int(row["validation_count"]) >= minimum_validation
        )
        if population_valid_fold_count < int(
            eligibility.get("min_valid_folds") or 0
        ):
            eligibility_reasons.append("minimum_population_folds_not_met")

        structured_snapshots: dict[
            tuple[str, str],
            dict[tuple[datetime, datetime | None], Any | None],
        ] = (
            {
                _structured_snapshot_key(
                    str(detector["input_alias"]),
                    dict(detector.get("where") or {}),
                ): detector_snapshots
            }
            if detector_type == _FACT_SNAPSHOT_DETECTOR and not self.retrospective_enabled
            else {}
        )
        for spec in enriched_specs:
            if str(spec.get("operator") or "") not in _STRUCTURED_FACT_OPERATORS and not self.retrospective_enabled:
                continue
            alias = str(spec["input_alias"])
            where = dict(spec.get("where") or {})
            key = _structured_snapshot_key(alias, where)
            if key not in structured_snapshots:
                requirement = fact_requirements.get(alias) or {}
                exact_alignment = (
                    str(requirement.get("alignment") or "") == "exact_interval"
                )
                selector = _causal_snapshot_series
                selector_options = {"ordered_revisions": ordered_revisions_by_alias.get(alias)}
                if self.retrospective_enabled:
                    from .retrospective_numeric import retrospective_snapshots
                    selector = retrospective_snapshots
                    selector_options = {"analysis_as_of": _utc(retrospective["analysis_as_of"], field="analysis_as_of")}
                structured_snapshots[key] = selector(
                    tuple(fact_records.get(alias) or ()),
                    snapshot_requests=[
                        (
                            _utc(row["decision_time"], field="decision_time"),
                            (
                                _utc(row["event_time"], field="event_time")
                                + step
                                if exact_alignment
                                else None
                            ),
                        )
                        for row in population_rows
                    ],
                    where=where,
                    alias=alias,
                    **selector_options,
                )

        if not eligibility_reasons:
            for row in population_rows:
                features, references, reasons, selected = _fact_features_for_event(
                    specs=enriched_specs,
                    requirements=fact_requirements,
                    records_by_alias=fact_records,
                    structured_snapshots=structured_snapshots,
                    gap_evidence=fact_gap_evidence,
                    decision_time=_utc(
                        row["decision_time"], field="decision_time"
                    ),
                    sample_time=(
                        _utc(row["event_time"], field="event_time") + step
                    ),
                    venue_close=float(row["event_price"]),
                    direction=1.0 if row["direction"] == "long" else -1.0,
                    retrospective=self.retrospective_enabled,
                )
                row["features"].update(features)
                row["fact_references"] = references
                selected_fact_material.update(selected)
                for reason in reasons:
                    if reason not in row["exclusion_reasons"]:
                        row["exclusion_reasons"].append(reason)
                        exclusion_counts[reason] += 1
                if reasons or any(
                    row["features"].get(str(spec["name"])) is None
                    for spec in enriched_specs
                ):
                    row["eligible"] = False
                    row["analysis_eligible"] = False
                    if "fact_feature_incomplete" not in row["exclusion_reasons"]:
                        row["exclusion_reasons"].append("fact_feature_incomplete")
                        exclusion_counts["fact_feature_incomplete"] += 1
                else:
                    row["analysis_eligible"] = True
        else:
            for row in event_results:
                row["eligible"] = False
                row["analysis_eligible"] = False

        complete_rows = [
            row for row in event_results if row["analysis_eligible"]
        ]
        if enriched_specs and not eligibility_reasons:
            complete_positives = sum(
                1
                for row in complete_rows
                if bool(row["outcomes"][primary_key]["positive"])
            )
            complete_negatives = len(complete_rows) - complete_positives
            complete_days = len(
                {str(row["decision_time"])[:10] for row in complete_rows}
            )
            if len(complete_rows) < int(eligibility.get("min_samples") or 0):
                eligibility_reasons.append("minimum_complete_sample_count_not_met")
            if min(complete_positives, complete_negatives) < int(
                eligibility.get("min_class_count") or 0
            ):
                eligibility_reasons.append("minimum_complete_class_count_not_met")
            if complete_days < int(
                eligibility.get("min_distinct_utc_days") or 0
            ):
                eligibility_reasons.append("minimum_complete_distinct_days_not_met")
        statistical_result, fold_results = calculate_event_statistics(
            statistics=statistics, outcomes=outcomes, interval_seconds=interval_seconds,
            baseline_specs=baseline_specs, enriched_specs=enriched_specs,
            population_rows=population_rows, complete_rows=complete_rows,
            eligibility_reasons=eligibility_reasons, eligibility=eligibility,
            folds=folds, primary_key=primary_key, minimum_validation=minimum_validation,
        )

        if detector.get("evaluation_trigger") == _REQUIRED_FACTS_AVAILABLE:
            for row in event_results:
                opened = _utc(row["event_time"], field="event_time")
                candle = candle_by_open[opened]
                row["timing"] = {
                    "schema_version": "research.check_sample_timing.v1",
                    "sample_end": _iso(_utc(candle["close_time"], field="candle.close_time")),
                    "primary_known_at": _iso(_utc(
                        candle.get("known_at") or candle["close_time"], field="candle.known_at"
                    )),
                    **trigger_readiness.get(opened, {
                        "checked_at": None, "required_inputs": [],
                    }),
                    "evaluation_end_exclusive": _iso(evaluation_end),
                    "decision_time": row["decision_time"],
                    "outcome_price_time": row.get("entry_sample_time"),
                    "exclusion_reasons": list(row["exclusion_reasons"]),
                }

        outcome_resolution = {
            str(horizon): {
                "horizon": horizon,
                "horizon_kind": outcomes["horizon_kind"],
                "resolved_count": counts.get("resolved", 0),
                "unresolved_count": sum(
                    count
                    for reason, count in counts.items()
                    if reason.startswith("unresolved:")
                ),
                "unresolved_reasons": {
                    reason.split(":", 1)[1]: count
                    for reason, count in sorted(counts.items())
                    if reason.startswith("unresolved:")
                },
            }
            for horizon, counts in sorted(horizon_resolution.items())
        }
        invalidation_summary = {
            "configured": bool(outcomes.get("invalidation")),
            "resolved_count": sum(
                count
                for reason, count in invalidation_resolution.items()
                if reason.startswith("resolved:")
            ),
            "unresolved_count": sum(
                count
                for reason, count in invalidation_resolution.items()
                if reason.startswith("unresolved:")
            ),
            "invalidated_count": invalidation_resolution.get(
                "resolved:invalidated", 0
            ),
            "not_invalidated_count": invalidation_resolution.get(
                "resolved:not_invalidated", 0
            ),
            "unresolved_reasons": {
                reason.split(":", 1)[1]: count
                for reason, count in sorted(invalidation_resolution.items())
                if reason.startswith("unresolved:")
            },
        }
        analysis_status = (
            "insufficient_evidence" if eligibility_reasons else "completed"
        )
        transition_actions = [
            str(action.get("action") or "")
            for transition in gap_transitions
            for action in transition.get("actions") or []
            if isinstance(action, Mapping)
        ]
        return {
            "schema_version": self.result_schema_version,
            "check_family": EVENT_FACT_ANALYSIS,
            "status": "completed",
            "analysis_status": analysis_status,
            "event_ownership": (
                "indicator"
                if detector_type == _INDICATOR_EVENT_DETECTOR
                else "check"
            ),
            **(
                {"sampling": "primary_bar_close"}
                if detector_type == _FACT_SNAPSHOT_DETECTOR
                else {}
            ),
            "sample_count": len(population_rows),
            "analysis_sample_count": len(complete_rows),
            "candidate_count": len(event_results),
            "direction_counts": dict(
                sorted(Counter(row["direction"] for row in event_results).items())
            ),
            "primary_outcome_counts": {
                "positive": positives,
                "nonpositive": negatives,
            },
            "distinct_utc_days": distinct_days,
            "eligibility": {
                "eligible": not eligibility_reasons,
                "criteria": eligibility,
                "reasons": eligibility_reasons,
                "exclusions": dict(sorted(exclusion_counts.items())),
                "population_count": len(population_rows),
                "analysis_complete_case_count": len(complete_rows),
                "population_folds": population_fold_counts,
                "population_valid_fold_count": population_valid_fold_count,
                "enriched_features_evaluated": not bool(eligibility_reasons),
            },
            "gap_decision": {
                "policy": plan.gap_policy,
                "recorded_gap_count": gap_count,
                **(
                    {
                        "check_action": (
                            "fact_samples_excluded"
                            if gap_count
                            else "no_gap_action_required"
                        )
                    }
                    if detector_type == _FACT_SNAPSHOT_DETECTOR
                    else {
                        "indicator_action": (
                            "reset_and_rewarm"
                            if "reset_and_rewarm" in transition_actions
                            else "continued_with_degraded_status"
                            if "continued_degraded" in transition_actions
                            else "no_gap_action_required"
                        )
                    }
                ),
                "degraded": bool(
                    gap_count and plan.gap_policy == GAP_POLICY_CONTINUE_DEGRADED
                ),
                "transitions": gap_transitions,
            },
            "outcome_resolution": outcome_resolution,
            **(
                {"descriptive_outcomes": _descriptive_outcomes(population_rows, outcomes["horizons"])}
                if self.descriptive_outcomes_enabled else {}
            ),
            "invalidation_resolution": invalidation_summary,
            "statistics": statistical_result,
            "events": event_results,
            "hashes": {
                "event_population_hash": semantic_hash(
                    {"events": event_results}
                ),
                "selected_facts_hash": semantic_hash(
                    {
                        "facts": [
                            selected_fact_material[key]
                            for key in sorted(selected_fact_material)
                        ]
                    }
                ),
                "feature_matrix_hash": semantic_hash(
                    {
                        "rows": [
                            {
                                "event_time": row["event_time"],
                                "decision_time": row["decision_time"],
                                "features": row["features"],
                                "primary_outcome": row["outcomes"].get(primary_key),
                                "eligible": row["eligible"],
                            }
                            for row in event_results
                        ]
                    }
                ),
                "fold_assignment_hash": semantic_hash(
                    {
                        "folds": folds,
                        "fold_results": fold_results,
                    }
                ),
                "statistics_hash": semantic_hash(statistical_result),
                "indicator_gap_semantics_hash": semantic_hash(
                    {
                        "schema_version": "check_gap_transition_material.v1",
                        "policy": plan.gap_policy,
                        "gap_transitions": gap_transitions,
                        "rejection": gap_rejection,
                    }
                ),
            },
            "data_quality": data_quality,
            "caveats": (
                [
                    (
                        "Dataset gaps were retained and affected research samples were excluded."
                        if detector_type == _FACT_SNAPSHOT_DETECTOR
                        else "Dataset gaps were retained and execution continued explicitly."
                    )
                ]
                if gap_count and plan.gap_policy == GAP_POLICY_CONTINUE_DEGRADED
                else []
            ),
        }


__all__ = [
    "EVENT_FACT_ANALYSIS",
    "EVENT_FACT_EVALUATOR_VERSION",
    "EVENT_FACT_RESULT_VERSION",
    "LEGACY_EVENT_FACT_EVALUATOR_VERSION",
    "LEGACY_EVENT_FACT_RESULT_VERSION",
    "EventFactEvaluator",
    "normalize_event_fact_configuration",
]
