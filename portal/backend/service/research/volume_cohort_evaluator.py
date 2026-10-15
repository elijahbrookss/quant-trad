"""Outcome-blind volume-cohort matching over configured raw candle events."""
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import timedelta
import math
import numpy as np

from core.execution_control import execution_checkpoint
from market_data.frozen import semantic_hash
from .candle_distribution_evaluator import (
    CandleDistributionEvaluator, formation_context, describe,
    normalize_configuration as normalize_distribution, OUTCOME_SCHEMA as DISTRIBUTION_SCHEMA)
from .controlled_reversal_evaluator import prepare_pairs, summarize_pairs, _distribution
from .crossing_state_comparison import _balance
from .raw_candle_evaluator import _prepare, _clock, _iso, _integer, prepare_predicate_candidates

OUTCOME_SCHEMA = "volume_cohort_matched.v1"
RESULT_SCHEMA = "volume_cohort_matched_result.v1"
FEATURES = ("formation_body_fraction", "formation_range_fraction", "prior_rms_return")
RULES = {
    "cohort_choices": ["volume_gt_previous_vs_lte"],
    "matching_policy_choices": ["same_date_hour"],
    "features": list(FEATURES), "feature_transform": "log positive absolute body, range and prior RMS",
    "prior_rms": "N prior completed candles, N-1 contiguous close returns; no imputation",
    "selection": "chronological increasing-volume; minimum normalized squared feature distance; no control reuse",
    "tie_break": "absolute event-time distance then earlier control",
    "pair_intervals": "closed nominal entry-to-target must be disjoint; no future availability selection",
    "joint_nonoverlap": "closed prior-feature-start to target across both arms, chosen before outcomes",
    "support": {"complete_pairs": 100, "utc_days": 10, "matching_fraction": .5,
        "outcome_completeness": .95, "absolute_smd_max": .1},
    "nonoverlap_support": {"complete_pairs": 30, "utc_days": 6},
    "inference": "descriptive_only", "event_limit": 10000,
    "limits": {"evaluation_days": 32, "candles": 100000, "candidate_comparisons": 3000000},
}


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    if outcomes.get("schema_version") != OUTCOME_SCHEMA:
        raise ValueError("volume_cohort_invalid: explicit schema required")
    if isinstance(scope.get("warmup_bars", 1), bool):
        raise ValueError("volume_cohort_invalid: warmup must be integer one")
    c = outcomes.get("comparison")
    if not isinstance(c, dict) or set(c) != {"cohort", "prior_volatility_bars", "caliper_ratios", "matching_policy"}:
        raise ValueError("volume_cohort_invalid: explicit comparison choices required")
    if c["cohort"] not in RULES["cohort_choices"] or c["matching_policy"] not in RULES["matching_policy_choices"]:
        raise ValueError("volume_cohort_invalid: unsupported cohort or matching policy")
    _integer(c["prior_volatility_bars"], field="prior_volatility_bars", minimum=2, maximum=10000)
    ratios = c["caliper_ratios"]
    if not isinstance(ratios, dict) or set(ratios) != set(FEATURES):
        raise ValueError("volume_cohort_invalid: explicit ratio for every matching feature required")
    for v in ratios.values():
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not 1 < v <= 10:
            raise ValueError("volume_cohort_invalid: finite caliper ratios must be >1 and <=10")
    raw_outcomes = {k:v for k,v in outcomes.items() if k != "comparison"}
    raw_outcomes["schema_version"] = DISTRIBUTION_SCHEMA
    # V6's one-bar binding is intentionally reused; longer feature context may
    # be unavailable at the start rather than silently expanding the Dataset.
    normalized = normalize_distribution(detector=detector, outcomes=raw_outcomes,
        statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
    if len(normalized["forward_bars"]) != 1:
        raise ValueError("volume_cohort_invalid: one prospectively chosen primary horizon required")
    start, end = (_clock(scope[k], k) for k in ("start", "end"))
    if not timedelta(0) < end-start <= timedelta(days=RULES["limits"]["evaluation_days"]):
        raise ValueError("volume_cohort_invalid: evaluation range must be at most 32 days")
    return {**normalized, "schema_version": OUTCOME_SCHEMA, "comparison": {
        **c, "caliper_ratios": {f:float(ratios[f]) for f in FEATURES}}}


def prepare_volume_candidates(frame, rows, *, detector, start, end, step, choices, lag, horizon):
    """No entry prices, outcome status or future observations enter this function."""
    selected, exclusions, evaluated = prepare_predicate_candidates(
        frame, rows, detector=detector, start=start, end=end, step=step)
    by_open = {r["open_time"]:r for r in rows}
    ledger, candidates = [], []
    for _, current, prior, matched, decision in selected:
        execution_checkpoint()
        if not matched:
            continue
        if len(ledger) >= RULES["event_limit"]:
            raise ValueError("volume_cohort_limit: full original universe exceeds 10000; no truncation")
        if prior is None:
            raise ValueError("volume_cohort_invalid: previous candle must be declared by detector")
        context = formation_context(current, by_open, step=step, decision=decision,
            prior_bars=choices["prior_volatility_bars"])
        opened = current["open_time"]
        values = {**context, "formation_body_fraction": abs(context["formation_body_fraction"])}
        missing = [f for f in FEATURES if values[f] is None or not math.isfinite(values[f]) or values[f] <= 0]
        # Known-at is part of eligibility, not a fabricated historical live clock.
        if decision > current["close_time"]:
            missing.append("formation_known_after_close")
        candidate = {"event_time":_iso(opened), "event_close_time":_iso(current["close_time"]),
            "decision_known_at":_iso(decision), "day":current["close_time"].date().isoformat(),
            "stratum":_iso(current["close_time"].floor("h")),
            "entry_time":_iso(opened+(lag+1)*step), "target_time":_iso(opened+(lag+1+horizon)*step),
            "feature_start":_iso(opened-choices["prior_volatility_bars"]*step),
            "treated":current["volume"] > prior["volume"],
            "cohort":"increasing_volume" if current["volume"] > prior["volume"] else "non_increasing_volume",
            "volume":current["volume"], "previous_volume":prior["volume"],
            "context":context, "feature_missing":missing,
            "features":{f:math.log(values[f]) for f in FEATURES} if not missing else None}
        ledger.append(candidate)
        if not missing:
            candidates.append(candidate)
    return ledger, candidates, dict(exclusions), evaluated


def balance_pairs(pairs):
    result = {}
    for f in FEATURES:
        value = _balance(*(np.asarray([p[arm]["features"][f] for p in pairs], dtype=float)
            for arm in ("treated", "control")))
        value["increasing_volume_mean"] = value.pop("crossing_mean")
        result[f] = value
    return result


def context_summary(candidates):
    """Expose the change of population using the existing numeric summaries."""
    result = {}
    for cohort in ("increasing_volume", "non_increasing_volume"):
        members = [c for c in candidates if c["cohort"] == cohort]
        fields = {}
        for f in (*FEATURES, "volume", "previous_volume"):
            values = [c["context"].get(f) if f in FEATURES else c[f] for c in members]
            valid = [v for v in values if v is not None and math.isfinite(v)]
            summary = _distribution(valid)
            summary.pop("positive_fraction")
            fields[f] = {**summary, "unavailable_count":len(values)-len(valid)}
        result[cohort] = {"event_count":len(members), "context":fields}
    return result


def overlap_diagnostics(candidates, start_key):
    """Closed intervals, both roles; components are not independent sample counts."""
    intervals = sorted((c[start_key],c["target_time"],c["event_time"]) for c in candidates)
    right, components, overlapping = None, 0, 0
    membership = {}
    for left, end, event in intervals:
        if right is None or left > right:
            components += 1
        else:
            overlapping += 1
        right = max(right or end, end)
        membership[event] = components
    return {"interval_count":len(intervals), "component_count":components,
        "intervals_joining_existing_component":overlapping, "component_by_event_time":membership}


@dataclass(frozen=True)
class VolumeCohortEvaluator:
    evaluator_id: str = "raw_forward_outcome"
    version: str = "7"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition, request):
        return CandleDistributionEvaluator().declare_requirements(definition=definition, request=request)

    def evaluate(self, *, plan, inputs):
        outcomes = inputs["outcomes"]
        choices = outcomes["comparison"]
        primary = next(r for r in plan.market_data_requirements if r["alias"] == "primary_bars")
        step = timedelta(seconds=primary["timeframe_seconds"])
        frame, rows = _prepare(inputs["candles"], step=step, volume_required=True)
        horizon = outcomes["forward_bars"][0]
        ledger, candidates, exclusions, evaluated = prepare_volume_candidates(frame, rows,
            detector=inputs["detector"], start=_clock(plan.evaluation_range["start"],"start"),
            end=_clock(plan.evaluation_range["end_exclusive"],"end"), step=step,
            choices=choices, lag=outcomes["entry_lag_bars"], horizon=horizon)
        pairs, unmatched, comparisons = prepare_pairs(candidates, features=FEATURES,
            calipers=tuple(math.log(choices["caliper_ratios"][f]) for f in FEATURES),
            support_start="feature_start")
        feature_hash = semantic_hash({"choices":choices, "ledger":ledger, "pairs":pairs, "unmatched":unmatched})
        # Pair identities and joint support are now frozen. Only now resolve any
        # future observations, through the existing raw/distribution procedure.
        baseline = CandleDistributionEvaluator().evaluate(plan=plan, inputs={**inputs,
            "outcomes":{k:v for k,v in outcomes.items() if k != "comparison"}})
        events = {e["event_time"]:e for e in baseline["events"]}
        if set(events) != {c["event_time"] for c in ledger}:
            raise ValueError("volume_cohort_invalid: raw and matching event universes disagree")
        for pair in pairs:
            pair["outcomes"] = {}
            for arm in ("treated", "control"):
                event = events[pair[arm]["event_time"]]
                value = event["outcomes"][str(horizon)]
                pair["outcomes"][arm] = ({**value, "forward_return":value["forward_return_pct"]}
                    if value["status"] == "resolved" else dict(value))
        eligible_treated = sum(c["treated"] for c in candidates)
        analysis = summarize_pairs(pairs, eligible_treated=eligible_treated, both_arm_days=True,
            features=FEATURES, treated_label="increasing_volume")
        selected_balance = balance_pairs(pairs)
        analysis["selected_pair_balance"] = selected_balance
        analysis["support_gates"]["selected_pair_balance"] = all(
            v["absolute_smd"] is not None and v["absolute_smd"] <= .1 for v in selected_balance.values())
        if not all(analysis["support_gates"].values()):
            analysis["decision"] = "unsupported"
        analysis["matching_fraction_denominator"] = "feature-eligible increasing-volume events"
        analysis["matching_fraction_original_treated"] = len(pairs)/sum(c["treated"] for c in ledger) if any(c["treated"] for c in ledger) else None
        complete = [p for p in pairs if all(v["status"] == "resolved" for v in p["outcomes"].values())]
        differences = [p["outcomes"]["treated"]["forward_return"]-p["outcomes"]["control"]["forward_return"] for p in complete]
        analysis["primary"]["median_paired_return_difference"] = float(np.median(differences)) if differences else None
        analysis["complete_pair_distributions"] = {arm:describe(
            [events[p[arm]["event_time"]] for p in complete],horizon) for arm in ("treated","control")}
        selected_times = {p[arm]["event_time"] for p in pairs for arm in ("treated","control")}
        coverage = defaultdict(Counter)
        for c in ledger:
            coverage[c["day"]][c["cohort"]+":original"] += 1
            coverage[c["day"]][c["cohort"]+(":feature_missing" if c["feature_missing"] else ":feature_eligible")] += 1
            if c["event_time"] in selected_times:
                coverage[c["day"]][c["cohort"]+":selected"] += 1
        ledger_by_time = {c["event_time"]:c for c in ledger}
        for event in baseline["events"]:
            event["matching_context"] = ledger_by_time[event["event_time"]]
        for pair in complete:
            for arm in ("treated", "control"):
                c = pair[arm]
                coverage[c["day"]][c["cohort"]+":complete_pair"] += 1
        calendar_days = [d["day"] for d in baseline["distribution"]["cohorts"]["increasing_volume"][str(horizon)]["days"]]
        for day in calendar_days:
            for cohort in ("increasing_volume", "non_increasing_volume"):
                for state in ("original", "feature_missing", "feature_eligible", "selected", "complete_pair"):
                    coverage[day].setdefault(cohort+":"+state,0)
        return {**baseline, "schema_version":RESULT_SCHEMA,
            "recommendation":"inspect_matched_volume_association_only",
            "comparison_rules":RULES, "comparison_choices":choices,
            "feature_frozen_hash":feature_hash, "feature_freeze_stage":"before_any_outcome_resolution",
            "matching_universe_count":len(ledger), "matching_feature_eligible_count":len(candidates),
            "matching_candidate_exclusions":exclusions, "matching_evaluated_bars":evaluated,
            "feature_missing_reasons":dict(Counter(reason for c in ledger for reason in c["feature_missing"])),
            "coverage_by_utc_day":{day:dict(v) for day,v in sorted(coverage.items())},
            "population_context":{population:context_summary(cs) for population,cs in (
                ("original",ledger),("feature_eligible",candidates),
                ("selected",[p[arm] for p in pairs for arm in ("treated","control")]),
                ("complete_pairs",[p[arm] for p in complete for arm in ("treated","control")]))},
            "candidate_comparisons":comparisons, "pairs":pairs, "unmatched":unmatched,
            "unselected_controls":[c["event_time"] for c in candidates if not c["treated"] and c["event_time"] not in selected_times],
            "volume_cohort_comparison":analysis,
            "dependence":{population:{name:overlap_diagnostics(cs,key) for name,key in
                (("outcome_support","entry_time"),("full_feature_support","feature_start"))}
                for population,cs in (("original",ledger),("selected_pairs",[p[arm] for p in pairs for arm in ("treated","control")]))},
            "caveats":baseline["caveats"]+[
                "Common-support matched association is a different estimand from the original cohort mean gap; a sparse matched subset cannot explain the full gap.",
                "Higher total candle volume is not signed buying pressure. The raw detector alone defines whether a trend filter exists.",
                "Day deletion and nonoverlap are dependence/influence diagnostics; no dependence-adjusted uncertainty, independent-N or significance claim is supplied.",
                "A smaller matched difference does not establish that volume never matters; a persistent difference is descriptive only.",
                "Contemporaneous volume/body conditioning is not causal adjustment or a profitability test.",
                "Missing/zero matching features remain in the original ledger; no imputation, trimming or outcome-driven rematching.",
                "Support gates are inherited diagnostics, not a power calculation; unsupported closes without changing choices."]}
