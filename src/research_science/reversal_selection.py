"""Fixed outcome-blind completed-reversal comparison selection.

Consumes the shared recognizer's immutable completions. No future-path reader,
first-passage classifier, model, power claim, rescue tuning or independent-N.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta
import math
from typing import Callable

from .recognition import plain
from .completed_event_context import CompletedEventContext, _log_ratio
from .study import stable_hash
from .weighted_summaries import first_crossing_quantiles, sequential_prefixes

SCHEMA = "reversal_selection_only.v1"
RULES = {
    "version": SCHEMA,
    "formation": "unchanged retreat-inclusive;retain all chains;no expiry or deduplication",
    "decision": "completion parent known_at;not an executed fill",
    "controls": "no completion at this clock;prior/pending/later formations allowed",
    "control_low": "own minimum of n trailing candles;case n includes whole formation",
    "features": ["prior96return RMS sigma", "fractional barrier distance r", "strictly prior12bar z"],
    "sigma": "RMS96 returns ending before decision candle;97 preceding closes",
    "z_pre12": "log(C[t-1]/C[t-13])/(sigma*sqrt(12))",
    "calipers": {"sigma_ratio": 1.25, "r_ratio": 1.10, "z_pre12_abs": .25},
    "distance": "sum squared log-ratio/caliper for sigma,r and delta_z/.25",
    "strata": "same28day block,UTC6h slot,prior12bar distinct completion count0/1/2+",
    "radius_days": 14,
    "order": "case clock,event ID;distance,absolute calendar distance,control clock,source identity",
    "reuse": "one control per chain;no control timestamp reuse across n or blocks",
    "support": "within-pair disjoint closed direct-history to nominal+12bar-end intervals",
    "dependence": "cross-pair overlaps and pair links;shared ancestry conservatively starts at recognition origin",
    "weighting": "equal case completion moments;split each matched moment across its matched chains",
    "diagnostics_only": ["q=r/(sigma*sqrt(12))", "prior96bar z", "decision body/range", "duration"],
    "numeric_coverage_pass_gates": None,
    "threshold_justification": "fixed design constants;no demonstrated feasibility or power",
    "outcomes": "NOT CALCULATED;nominal support is metadata,never a first-touch scan",
    "limits": {"candles": 12000, "candidate_comparisons": 20000000, "wall_seconds": 120,
               "input_bytes": 268435456, "result_bytes": 16777216},
}


def stage_availability(event):
    """A separately named v2 view; original event identity/record is unchanged."""
    by_name = {s.name: s for s in event.stages}
    result = []
    for stage in event.stages:
        role = stage.known_at
        if stage.name == "preceding_trough":
            role = by_name["rebound_start"].known_at
        elif stage.name == "rebound_end":
            role = by_name["reference_confirmed"].known_at
        elif stage.name == "lowest_push":
            role = event.known_at  # final formation-low role, not lowest-so-far
        result.append({"name": stage.name, "candle_id": stage.candle_id,
            "observed_at": plain(stage.observed_at), "candle_known_at": plain(stage.observed_at),
            "role_known_at": plain(role), "parent_known_at": plain(event.known_at),
            "usable_as_parent_stage_at": plain(max(role, event.known_at))})
    return {"schema_version": "reversal_stage_availability.v2", "stages": result}


def moment_weights(records):
    groups = Counter(r["decision_at"] for r in records)
    return {r["event_id"]: 1/(len(groups)*groups[r["decision_at"]]) for r in records} if groups else {}


def _distribution(records, weights, field):
    pairs = []
    for record in records:
        value = (record.get("features") or {}).get(field)
        # Duration is observed even when volatility/risk cannot enter matching.
        if field == "duration_hours" and "formation_candles" in record:
            value = record["formation_candles"]/4
        if value is not None and math.isfinite(value):
            pairs.append((value, weights[record["event_id"]]))
    mass = math.fsum(w for _, w in pairs)
    missing = max(0., math.fsum(weights.values())-mass)
    if not mass:
        return {"observed_weight": 0., "missing_weight": missing, "mean": None, "quantiles": {}}
    pairs.sort()
    quantiles = first_crossing_quantiles(pairs, (0., .25, .5, .75, .9, .95, 1.),
        total_weight=mass, prefixes=sequential_prefixes)
    return {"observed_weight": mass, "missing_weight": missing, "mean": math.fsum(x*w for x, w in pairs)/mass,
            "quantiles": quantiles, "quantile_rule": "first weighted CDF crossing;no interpolation"}


def _components(cases, pairs, *, recognition_origin):
    nodes = [(r["event_id"], r["support_start"], r["support_end"], "case", r["decision_at"]) for r in cases]
    nodes += [(p["control"]["control_id"], p["control"]["support_start"], p["control"]["support_end"], "control", p["control"]["decision_at"]) for p in pairs]
    parent = {n[0]: n[0] for n in nodes}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(x, y):
        parent[find(y)] = find(x)
    last = None
    end = None
    for key, start, stop, *_ in sorted(nodes, key=lambda n: (n[1], n[2], n[0])):
        if end is not None and start <= end:
            union(last, key)
            if stop > end:
                last, end = key, stop
        else:
            last, end = key, stop
    for pair in pairs:
        union(pair["event_id"], pair["control"]["control_id"])
    groups = defaultdict(list)
    for node in nodes:
        groups[find(node[0])].append(node)
    direct = []
    for members in groups.values():
        ids = sorted(n[0] for n in members)
        direct.append({"component_id": stable_hash(ids), "case_ids": [n[0] for n in members if n[3] == "case"],
            "control_ids": [n[0] for n in members if n[3] == "control"],
            "case_count": sum(n[3] == "case" for n in members), "control_count": sum(n[3] == "control" for n in members),
            "case_completion_moment_count": len({n[4] for n in members if n[3] == "case"}),
            "case_completion_clocks": sorted({n[4] for n in members if n[3] == "case"}),
            "earliest_support": min(n[1] for n in members), "latest_support": max(n[2] for n in members)})
    ancestry = [] if not nodes else [{
        "component_id": stable_hash({"origin": recognition_origin, "members": sorted(parent)}),
        "direct_component_ids": sorted(g["component_id"] for g in direct),
        "case_count": len(cases), "control_count": len(pairs),
        "case_completion_moment_count": len({r["decision_at"] for r in cases}),
        "earliest_support": recognition_origin, "latest_support": max(n[2] for n in nodes)}]
    return {"direct_support_and_pair_components": sorted(direct, key=lambda g: g["earliest_support"]),
        "ancestry_aware_components": ancestry,
        "ancestry_aware_component_count": len(ancestry),
        "ancestry_method": "conservative shared computation-origin ancestry;not an independence proof",
        "independent_sample_size": None}


def _select_comparisons(candles, events, *, evaluation_start, evaluation_end,
                        rules, checkpoint: Callable[[], None] = lambda: None,
                        max_comparisons=20000000, order_keys=None,
                        timing_view=stage_availability, population_ranges=()):
    """Run only inside the admitted Check. No access after own decision for features."""
    if not candles or len(candles) > rules["limits"]["candles"]:
        raise ValueError("reversal_selection_limit: invalid candle count")
    if type(max_comparisons) is not int or not 1 <= max_comparisons <= 20000000:
        raise ValueError("reversal_selection_limit: invalid comparison resource bound")
    context = CompletedEventContext(candles, events, evaluation_start=evaluation_start,
        evaluation_end=evaluation_end, checkpoint=checkpoint)
    step = timedelta(minutes=15)
    by_close, by_open = context.by_close, context.by_open
    completed_indices, base, closes = context.completed_indices, context.base, context.closes
    trailing_lows, geometry = context.trailing_lows, context.geometry
    eligible_controls = defaultdict(list)
    for i, f in base.items():
        if i not in completed_indices:
            eligible_controls[(f["block"], f["utc6h"], f["prior_completion_stratum"])].append(i)
    cases, pairs, used, comparisons = [], [], set(), 0
    for event in sorted(events, key=lambda e: (e.known_at,
            order_keys[e.event_id] if order_keys is not None else e.event_id)):
        checkpoint()
        if not evaluation_start <= event.known_at < evaluation_end:
            continue
        record = context.record(event, timing_view=timing_view, order_keys=order_keys)
        i = by_close[event.known_at]
        n = record["formation_candles"]
        start_index = max(0, min(i-n+1, i-97))
        cases.append(record)
        if record["features"] is None:
            record["match_status"] = "invalid_features_or_risk"
            continue
        f = base[i]
        rejects, best, eligible_count = Counter(), None, 0
        for j in eligible_controls[(f["block"], f["utc6h"], f["prior_completion_stratum"])]:
            comparisons += 1
            if comparisons > max_comparisons:
                raise ValueError("reversal_selection_limit: candidate comparison budget exceeded;no truncation")
            if comparisons % 128 == 0:
                checkpoint()
            reason = None
            distance_time = abs(candles[j].close_time-event.known_at)
            support_start = candles[max(0, min(j-n+1, j-97))].open_time
            support_end = candles[j].close_time+12*step
            if distance_time > timedelta(days=14): reason = "outside14days"
            elif j in used: reason = "control_already_used"
            elif j < n-1: reason = "insufficient_trailing_history"
            elif not (support_end < candles[start_index].open_time or event.known_at+12*step < support_start): reason = "within_pair_history_overlap"
            g = None if reason else geometry(j, n)
            if reason is None and g is None: reason = "invalid_control_features_or_risk"
            if reason is None:
                a = record["features"]
                if max(a["sigma"], g["sigma"])/min(a["sigma"], g["sigma"]) > 1.25: reason = "sigma_caliper"
                elif max(a["r"], g["r"])/min(a["r"], g["r"]) > 1.10: reason = "risk_distance_caliper"
                elif abs(a["z_pre12"]-g["z_pre12"]) > .25: reason = "prior_movement_caliper"
            if reason:
                rejects[reason] += 1
                continue
            eligible_count += 1
            score = (math.log(a["sigma"]/g["sigma"])/math.log(1.25))**2+(math.log(a["r"]/g["r"])/math.log(1.10))**2+((a["z_pre12"]-g["z_pre12"])/.25)**2
            key = (score, distance_time, candles[j].close_time, candles[j].source_identity_key)
            if best is None or key < best[0]:
                best = (key, j, g, support_start, support_end)
        record["candidate_rejections"] = dict(sorted(rejects.items()))
        record["eligible_candidate_count"] = eligible_count
        if best is None:
            record["unmatched_reason"] = "no_eligible_unused_control"
            continue
        key, j, g, support_start, support_end = best
        used.add(j)
        record["match_status"] = "matched"
        control = {"control_id": stable_hash({"clock": plain(candles[j].close_time), "source": candles[j].source_identity_key, "n": n}),
            "candle_id": candles[j].candle_id, "decision_at": plain(candles[j].known_at),
            "trailing_candles": n, "trailing_start": plain(candles[j-n+1].open_time),
            "boundary_geometry": {"E": closes[j], "L": trailing_lows(n)[j],
                "upper": closes[j]+(closes[j]-trailing_lows(n)[j]), "known_at": plain(candles[j].known_at)},
            "support_start": plain(support_start), "support_end": plain(support_end), "features": g}
        pairs.append({"event_id": event.event_id, "decision_at": record["decision_at"], "distance_squared": key[0], "control": control})
    result = _summarize_selection(cases, pairs, candles=candles,
        completed_indices=completed_indices, base=base, evaluation_start=evaluation_start,
        evaluation_end=evaluation_end, comparisons=comparisons, rules=rules)
    if population_ranges:
        result["populations"] = {}
        for name, start, end in population_ranges:
            subset = [r for r in cases if start <= by_close_time(r["decision_at"]) < end]
            members = {r["event_id"] for r in subset}
            selected = [p for p in pairs if p["event_id"] in members]
            examined = sum(sum(r.get("candidate_rejections", {}).values()) +
                           r.get("eligible_candidate_count", 0) for r in subset)
            summary = _summarize_selection(subset, selected, candles=candles,
                completed_indices=completed_indices, base=base, evaluation_start=start,
                evaluation_end=end, comparisons=examined, rules=rules, attach_weights=False)
            result["populations"][name] = {
                "evaluation_range": {"start": plain(start), "end_exclusive": plain(end)},
                "selection_summary": summary["selection_summary"], "dependence": summary["dependence"],
                "weighting": "Summary weights normalize within this population;root row weights normalize across combined population."}
    return result


def by_close_time(value):
    from .recognition import utc
    return utc(value)


def _summarize_selection(cases, pairs, *, candles, completed_indices, base,
                         evaluation_start, evaluation_end, comparisons, rules,
                         attach_weights=True):
    matched = [r for r in cases if r["match_status"] == "matched"]
    eligible = [r for r in cases if r["features"]]
    all_weights, eligible_weights, matched_weights = moment_weights(cases), moment_weights(eligible), moment_weights(matched)
    groups, matched_groups = Counter(r["decision_at"] for r in cases), Counter(r["decision_at"] for r in matched)
    if attach_weights:
        for record in cases:
            record["full_population_moment_weight"] = all_weights[record["event_id"]]
            record["matched_population_moment_weight"] = matched_weights.get(record["event_id"], 0.)
        for pair in pairs:
            pair["completion_moment_weight"] = matched_weights[pair["event_id"]]
    def counts(records):
        return {"chains": len(records), "completion_clocks": len({r["decision_at"] for r in records})}
    strata = defaultdict(lambda: {"all": 0, "matched": 0, "full_population_moment_mass": 0.,
        "retained_full_population_moment_mass": 0., "matched_population_moment_mass": 0.,
        "all_clocks": set(), "matched_clocks": set()})
    for r in cases:
        hours = r["formation_candles"]/4
        duration = "[0,3)" if hours < 3 else "[3,12)" if hours < 12 else "[12,48)" if hours < 48 else "[48,infinity)"
        for k in ("day:"+r["decision_at"][:10], "duration_hours:"+duration,
                  "block:"+str(r["block"])):
            strata[k]["all"] += 1
            strata[k]["matched"] += int(r["match_status"] == "matched")
            strata[k]["full_population_moment_mass"] += all_weights[r["event_id"]]
            strata[k]["all_clocks"].add(r["decision_at"])
            if r["match_status"] == "matched":
                strata[k]["retained_full_population_moment_mass"] += all_weights[r["event_id"]]
                strata[k]["matched_population_moment_mass"] += matched_weights[r["event_id"]]
                strata[k]["matched_clocks"].add(r["decision_at"])
    for group in strata.values():
        group["all_completion_clocks"] = len(group.pop("all_clocks"))
        group["any_matched_completion_clocks"] = len(group.pop("matched_clocks"))
    features = ("sigma", "r", "z_pre12", "q", "z_pre96", "body", "range", "duration_hours")
    controls = [{"event_id": p["event_id"], "features": p["control"]["features"]} for p in pairs]
    result = {"rules": rules, "outcomes_calculated": False, "cases": cases, "pairs": pairs,
        "selection_summary": {"all": counts(cases), "feature_eligible": counts(eligible),
            "matched": counts(matched), "fully_matched_completion_clocks": sum(matched_groups[t] == k for t, k in groups.items()),
            "partially_matched_completion_clocks": sum(0 < matched_groups[t] < k for t, k in groups.items()),
            "unmatched_completion_clocks": sum(not matched_groups[t] for t in groups),
            "retained_full_population_moment_mass": math.fsum(all_weights[r["event_id"]] for r in matched),
            "chains_per_completion_clock_histogram": dict(sorted(Counter(groups.values()).items())),
            "maximum_simultaneous_chains": max(groups.values(), default=0),
            "case_status_counts": dict(Counter(r["match_status"] for r in cases)),
            "candidate_rejections": dict(sum((Counter(r.get("candidate_rejections", {})) for r in cases), Counter())),
            "candidate_comparisons": comparisons, "strata_chain_counts": dict(sorted(strata.items())),
            "control_universe": {"ordinary_clocks": sum(evaluation_start <= b.close_time < evaluation_end and i not in completed_indices for i, b in enumerate(candles)),
                "ordinary_clocks_with_valid_prior_features": sum(evaluation_start <= candles[i].close_time < evaluation_end and i not in completed_indices for i in base),
                "per_case_n_risk_and_support": "Eligibility/rejections recorded separately per case;control r depends on case n"},
            "context_distributions": {name: {"all_moments": _distribution(cases, all_weights, name),
                "feature_eligible_moments": _distribution(eligible, eligible_weights, name),
                "matched_moments": _distribution(matched, matched_weights, name),
                "matched_controls": _distribution(controls, matched_weights, name)} for name in features},
            "coverage_pass_gate": None, "power_claim": False, "independence_claim": False},
        "dependence": {"all_cases_and_selected_controls": _components(cases, pairs, recognition_origin=plain(candles[0].open_time)),
                       "matched_cases_and_controls": _components(matched, pairs, recognition_origin=plain(candles[0].open_time))}}
    result["selection_manifest_hash"] = stable_hash({"rules": RULES, "cases": cases, "pairs": pairs, "dependence": result["dependence"]})
    return result


def select_comparisons(candles, events, *, evaluation_start, evaluation_end,
                       checkpoint: Callable[[], None] = lambda: None,
                       max_comparisons=20000000):
    """Historical v1 entry point; defaults and serialized scientific output unchanged."""
    return _select_comparisons(candles, events, evaluation_start=evaluation_start,
        evaluation_end=evaluation_end, rules=RULES, checkpoint=checkpoint,
        max_comparisons=max_comparisons)
