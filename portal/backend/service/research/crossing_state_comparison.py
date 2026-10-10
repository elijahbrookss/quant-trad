"""Check-owned fixed matching over public snapshots; no source or Indicator reconstruction."""
from __future__ import annotations

from bisect import bisect_left, insort
from collections import Counter
import hashlib
import json
import math

import numpy as np

from core.execution_control import execution_checkpoint

CONTRACT = "crossing_state_matched_pairs.v1"
MAX_COMPARISONS = 20_000_000
HORIZON = 7200


def _distribution(values):
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    return {"count": int(values.size), "mean": float(values.mean()) if values.size else None,
            "minimum": float(values.min()) if values.size else None,
            "maximum": float(values.max()) if values.size else None}


def prepare_pairs(*, times, known, observable, metric, shock, segment, baseline,
                  sample_times, strata, months, stop, evaluation_year):
    """Freeze matches before the caller computes future outcomes or path quality."""
    previous = np.zeros(len(times), dtype=bool)
    previous[1:] = (observable[1:] & observable[:-1]
        & (segment[1:] == segment[:-1]) & (known[:-1] <= known[1:]))
    previous_z = np.r_[np.nan, metric[:-1]]
    transition = previous & (metric > 2) & (previous_z <= 2)
    if np.any(previous & (shock != transition)):
        raise ValueError("crossing_state_invalid: public event contradicts consecutive public state")
    high = observable & (metric > 2)
    episodes = np.full(len(times), -1, dtype=np.int64)
    episode = -1
    for i in np.flatnonzero(high):
        execution_checkpoint()
        if i == 0 or not high[i-1] or not previous[i]:
            episode += 1
        episodes[i] = episode
    reasons = np.full(len(times), "", dtype=object)
    for mask, reason in [
        (~previous, "previous_public_state_unavailable"),
        (~np.isfinite(baseline) | (baseline <= 0), "nonpositive_or_unavailable_prior_risk"),
        ((sample_times < 0) | (sample_times != known), "entry_not_at_decision"),
        (sample_times + HORIZON >= stop, "nominal_endpoint_outside_year"),
    ]:
        reasons[high & mask & (reasons == "")] = reason
    eligible = high & (reasons == "")
    crossing = np.flatnonzero(eligible & shock)
    crossing = crossing[np.lexsort((times[crossing], known[crossing]))]
    controls = np.flatnonzero(eligible & ~shock & (previous_z > 2))
    controls = controls[np.lexsort((times[controls], known[controls]))]
    rms = np.sqrt(np.maximum(baseline, 0)) * 10000
    indexed = {int(k): controls[strata[controls] == k] for k in np.unique(strata[controls])}
    used = set()
    pairs = []
    unmatched = []
    comparisons = 0
    for i in crossing:
        execution_checkpoint()
        pool = indexed.get(int(strata[i]), np.array([], dtype=int))
        clocks = known[pool]
        left = np.searchsorted(clocks, known[i] - 7*86400, side="left")
        right = np.searchsorted(clocks, known[i] + 7*86400, side="right")
        best = None
        for offset in range(left, right, 2048):
            execution_checkpoint()
            batch = pool[offset:min(offset+2048, right)]
            comparisons += len(batch)
            if comparisons > MAX_COMPARISONS:
                raise ValueError(f"crossing_state_resource_limit: candidate comparisons exceed {MAX_COMPARISONS}")
            for j in batch:
                if int(j) in used or episodes[i] == episodes[j]:
                    continue
                if abs(sample_times[i]-sample_times[j]) < HORIZON:
                    continue
                dz = abs(metric[i]-metric[j])
                ratio = max(rms[i], rms[j]) / min(rms[i], rms[j])
                if dz > .25 or ratio > 1.25:
                    continue
                # IEEE binary64, exact inclusive comparisons; no rounding or fuzzy ties.
                score = max(dz/.25, abs(math.log(rms[i]/rms[j]))/math.log(1.25))
                rank = (score, abs(known[i]-known[j]), known[j], times[j], int(j))
                if best is None or rank < best:
                    best = rank
        if best is None:
            unmatched.append(int(i))
        else:
            j = best[-1]
            used.add(j)
            pairs.append((int(i), j))
    pairs = np.asarray(pairs, dtype=np.int64).reshape(-1, 2)
    digest = hashlib.sha256()
    for i, j in pairs:
        execution_checkpoint()
        row = [int(times[i]), int(times[j]), int(known[i]), int(known[j])]
        digest.update(json.dumps(row, separators=(",", ":")).encode()+b"\n")
    unmatched_identities = []
    for i in unmatched:
        execution_checkpoint()
        unmatched_identities.append({
            "source_open_epoch": int(times[i]),
            "decision_known_at_epoch": int(known[i]),
            "reason": "no_unused_control_satisfies_all_fixed_constraints",
        })
    strata_rows = []
    matched_crossings = pairs[:, 0]
    for s in sorted(set(strata[crossing])):
        execution_checkpoint()
        ids = crossing[strata[crossing] == s]
        matched = matched_crossings[strata[matched_crossings] == s]
        strata_rows.append({"stratum": int(s), "eligible_crossings": len(ids),
            "matched_crossings": len(matched), "unmatched_crossings": len(ids)-len(matched),
            "current_z": _distribution(metric[ids])})
    z_support = []
    # Quarter-z bins document the same fixed caliper scale, never fit outcomes.
    z_bins = np.floor((metric[crossing]-2)/.25).astype(np.int64)
    for bin_id in np.unique(z_bins):
        execution_checkpoint()
        ids = crossing[z_bins == bin_id]
        z_support.append({"z_lower": float(2+bin_id*.25), "z_upper_exclusive": float(2+(bin_id+1)*.25),
                          "eligible_crossings": len(ids), "matched_crossings": int(np.isin(ids, matched_crossings).sum())})
    monthly = []
    year = str(evaluation_year)
    for month in (f"{year}-{i:02d}" for i in range(1, 13)):
        denominator = int(np.sum(months[crossing] == month))
        matched = int(np.sum(months[matched_crossings] == month))
        monthly.append({"month": month, "eligible_crossings": denominator,
                        "matched_pairs": matched, "fraction": matched/denominator if denominator else None})
    return {"pairs": pairs, "episodes": episodes, "rms": rms,
        "accounting": {"eligible_crossings": int(crossing.size), "eligible_controls": int(controls.size),
            "matched_pairs": len(pairs), "unmatched_crossings": len(unmatched),
            "unmatched_crossing_identities": unmatched_identities,
            "unmatched_reason": "no_unused_control_satisfies_all_fixed_constraints",
            "match_fraction": len(pairs)/len(crossing) if len(crossing) else None,
            "monthly": monthly, "strata": strata_rows, "current_z_support": z_support,
            "unmatched_current_z": _distribution(metric[unmatched]),
            "unmatched_prior_rms_bps": _distribution(rms[unmatched]),
            "high_state_exclusions": dict(Counter(reasons[high & ~eligible])),
            "high_state_crossing_exclusions": dict(Counter(reasons[high & shock & ~eligible])),
            "high_state_non_crossing_exclusions": dict(Counter(reasons[high & ~shock & ~eligible])),
            "candidate_comparisons": comparisons, "candidate_comparison_limit": MAX_COMPARISONS,
            "feature_pair_map_sha256_json_lines": digest.hexdigest(),
            "pair_map_columns": ["crossing_source_open_epoch", "control_source_open_epoch",
                                 "crossing_decision_known_at_epoch", "control_decision_known_at_epoch"],
            "matching_uses_future_outcomes": False}}


def _balance(left, right):
    valid = np.isfinite(left) & np.isfinite(right)
    left, right = left[valid], right[valid]
    if not len(left):
        return {"pair_count": 0, "absolute_smd": None, "crossing_mean": None, "control_mean": None}
    difference = float(left.mean()-right.mean())
    pooled = math.sqrt(float((left.var()+right.var())/2))
    smd = abs(difference)/pooled if pooled else (0. if difference == 0 else None)
    return {"pair_count": len(left), "absolute_smd": smd,
            "crossing_mean": float(left.mean()), "control_mean": float(right.mean()),
            "pooled_population_sd": pooled, "undefined_zero_sd": pooled == 0 and difference != 0}


def _summary(pairs, values, baseline):
    if not len(pairs):
        return {"pair_count": 0, "raw_risk_contrast": None, "crossing_mean": None,
                "persistent_high_mean": None, "ratio_contrast": None, "relative_raw_contrast": None}
    a, b = pairs.T
    control_mean = float(values[b].mean())
    difference = float(np.mean(values[a]-values[b]))
    return {"pair_count": len(pairs), "raw_risk_contrast": difference,
            "crossing_mean": float(values[a].mean()), "persistent_high_mean": control_mean,
            "ratio_contrast": float(np.mean(values[a]/baseline[a]-values[b]/baseline[b])),
            "relative_raw_contrast": difference/control_mean if control_mean else None}


def joint_nonoverlap(pairs, starts):
    """Keep whole fixed pairs, checking against both arms of every accepted pair."""
    occupied = []
    keep = []
    for i, j in pairs:
        execution_checkpoint()
        candidates = [int(starts[i]), int(starts[j])]
        accepted = True
        for start in candidates:
            pos = bisect_left(occupied, start)
            if ((pos and occupied[pos-1]+HORIZON > start)
                    or (pos < len(occupied) and occupied[pos] < start+HORIZON)):
                accepted = False
                break
        if accepted:
            keep.append((int(i), int(j)))
            for start in candidates:
                insort(occupied, start)
    return np.asarray(keep, dtype=np.int64).reshape(-1, 2)


def finish_pairs(*, prepared, times, known, metric, atr_ratio, baseline, sample_times,
                 months, days, weeks, values, outcome_known, outcome_reasons, overlap):
    pairs = prepared["pairs"]
    complete = np.isfinite(values[pairs]).all(axis=1)
    retained = pairs[complete]
    a, b = retained.T
    thin = joint_nonoverlap(retained, sample_times)
    primary = _summary(retained, values, baseline)
    sensitivity = _summary(thin, values, baseline)
    sensitivity.update(retained_months=len(set(months[thin[:, 0]])), rejected_pairs=len(retained)-len(thin),
                       interpretation="joint_interval_sensitivity_not_independent_observations")
    balances = {"current_atr_zscore": _balance(metric[a], metric[b]),
                "log_prior_rms": _balance(np.log(prepared["rms"][a]), np.log(prepared["rms"][b])),
                "atr_ratio_unmatched_diagnostic": _balance(atr_ratio[a], atr_ratio[b])}
    monthly = []
    for row in prepared["accounting"]["monthly"]:
        mask = months[a] == row["month"]
        frozen_month = months[pairs[:, 0]] == row["month"]
        monthly.append({**row, "complete_pairs": int(mask.sum()),
            "dropped_whole_pairs": int(np.sum(frozen_month & ~complete)),
            "crossing_outcome_reasons": dict(Counter(outcome_reasons[pairs[frozen_month & ~np.isfinite(values[pairs[:, 0]]), 0]])),
            "control_outcome_reasons": dict(Counter(outcome_reasons[pairs[frozen_month & ~np.isfinite(values[pairs[:, 1]]), 1]])),
            **_summary(retained[mask], values, baseline)})
    deletions = {}
    contribution = {}
    for name, labels in [("day", days), ("week", weeks)]:
        rows = []
        for key in sorted(set(labels[retained.flatten()])):
            execution_checkpoint()
            keep = (labels[a] != key) & (labels[b] != key)
            rows.append({"removed": str(key), "removed_pairs": int((~keep).sum()),
                         **_summary(retained[keep], values, baseline)})
        deletions[f"leave_one_{name}_out"] = rows
        contribution[name] = {"distinct": len(rows), "largest_pair_membership": max((x["removed_pairs"] for x in rows), default=0),
            "membership_rule": "whole_pair_if_either_arm_in_period"}
    gates = {
        "overall_matching": (prepared["accounting"]["match_fraction"] or 0) >= .70,
        "every_month_matching": all((x["fraction"] or 0) >= .50 for x in monthly),
        "outcome_completeness": bool(len(pairs)) and len(retained)/len(pairs) >= .95,
        "complete_pairs": len(retained) >= 200,
        "every_month_pairs": all(x["complete_pairs"] >= 10 for x in monthly),
        "current_z_balance": balances["current_atr_zscore"]["absolute_smd"] is not None and balances["current_atr_zscore"]["absolute_smd"] <= .10,
        "log_prior_rms_balance": balances["log_prior_rms"]["absolute_smd"] is not None and balances["log_prior_rms"]["absolute_smd"] <= .10,
    }
    sensitivity_supported = len(thin) >= 50 and sensitivity["retained_months"] >= 6
    descriptive = {"positive_primary": (primary["raw_risk_contrast"] or 0) > 0,
        "at_least_nine_positive_months": sum((r["raw_risk_contrast"] or 0) > 0 for r in monthly) >= 9,
        "all_week_deletions_positive": bool(deletions["leave_one_week_out"]) and all((r["raw_risk_contrast"] or 0) > 0 for r in deletions["leave_one_week_out"]),
        "joint_nonoverlap_supported": sensitivity_supported,
        "positive_joint_nonoverlap": (sensitivity["raw_risk_contrast"] or 0) > 0}
    records = []
    for k, (i, j) in enumerate(pairs):
        execution_checkpoint()
        arms = []
        for index in (i, j):
            arms.append({"source_open_epoch": int(times[index]), "decision_known_at_epoch": int(known[index]),
                "entry_close_epoch": int(sample_times[index]), "target_close_epoch": int(sample_times[index]+HORIZON),
                "episode": int(prepared["episodes"][index]), "atr_zscore": float(metric[index]),
                "prior_rms_bps": float(prepared["rms"][index]),
                "raw_risk": float(values[index]) if np.isfinite(values[index]) else None,
                "outcome_known_at_epoch": int(outcome_known[index]) if np.isfinite(outcome_known[index]) else None,
                "outcome_exclusion": str(outcome_reasons[index])})
        records.append({"crossing": arms[0], "persistent_high": arms[1], "both_outcomes_complete": bool(complete[k])})
    flat = retained.flatten()
    spans = sorted(int(x) for x in sample_times[flat])
    union_seconds = 0
    right = -1
    for left in spans:
        execution_checkpoint()
        union_seconds += max(0, left+HORIZON-max(left, right))
        right = max(right, left+HORIZON)
    return {"schema_version": CONTRACT, "analysis_status": "descriptive_only" if all(gates.values()) else "unsupported",
        "matching": prepared["accounting"], "primary": primary, "monthly": monthly,
        "outcome_missingness": {"dropped_whole_pairs": int((~complete).sum()),
            "complete_fraction": len(retained)/len(pairs) if len(pairs) else None,
            "crossing_reasons": dict(Counter(outcome_reasons[pairs[~np.isfinite(values[pairs[:, 0]]), 0]])),
            "control_reasons": dict(Counter(outcome_reasons[pairs[~np.isfinite(values[pairs[:, 1]]), 1]])),
            "rematched": False},
        "balance": balances, "support_gates": gates, "descriptive_investment_gates": descriptive,
        "per_asset_investment_criteria_met": all(gates.values()) and all(descriptive.values()),
        "cross_asset_decision": "requires_both_prespecified_asset_results_no_single_asset_promotion",
        "dependence": {**overlap(sample_times[flat], sample_times[flat]+HORIZON),
            "distinct_high_state_episodes": len(set(prepared["episodes"][flat])),
            "outcome_interval_union_seconds": union_seconds,
            "reused_outcome_interval_seconds": len(flat)*HORIZON-union_seconds,
            "calendar_contribution": contribution},
        "joint_nonoverlap": sensitivity, **deletions, "pairs": records,
        "interpretation": "Retrospective matched development association, not causal, independent, significant, live-available, profitable or promotion evidence. Matching precedes future outcomes; missingness never rematches. Reserved years are excluded."}
